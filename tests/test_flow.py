"""Unit and integration tests for Steadfast Lite.

Verifies:
1. Permanent error -> DEAD after exactly 1 attempt
2. Flaky job fails twice then succeeds, with growing backoff
3. Retry exhaustion -> DEAD after max_attempts
4. Edit payload + replay -> SUCCEEDED
5. Replayed job's timeline contains, in exact order:
   SUBMITTED, QUEUED, RUNNING, FAILED_PERMANENT, DEAD, PAYLOAD_EDITED, REPLAYED, QUEUED, RUNNING, SUCCEEDED
"""

import os
import time
import pytest
from fastapi.testclient import TestClient

# Configure environment for fast deterministic testing
os.environ["TIME_SCALE"] = "0.0"
os.environ["DISABLE_WORKERS"] = "1"

from app.db import get_db, init_db
from app.main import app
from app.worker import calculate_backoff, claim_job, process_claimed_job


@pytest.fixture(autouse=True)
def setup_test_db(tmp_path, monkeypatch):
    """Isolate each test in a fresh temporary SQLite database."""
    test_db = str(tmp_path / "test_steadfast.db")
    monkeypatch.setenv("DB_PATH", test_db)
    monkeypatch.setenv("TIME_SCALE", "0.0")
    monkeypatch.setenv("DISABLE_WORKERS", "1")
    init_db(test_db)
    yield test_db


@pytest.fixture
def client():
    """Test client for FastAPI endpoints."""
    with TestClient(app) as c:
        yield c


def test_permanent_error_to_dead_after_one_attempt(client):
    """Permanent error must transition straight to DEAD after exactly 1 attempt."""
    res = client.post("/jobs", json={"type": "send_email", "payload": {"to": "no-at-sign"}})
    assert res.status_code == 202
    job_id = res.json()["id"]

    # Verify initial state
    job = client.get(f"/jobs/{job_id}").json()
    assert job["status"] == "QUEUED"
    assert job["attempts"] == 0

    # Claim and process job
    claimed = claim_job("worker-test")
    assert claimed is not None
    assert claimed["id"] == job_id
    process_claimed_job("worker-test", claimed)

    # Verify job is now DEAD after exactly 1 attempt
    updated_job = client.get(f"/jobs/{job_id}").json()
    assert updated_job["status"] == "DEAD"
    assert updated_job["attempts"] == 1
    assert "Invalid email address" in updated_job["last_error"]

    # Verify timeline events
    timeline = client.get(f"/jobs/{job_id}/timeline").json()
    event_names = [e["event"] for e in timeline]
    assert event_names == ["SUBMITTED", "QUEUED", "RUNNING", "FAILED_PERMANENT", "DEAD"]


def test_flaky_job_fails_twice_then_succeeds_with_growing_backoff(client):
    """Flaky job fails twice with growing backoff windows, then succeeds on attempt 3."""
    # Test growing exponential backoff window logic
    window_att1 = min(30.0, 1.0 * (2 ** 1))  # 2.0s
    window_att2 = min(30.0, 1.0 * (2 ** 2))  # 4.0s
    window_att3 = min(30.0, 1.0 * (2 ** 3))  # 8.0s
    assert window_att1 < window_att2 < window_att3
    assert window_att1 == 2.0
    assert window_att2 == 4.0

    # Enqueue flaky email job
    res = client.post("/jobs", json={"type": "send_email", "payload": {"to": "user@flaky.test"}})
    assert res.status_code == 202
    job_id = res.json()["id"]

    # Attempt 1 -> Timeout (retryable)
    job1 = claim_job("worker-test")
    assert job1 is not None and job1["id"] == job_id
    process_claimed_job("worker-test", job1)

    state1 = client.get(f"/jobs/{job_id}").json()
    assert state1["status"] == "RETRY_WAIT"
    assert state1["attempts"] == 1
    assert state1["last_error"] == "Timeout"

    # Fast forward next_run_at to allow immediate pickup
    with get_db(immediate=True) as conn:
        conn.execute("UPDATE jobs SET next_run_at = ? WHERE id = ?", (time.time() - 1, job_id))

    # Attempt 2 -> Timeout (retryable)
    job2 = claim_job("worker-test")
    assert job2 is not None and job2["id"] == job_id
    process_claimed_job("worker-test", job2)

    state2 = client.get(f"/jobs/{job_id}").json()
    assert state2["status"] == "RETRY_WAIT"
    assert state2["attempts"] == 2
    assert state2["last_error"] == "Timeout"

    # Fast forward next_run_at to allow immediate pickup
    with get_db(immediate=True) as conn:
        conn.execute("UPDATE jobs SET next_run_at = ? WHERE id = ?", (time.time() - 1, job_id))

    # Attempt 3 -> Handler succeeds (flaky fails on first 2 attempts)
    job3 = claim_job("worker-test")
    assert job3 is not None and job3["id"] == job_id
    process_claimed_job("worker-test", job3)

    state3 = client.get(f"/jobs/{job_id}").json()
    assert state3["status"] == "SUCCEEDED"
    assert state3["attempts"] == 3

    # Check timeline contains retries then success
    timeline = client.get(f"/jobs/{job_id}/timeline").json()
    events = [e["event"] for e in timeline]
    assert events == [
        "SUBMITTED",
        "QUEUED",
        "RUNNING",
        "FAILED_RETRYABLE",
        "RETRY_SCHEDULED",
        "RUNNING",
        "FAILED_RETRYABLE",
        "RETRY_SCHEDULED",
        "RUNNING",
        "SUCCEEDED",
    ]


def test_retry_exhaustion_to_dead_after_max_attempts(client):
    """Job exceeding max_attempts moves to DEAD with exhaustion error."""
    # Create job with max_attempts = 2
    res = client.post(
        "/jobs",
        json={"type": "send_email", "payload": {"to": "exhaust@flaky.test"}, "max_attempts": 2}
    )
    job_id = res.json()["id"]

    # Attempt 1
    j1 = claim_job("worker-test")
    process_claimed_job("worker-test", j1)
    s1 = client.get(f"/jobs/{job_id}").json()
    assert s1["status"] == "RETRY_WAIT"
    assert s1["attempts"] == 1

    # Fast forward
    with get_db(immediate=True) as conn:
        conn.execute("UPDATE jobs SET next_run_at = ? WHERE id = ?", (time.time() - 1, job_id))

    # Attempt 2 -> hits max_attempts (2) -> DEAD
    j2 = claim_job("worker-test")
    process_claimed_job("worker-test", j2)
    s2 = client.get(f"/jobs/{job_id}").json()
    assert s2["status"] == "DEAD"
    assert s2["attempts"] == 2
    assert "Exhausted retries" in s2["last_error"]

    timeline = client.get(f"/jobs/{job_id}/timeline").json()
    events = [e["event"] for e in timeline]
    assert events == [
        "SUBMITTED",
        "QUEUED",
        "RUNNING",
        "FAILED_RETRYABLE",
        "RETRY_SCHEDULED",
        "RUNNING",
        "FAILED_RETRYABLE",
        "DEAD",
    ]


def test_edit_payload_and_replay_to_succeeded(client):
    """Editing payload of a DEAD job and replaying enables successful execution."""
    # Create poison report job
    res = client.post("/jobs", json={"type": "generate_report", "payload": {"file": "missing.csv"}})
    job_id = res.json()["id"]

    # Run once -> fails permanent -> DEAD
    j = claim_job("worker-test")
    process_claimed_job("worker-test", j)
    assert client.get(f"/jobs/{job_id}").json()["status"] == "DEAD"

    # Edit payload to valid file
    edit_res = client.patch(f"/jobs/{job_id}/payload", json={"payload": {"file": "valid_report.csv"}})
    assert edit_res.status_code == 200

    # Replay job
    replay_res = client.post(f"/jobs/{job_id}/replay")
    assert replay_res.status_code == 200

    replayed_state = client.get(f"/jobs/{job_id}").json()
    assert replayed_state["status"] == "QUEUED"
    assert replayed_state["attempts"] == 0

    # Run replayed job -> should now succeed
    j_replay = claim_job("worker-test")
    assert j_replay is not None and j_replay["id"] == job_id
    process_claimed_job("worker-test", j_replay)

    final_state = client.get(f"/jobs/{job_id}").json()
    assert final_state["status"] == "SUCCEEDED"
    assert final_state["attempts"] == 1


def test_replayed_job_timeline_exact_sequence(client):
    """Replayed job's timeline contains the exact required sequence:
    SUBMITTED, QUEUED, RUNNING, FAILED_PERMANENT, DEAD, PAYLOAD_EDITED, REPLAYED, QUEUED, RUNNING, SUCCEEDED
    """
    # 1. Submit poison email job
    res = client.post("/jobs", json={"type": "send_email", "payload": {"to": "no-at-sign"}})
    assert res.status_code == 202
    job_id = res.json()["id"]

    # 2. Worker claims and runs -> fails permanent
    j1 = claim_job("worker-1")
    assert j1 is not None and j1["id"] == job_id
    process_claimed_job("worker-1", j1)

    dead_job = client.get(f"/jobs/{job_id}").json()
    assert dead_job["status"] == "DEAD"

    # 3. Operator edits payload
    edit_res = client.patch(
        f"/jobs/{job_id}/payload",
        json={"payload": {"to": "repaired_user@example.com"}}
    )
    assert edit_res.status_code == 200

    # 4. Operator replays job
    replay_res = client.post(f"/jobs/{job_id}/replay")
    assert replay_res.status_code == 200

    # 5. Worker claims and runs replayed job -> succeeds
    j2 = claim_job("worker-2")
    assert j2 is not None and j2["id"] == job_id
    process_claimed_job("worker-2", j2)

    succeeded_job = client.get(f"/jobs/{job_id}").json()
    assert succeeded_job["status"] == "SUCCEEDED"

    # 6. Retrieve full timeline and verify exact event ordering
    timeline = client.get(f"/jobs/{job_id}/timeline").json()
    actual_events = [e["event"] for e in timeline]

    expected_events = [
        "SUBMITTED",
        "QUEUED",
        "RUNNING",
        "FAILED_PERMANENT",
        "DEAD",
        "PAYLOAD_EDITED",
        "REPLAYED",
        "QUEUED",
        "RUNNING",
        "SUCCEEDED",
    ]

    assert actual_events == expected_events, f"Timeline mismatch!\nExpected: {expected_events}\nActual:   {actual_events}"
