"""FastAPI application for Steadfast Lite."""

import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.db import get_db, init_db
from app.worker import worker_pool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("steadfast.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager: initializes database and starts background worker threads."""
    logger.info("Initializing database...")
    init_db()
    # Check if workers should start (disabled in pure unit tests if desired, but enabled by default)
    if os.environ.get("DISABLE_WORKERS", "").lower() not in ("1", "true", "yes"):
        logger.info("Starting background worker threads...")
        worker_pool.start()
    yield
    logger.info("Stopping background worker threads...")
    worker_pool.stop()


app = FastAPI(
    title="Steadfast Lite",
    description="Resilient job processing and timeline visibility prototype",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static folder
static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")


class CreateJobRequest(BaseModel):
    type: str = Field(..., description="Job type, e.g. send_email, generate_report")
    payload: Any = Field(..., description="Job payload dict or string")
    max_attempts: Optional[int] = Field(3, description="Maximum retry attempts")


class EditPayloadRequest(BaseModel):
    payload: Any = Field(..., description="New payload for the DEAD job")


@app.get("/")
def serve_dashboard():
    """Serves the single-page dashboard."""
    index_path = os.path.join(static_dir, "index.html")
    if not os.path.exists(index_path):
        return {"message": "Steadfast Lite API is running. index.html not yet present."}
    return FileResponse(index_path, media_type="text/html")


@app.post("/jobs", status_code=status.HTTP_202_ACCEPTED)
def submit_job(req: CreateJobRequest, response: Response):
    """Enqueues a new job and atomically writes SUBMITTED and QUEUED events."""
    job_id = f"job-{uuid.uuid4().hex[:8]}"
    now = time.time()
    payload_str = json.dumps(req.payload) if not isinstance(req.payload, str) else req.payload
    max_attempts = req.max_attempts if req.max_attempts is not None and req.max_attempts > 0 else 3

    with get_db(immediate=True) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO jobs (id, type, payload, status, attempts, max_attempts, next_run_at, created_at, updated_at)
            VALUES (?, ?, ?, 'QUEUED', 0, ?, ?, ?, ?)
            """,
            (job_id, req.type, payload_str, max_attempts, now, now, now)
        )
        cursor.execute(
            """
            INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
            VALUES (?, ?, 'SUBMITTED', 'client', 0, 'Job submitted via API')
            """,
            (job_id, now)
        )
        cursor.execute(
            """
            INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
            VALUES (?, ?, 'QUEUED', 'system', 0, 'Job queued for worker pickup')
            """,
            (job_id, now)
        )

    return {
        "id": job_id,
        "type": req.type,
        "status": "QUEUED",
        "attempts": 0,
        "max_attempts": max_attempts,
        "created_at": now,
    }


@app.get("/jobs")
def list_jobs(limit: int = 100):
    """List jobs, newest first, default limit 100."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, type, payload, status, attempts, max_attempts, next_run_at, last_error, created_at, updated_at
            FROM jobs
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,)
        )
        jobs = [dict(row) for row in cursor.fetchall()]
        return jobs


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    """Retrieve details of a single job."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, type, payload, status, attempts, max_attempts, next_run_at, last_error, created_at, updated_at
            FROM jobs
            WHERE id = ?
            """,
            (job_id,)
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        return dict(row)


@app.get("/jobs/{job_id}/timeline")
def get_job_timeline(job_id: str):
    """Retrieve ordered events for a job."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM jobs WHERE id = ?", (job_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

        cursor.execute(
            """
            SELECT id, job_id, ts, event, worker_id, attempt, detail
            FROM job_events
            WHERE job_id = ?
            ORDER BY id ASC
            """,
            (job_id,)
        )
        return [dict(row) for row in cursor.fetchall()]


@app.get("/dlq")
def get_dlq():
    """Retrieve DEAD jobs (Dead-Letter Queue) with last_error."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, type, payload, status, attempts, max_attempts, next_run_at, last_error, created_at, updated_at
            FROM jobs
            WHERE status = 'DEAD'
            ORDER BY updated_at DESC
            """
        )
        return [dict(row) for row in cursor.fetchall()]


@app.patch("/jobs/{job_id}/payload")
def edit_job_payload(job_id: str, body: Dict[str, Any] = Body(...)):
    """Edit payload of a DEAD job. Records a PAYLOAD_EDITED event."""
    new_payload = body.get("payload", body)
    payload_str = json.dumps(new_payload) if not isinstance(new_payload, str) else new_payload
    now = time.time()

    with get_db(immediate=True) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, status, attempts FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        if row["status"] != "DEAD":
            raise HTTPException(status_code=400, detail="Only DEAD jobs can have their payload edited")

        current_attempts = row["attempts"]
        cursor.execute(
            """
            UPDATE jobs
            SET payload = ?, updated_at = ?
            WHERE id = ?
            """,
            (payload_str, now, job_id)
        )
        cursor.execute(
            """
            INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
            VALUES (?, ?, 'PAYLOAD_EDITED', 'operator', ?, 'Payload updated by operator')
            """,
            (job_id, now, current_attempts)
        )

    return {"status": "ok", "job_id": job_id, "payload": payload_str}


@app.post("/jobs/{job_id}/replay")
def replay_job(job_id: str):
    """Replay a DEAD job: resets attempts=0, status=QUEUED, records REPLAYED and QUEUED events."""
    now = time.time()
    with get_db(immediate=True) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, status FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        if row["status"] != "DEAD":
            raise HTTPException(status_code=400, detail="Only DEAD jobs can be replayed")

        cursor.execute(
            """
            UPDATE jobs
            SET status = 'QUEUED', attempts = 0, next_run_at = ?, last_error = NULL, updated_at = ?
            WHERE id = ?
            """,
            (now, now, job_id)
        )
        cursor.execute(
            """
            INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
            VALUES (?, ?, 'REPLAYED', 'operator', 0, 'Job replayed by operator')
            """,
            (job_id, now)
        )
        cursor.execute(
            """
            INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
            VALUES (?, ?, 'QUEUED', 'system', 0, 'Job re-queued for execution')
            """,
            (job_id, now)
        )

    return {"status": "replayed", "job_id": job_id}


@app.get("/stats")
def get_stats():
    """Returns counts per status, DLQ size, total retries, stuck jobs (>60s running), and recent 20 events."""
    now = time.time()
    with get_db() as conn:
        cursor = conn.cursor()

        # Status counts
        cursor.execute("SELECT status, COUNT(*) as cnt FROM jobs GROUP BY status")
        counts = {"QUEUED": 0, "RUNNING": 0, "RETRY_WAIT": 0, "SUCCEEDED": 0, "DEAD": 0}
        for row in cursor.fetchall():
            if row["status"] in counts:
                counts[row["status"]] = row["cnt"]

        # Total retries
        cursor.execute("SELECT COUNT(*) as cnt FROM job_events WHERE event = 'RETRY_SCHEDULED'")
        total_retries = cursor.fetchone()["cnt"]

        # Stuck jobs: RUNNING for more than 60 seconds
        cursor.execute(
            "SELECT COUNT(*) as cnt FROM jobs WHERE status = 'RUNNING' AND (? - updated_at) > 60",
            (now,)
        )
        stuck_jobs = cursor.fetchone()["cnt"]

        # 20 most recent events
        cursor.execute(
            """
            SELECT id, job_id, ts, event, worker_id, attempt, detail
            FROM job_events
            ORDER BY id DESC
            LIMIT 20
            """
        )
        recent_events = [dict(row) for row in cursor.fetchall()]

    return {
        "counts": counts,
        "dlq_size": counts["DEAD"],
        "total_retries": total_retries,
        "stuck_jobs": stuck_jobs,
        "recent_events": recent_events,
    }


@app.post("/demo/seed", status_code=status.HTTP_202_ACCEPTED)
def demo_seed():
    """Submit 30 mixed jobs: 20 successful jobs, 6 flaky retryable jobs, and 4 poison jobs."""
    job_templates = []

    # 10 good emails
    for i in range(1, 11):
        job_templates.append({
            "type": "send_email",
            "payload": {"to": f"customer{i}@example.com", "subject": f"Welcome #{i}"}
        })

    # 10 good reports
    reports = ["q1_sales.csv", "q2_revenue.csv", "inventory_2026.csv", "audit_log.csv",
               "performance_metrics.csv", "churn_analysis.csv", "regional_tax.csv",
               "user_growth.csv", "pipeline_forecast.csv", "payroll_october.csv"]
    for r in reports:
        job_templates.append({
            "type": "generate_report",
            "payload": {"file": r, "format": "csv"}
        })

    # 6 flaky jobs (timeout on first 2 attempts, then succeed)
    flaky_users = ["sync", "webhook", "alerts", "billing", "notifications", "support"]
    for u in flaky_users:
        job_templates.append({
            "type": "send_email",
            "payload": {"to": f"{u}@flaky.test", "subject": "Automated alert"}
        })

    # 4 poison jobs (2 bad email format, 2 missing file reports)
    job_templates.append({
        "type": "send_email",
        "payload": {"to": "invalid-address-without-at", "subject": "Poison job 1"}
    })
    job_templates.append({
        "type": "send_email",
        "payload": {"to": "corrupt_recipient_missing_domain", "subject": "Poison job 2"}
    })
    job_templates.append({
        "type": "generate_report",
        "payload": {"file": "missing.csv", "format": "csv"}
    })
    job_templates.append({
        "type": "generate_report",
        "payload": {"file": "archive_missing.csv", "format": "csv"}
    })

    created_ids = []
    now = time.time()
    with get_db(immediate=True) as conn:
        cursor = conn.cursor()
        for idx, item in enumerate(job_templates):
            job_id = f"job-{uuid.uuid4().hex[:8]}"
            payload_str = json.dumps(item["payload"])
            ts = now + (idx * 0.001)  # tiny offset for stable chronological ordering
            cursor.execute(
                """
                INSERT INTO jobs (id, type, payload, status, attempts, max_attempts, next_run_at, created_at, updated_at)
                VALUES (?, ?, ?, 'QUEUED', 0, 3, ?, ?, ?)
                """,
                (job_id, item["type"], payload_str, ts, ts, ts)
            )
            cursor.execute(
                """
                INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                VALUES (?, ?, 'SUBMITTED', 'seed_script', 0, 'Demo batch submission')
                """,
                (job_id, ts)
            )
            cursor.execute(
                """
                INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                VALUES (?, ?, 'QUEUED', 'system', 0, 'Enqueued for workers')
                """,
                (job_id, ts)
            )
            created_ids.append(job_id)

    return {"seeded": len(created_ids), "job_ids": created_ids}


@app.post("/demo/poison", status_code=status.HTTP_202_ACCEPTED)
def demo_poison():
    """Submit one poison send_email job (bad address with no '@')."""
    job_id = f"job-{uuid.uuid4().hex[:8]}"
    now = time.time()
    payload = {"to": "poison_address_no_at_sign", "subject": "Defective Payload"}
    payload_str = json.dumps(payload)

    with get_db(immediate=True) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO jobs (id, type, payload, status, attempts, max_attempts, next_run_at, created_at, updated_at)
            VALUES (?, 'send_email', ?, 'QUEUED', 0, 3, ?, ?, ?)
            """,
            (job_id, payload_str, now, now, now)
        )
        cursor.execute(
            """
            INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
            VALUES (?, ?, 'SUBMITTED', 'demo_poison', 0, 'Poison job injected via API')
            """,
            (job_id, now)
        )
        cursor.execute(
            """
            INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
            VALUES (?, ?, 'QUEUED', 'system', 0, 'Enqueued for workers')
            """,
            (job_id, now)
        )

    return {"status": "poison_submitted", "job_id": job_id}
