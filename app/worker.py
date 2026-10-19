"""Background worker threads, job claiming, backoff calculation, and execution."""

import json
import logging
import random
import threading
import time
from typing import Any, Dict, List, Optional

from app.db import get_db
from app.handlers import HANDLERS, PermanentJobError, RetryableJobError, get_time_scale

logger = logging.getLogger("steadfast.worker")


def calculate_backoff(attempt: int, base: float = 1.0, cap: float = 30.0, jitter: bool = True) -> float:
    """Exponential backoff with full jitter: random between 0 and 1s*2^attempts, capped at 30s.
    
    A configurable time scale is multiplied to allow blazing fast tests.
    """
    scale = get_time_scale()
    max_window = min(cap, base * (2 ** attempt))
    if jitter:
        backoff = random.uniform(0.0, max_window)
    else:
        backoff = max_window
    return backoff * scale


def claim_job(worker_id: str) -> Optional[Dict[str, Any]]:
    """Atomically claim an eligible job using BEGIN IMMEDIATE.
    
    Two workers will never take the same job because BEGIN IMMEDIATE acquires
    a reserved write lock, and the status is updated to RUNNING within the same transaction.
    """
    now = time.time()
    with get_db(immediate=True) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, type, payload, status, attempts, max_attempts, created_at
            FROM jobs
            WHERE status IN ('QUEUED', 'RETRY_WAIT') AND next_run_at <= ?
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (now,)
        )
        row = cursor.fetchone()
        if not row:
            return None

        job = dict(row)
        next_attempt = job["attempts"] + 1

        # Atomically mark RUNNING and record RUNNING event
        cursor.execute(
            """
            UPDATE jobs
            SET status = 'RUNNING', updated_at = ?
            WHERE id = ?
            """,
            (now, job["id"])
        )
        cursor.execute(
            """
            INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
            VALUES (?, ?, 'RUNNING', ?, ?, ?)
            """,
            (job["id"], now, worker_id, next_attempt, f"Claimed by {worker_id}")
        )
        return job


def process_claimed_job(worker_id: str, job: Dict[str, Any]) -> None:
    """Execute the handler for a claimed job and record the outcome atomically."""
    job_id = job["id"]
    job_type = job["type"]
    current_attempt = job["attempts"] + 1
    max_attempts = job["max_attempts"]

    # Parse payload JSON
    try:
        raw_payload = job["payload"]
        payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
    except Exception as exc:
        err_msg = f"Malformed payload JSON: {exc}"
        now = time.time()
        with get_db(immediate=True) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE jobs
                SET status = 'DEAD', attempts = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (current_attempt, err_msg, now, job_id)
            )
            cursor.execute(
                """
                INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                VALUES (?, ?, 'FAILED_PERMANENT', ?, ?, ?)
                """,
                (job_id, now, worker_id, current_attempt, err_msg)
            )
            cursor.execute(
                """
                INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                VALUES (?, ?, 'DEAD', ?, ?, ?)
                """,
                (job_id, now, worker_id, current_attempt, f"Moved to DLQ: {err_msg}")
            )
        return

    handler = HANDLERS.get(job_type)
    if not handler:
        err_msg = f"Unknown job type: '{job_type}'"
        now = time.time()
        with get_db(immediate=True) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE jobs
                SET status = 'DEAD', attempts = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (current_attempt, err_msg, now, job_id)
            )
            cursor.execute(
                """
                INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                VALUES (?, ?, 'FAILED_PERMANENT', ?, ?, ?)
                """,
                (job_id, now, worker_id, current_attempt, err_msg)
            )
            cursor.execute(
                """
                INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                VALUES (?, ?, 'DEAD', ?, ?, ?)
                """,
                (job_id, now, worker_id, current_attempt, f"Moved to DLQ: {err_msg}")
            )
        return

    # Execute handler
    try:
        handler(payload, attempt=job["attempts"])
        # Succeeded!
        now = time.time()
        with get_db(immediate=True) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE jobs
                SET status = 'SUCCEEDED', attempts = ?, updated_at = ?
                WHERE id = ?
                """,
                (current_attempt, now, job_id)
            )
            cursor.execute(
                """
                INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                VALUES (?, ?, 'SUCCEEDED', ?, ?, ?)
                """,
                (job_id, now, worker_id, current_attempt, "Execution completed successfully")
            )

    except PermanentJobError as p_err:
        err_msg = str(p_err)
        now = time.time()
        with get_db(immediate=True) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE jobs
                SET status = 'DEAD', attempts = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (current_attempt, err_msg, now, job_id)
            )
            cursor.execute(
                """
                INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                VALUES (?, ?, 'FAILED_PERMANENT', ?, ?, ?)
                """,
                (job_id, now, worker_id, current_attempt, err_msg)
            )
            cursor.execute(
                """
                INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                VALUES (?, ?, 'DEAD', ?, ?, ?)
                """,
                (job_id, now, worker_id, current_attempt, f"Moved to DLQ: {err_msg}")
            )

    except RetryableJobError as r_err:
        err_msg = str(r_err)
        now = time.time()
        if current_attempt < max_attempts:
            backoff = calculate_backoff(current_attempt)
            next_run = now + backoff
            detail_msg = f"Backoff {backoff:.2f}s (attempt {current_attempt}/{max_attempts})"
            with get_db(immediate=True) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = 'RETRY_WAIT', attempts = ?, next_run_at = ?, last_error = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (current_attempt, next_run, err_msg, now, job_id)
                )
                cursor.execute(
                    """
                    INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                    VALUES (?, ?, 'FAILED_RETRYABLE', ?, ?, ?)
                    """,
                    (job_id, now, worker_id, current_attempt, err_msg)
                )
                cursor.execute(
                    """
                    INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                    VALUES (?, ?, 'RETRY_SCHEDULED', ?, ?, ?)
                    """,
                    (job_id, now, worker_id, current_attempt, detail_msg)
                )
        else:
            exhausted_msg = f"Exhausted retries ({current_attempt}/{max_attempts}): {err_msg}"
            with get_db(immediate=True) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = 'DEAD', attempts = ?, last_error = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (current_attempt, exhausted_msg, now, job_id)
                )
                cursor.execute(
                    """
                    INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                    VALUES (?, ?, 'FAILED_RETRYABLE', ?, ?, ?)
                    """,
                    (job_id, now, worker_id, current_attempt, err_msg)
                )
                cursor.execute(
                    """
                    INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                    VALUES (?, ?, 'DEAD', ?, ?, ?)
                    """,
                    (job_id, now, worker_id, current_attempt, exhausted_msg)
                )

    except Exception as exc:
        err_msg = f"Unexpected failure: {exc}"
        now = time.time()
        with get_db(immediate=True) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE jobs
                SET status = 'DEAD', attempts = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (current_attempt, err_msg, now, job_id)
            )
            cursor.execute(
                """
                INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                VALUES (?, ?, 'FAILED_PERMANENT', ?, ?, ?)
                """,
                (job_id, now, worker_id, current_attempt, err_msg)
            )
            cursor.execute(
                """
                INSERT INTO job_events (job_id, ts, event, worker_id, attempt, detail)
                VALUES (?, ?, 'DEAD', ?, ?, ?)
                """,
                (job_id, now, worker_id, current_attempt, f"Moved to DLQ: {err_msg}")
            )


class WorkerThread(threading.Thread):
    """Background worker thread that continually claims and executes jobs."""

    def __init__(self, worker_id: str, stop_event: threading.Event, poll_interval: float = 0.2):
        super().__init__(name=worker_id, daemon=True)
        self.worker_id = worker_id
        self.stop_event = stop_event
        self.poll_interval = poll_interval

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                job = claim_job(self.worker_id)
                if job:
                    process_claimed_job(self.worker_id, job)
                else:
                    self.stop_event.wait(self.poll_interval)
            except Exception as exc:
                logger.error(f"Worker {self.worker_id} error: {exc}", exc_info=True)
                self.stop_event.wait(self.poll_interval)


class WorkerPool:
    """Manages worker background threads."""

    def __init__(self, num_workers: int = 2):
        self.num_workers = num_workers
        self.stop_event = threading.Event()
        self.threads: List[WorkerThread] = []

    def start(self) -> None:
        self.stop_event.clear()
        self.threads = [
            WorkerThread(worker_id=f"worker-{i+1}", stop_event=self.stop_event)
            for i in range(self.num_workers)
        ]
        for t in self.threads:
            t.start()
        logger.info(f"Started {len(self.threads)} worker threads.")

    def stop(self, timeout: float = 2.0) -> None:
        self.stop_event.set()
        for t in self.threads:
            t.join(timeout=timeout)
        self.threads.clear()
        logger.info("Worker pool stopped.")

    def is_running(self) -> bool:
        return any(t.is_alive() for t in self.threads)


worker_pool = WorkerPool(num_workers=2)
