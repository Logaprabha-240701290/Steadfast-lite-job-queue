"""Database connection and schema management for Steadfast Lite."""

import os
import sqlite3
from contextlib import contextmanager
from typing import Generator

DEFAULT_DB_PATH = os.environ.get("DB_PATH", "steadfast.db")


def get_db_path() -> str:
    """Return the currently configured database path."""
    return os.environ.get("DB_PATH", DEFAULT_DB_PATH)


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    """Create and return a SQLite connection with WAL mode and row factory."""
    path = db_path or get_db_path()
    # isolation_level=None enables autocommit mode, allowing manual BEGIN IMMEDIATE / COMMIT
    conn = sqlite3.connect(path, timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@contextmanager
def get_db(immediate: bool = False, db_path: str | None = None) -> Generator[sqlite3.Connection, None, None]:
    """Context manager for database transactions.

    Uses BEGIN IMMEDIATE when immediate=True to ensure atomic claims without race conditions.
    """
    conn = get_connection(db_path)
    try:
        if immediate:
            conn.execute("BEGIN IMMEDIATE")
        else:
            conn.execute("BEGIN")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise
    finally:
        conn.close()


def init_db(db_path: str | None = None) -> None:
    """Initialize database tables and indexes."""
    with get_db(immediate=True, db_path=db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                next_run_at REAL NOT NULL,
                last_error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_jobs_claim
            ON jobs (status, next_run_at);
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                ts REAL NOT NULL,
                event TEXT NOT NULL,
                worker_id TEXT,
                attempt INTEGER,
                detail TEXT,
                FOREIGN KEY(job_id) REFERENCES jobs(id)
            );
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_job_events_job_id
            ON job_events (job_id, id ASC);
            """
        )
