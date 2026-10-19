"""Simulated job handlers and error classifications for Steadfast Lite."""

import os
import random
import time
from typing import Any, Callable, Dict


class JobError(Exception):
    """Base exception for job failures."""
    pass


class PermanentJobError(JobError):
    """Non-retryable error indicating poison jobs or invalid configurations."""
    pass


class RetryableJobError(JobError):
    """Transient error suitable for backoff and retry."""
    pass


def get_time_scale() -> float:
    """Configurable time scale for simulation delay (set to 0.0 or low in tests)."""
    val = os.environ.get("TIME_SCALE", "1.0")
    try:
        return float(val)
    except ValueError:
        return 1.0


def send_email(payload: Dict[str, Any], attempt: int = 0) -> Dict[str, Any]:
    """Simulates sending an email.
    
    - No "@" in address -> PermanentJobError("Invalid email address")
    - Address ends with "@flaky.test" -> RetryableJobError("Timeout") on first 2 attempts (< 2), then succeeds
    - Otherwise succeeds after simulated 1-2s delay (scaled by TIME_SCALE)
    """
    to_addr = str(payload.get("to", "")).strip()
    if "@" not in to_addr:
        raise PermanentJobError("Invalid email address")

    if to_addr.endswith("@flaky.test") and attempt < 2:
        raise RetryableJobError("Timeout")

    scale = get_time_scale()
    if scale > 0:
        time.sleep(random.uniform(1.0, 2.0) * scale)

    return {"status": "sent", "to": to_addr}


def generate_report(payload: Dict[str, Any], attempt: int = 0) -> Dict[str, Any]:
    """Simulates report generation.
    
    - "missing.csv" in filename -> PermanentJobError("File not found")
    - Otherwise succeeds after simulated 1-2s delay (scaled by TIME_SCALE)
    """
    file_name = str(payload.get("file", "")).strip()
    if "missing.csv" in file_name or file_name == "missing.csv":
        raise PermanentJobError("File not found")

    scale = get_time_scale()
    if scale > 0:
        time.sleep(random.uniform(1.0, 2.0) * scale)

    return {"status": "generated", "file": file_name}


HANDLERS: Dict[str, Callable[[Dict[str, Any], int], Dict[str, Any]]] = {
    "send_email": send_email,
    "generate_report": generate_report,
}
