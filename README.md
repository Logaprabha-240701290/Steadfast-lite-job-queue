# ⚡ Steadfast Lite

A resilient, zero-visibility-gap background job processing prototype built with **FastAPI**, **SQLite (WAL mode)**, and a **zero-build dashboard**.

Steadfast Lite demonstrates two fundamental production guarantees:
1. **Poison Job Containment**: Non-retryable ("poison") jobs never retry infinitely. They are classified into retryable vs. permanent errors, capped by max attempts, relegated to a **Dead-Letter Queue (DLQ)**, and made inspectable, editable, and replayable by an operator.
2. **Zero-Visibility Timeline**: Every single state change is recorded atomically in `job_events` in the **same transaction** as the job state update, giving a parcel-tracking visual audit trail and real-time dashboard.

---

## 🚀 Quickstart

### Prerequisites
- Python 3.10+ installed
- No Docker, no external database needed (uses standard Python `sqlite3`)

### Windows (PowerShell)
```powershell
# 1. Create and activate virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the application
python -m uvicorn app.main:app --reload --port 8000
```

### macOS / Linux (Bash / Zsh)
```bash
# 1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the application
python3 -m uvicorn app.main:app --reload --port 8000
```

Open your browser to: **[http://localhost:8000](http://localhost:8000)**

> [!NOTE]
> The SQLite database (`steadfast.db`) and all tables/indexes are created automatically on first run in WAL mode (`PRAGMA journal_mode = WAL;`).

---

## 🛠 VS Code Setup

Steadfast Lite comes preconfigured for VS Code:
- **Press `F5`**: Launches **"Run Steadfast Lite"** using `debugpy` and `uvicorn app.main:app --reload --port 8000`.
- **Run Tasks**: Open the Command Palette (`Ctrl+Shift+P` / `Cmd+Shift+P`), select **Tasks: Run Task**:
  - `Run app`: Runs the uvicorn development server.
  - `Run tests`: Runs pytest (`pytest -v`).

---

## 🧪 Running Tests

To run the full automated test suite:
```bash
pytest -v
```

All 5 core behavioral flows are verified:
1. `test_permanent_error_to_dead_after_one_attempt` — Invalid email fails immediately to `DEAD` after 1 attempt without infinite looping.
2. `test_flaky_job_fails_twice_then_succeeds_with_growing_backoff` — Transient `@flaky.test` timeouts back off exponentially with jitter and succeed on attempt 3.
3. `test_retry_exhaustion_to_dead_after_max_attempts` — Repeated transient failures transition to `DEAD` upon hitting `max_attempts`.
4. `test_edit_payload_and_replay_to_succeeded` — Operator edits defective payload in the DLQ and replays it to success.
5. `test_replayed_job_timeline_exact_sequence` — Validates the exact parcel-tracking audit trail:
   `SUBMITTED` $\rightarrow$ `QUEUED` $\rightarrow$ `RUNNING` $\rightarrow$ `FAILED_PERMANENT` $\rightarrow$ `DEAD` $\rightarrow$ `PAYLOAD_EDITED` $\rightarrow$ `REPLAYED` $\rightarrow$ `QUEUED` $\rightarrow$ `RUNNING` $\rightarrow$ `SUCCEEDED`.

---

## 🎬 3-Minute Demo Script (Hackathon Presentation)

Follow this script to demonstrate the prototype to judges:

### Minute 1: Zero Visibility Solved (Parcel Tracking)
1. Open **[http://localhost:8000](http://localhost:8000)**. Point out the real-time status counters: `Queued`, `Running`, `Retry Wait`, `Succeeded`, and `DEAD (DLQ)`.
2. Click the **"🌱 Seed 30 mixed jobs"** button.
3. Notice the live table immediately fill up with 30 jobs. Watch the two worker threads concurrently process the batch.
4. Click on any job row (or the **🔍 Timeline** button).
5. **Show the parcel-tracking timeline**: Point out how every single transition (`SUBMITTED` $\rightarrow$ `QUEUED` $\rightarrow$ `RUNNING` $\rightarrow$ `SUCCEEDED` or `RETRY_WAIT`) is recorded with an exact timestamp, worker ID, and attempt number.
6. Highlight that if an engineer asks *"Where is my job and why did it fail?"*, the timeline gives the definitive answer instantly.

### Minute 2: Poison Job Containment (No Infinite Retries)
1. Point out the **DEAD (DLQ)** counter card in bright red. Notice that out of the 30 jobs, 4 poison jobs were instantly isolated.
2. Click the **"🧪 Submit poison job"** button. Watch it immediately enter the system and get classified as a permanent failure (`FAILED_PERMANENT` $\rightarrow$ `DEAD`).
3. Explain the architecture:
   - **Transient errors** (like `@flaky.test` timeouts) back off exponentially with full jitter (`random(0, 1s * 2^attempts)`).
   - **Poison jobs** (syntax errors, missing fields, invalid recipients) fail permanently on attempt 1 without wasting compute or retrying indefinitely.
4. Point to the **"Stuck jobs: 0"** banner, explaining that jobs running for $>60\text{s}$ trigger a high-visibility alert.

### Minute 3: DLQ Payload Repair & Instant Replay
1. Switch to the **"Dead-Letter Queue (DLQ)"** tab.
2. Select a failed job (e.g., `invalid-address-without-at` or the poison job).
3. Show the error box displaying: `Invalid email address`.
4. In the inline payload editor, edit the JSON to fix the email address:
   ```json
   {
     "to": "fixed_user@example.com",
     "subject": "Repaired Payload"
   }
   ```
5. Click **"💾 Save Fix"**. Notice the toast notification and `PAYLOAD_EDITED` audit event.
6. Click **"🔄 Replay Job"**.
7. Watch the job instantly return to `QUEUED`, get picked up by an active worker, and transition to `SUCCEEDED`.
8. Click **"🔍 View Timeline"** on the repaired job to reveal the complete end-to-end lifecycle from initial poison failure through operator repair to final success!

---

## 🏛 Architecture & Invariants

- **Atomic Claiming**: Worker threads claim jobs using `BEGIN IMMEDIATE` transactions in SQLite WAL mode. This guarantees two workers can never claim the same job concurrently.
- **Single-Transaction State & Event Updates**: Every status change updates `jobs` and inserts into `job_events` in the exact same transaction.
- **Error Classification**:
  - `PermanentJobError` $\rightarrow$ immediate `DEAD` (DLQ).
  - `RetryableJobError` $\rightarrow$ `RETRY_WAIT` with full jitter exponential backoff until `max_attempts`.
- **Fast Testing**: Configurable `TIME_SCALE` allows simulated work delays and exponential backoffs to execute in milliseconds under test.
