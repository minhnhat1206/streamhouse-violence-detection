"""
Streamhouse Pipeline Manager
=============================
Tự động hoá vòng đời dữ liệu HOT → WARM → COLD:

  STARTUP  : Chờ Flink sẵn sàng, submit tất cả streaming jobs còn thiếu
  WATCHDOG : Cứ mỗi CHECK_INTERVAL giây, kiểm tra và restart job bị chết
  ARCHIVAL : Mỗi ngày lúc ARCHIVE_HOUR:00, trigger batch Paimon → Iceberg

Chạy như Docker service (container: pipeline-manager).
Sử dụng Flink REST API (port 8081) — không cần docker exec, không cần Airflow.

Environment variables:
  FLINK_API              - Flink REST endpoint  (default: http://jobmanager:8081)
  FLINK_JM_ADDRESS       - JobManager hostname   (default: jobmanager)
  FLINK_JM_RPC_PORT      - JobManager RPC port   (default: 6123)
  SCRIPTS_DIR            - Path to PyFlink scripts (default: /opt/flink/scripts)
  CHECK_INTERVAL_SECONDS - Watchdog interval     (default: 300 = 5 min)
  ARCHIVE_HOUR           - Hour to run archival  (default: 2 = 02:00 AM)
  STARTUP_WAIT_SECS      - Max wait for Flink JM (default: 180)
"""

import json
import logging
import os
import subprocess
import time
import urllib.request
import urllib.error
from datetime import datetime
from typing import Optional

# ── Configuration ───────────────────────────────────────────────────────────────
FLINK_API          = os.getenv("FLINK_API",              "http://jobmanager:8081")
FLINK_JM_ADDRESS   = os.getenv("FLINK_JM_ADDRESS",       "jobmanager")
FLINK_JM_RPC_PORT  = os.getenv("FLINK_JM_RPC_PORT",      "6123")
SCRIPTS_DIR        = os.getenv("SCRIPTS_DIR",             "/opt/flink/scripts")
CHECK_INTERVAL     = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))   # 5 min
ARCHIVE_HOUR       = int(os.getenv("ARCHIVE_HOUR",            "2"))    # 02:00
STARTUP_WAIT_SECS  = int(os.getenv("STARTUP_WAIT_SECS",      "180"))   # 3 min

# ── Streaming jobs — MUST always be running ─────────────────────────────────────
# key: substring phải tìm thấy trong tên Flink job (Flink đặt tên theo sink table)
# THỨ TỰ QUAN TRỌNG: validator phải chạy TRƯỚC sink jobs vì sink jobs đọc từ
# hot-violence-alerts-valid (output của validator). Pipeline-manager submit tuần tự.
STREAMING_JOBS: dict[str, dict] = {
    "Contract Validator": {
        "script":      f"{SCRIPTS_DIR}/data_contract_validator.py",
        "description": "Kafka urban-safety-alerts → hot-violence-alerts-valid (DATA CONTRACT)",
    },
    "hot_violence_alerts": {
        "script":      f"{SCRIPTS_DIR}/sink_to_fluss.py",
        "description": "Kafka hot-violence-alerts-valid → Fluss (HOT layer)",
    },
    "violence_incidents": {
        "script":      f"{SCRIPTS_DIR}/sink_to_paimon.py",
        "description": "Kafka hot-violence-alerts-valid → Paimon (WARM layer)",
    },
    "daily_incident_stats": {
        "script":      f"{SCRIPTS_DIR}/aggregate_paimon.py",
        "description": "Paimon CDC → daily_stats + camera_stats (WARM gold)",
    },
}

# ── Batch archival job ──────────────────────────────────────────────────────────
ARCHIVE_SCRIPT = f"{SCRIPTS_DIR}/archive_to_iceberg.py"

# ── Logging ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline-manager")

# ── Flink REST helpers ──────────────────────────────────────────────────────────

def flink_get(path: str) -> Optional[dict]:
    """GET từ Flink REST API, trả None nếu lỗi."""
    try:
        url = f"{FLINK_API}{path}"
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError as exc:
        log.error("Flink API unreachable [GET %s]: %s", path, exc.reason)
    except Exception as exc:
        log.error("Flink API error [GET %s]: %s", path, exc)
    return None


# Các trạng thái Flink được coi là "đang hoạt động" — không cần re-submit
_ACTIVE_STATES = {"RUNNING", "RESTARTING", "RECONCILING", "CREATED", "INITIALIZING"}


def get_running_job_names() -> set[str]:
    """Trả set tên các Flink job đang ở trạng thái active (RUNNING hoặc RESTARTING)."""
    data = flink_get("/jobs/overview")
    if not data:
        return set()
    return {
        j["name"]
        for j in data.get("jobs", [])
        if j.get("state") in _ACTIVE_STATES
    }


def is_job_running(key: str, running_names: set[str]) -> bool:
    """Kiểm tra job có đang chạy không (khớp substring với tên Flink job)."""
    return any(key in name for name in running_names)


# ── Job submission ───────────────────────────────────────────────────────────────

def _run_flink(args: list[str], timeout: int = 180) -> tuple[bool, str]:
    """
    Chạy lệnh /opt/flink/bin/flink với các args cho trước.
    Trả (success, stderr_snippet).
    Không dùng -D flags trước subcommand vì bị parse là JVM args.
    FLINK_PROPERTIES env var đã set rest.address + jobmanager.rpc.address.
    """
    cmd = ["/opt/flink/bin/flink"] + args

    log.debug("CMD: %s", " ".join(cmd))

    # Dùng unique TMPDIR cho mỗi lần submit để tránh symlink conflict
    # khi Flink tạo Python environment (PythonEnvUtils.createSymbolicLink)
    import tempfile
    unique_tmp = tempfile.mkdtemp(prefix="flink-pm-")
    env = os.environ.copy()
    env["TMPDIR"] = unique_tmp
    env["JAVA_TOOL_OPTIONS"] = f"-Djava.io.tmpdir={unique_tmp}"

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        if result.returncode == 0:
            return True, ""
        # Extract meaningful error — filter out noisy JDK/Hadoop warnings
        err = result.stderr or result.stdout
        noise_prefixes = (
            "WARNING: Unknown module",
            "WARN  org.apache.hadoop",
            "INFO  org.apache.hadoop",
        )
        meaningful = [
            l.strip() for l in err.splitlines()
            if l.strip() and not any(l.strip().startswith(p) for p in noise_prefixes)
        ]
        snippet = " | ".join(meaningful[-8:])[:600]
        return False, snippet or err[:300]
    except subprocess.TimeoutExpired:
        return False, f"Timed out after {timeout}s"
    except FileNotFoundError:
        return False, "flink CLI not found — is this the Flink image?"
    finally:
        # Dọn unique tmp dir sau mỗi submission
        import shutil
        shutil.rmtree(unique_tmp, ignore_errors=True)


def submit_streaming_job(job_key: str, script: str) -> bool:
    """Submit một PyFlink streaming job (--detached, chạy mãi mãi)."""
    log.info("Submitting streaming job: %s", job_key)
    log.info("  script: %s", script)

    # Không dùng --pyFiles vì các scripts không import nhau.
    # Tránh symlink conflict khi Flink tạo temp Python env cho nhiều submissions.
    ok, err = _run_flink([
        "run",
        "--detached",
        "--python", script,
    ])

    if ok:
        log.info("✓ Streaming job '%s' submitted successfully.", job_key)
    else:
        log.error("✗ Failed to submit '%s': %s", job_key, err)
    return ok


def run_archive_job() -> bool:
    """Chạy batch job Paimon → Iceberg (blocking, chờ hoàn thành)."""
    log.info("Starting archival batch job: Paimon WARM → Iceberg COLD")
    log.info("  script: %s", ARCHIVE_SCRIPT)

    ok, err = _run_flink(
        args=[
            "run",
            "--python", ARCHIVE_SCRIPT,
            "-Dexecution.runtime-mode=BATCH",
            "-Dpipeline.name=daily_archive_paimon_to_iceberg",
        ],
        timeout=7200,   # archival có thể mất đến 2 tiếng
    )

    if ok:
        log.info("✓ Archival completed successfully.")
    else:
        log.error("✗ Archival failed: %s", err)
    return ok


# ── Watchdog logic ───────────────────────────────────────────────────────────────

def cancel_duplicate_jobs(key: str, running_names_with_ids: list[dict]) -> None:
    """
    Nếu có nhiều hơn 1 job chạy cùng tên (key), cancel các bản cũ hơn.
    Giữ lại job MỚI NHẤT (có start-time cao nhất).
    """
    matches = [j for j in running_names_with_ids if key in j.get("name", "")]
    if len(matches) <= 1:
        return

    log.warning("Duplicate jobs detected for '%s': %d instances — cancelling older ones.", key, len(matches))
    # Sort by start-time descending: keep newest, cancel rest
    sorted_jobs = sorted(matches, key=lambda j: j.get("start-time", 0), reverse=True)
    for job in sorted_jobs[1:]:
        jid = job.get("jid", "")
        log.info("  Cancelling duplicate job %s (%s)", jid[:8], job.get("name", "")[:40])
        try:
            req = urllib.request.Request(
                f"{FLINK_API}/jobs/{jid}?mode=cancel",
                method="PATCH",
            )
            urllib.request.urlopen(req, timeout=10)
            log.info("  Cancelled job %s", jid[:8])
        except Exception as exc:
            log.warning("  Could not cancel job %s: %s", jid[:8], exc)


def get_running_jobs_detailed() -> list[dict]:
    """Fetch active Flink jobs with full metadata (jid, name, start-time)."""
    data = flink_get("/jobs/overview")
    if not data:
        return []
    return [j for j in data.get("jobs", []) if j.get("state") in _ACTIVE_STATES]


def watchdog_tick() -> bool:
    """
    Kiểm tra tất cả streaming jobs.
    Huỷ duplicate, re-submit job bị thiếu.
    Trả True nếu tất cả OK, False nếu có job phải restart.
    """
    running_detailed = get_running_jobs_detailed()
    running = {j["name"] for j in running_detailed}

    if running:
        log.info("Running Flink jobs: %s", sorted(running))
    else:
        log.warning("No Flink jobs running (Flink may still be starting).")

    # Cancel duplicates trước khi kiểm tra
    for key in STREAMING_JOBS:
        cancel_duplicate_jobs(key, running_detailed)

    all_ok = True
    for key, cfg in STREAMING_JOBS.items():
        if is_job_running(key, running):
            log.info("  ✓ %-30s — %s", key, cfg["description"])
        else:
            log.warning("  ✗ %-30s NOT running — submitting...", key)
            submit_streaming_job(key, cfg["script"])
            all_ok = False
            # Đợi Flink dọn temp Python env trước khi submit job tiếp theo
            time.sleep(5)

    return all_ok


# ── Archival schedule ────────────────────────────────────────────────────────────

def should_run_archival(last_archive: Optional[datetime]) -> bool:
    """
    Trả True nếu đã đến giờ chạy archival hôm nay và chưa chạy.
    Logic: cứ mỗi ngày, chạy một lần sau ARCHIVE_HOUR:00.
    """
    now = datetime.now()
    if last_archive is None:
        # Lần đầu: chạy nếu đã qua giờ archive hôm nay
        return now.hour >= ARCHIVE_HOUR
    # Chạy nếu ngày hôm nay > ngày archive cuối và đã qua giờ archive
    return (now.date() > last_archive.date()) and (now.hour >= ARCHIVE_HOUR)


# ── Startup wait ─────────────────────────────────────────────────────────────────

def wait_for_flink(timeout: int = 180) -> bool:
    """
    Đợi Flink JobManager sẵn sàng và có ít nhất 1 task slot.
    Kiểm tra mỗi 10 giây.
    """
    log.info("Waiting for Flink JobManager at %s ...", FLINK_API)
    deadline = time.time() + timeout

    while time.time() < deadline:
        data = flink_get("/overview")
        if data:
            slots     = data.get("slots-available", 0)
            taskmanagers = data.get("taskmanagers", 0)
            log.info(
                "  Flink: taskmanagers=%d, slots-available=%d",
                taskmanagers, slots,
            )
            if slots > 0:
                log.info("✓ Flink is ready.")
                return True
            # TaskManager đã kết nối nhưng chưa có slot — đợi thêm
            log.info("  Waiting for task slots...")

        time.sleep(10)

    log.warning(
        "Flink did not become ready within %ds. Proceeding anyway.", timeout
    )
    return False


# ── Main loop ────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("Streamhouse Pipeline Manager v1.0")
    log.info("  Streaming jobs   : %s", list(STREAMING_JOBS.keys()))
    log.info("  Check interval   : %ds (%.1f min)", CHECK_INTERVAL, CHECK_INTERVAL / 60)
    log.info("  Archival at      : %02d:00 daily", ARCHIVE_HOUR)
    log.info("  Scripts dir      : %s", SCRIPTS_DIR)
    log.info("  Flink API        : %s", FLINK_API)
    log.info("=" * 60)

    # 1. Đợi Flink JM + TaskManager sẵn sàng
    wait_for_flink(STARTUP_WAIT_SECS)

    # 2. Startup check — submit tất cả jobs còn thiếu
    log.info("--- Startup: initial job check ---")
    watchdog_tick()

    last_archive: Optional[datetime] = None

    # 3. Main loop
    while True:
        time.sleep(CHECK_INTERVAL)

        log.info("--- Watchdog tick @ %s ---", datetime.now().strftime("%H:%M:%S"))
        watchdog_tick()

        # Archival check
        if should_run_archival(last_archive):
            log.info("--- Daily archival triggered @ %s ---",
                     datetime.now().strftime("%Y-%m-%d %H:%M"))
            if run_archive_job():
                last_archive = datetime.now()
            else:
                log.warning(
                    "Archival failed — will retry next check interval."
                )


if __name__ == "__main__":
    main()
