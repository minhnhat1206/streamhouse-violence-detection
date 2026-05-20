"""
Streamhouse Pipeline Manager
=============================
Tự động hoá vòng đời dữ liệu HOT → WARM → COLD:

  STARTUP  : Chờ Flink sẵn sàng, submit tất cả streaming jobs còn thiếu
  WATCHDOG : Cứ mỗi CHECK_INTERVAL giây, kiểm tra và restart job bị chết
  TIERING  : Cứ mỗi TIERING_INTERVAL_MINS phút, di chuyển dữ liệu cũ Fluss → Paimon
  ARCHIVAL : Mỗi ngày lúc ARCHIVE_HOUR:00, trigger batch Paimon → Iceberg

Chạy như Docker service (container: pipeline-manager).
Sử dụng Flink REST API (port 8081) — không cần docker exec, không cần Airflow.

True Streamhouse Tiering (không dùng dual-write):
  Kafka → sink_to_fluss_enriched.py → Fluss HOT (write once, với location enrichment)
                                           │
                    mỗi TIERING_INTERVAL_MINS phút: tier_fluss_to_paimon.py
                    Phase 1: INSERT aged (>TIERING_HOURS) → Paimon WARM
                    Phase 2: DELETE aged from Fluss (best-effort)

Environment variables:
  FLINK_API              - Flink REST endpoint         (default: http://jobmanager:8081)
  FLINK_JM_ADDRESS       - JobManager hostname          (default: jobmanager)
  FLINK_JM_RPC_PORT      - JobManager RPC port          (default: 6123)
  SCRIPTS_DIR            - Path to PyFlink scripts      (default: /opt/flink/scripts)
  CHECK_INTERVAL_SECONDS - Watchdog interval            (default: 300 = 5 min)
  TIERING_INTERVAL_MINS  - Tiering interval in minutes  (default: 30)
  ARCHIVE_HOUR           - Hour to run archival         (default: 2 = 02:00 AM)
  STARTUP_WAIT_SECS      - Max wait for Flink JM        (default: 180)
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
FLINK_API              = os.getenv("FLINK_API",              "http://jobmanager:8081")
FLINK_JM_ADDRESS       = os.getenv("FLINK_JM_ADDRESS",       "jobmanager")
FLINK_JM_RPC_PORT      = os.getenv("FLINK_JM_RPC_PORT",      "6123")
SCRIPTS_DIR            = os.getenv("SCRIPTS_DIR",             "/opt/flink/scripts")
CHECK_INTERVAL         = int(os.getenv("CHECK_INTERVAL_SECONDS",  "300"))  # 5 min
TIERING_INTERVAL_MINS  = int(os.getenv("TIERING_INTERVAL_MINS",    "30"))  # 30 min
ARCHIVE_HOUR           = int(os.getenv("ARCHIVE_HOUR",              "2"))  # 02:00
STARTUP_WAIT_SECS      = int(os.getenv("STARTUP_WAIT_SECS",        "180"))  # 3 min

# ── Streaming jobs — MUST always be running ─────────────────────────────────────
# key: substring phải tìm thấy trong tên Flink job (Flink đặt tên theo sink table)
# THỨ TỰ QUAN TRỌNG: validator phải chạy TRƯỚC sink jobs vì sink jobs đọc từ
# hot-violence-alerts-valid (output của validator). Pipeline-manager submit tuần tự.
#
# True Streamhouse Tiering:
#   - sink_to_fluss_enriched.py: write-once → Fluss HOT (với temporal join enrichment)
#   - sink_to_paimon_star.py đã BỊ XÓA khỏi STREAMING_JOBS (không còn dual-write)
#   - Paimon WARM được populate bởi tier_fluss_to_paimon.py (run mỗi TIERING_INTERVAL_MINS)
STREAMING_JOBS: dict[str, dict] = {
    "Contract Validator": {
        "script":      f"{SCRIPTS_DIR}/data_contract_validator.py",
        "description": "Kafka urban-safety-alerts → hot-violence-alerts-valid (DATA CONTRACT)",
    },
    "hot_violence_alerts": {
        "script":         f"{SCRIPTS_DIR}/sink_to_fluss_enriched.py",
        "description":    "Kafka → temporal join dim_camera → Fluss HOT (write-once, enriched)",
        "submit_timeout": 400,  # cần thời gian init catalogs + DDL migration + compile plan
    },
    "daily_incident_stats": {
        "script":         f"{SCRIPTS_DIR}/aggregate_paimon.py",
        "description":    "Paimon CDC → daily_incident_stats + camera_stats (WARM gold)",
        "submit_timeout": 400,
    },
}

# ── Periodic tiering job: Fluss HOT → Paimon WARM ──────────────────────────────
TIERING_SCRIPT = f"{SCRIPTS_DIR}/tier_fluss_to_paimon.py"

# ── Batch archival job: Paimon WARM → Iceberg COLD ─────────────────────────────
ARCHIVE_SCRIPT = f"{SCRIPTS_DIR}/archive_to_iceberg.py"

# ── Logging ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline-manager")
log.info("True Streamhouse Tiering enabled — tier_fluss_to_paimon.py every %d min", TIERING_INTERVAL_MINS)

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


def submit_streaming_job(job_key: str, cfg: dict) -> bool:
    """Submit một streaming job (Python script hoặc Java JAR)."""
    if cfg.get("is_jar"):
        return _submit_jar_job(job_key, cfg)
    return _submit_python_job(job_key, cfg["script"], timeout=cfg.get("submit_timeout", 180))


def _submit_python_job(job_key: str, script: str, timeout: int = 180) -> bool:
    """Submit PyFlink streaming job (--detached, chạy mãi mãi)."""
    log.info("Submitting streaming job: %s", job_key)
    log.info("  script: %s", script)

    ok, err = _run_flink([
        "run",
        "--detached",
        "--python", script,
    ], timeout=timeout)
    if ok:
        log.info("✓ Streaming job '%s' submitted successfully.", job_key)
    else:
        log.error("✗ Failed to submit '%s': %s", job_key, err)
    return ok


def _submit_jar_job(job_key: str, cfg: dict) -> bool:
    """Submit Java JAR job — dùng cho Fluss Tiering Service."""
    jar = cfg["script"]
    main_class = cfg.get("main_class", "")
    extra_args = cfg.get("jar_args", [])

    log.info("Submitting JAR job: %s", job_key)
    log.info("  jar: %s  class: %s", jar, main_class)

    cmd = ["run", "--detached"]
    if main_class:
        cmd += ["-c", main_class]
    cmd.append(jar)
    cmd.extend(extra_args)

    ok, err = _run_flink(cmd)
    if ok:
        log.info("✓ JAR job '%s' submitted.", job_key)
    else:
        log.error("✗ Failed to submit JAR '%s': %s", job_key, err)
    return ok


def _seed_dim_camera_via_gateway() -> bool:
    """
    Seed dim_camera (Fluss) via Flink SQL Gateway REST API (streaming mode).

    Batch mode INSERT into Fluss primary key tables does NOT commit data because
    Fluss relies on streaming checkpoints for durability. The SQL Gateway runs
    in streaming mode by default, so INSERT via HTTP is the correct approach.

    Idempotent: dim_camera uses PRIMARY KEY (camera_id) → duplicate INSERTs
    are treated as upserts (UPDATE_AFTER), so safe to re-run.
    """
    gateway = os.getenv("FLINK_GATEWAY_URL", "http://flink-sql-gateway:8083")

    cameras = [
        ("cam_01", "Đường Nguyễn Huệ",         "Phường Bến Nghé",          "Quận 1", 10.77845, 106.70014),
        ("cam_02", "Đường Lê Lợi",              "Phường Nguyễn Thái Bình",  "Quận 1", 10.77322, 106.69453),
        ("cam_03", "Đường Nguyễn Thái Học",     "Phường Bến Thành",         "Quận 1", 10.77407, 106.70229),
        ("cam_04", "Đường Lê Thánh Tôn",        "Phường Cầu Ông Lãnh",      "Quận 1", 10.77613, 106.69705),
        ("cam_05", "Đường Pasteur",              "Phường Phạm Ngũ Lão",      "Quận 1", 10.77157, 106.70435),
        ("cam_06", "Đường Trần Hưng Đạo",       "Phường Tân Định",          "Quận 1", 10.77336, 106.70019),
        ("cam_07", "Đường Đồng Khởi",           "Phường Đa Kao",            "Quận 1", 10.77833, 106.69332),
        ("cam_08", "Đường Hai Bà Trưng",        "Phường Bến Thành",         "Quận 1", 10.78446, 106.70214),
        ("cam_09", "Đường Nguyễn Du",           "Phường Nguyễn Cư Trinh",   "Quận 1", 10.77002, 106.70027),
        ("cam_10", "Đường Võ Văn Kiệt",         "Phường Cầu Kho",           "Quận 1", 10.78266, 106.70826),
        ("cam_11", "Đường Nguyễn Công Trứ",     "Phường Tân Định",          "Quận 1", 10.77552, 106.70748),
        ("cam_12", "Đường Công Trường Mê Linh", "Phường Nguyễn Thái Bình",  "Quận 1", 10.77956, 106.70549),
        ("cam_13", "Đường Hàm Nghi",            "Phường Phạm Ngũ Lão",      "Quận 1", 10.78320, 106.69630),
        ("cam_14", "Đường Nguyễn Bỉnh Khiêm",  "Phường Bến Nghé",          "Quận 1", 10.78074, 106.70235),
        ("cam_15", "Đường Trương Định",         "Phường Đa Kao",            "Quận 1", 10.77709, 106.69288),
    ]

    def _gw_exec(session_id: str, sql: str, timeout: int = 60) -> str:
        resp = urllib.request.urlopen(
            urllib.request.Request(
                f"{gateway}/v1/sessions/{session_id}/statements",
                data=json.dumps({"statement": sql}).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            ),
            timeout=30,
        )
        op = json.loads(resp.read())["operationHandle"]
        deadline = time.time() + timeout
        while time.time() < deadline:
            sr = json.loads(urllib.request.urlopen(
                f"{gateway}/v1/sessions/{session_id}/operations/{op}/status", timeout=10
            ).read())
            if sr.get("status") in ("FINISHED", "ERROR", "CLOSED", "CANCELED"):
                return sr.get("status", "UNKNOWN")
            time.sleep(2)
        return "TIMEOUT"

    try:
        sess_resp = json.loads(urllib.request.urlopen(
            urllib.request.Request(
                f"{gateway}/v1/sessions",
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            ),
            timeout=30,
        ).read())
        sid = sess_resp["sessionHandle"]
        log.info("SQL Gateway session for dim_camera seeding: %s", sid)

        fluss_coord = os.getenv("FLUSS_COORDINATOR", "fluss-coordinator:9123")
        _gw_exec(sid, f"CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='{fluss_coord}')")
        _gw_exec(sid, "USE CATALOG fluss")
        _gw_exec(sid, "CREATE DATABASE IF NOT EXISTS security")
        _gw_exec(sid, "USE security")

        rows_sql = ",\n    ".join(
            f"('{cid}', '{loc}', '{ward}', '{dist}', {lat}, {lon}, 'ACTIVE', "
            f"TIMESTAMP '2025-01-01 00:00:00')"
            for cid, loc, ward, dist, lat, lon in cameras
        )
        st = _gw_exec(sid, f"INSERT INTO dim_camera VALUES\n    {rows_sql}", timeout=120)
        if st == "FINISHED":
            log.info("✓ dim_camera seeded with %d cameras via SQL Gateway.", len(cameras))
            return True
        else:
            log.warning("dim_camera seed via Gateway returned status=%s", st)
            return False

    except Exception as exc:
        log.warning("dim_camera seed via SQL Gateway failed: %s — dim_camera may be empty.", exc)
        return False


def _run_star_schema_setup() -> bool:
    """
    Chạy setup_star_schema.py để tạo DDL tables (batch mode).
    Sau đó seed dim_camera qua SQL Gateway (streaming mode).

    Note: setup_star_schema.py dùng batch mode cho DDL (CREATE TABLE).
    Nhưng INSERT vào Fluss primary key table cần streaming checkpoint →
    dùng SQL Gateway để seed dim_camera sau khi DDL hoàn thành.
    """
    setup_script = f"{SCRIPTS_DIR}/setup_star_schema.py"
    if not os.path.exists(setup_script):
        log.warning("setup_star_schema.py not found at %s — skipping.", setup_script)
        return True

    log.info("Running star schema DDL setup: %s", setup_script)
    ok, err = _run_flink(
        args=[
            "run",
            "--python", setup_script,
            "-Dexecution.runtime-mode=BATCH",
            "-Dpipeline.name=setup_star_schema",
        ],
        timeout=600,
    )
    if ok:
        log.info("✓ Star schema DDL complete (tables created/verified).")
    else:
        log.warning("Star schema DDL failed (non-fatal): %s", err)

    # Seed dim_camera via SQL Gateway (streaming mode required for Fluss commit)
    log.info("Seeding dim_camera via SQL Gateway (streaming mode)...")
    _seed_dim_camera_via_gateway()
    return ok


def should_run_tiering(last_tiering: Optional[datetime]) -> bool:
    """
    Trả True nếu đã đến lúc chạy tiering (cứ mỗi TIERING_INTERVAL_MINS phút).
    Lần đầu tiên sau khởi động: chạy ngay.
    """
    if last_tiering is None:
        return True
    elapsed = (datetime.now() - last_tiering).total_seconds()
    return elapsed >= TIERING_INTERVAL_MINS * 60


def run_tiering_job() -> bool:
    """
    Chạy tier_fluss_to_paimon.py (blocking — không dùng --detached).
    Timeout 600s (10 phút): bao gồm Phase1 wait 120s + Phase2 delete 60s + overhead.
    """
    log.info("Starting tiering job: Fluss HOT → Paimon WARM")
    log.info("  script: %s", TIERING_SCRIPT)

    ok, err = _run_flink(
        args=[
            "run",
            "--python", TIERING_SCRIPT,
            f"-Dpipeline.name=tier_fluss_to_paimon",
        ],
        timeout=600,
    )

    if ok:
        log.info("✓ Tiering job completed successfully.")
    else:
        log.error("✗ Tiering job failed: %s", err)
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
            submit_streaming_job(key, cfg)
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
    log.info("Streamhouse Pipeline Manager v2.0  (True Tiering)")
    log.info("  Streaming jobs   : %s", list(STREAMING_JOBS.keys()))
    log.info("  Check interval   : %ds (%.1f min)", CHECK_INTERVAL, CHECK_INTERVAL / 60)
    log.info("  Tiering every    : %d min  (Fluss HOT → Paimon WARM)", TIERING_INTERVAL_MINS)
    log.info("  Archival at      : %02d:00 daily  (Paimon WARM → Iceberg COLD)", ARCHIVE_HOUR)
    log.info("  Scripts dir      : %s", SCRIPTS_DIR)
    log.info("  Flink API        : %s", FLINK_API)
    log.info("=" * 60)

    # 1. Đợi Flink JM + TaskManager sẵn sàng
    wait_for_flink(STARTUP_WAIT_SECS)

    # 1.5 One-time star schema setup (Task 1.2: DDL + dim table seeding)
    _run_star_schema_setup()

    # 2. Startup check — submit tất cả jobs còn thiếu
    log.info("--- Startup: initial job check ---")
    watchdog_tick()

    # Tránh chạy archival ngay khi restart ban ngày (e.g. 15:22 >= ARCHIVE_HOUR=2)
    # Nếu đã qua giờ archive hôm nay → đánh dấu đã chạy hôm nay (bỏ qua cho đến ngày mai)
    _now = datetime.now()
    last_archive: Optional[datetime] = (
        _now if _now.hour >= ARCHIVE_HOUR else None
    )
    if last_archive is not None:
        log.info(
            "Archive already past for today (%02d:00). Next archival: tomorrow %02d:00.",
            ARCHIVE_HOUR, ARCHIVE_HOUR,
        )

    # Tiering: bắt đầu chạy ngay sau khởi động (first run)
    last_tiering: Optional[datetime] = None

    # 3. Main loop
    while True:
        time.sleep(CHECK_INTERVAL)

        log.info("--- Watchdog tick @ %s ---", datetime.now().strftime("%H:%M:%S"))
        watchdog_tick()

        # Tiering check (Fluss HOT → Paimon WARM)
        if should_run_tiering(last_tiering):
            log.info("--- Tiering triggered @ %s ---",
                     datetime.now().strftime("%Y-%m-%d %H:%M"))
            if run_tiering_job():
                last_tiering = datetime.now()
            else:
                log.warning("Tiering failed — will retry next check interval.")

        # Archival check (Paimon WARM → Iceberg COLD)
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
