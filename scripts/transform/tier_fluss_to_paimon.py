"""
Streamhouse Tiering Job: Fluss HOT → Paimon WARM.
=================================================
Moves aged data from Fluss HOT to Paimon WARM (true Streamhouse MOVE, not copy).

Phase 1 — INSERT (mandatory):
  Reads records older than TIERING_HOURS from Fluss hot_violence_alerts.
  Writes to both Paimon tables (streaming job + checkpoint wait).
  Submits as a streaming job, waits TIERING_PHASE1_WAIT_SECS for Paimon
  checkpoints to commit (4× checkpoint_interval=30s → default 120s), then
  cancels the streaming job.

Phase 2 — DELETE (best-effort):
  Deletes aged records from Fluss HOT via streaming DELETE.
  If Fluss DELETE is unsupported by the connector version, logs a warning
  and exits cleanly (Phase 1 data is still committed to Paimon WARM).
  Aged records in Fluss will not be returned by HOT queries anyway, because
  the chatbot routes queries by time_period: <1 hour → Fluss, ≥1 hour → Paimon.

Run by pipeline_manager.py every TIERING_INTERVAL_MINS minutes (blocking).
    flink run -py /opt/flink/scripts/tier_fluss_to_paimon.py

Environment variables:
  TIERING_HOURS              - Minimum age before tiering (default: 1)
  TIERING_PHASE1_WAIT_SECS   - Seconds to wait for Paimon checkpoints (default: 120)
  TIERING_PHASE2_WAIT_SECS   - Timeout for Fluss DELETE phase (default: 60)
  FLUSS_COORDINATOR          - Fluss coordinator address (default: fluss-coordinator:9123)
  S3_ENDPOINT / MINIO_ROOT_USER / MINIO_ROOT_PASSWORD / PAIMON_WAREHOUSE
"""
import os
import time
from datetime import datetime, timezone, timedelta

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.statement_set import StatementSet

TIERING_HOURS    = int(os.getenv("TIERING_HOURS",            "1"))
PHASE1_WAIT_SECS = int(os.getenv("TIERING_PHASE1_WAIT_SECS", "120"))
PHASE2_WAIT_SECS = int(os.getenv("TIERING_PHASE2_WAIT_SECS",  "60"))


# ── Helpers ──────────────────────────────────────────────────────────────────────

def _make_streaming_env() -> StreamTableEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30000)
    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("table.exec.resource.default-parallelism", "1")
    # Mark Fluss source as idle after 15s with no new data matching filter.
    # This advances watermarks and lets Paimon flush partial windows.
    t_env.get_config().set("table.exec.source.idle-timeout", "15s")
    return t_env


def _register_fluss(t_env: StreamTableEnvironment) -> None:
    fluss_coord = os.getenv("FLUSS_COORDINATOR", "fluss-coordinator:9123")
    t_env.execute_sql(f"""
        CREATE CATALOG fluss WITH (
            'type'              = 'fluss',
            'bootstrap.servers' = '{fluss_coord}'
        )
    """)
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS `fluss`.`security`")


def _register_paimon(t_env: StreamTableEnvironment) -> None:
    s3_endpoint   = os.getenv("S3_ENDPOINT",         "http://minio:9000")
    s3_access_key = os.getenv("MINIO_ROOT_USER",     "minio")
    s3_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")
    warehouse     = os.getenv("PAIMON_WAREHOUSE",    "s3://warehouse/paimon")
    t_env.execute_sql(f"""
        CREATE CATALOG paimon WITH (
            'type'                 = 'paimon',
            'warehouse'            = '{warehouse}',
            's3.endpoint'          = '{s3_endpoint}',
            's3.access-key'        = '{s3_access_key}',
            's3.secret-key'        = '{s3_secret_key}',
            's3.path.style.access' = 'true'
        )
    """)
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS `paimon`.`security`")


def _ensure_paimon_tables(t_env: StreamTableEnvironment) -> None:
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`fact_violence_incidents` (
            incident_id STRING,
            camera_id   STRING,
            `timestamp` TIMESTAMP(3),
            date_id     DATE,
            risk_score  DOUBLE,
            confidence  DOUBLE,
            is_violent  BOOLEAN,
            event_type  STRING,
            location    STRING,
            ward_id     STRING,
            district    STRING,
            frame_url   STRING,
            PRIMARY KEY (incident_id) NOT ENFORCED
        ) WITH (
            'merge-engine'       = 'deduplicate',
            'changelog-producer' = 'input',
            'bucket'             = '4'
        )
    """)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`violence_incidents` (
            incident_id      STRING,
            camera_id        STRING,
            `timestamp`      TIMESTAMP(3),
            risk_score       DOUBLE,
            confidence       DOUBLE,
            is_violent       BOOLEAN,
            event_type       STRING,
            location         STRING,
            is_deleted       BOOLEAN,
            frame_url        STRING,
            thumbnail_b64    STRING,
            frame_capture_ts BIGINT,
            PRIMARY KEY (incident_id) NOT ENFORCED
        ) WITH (
            'merge-engine'       = 'deduplicate',
            'changelog-producer' = 'input',
            'bucket'             = '4'
        )
    """)


# ── Phase 1: Fluss → Paimon ───────────────────────────────────────────────────────

def phase1_fluss_to_paimon(cutoff_str: str) -> bool:
    """
    INSERT aged data from Fluss HOT → both Paimon WARM tables.
    Uses streaming mode: submits job, waits for checkpoints, then cancels.
    Returns True if job was submitted and checkpoint wait completed.
    """
    print(f"[INFO] Phase 1: INSERT Fluss aged data (ts < {cutoff_str}) → Paimon WARM...")

    t_env = _make_streaming_env()
    _register_fluss(t_env)
    _register_paimon(t_env)
    _ensure_paimon_tables(t_env)

    stmt: StatementSet = t_env.create_statement_set()

    # Insert into star-schema fact table
    stmt.add_insert_sql(f"""
        INSERT INTO `paimon`.`security`.`fact_violence_incidents`
        SELECT
            incident_id,
            camera_id,
            `timestamp`,
            CAST(`timestamp` AS DATE)   AS date_id,
            risk_score,
            confidence,
            is_violent,
            event_type,
            location,
            ward_id,
            district,
            CAST(NULL AS STRING)        AS frame_url
        FROM `fluss`.`security`.`hot_violence_alerts`
        WHERE `timestamp` < TO_TIMESTAMP('{cutoff_str}')
    """)

    # Insert into backward-compat table (aggregate_paimon.py reads this)
    stmt.add_insert_sql(f"""
        INSERT INTO `paimon`.`security`.`violence_incidents`
        SELECT
            incident_id,
            camera_id,
            `timestamp`,
            risk_score,
            confidence,
            is_violent,
            event_type,
            location,
            CAST(false AS BOOLEAN)      AS is_deleted,
            CAST(NULL AS STRING)        AS frame_url,
            CAST(NULL AS STRING)        AS thumbnail_b64,
            CAST(NULL AS BIGINT)        AS frame_capture_ts
        FROM `fluss`.`security`.`hot_violence_alerts`
        WHERE `timestamp` < TO_TIMESTAMP('{cutoff_str}')
    """)

    result = stmt.execute()

    job_client = None
    try:
        job_client = result.get_job_client()
        if job_client:
            print(f"[INFO] Phase 1 job submitted: {job_client.get_job_id()}")
    except Exception as e:
        print(f"[WARN] Could not retrieve Phase 1 job client: {e}")

    # Wait for Paimon to commit via checkpoints.
    # Checkpoint interval = 30s → PHASE1_WAIT_SECS=120s gives ~4 commits.
    print(f"[INFO] Waiting {PHASE1_WAIT_SECS}s for Paimon checkpoint commits "
          f"({PHASE1_WAIT_SECS // 30} × 30s intervals)...")
    time.sleep(PHASE1_WAIT_SECS)

    # Cancel the streaming job — data written before cancellation is committed.
    if job_client:
        try:
            job_client.cancel()
            print("[INFO] Phase 1 job cancelled. Aged data committed to Paimon WARM.")
        except Exception as e:
            print(f"[WARN] Could not cancel Phase 1 job: {e} (job may already be done)")

    return True


# ── Phase 2: DELETE from Fluss ────────────────────────────────────────────────────

def phase2_delete_from_fluss(cutoff_str: str) -> bool:
    """
    DELETE aged records from Fluss HOT (best-effort).
    If the Fluss connector does not support streaming DELETE, logs a warning
    and returns False — callers treat this as non-fatal.
    """
    print(f"[INFO] Phase 2: DELETE from Fluss HOT where timestamp < {cutoff_str}...")

    t_env = _make_streaming_env()
    _register_fluss(t_env)

    try:
        result = t_env.execute_sql(f"""
            DELETE FROM `fluss`.`security`.`hot_violence_alerts`
            WHERE `timestamp` < TO_TIMESTAMP('{cutoff_str}')
        """)

        job_client = None
        try:
            job_client = result.get_job_client()
        except Exception:
            pass

        print(f"[INFO] Phase 2 DELETE job running. Waiting up to {PHASE2_WAIT_SECS}s...")
        try:
            result.wait(timeout_ms=PHASE2_WAIT_SECS * 1000)
            print("[INFO] Phase 2 DELETE completed. Aged records removed from Fluss HOT.")
            return True
        except Exception as e:
            print(f"[WARN] Phase 2 DELETE timed out ({e}). Cancelling DELETE job...")
            if job_client:
                try:
                    job_client.cancel()
                except Exception:
                    pass
            print("[WARN] Aged records may still exist in Fluss HOT. "
                  "They will not be served by HOT queries (chatbot routes >1h → Paimon).")
            return False

    except Exception as e:
        print(f"[WARN] Phase 2 DELETE failed: {e}")
        print("[WARN] Fluss streaming DELETE may be unsupported in this connector version.")
        print("[WARN] Data is still tiered to Paimon WARM (Phase 1 succeeded). "
              "Aged Fluss records will be ignored by time-bounded query routing.")
        return False


# ── Main ─────────────────────────────────────────────────────────────────────────

def main() -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=TIERING_HOURS)
    cutoff_str = cutoff.strftime('%Y-%m-%d %H:%M:%S')

    print("=" * 60)
    print(f"Streamhouse Tiering Job")
    print(f"  Cutoff : {cutoff_str} UTC  (data older than {TIERING_HOURS}h)")
    print(f"  Phase1 wait: {PHASE1_WAIT_SECS}s  |  Phase2 timeout: {PHASE2_WAIT_SECS}s")
    print("=" * 60)

    # Phase 1 is mandatory — abort if it fails
    if not phase1_fluss_to_paimon(cutoff_str):
        print("[ERROR] Phase 1 failed. Tiering aborted.")
        raise SystemExit(1)

    # Phase 2 is best-effort — failure does not abort the job
    phase2_ok = phase2_delete_from_fluss(cutoff_str)

    result_tag = "COMPLETE" if phase2_ok else "PARTIAL (Phase 1 OK, Phase 2 skipped)"
    print("=" * 60)
    print(f"Tiering {result_tag}  |  cutoff={cutoff_str}")
    print("=" * 60)


if __name__ == "__main__":
    main()
