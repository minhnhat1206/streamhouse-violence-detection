"""
Submit Flink SQL Pipeline via SQL Gateway REST API.
Pipeline: Kafka (urban-safety-alerts) → Paimon (WARM) + Fluss (HOT)

Usage:
    python submit_pipeline.py
    python submit_pipeline.py --dry-run   # print SQL only
"""

import argparse
import sys
import time
import json
import urllib.request
import urllib.error
from typing import Optional

GATEWAY = "http://localhost:8083"

# ── SQL STATEMENTS ─────────────────────────────────────────────────────────────

SQL_CREATE_PAIMON_CATALOG = """
CREATE CATALOG paimon_cat WITH (
  'type' = 'paimon',
  'warehouse' = 's3://warehouse/paimon',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key' = 'minio',
  's3.secret-key' = 'mypassword',
  's3.path.style.access' = 'true'
)
"""

SQL_USE_PAIMON = "USE CATALOG paimon_cat"

SQL_CREATE_PAIMON_DB = "CREATE DATABASE IF NOT EXISTS security"

SQL_CREATE_PAIMON_TABLE = """
CREATE TABLE IF NOT EXISTS paimon_cat.security.violence_incidents (
  incident_id   STRING,
  camera_id     STRING,
  ts            TIMESTAMP(3),
  risk_score    DOUBLE,
  label         STRING,
  location      STRING,
  district      STRING,
  city          STRING,
  latitude      DOUBLE,
  longitude     DOUBLE,
  model_version STRING,
  frame_path    STRING,
  source        STRING,
  PRIMARY KEY (incident_id) NOT ENFORCED
) WITH (
  'merge-engine' = 'deduplicate',
  'bucket' = '4',
  'file.format' = 'parquet',
  'changelog-producer' = 'input'
)
"""

SQL_CREATE_FLUSS_CATALOG = """
CREATE CATALOG fluss_cat WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'fluss-coordinator:9123'
)
"""

SQL_CREATE_KAFKA_SOURCE = """
CREATE TEMPORARY TABLE kafka_alerts (
  incident_id   STRING,
  camera_id     STRING,
  `timestamp`   STRING,
  risk_score    DOUBLE,
  label         STRING,
  location      STRING,
  district      STRING,
  city          STRING,
  latitude      DOUBLE,
  longitude     DOUBLE,
  model_version STRING,
  frame_path    STRING,
  source        STRING,
  proc_time     AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'urban-safety-alerts',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-rtsp-pipeline-v1',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
)
"""

# Filter: only non-normal events go to Paimon WARM (avoid flooding with normal events)
SQL_INSERT_PAIMON = """
INSERT INTO paimon_cat.security.violence_incidents
SELECT
  incident_id,
  camera_id,
  TO_TIMESTAMP(`timestamp`),
  risk_score,
  label,
  location,
  district,
  city,
  latitude,
  longitude,
  model_version,
  frame_path,
  source
FROM kafka_alerts
WHERE label IN ('violence', 'crowd', 'anomaly')
  OR risk_score > 0.5
"""

# ── REST HELPERS ───────────────────────────────────────────────────────────────

def _post(url: str, body: dict) -> dict:
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _get(url: str) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        # Extract inner Flink error message
        if "Caused by:" in body:
            lines = [l for l in body.split("\\n") if "Caused by:" in l or "SqlParserException" in l or "SqlExecutionException" in l]
            raise RuntimeError("Flink error: " + " | ".join(lines[:3])) from None
        raise RuntimeError(f"HTTP {e.code}: {body[:300]}") from None


def create_session() -> str:
    resp = _post(f"{GATEWAY}/v1/sessions", {})
    sid = resp["sessionHandle"]
    print(f"  Session created: {sid}")
    return sid


def submit_statement(sid: str, sql: str) -> Optional[str]:
    sql_clean = sql.strip()
    print(f"  SQL: {sql_clean[:80]}{'...' if len(sql_clean) > 80 else ''}")
    resp = _post(f"{GATEWAY}/v1/sessions/{sid}/statements", {"statement": sql_clean})
    return resp.get("operationHandle")


def wait_for_statement(sid: str, op_id: str, timeout: int = 60) -> dict:
    deadline = time.time() + timeout
    token = 0
    while time.time() < deadline:
        resp = _get(f"{GATEWAY}/v1/sessions/{sid}/operations/{op_id}/result/{token}")
        status = resp.get("resultType", "NOT_READY")
        if status == "NOT_READY":
            time.sleep(2)
            continue
        if status in ("PAYLOAD", "EOS"):
            return resp
        if status == "ERROR":
            raise RuntimeError(f"Statement failed: {resp}")
        time.sleep(1)
    raise TimeoutError(f"Statement timed out after {timeout}s")


def run_sql(sid: str, sql: str, wait: bool = True, timeout: int = 60) -> Optional[dict]:
    op = submit_statement(sid, sql)
    if not op:
        return None
    if wait:
        result = wait_for_statement(sid, op, timeout)
        rows = result.get("results", {}).get("data", [])
        if rows:
            print("    -> %s" % rows[:3])
        return result
    return {"operationHandle": op}


# ── PIPELINE SUBMISSION ────────────────────────────────────────────────────────

def submit_pipeline(dry_run: bool = False):
    statements = [
        ("Create Paimon catalog", SQL_CREATE_PAIMON_CATALOG, True, 30),
        ("Create Paimon database", SQL_CREATE_PAIMON_DB, True, 30),
        ("Create Paimon table violence_incidents", SQL_CREATE_PAIMON_TABLE, True, 60),
        ("Create Kafka source table", SQL_CREATE_KAFKA_SOURCE, True, 30),
        ("Submit INSERT -> Paimon (streaming job)", SQL_INSERT_PAIMON, False, 0),
    ]

    if dry_run:
        print("\n=== DRY RUN — SQL statements ===")
        for name, sql, *_ in statements:
            print(f"\n-- {name} --\n{sql.strip()}")
        return

    print("\n=== Submitting Flink SQL Pipeline ===")
    try:
        sid = create_session()
    except Exception as e:
        print(f"[ERROR] Cannot reach Flink SQL Gateway at {GATEWAY}: {e}")
        sys.exit(1)

    for name, sql, wait, timeout in statements:
        print(f"\n[{name}]")
        try:
            if wait:
                run_sql(sid, sql.strip(), wait=True, timeout=timeout)
                print("  [OK] Done")
            else:
                result = run_sql(sid, sql.strip(), wait=False)
                op = result.get("operationHandle", "?")
                print("  [OK] Job submitted (op: %s)" % op)
                print("    -> Streaming job running in background")
        except Exception as e:
            msg = str(e)
            print("  [ERR] %s" % msg)
            if "already exists" in msg.lower():
                print("    (already exists -- continuing)")
            else:
                print("    Aborting pipeline submission.")
                break

    print("\n=== Pipeline submission complete ===")
    print("Check Flink Web UI: http://localhost:8081")


# ── MAIN ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit Flink SQL pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only, don't submit")
    parser.add_argument("--gateway", default=GATEWAY, help=f"Flink SQL Gateway URL (default: {GATEWAY})")
    args = parser.parse_args()

    GATEWAY = args.gateway
    submit_pipeline(dry_run=args.dry_run)
