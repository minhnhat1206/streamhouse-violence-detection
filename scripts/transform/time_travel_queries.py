"""
Demo Time Travel Queries for Paimon (Warm) and Iceberg (Cold).
Requires: Flink JobManager, MinIO, Hive Metastore running.
Run inside Flink JobManager:
    python /opt/flink/scripts/time_travel_queries.py
"""
import os
import sys
import time
from pyflink.table import EnvironmentSettings, TableEnvironment


def run_query(t_env, label, sql):
    """Execute a query with isolated error handling. Returns True if succeeded."""
    print(f"\n{'─' * 60}")
    print(f"[{label}]")
    print(f"SQL: {sql}")
    print("─" * 60)
    sys.stdout.flush()
    try:
        t_env.execute_sql(sql).print()
        return True
    except Exception as e:
        print(f"[SKIP] {e}")
        return False


def main():
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    # MinIO/S3 config
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("MINIO_ROOT_USER", "minio")
    s3_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")

    # Paimon config
    paimon_warehouse = os.getenv("PAIMON_WAREHOUSE", "s3://warehouse/paimon")

    # Iceberg config
    metastore_uri = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
    iceberg_warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/iceberg_warehouse")

    # 1. Register Paimon Catalog
    print("[INFO] Registering Paimon Catalog...")
    t_env.execute_sql(f"""
        CREATE CATALOG paimon WITH (
            'type' = 'paimon',
            'warehouse' = '{paimon_warehouse}',
            's3.endpoint' = '{s3_endpoint}',
            's3.access-key' = '{s3_access_key}',
            's3.secret-key' = '{s3_secret_key}',
            's3.path.style.access' = 'true'
        )
    """)

    # 2. Register Iceberg Catalog
    print("[INFO] Registering Iceberg Catalog...")
    t_env.execute_sql(f"""
        CREATE CATALOG iceberg WITH (
            'type' = 'iceberg',
            'catalog-type' = 'hive',
            'uri' = '{metastore_uri}',
            'warehouse' = '{iceberg_warehouse}',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = '{s3_endpoint}',
            's3.access-key-id' = '{s3_access_key}',
            's3.secret-access-key' = '{s3_secret_key}',
            's3.path-style-access' = 'true',
            'client.region' = 'us-east-1'
        )
    """)

    results = {}

    # ── PAIMON TIME TRAVEL ──────────────────────────────────────
    print("\n" + "=" * 60)
    print("=== PAIMON (WARM LAYER) TIME TRAVEL ===")
    print("=" * 60)

    # 1) List snapshots — find valid snapshot range
    results["snapshots"] = run_query(t_env, "PAIMON 1/4 — List Snapshots",
        "SELECT snapshot_id, schema_id, commit_kind, commit_time, total_record_count "
        "FROM paimon.security.`violence_incidents$snapshots` "
        "ORDER BY snapshot_id DESC LIMIT 10"
    )

    # 2) Time Travel by Snapshot ID — use MIN+5 to avoid TTL race condition
    snapshot_id = None
    try:
        result = t_env.execute_sql(
            "SELECT MIN(snapshot_id) + 5 FROM paimon.security.`violence_incidents$snapshots`"
        )
        with result.collect() as rows:
            for row in rows:
                snapshot_id = row[0]
    except Exception:
        pass

    if snapshot_id:
        results["snapshot_id"] = run_query(t_env, f"PAIMON 2/4 — Travel to Snapshot #{snapshot_id}",
            f"SELECT incident_id, camera_id, risk_score, `timestamp` "
            f"FROM paimon.security.violence_incidents "
            f"/*+ OPTIONS('scan.snapshot-id' = '{snapshot_id}') */ "
            f"LIMIT 5"
        )
    else:
        print(f"\n[SKIP] PAIMON 2/4 — No snapshots available, cannot travel by snapshot-id")
        results["snapshot_id"] = False

    # 3) Time Travel by Timestamp — 5 minutes ago
    five_min_ago = int(time.time() * 1000) - 300_000
    results["timestamp"] = run_query(t_env, f"PAIMON 3/4 — Travel to 5 min ago (ts={five_min_ago})",
        f"SELECT incident_id, camera_id, risk_score, `timestamp` "
        f"FROM paimon.security.violence_incidents "
        f"/*+ OPTIONS('scan.timestamp-millis' = '{five_min_ago}') */ "
        f"LIMIT 5"
    )

    # 4) Audit Log — CDC changelog
    results["audit"] = run_query(t_env, "PAIMON 4/4 — Audit Log (CDC Changelog)",
        "SELECT rowkind, incident_id, camera_id, risk_score "
        "FROM paimon.security.`violence_incidents$audit_log` "
        "LIMIT 10"
    )

    # ── ICEBERG TIME TRAVEL ─────────────────────────────────────
    print("\n" + "=" * 60)
    print("=== ICEBERG (COLD LAYER) TIME TRAVEL ===")
    print("=" * 60)

    current_millis = int(time.time() * 1000)
    results["iceberg"] = run_query(t_env, f"ICEBERG 1/1 — Travel to now (ts={current_millis})",
        f"SELECT incident_id, camera_id, risk_score "
        f"FROM iceberg.security.historical_violence_incidents "
        f"/*+ OPTIONS('as-of-timestamp' = '{current_millis}') */ "
        f"LIMIT 5"
    )
    if not results["iceberg"]:
        print("(Expected if archive_to_iceberg.py has not been run yet)")

    # ── SUMMARY ─────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("=== SUMMARY ===")
    print("=" * 60)
    for name, ok in results.items():
        status = "PASS" if ok else "SKIP"
        print(f"  [{status}] {name}")
    passed = sum(1 for v in results.values() if v)
    print(f"\nTotal: {passed}/{len(results)} queries succeeded.")


if __name__ == '__main__':
    main()
