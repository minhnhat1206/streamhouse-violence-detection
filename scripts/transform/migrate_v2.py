"""
Migration v1 → v2 (chạy MỘT LẦN, batch, sau init_star_schema_v2.py).

Việc:
  1. Backup: RENAME paimon.security.violence_incidents → violence_incidents_v1_backup
     (bảng cũ merge-engine='deduplicate' không đổi được → phải tạo bảng mới).
  2. Tạo lại violence_incidents theo schema v2 (partial-update, +incident_uid,
     +people_json, +people_count).
  3. Backfill: copy dữ liệu cũ sang, sinh incident_uid giả cho event lịch sử bằng
     đúng logic gaps-and-islands của view sessionized (gap 30s violent / 60s normal):
         incident_uid = 'legacy_' || camera_id || '_' || session_idx   (chỉ event violent)
  4. DROP fact_violence_incidents cũ (bảng fact trùng lặp, đã thay bằng
     fact_violence_incident do build_incident_facts.py đảm nhiệm).

Sau khi chạy: chạy build_incident_facts.py với BUILD_LOOKBACK_HOURS đủ lớn
(ví dụ 8760) để build fact cho toàn bộ dữ liệu lịch sử.

    flink run -py /opt/flink/scripts/migrate_v2.py
"""
import os

from pyflink.table import EnvironmentSettings, TableEnvironment


def _register_paimon(t_env: TableEnvironment) -> None:
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
    t_env.execute_sql("USE CATALOG paimon")
    t_env.execute_sql("USE security")


def _table_exists(t_env: TableEnvironment, name: str) -> bool:
    with t_env.execute_sql("SHOW TABLES").collect() as rs:
        return any(r[0] == name for r in rs)


def _column_exists(t_env: TableEnvironment, table: str, column: str) -> bool:
    try:
        with t_env.execute_sql(f"DESCRIBE `{table}`").collect() as rs:
            return any(r[0] == column for r in rs)
    except Exception:
        return False


def main() -> None:
    settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(settings)
    # Tránh "Insufficient number of network buffers" trên TaskManager nhỏ
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("table.exec.resource.default-parallelism", "1")
    _register_paimon(t_env)

    # ── 0. Idempotency guard ─────────────────────────────────────────────────
    if _column_exists(t_env, "violence_incidents", "incident_uid"):
        print("[INFO] violence_incidents already has incident_uid — migration already done.")
    else:
        # ── 1. Backup old table ──────────────────────────────────────────────
        if _table_exists(t_env, "violence_incidents_v1_backup"):
            print("[WARN] violence_incidents_v1_backup already exists — keeping it.")
        else:
            print("[INFO] Renaming violence_incidents → violence_incidents_v1_backup...")
            t_env.execute_sql(
                "ALTER TABLE `violence_incidents` RENAME TO `violence_incidents_v1_backup`"
            )

        # ── 2. Create v2 table ───────────────────────────────────────────────
        print("[INFO] Creating violence_incidents (v2, partial-update)...")
        t_env.execute_sql("""
            CREATE TABLE IF NOT EXISTS `violence_incidents` (
                incident_id      STRING,
                incident_uid     STRING,
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
                people_json      STRING,
                people_count     INT,
                PRIMARY KEY (incident_id) NOT ENFORCED
            ) WITH (
                'merge-engine'       = 'partial-update',
                'changelog-producer' = 'lookup',
                'bucket'             = '4'
            )
        """)

        # ── 3. Backfill với legacy incident_uid (gaps-and-islands 30s/60s) ───
        # Cùng logic với view iceberg.default.violence_incidents_sessionized cũ.
        print("[INFO] Backfilling data with legacy incident_uid...")
        t_env.execute_sql("""
            INSERT INTO `violence_incidents`
            WITH flagged AS (
                SELECT *,
                    CASE
                        WHEN is_violent = true AND (
                            LAG(`timestamp`) OVER (
                                PARTITION BY camera_id, is_violent ORDER BY `timestamp`) IS NULL
                            OR `timestamp` > LAG(`timestamp`) OVER (
                                PARTITION BY camera_id, is_violent ORDER BY `timestamp`)
                                + INTERVAL '30' SECOND
                        ) THEN 1
                        WHEN is_violent = false AND (
                            LAG(`timestamp`) OVER (
                                PARTITION BY camera_id, is_violent ORDER BY `timestamp`) IS NULL
                            OR `timestamp` > LAG(`timestamp`) OVER (
                                PARTITION BY camera_id, is_violent ORDER BY `timestamp`)
                                + INTERVAL '60' SECOND
                        ) THEN 1
                        ELSE 0
                    END AS is_new_session
                FROM `violence_incidents_v1_backup`
            ),
            sessioned AS (
                SELECT *,
                    SUM(is_new_session) OVER (
                        PARTITION BY camera_id, is_violent ORDER BY `timestamp`
                    ) AS session_idx
                FROM flagged
            )
            SELECT
                incident_id,
                CASE WHEN is_violent = true
                     THEN CONCAT('legacy_', camera_id, '_', CAST(session_idx AS STRING))
                     ELSE CAST(NULL AS STRING)
                END                       AS incident_uid,
                camera_id,
                `timestamp`,
                risk_score,
                confidence,
                is_violent,
                event_type,
                location,
                is_deleted,
                frame_url,
                thumbnail_b64,
                frame_capture_ts,
                CAST(NULL AS STRING)      AS people_json,
                CAST(NULL AS INT)         AS people_count
            FROM sessioned
        """).wait()
        print("[INFO] Backfill complete.")

    # ── 4. Drop bảng fact trùng lặp cũ ───────────────────────────────────────
    if _table_exists(t_env, "fact_violence_incidents"):
        print("[INFO] Dropping obsolete fact_violence_incidents (replaced by "
              "fact_violence_incident, grain = 1 vụ)...")
        t_env.execute_sql("DROP TABLE `fact_violence_incidents`")

    print("[SUCCESS] Migration v2 complete. Next: run build_incident_facts.py "
          "with BUILD_LOOKBACK_HOURS=8760 to build facts for historical data.")


if __name__ == "__main__":
    main()
