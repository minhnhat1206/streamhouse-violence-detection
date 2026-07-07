"""
Flink Batch Job: Paimon → Iceberg Archival (Cold Storage) v2.

Archives 2 bảng (older than ARCHIVE_INTERVAL_DAYS):
  1. fact_violence_incident → iceberg.security.historical_incident_facts
     (grain = 1 VỤ, GIỮ frame_url/duration/people_count — bản cũ mất frame_url)
  2. violence_incidents (event grain) → iceberg.security.historical_violence_incidents
     (giữ nguyên schema cũ cho tương thích)

Uses NOT EXISTS to avoid duplicates on re-runs.

Designed to run on a daily schedule (pipeline_manager, ARCHIVE_HOUR).
Run inside Flink JobManager:
    flink run -py /opt/flink/scripts/archive_to_iceberg.py
"""
import os
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    # Shared S3/MinIO config
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("MINIO_ROOT_USER", "minio")
    s3_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")

    # Paimon config
    paimon_warehouse = os.getenv("PAIMON_WAREHOUSE", "s3://warehouse/paimon")

    # Iceberg config
    metastore_uri = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
    iceberg_warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/iceberg_warehouse")

    # 1. Register Paimon Catalog (source)
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

    # 2. Register Iceberg Catalog (target)
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

    # 3. Ensure Iceberg target table exists (idempotent after hard reset)
    print("[INFO] Ensuring Iceberg target table exists...")
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS `iceberg`.`security`")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `iceberg`.`security`.`historical_violence_incidents` (
            incident_id  STRING,
            camera_id    STRING,
            `timestamp`  TIMESTAMP(6),
            risk_score   DOUBLE,
            confidence   DOUBLE,
            is_violent   BOOLEAN,
            event_type   STRING,
            location     STRING,
            incident_date DATE
        ) PARTITIONED BY (incident_date)
        WITH (
            'format-version' = '2'
        )
    """)

    # 3b. Fact archive table (grain = 1 vụ, GIỮ frame_url)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `iceberg`.`security`.`historical_incident_facts` (
            incident_id    STRING,
            camera_id      STRING,
            date_id        DATE,
            time_id        INT,
            event_type_id  INT,
            start_ts       TIMESTAMP(6),
            end_ts         TIMESTAMP(6),
            duration_sec   INT,
            event_count    BIGINT,
            max_risk_score DOUBLE,
            avg_confidence DOUBLE,
            is_violent     BOOLEAN,
            people_count   INT,
            frame_url      STRING
        ) PARTITIONED BY (date_id)
        WITH (
            'format-version' = '2'
        )
    """)

    # 4. Archive aged data: Paimon → Iceberg (deduplicated via NOT EXISTS)
    # NOTE: Production filter is '7' DAY. For fresh-reset testing, archive all data.
    archive_interval = os.getenv("ARCHIVE_INTERVAL_DAYS", "7")

    print(f"[INFO] Archiving incident FACTS older than {archive_interval} days...")
    t_env.execute_sql(f"""
        INSERT INTO iceberg.security.historical_incident_facts
        SELECT
            f.incident_id,
            f.camera_id,
            f.date_id,
            f.time_id,
            f.event_type_id,
            CAST(f.start_ts AS TIMESTAMP(6)),
            CAST(f.end_ts AS TIMESTAMP(6)),
            f.duration_sec,
            f.event_count,
            f.max_risk_score,
            f.avg_confidence,
            f.is_violent,
            f.people_count,
            f.frame_url
        FROM paimon.security.fact_violence_incident f
        WHERE f.start_ts < LOCALTIMESTAMP - INTERVAL '{archive_interval}' DAY
          AND NOT EXISTS (
              SELECT 1 FROM iceberg.security.historical_incident_facts i
              WHERE i.incident_id = f.incident_id
          )
    """).wait()

    print(f"[INFO] Archiving raw EVENTS older than {archive_interval} days...")
    result = t_env.execute_sql(f"""
        INSERT INTO iceberg.security.historical_violence_incidents
        SELECT
            p.incident_id,
            p.camera_id,
            p.`timestamp`,
            p.risk_score,
            p.confidence,
            p.is_violent,
            p.event_type,
            p.location,
            CAST(p.`timestamp` AS DATE) AS incident_date
        FROM paimon.security.violence_incidents p
        WHERE p.`timestamp` < LOCALTIMESTAMP - INTERVAL '{archive_interval}' DAY
          AND p.is_deleted = false
          AND NOT EXISTS (
              SELECT 1 FROM iceberg.security.historical_violence_incidents i
              WHERE i.incident_id = p.incident_id
          )
    """)

    print("[SUCCESS] Archival job completed.")
    print(result.get_job_client().get_job_status())


if __name__ == '__main__':
    main()
