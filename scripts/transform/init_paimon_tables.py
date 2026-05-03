"""
Initialize Paimon Catalog and Tables (Warm Storage Layer).
Creates: paimon.security.violence_incidents (deduplicate merge engine)

Run inside Flink JobManager:
    python /opt/flink/scripts/init_paimon_tables.py
"""
import os
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    # Paimon + S3 JARs are pre-loaded in /opt/flink/lib/ (system classpath)

    # MinIO/S3 config
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("MINIO_ROOT_USER", "minio")
    s3_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")
    warehouse_path = os.getenv("PAIMON_WAREHOUSE", "s3://warehouse/paimon")

    # 1. Create Paimon Catalog (filesystem-based with S3/MinIO)
    print("[INFO] Creating Paimon Catalog...")
    t_env.execute_sql(f"""
        CREATE CATALOG paimon WITH (
            'type' = 'paimon',
            'warehouse' = '{warehouse_path}',
            's3.endpoint' = '{s3_endpoint}',
            's3.access-key' = '{s3_access_key}',
            's3.secret-key' = '{s3_secret_key}',
            's3.path.style.access' = 'true'
        )
    """)

    t_env.execute_sql("USE CATALOG paimon")

    # 2. Create Database
    print("[INFO] Creating Database 'security'...")
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS security")
    t_env.execute_sql("USE security")

    # 3. Create validated incidents table (deduplicate merge engine — latest row wins per PK)
    print("[INFO] Creating Table 'violence_incidents'...")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS violence_incidents (
            incident_id STRING,
            camera_id STRING,
            `timestamp` TIMESTAMP(3),
            risk_score DOUBLE,
            confidence DOUBLE,
            is_violent BOOLEAN,
            event_type STRING,
            location STRING,
            is_deleted BOOLEAN,
            frame_url STRING,
            thumbnail_b64 STRING,
            frame_capture_ts BIGINT,
            PRIMARY KEY (incident_id) NOT ENFORCED
        ) WITH (
            'merge-engine' = 'deduplicate',
            'changelog-producer' = 'input',
            'snapshot.time-retained' = '7d',
            'snapshot.num-retained.min' = '5',
            'snapshot.num-retained.max' = '50'
        )
    """)

    # 4. Create daily incident stats table (deduplicate — latest aggregation per date+location)
    print("[INFO] Creating Table 'daily_incident_stats'...")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS daily_incident_stats (
            stat_date DATE,
            location STRING,
            total_incidents BIGINT,
            violent_incidents BIGINT,
            avg_risk_score DOUBLE,
            max_risk_score DOUBLE,
            PRIMARY KEY (stat_date, location) NOT ENFORCED
        ) WITH (
            'merge-engine' = 'deduplicate',
            'changelog-producer' = 'input',
            'snapshot.time-retained' = '30d',
            'snapshot.num-retained.min' = '5',
            'snapshot.num-retained.max' = '50'
        )
    """)

    # 5. Create camera stats table (deduplicate — latest aggregation per date+camera)
    print("[INFO] Creating Table 'camera_stats'...")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS camera_stats (
            stat_date DATE,
            camera_id STRING,
            total_incidents BIGINT,
            violent_incidents BIGINT,
            avg_risk_score DOUBLE,
            avg_confidence DOUBLE,
            PRIMARY KEY (stat_date, camera_id) NOT ENFORCED
        ) WITH (
            'merge-engine' = 'deduplicate',
            'changelog-producer' = 'input',
            'snapshot.time-retained' = '30d',
            'snapshot.num-retained.min' = '5',
            'snapshot.num-retained.max' = '50'
        )
    """)

    print("[SUCCESS] Paimon Catalog and all tables initialized successfully.")
    print("  - violence_incidents (deduplicate)")
    print("  - daily_incident_stats (deduplicate)")
    print("  - camera_stats (deduplicate)")


if __name__ == '__main__':
    main()
