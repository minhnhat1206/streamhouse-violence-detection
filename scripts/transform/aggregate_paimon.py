"""
Flink Streaming Job: Paimon Aggregation (Warm Gold Layer).
Reads CDC changelog from Paimon 'violence_incidents' and produces:
  - daily_incident_stats: daily aggregation by location
  - camera_stats: daily aggregation by camera

Run inside Flink JobManager:
    flink run -py /opt/flink/scripts/aggregate_paimon.py
"""
import os
from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.statement_set import StatementSet


def main():
    # Setup Stream Table Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30000)  # 30s — Paimon requires checkpointing
    t_env = StreamTableEnvironment.create(env)

    # JARs pre-loaded in /opt/flink/lib/

    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("MINIO_ROOT_USER", "minio")
    s3_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")
    warehouse_path = os.getenv("PAIMON_WAREHOUSE", "s3://warehouse/paimon")

    # 1. Register Paimon Catalog
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

    # 2. Use StatementSet to submit multiple INSERT jobs as one Flink job
    stmt_set: StatementSet = t_env.create_statement_set()

    # 3. Daily incident stats aggregation
    # Reads CDC changelog from violence_incidents, groups by date + location
    # Includes frame capture statistics
    print("[INFO] Adding INSERT: daily_incident_stats aggregation...")
    stmt_set.add_insert_sql("""
        INSERT INTO paimon.security.daily_incident_stats
        SELECT
            CAST(`timestamp` AS DATE) AS stat_date,
            location,
            COUNT(*) AS total_incidents,
            COUNT(*) FILTER (WHERE is_violent = true) AS violent_incidents,
            AVG(risk_score) AS avg_risk_score,
            MAX(risk_score) AS max_risk_score
        FROM paimon.security.violence_incidents
        GROUP BY CAST(`timestamp` AS DATE), location
    """)

    # 4. Camera stats aggregation
    # Reads CDC changelog from violence_incidents, groups by date + camera
    # Includes frame capture statistics
    print("[INFO] Adding INSERT: camera_stats aggregation...")
    stmt_set.add_insert_sql("""
        INSERT INTO paimon.security.camera_stats
        SELECT
            CAST(`timestamp` AS DATE) AS stat_date,
            camera_id,
            COUNT(*) AS total_incidents,
            COUNT(*) FILTER (WHERE is_violent = true) AS violent_incidents,
            AVG(risk_score) AS avg_risk_score,
            AVG(confidence) AS avg_confidence
        FROM paimon.security.violence_incidents
        GROUP BY CAST(`timestamp` AS DATE), camera_id
    """)

    # 5. Execute both INSERT statements as a single Flink job
    print("[INFO] Starting Flink job: Paimon Aggregation (daily_incident_stats + camera_stats)...")
    stmt_set.execute()


if __name__ == '__main__':
    main()
