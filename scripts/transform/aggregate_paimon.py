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

    # Force parallelism=1 for ALL table operations.
    # Without this, Paimon source inherits bucket count ('bucket'='4') → uses 4 task slots.
    # 4 streaming jobs × 4 slots = 16 slots needed, starving SQL Gateway queries.
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("table.exec.resource.default-parallelism", "1")

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

    # 1b. Ensure Paimon tables exist (idempotent after hard reset)
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS `paimon`.`security`")
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
            'merge-engine' = 'deduplicate',
            'changelog-producer' = 'input',
            'bucket' = '4'
        )
    """)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`daily_incident_stats` (
            stat_date         DATE,
            location          STRING,
            total_incidents   BIGINT,
            violent_incidents BIGINT,
            avg_risk_score    DOUBLE,
            max_risk_score    DOUBLE,
            PRIMARY KEY (stat_date, location) NOT ENFORCED
        ) WITH (
            'merge-engine' = 'deduplicate',
            'bucket' = '4'
        )
    """)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`camera_stats` (
            stat_date         DATE,
            camera_id         STRING,
            total_incidents   BIGINT,
            violent_incidents BIGINT,
            avg_risk_score    DOUBLE,
            avg_confidence    DOUBLE,
            PRIMARY KEY (stat_date, camera_id) NOT ENFORCED
        ) WITH (
            'merge-engine' = 'deduplicate',
            'bucket' = '4'
        )
    """)
    print("[INFO] Paimon aggregation tables ready.")

    # 2. Create a temporary source table with scan.parallelism=1 as a real table
    #    property (not a hint). This is needed because the Paimon connector assigns
    #    source parallelism = bucket count ('bucket'='4') and the OPTIONS() hint
    #    does NOT override it in this Paimon/Flink version combination.
    # Build the direct path to the Paimon table (connector approach, not catalog)
    # This allows setting scan.parallelism=1 as a real table property, overriding
    # the bucket-based source parallelism that the catalog connector assigns.
    table_path = f"{warehouse_path}/security.db/violence_incidents"
    print("[INFO] Creating temporary source table vi_stream (scan.parallelism=1)...")
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE vi_stream (
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
            'connector' = 'paimon',
            'path' = '{table_path}',
            's3.endpoint' = '{s3_endpoint}',
            's3.access-key' = '{s3_access_key}',
            's3.secret-key' = '{s3_secret_key}',
            's3.path.style.access' = 'true',
            'merge-engine' = 'deduplicate',
            'changelog-producer' = 'input',
            'scan.parallelism' = '1'
        )
    """)

    # 3. Use StatementSet to submit multiple INSERT jobs as one Flink job
    stmt_set: StatementSet = t_env.create_statement_set()

    # 4. Daily incident stats aggregation
    # Reads CDC changelog from vi_stream (p=1), groups by date + location
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
        FROM vi_stream
        GROUP BY CAST(`timestamp` AS DATE), location
    """)

    # 5. Camera stats aggregation
    # Reads CDC changelog from vi_stream (p=1), groups by date + camera
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
        FROM vi_stream
        GROUP BY CAST(`timestamp` AS DATE), camera_id
    """)

    # 5. Execute both INSERT statements as a single Flink job
    print("[INFO] Starting Flink job: Paimon Aggregation (daily_incident_stats + camera_stats)...")
    stmt_set.execute()


if __name__ == '__main__':
    main()
