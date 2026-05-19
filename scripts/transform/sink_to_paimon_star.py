"""
Flink Streaming Job: Kafka → Temporal Join → Paimon (Star Schema).

Implements Task 1.3 của Streamhouse Completion Plan:
  1. Đọc validated events từ Kafka: hot-violence-alerts-valid
  2. Temporal join với Fluss dim_camera (FOR SYSTEM_TIME AS OF proc_time)
     → Enrich với location, ward_id, district tại thời điểm event
  3. Ghi song song vào 2 Paimon tables:
     - fact_violence_incidents  (star schema, enriched)
     - violence_incidents       (backward compat — aggregate_paimon.py dùng bảng này)

Run inside Flink JobManager:
    flink run -py /opt/flink/scripts/sink_to_paimon_star.py
"""
import os
from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.statement_set import StatementSet


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30000)  # Paimon cần checkpointing để commit
    t_env = StreamTableEnvironment.create(env)

    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("table.exec.resource.default-parallelism", "1")

    kafka_broker   = os.getenv("KAFKA_BROKER",        "kafka:9092")
    s3_endpoint    = os.getenv("S3_ENDPOINT",         "http://minio:9000")
    s3_access_key  = os.getenv("MINIO_ROOT_USER",     "minio")
    s3_secret_key  = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")
    warehouse_path = os.getenv("PAIMON_WAREHOUSE",    "s3://warehouse/paimon")
    fluss_coord    = os.getenv("FLUSS_COORDINATOR",   "fluss-coordinator:9123")

    # ── 1. Register Fluss Catalog (dim_camera temporal table) ────────────────
    t_env.execute_sql(f"""
        CREATE CATALOG fluss WITH (
            'type'              = 'fluss',
            'bootstrap.servers' = '{fluss_coord}'
        )
    """)
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS `fluss`.`security`")

    # Ensure dim_camera exists (idempotent)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `fluss`.`security`.`dim_camera` (
            camera_id   STRING,
            location    STRING,
            ward_id     STRING,
            district    STRING,
            latitude    DOUBLE,
            longitude   DOUBLE,
            status      STRING,
            updated_at  TIMESTAMP(3),
            PRIMARY KEY (camera_id) NOT ENFORCED
        ) WITH (
            'bucket.num' = '3'
        )
    """)

    # ── 2. Register Paimon Catalog ────────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE CATALOG paimon WITH (
            'type'                 = 'paimon',
            'warehouse'            = '{warehouse_path}',
            's3.endpoint'          = '{s3_endpoint}',
            's3.access-key'        = '{s3_access_key}',
            's3.secret-key'        = '{s3_secret_key}',
            's3.path.style.access' = 'true'
        )
    """)
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS `paimon`.`security`")

    # Ensure fact_violence_incidents exists (star schema)
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

    # Ensure violence_incidents exists (backward compat for aggregate_paimon.py)
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
    print("[INFO] Paimon tables ready.")

    # ── 3. Kafka Source with PROCTIME for temporal join ───────────────────────
    # proc_time AS PROCTIME() là processing time attribute bắt buộc cho temporal join
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE kafka_valid_alerts (
            event_id   STRING,
            camera_id  STRING,
            `timestamp` STRING,
            risk_score  DOUBLE,
            confidence  DOUBLE,
            is_violent  BOOLEAN,
            event_type  STRING,
            location    STRING,
            metadata    STRING,
            is_valid    BOOLEAN,
            row_time  AS TO_TIMESTAMP(SUBSTR(REPLACE(REPLACE(`timestamp`, 'T', ' '), 'Z', ''), 1, 23)),
            proc_time AS PROCTIME(),
            WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = 'hot-violence-alerts-valid',
            'properties.bootstrap.servers'  = '{kafka_broker}',
            'properties.group.id'           = 'paimon-star-sink-group',
            'scan.startup.mode'             = 'latest-offset',
            'format'                        = 'json'
        )
    """)
    print("[INFO] Kafka source table ready.")

    # ── 4. Submit both INSERTs as one Flink job via StatementSet ─────────────
    stmt_set: StatementSet = t_env.create_statement_set()

    # INSERT into fact_violence_incidents with temporal join enrichment
    stmt_set.add_insert_sql("""
        INSERT INTO `paimon`.`security`.`fact_violence_incidents`
        SELECT
            a.event_id                                           AS incident_id,
            a.camera_id,
            a.row_time                                           AS `timestamp`,
            CAST(a.row_time AS DATE)                             AS date_id,
            a.risk_score,
            a.confidence,
            a.is_violent,
            a.event_type,
            COALESCE(c.location, a.location, 'Unknown')          AS location,
            COALESCE(c.ward_id,  'Unknown')                      AS ward_id,
            COALESCE(c.district, 'Unknown')                      AS district,
            CAST(NULL AS STRING)                                 AS frame_url
        FROM kafka_valid_alerts AS a
        LEFT JOIN `fluss`.`security`.`dim_camera`
            FOR SYSTEM_TIME AS OF a.proc_time AS c
        ON a.camera_id = c.camera_id
        WHERE a.is_valid = true
    """)

    # INSERT into violence_incidents (backward compat — keeps aggregate_paimon working)
    stmt_set.add_insert_sql("""
        INSERT INTO `paimon`.`security`.`violence_incidents`
        SELECT
            a.event_id                       AS incident_id,
            a.camera_id,
            a.row_time                       AS `timestamp`,
            a.risk_score,
            a.confidence,
            a.is_violent,
            a.event_type,
            COALESCE(c.location, a.location, 'Unknown') AS location,
            CAST(false AS BOOLEAN)           AS is_deleted,
            CAST(NULL AS STRING)             AS frame_url,
            CAST(NULL AS STRING)             AS thumbnail_b64,
            CAST(NULL AS BIGINT)             AS frame_capture_ts
        FROM kafka_valid_alerts AS a
        LEFT JOIN `fluss`.`security`.`dim_camera`
            FOR SYSTEM_TIME AS OF a.proc_time AS c
        ON a.camera_id = c.camera_id
        WHERE a.is_valid = true
    """)

    print("[INFO] Starting Flink job: Kafka → Temporal Join → Paimon Star Schema...")
    stmt_set.execute()


if __name__ == "__main__":
    main()
