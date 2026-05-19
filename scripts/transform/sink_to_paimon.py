"""
Flink Streaming Job: Kafka → Paimon (Warm Storage).
Reads validated events from Kafka topic 'hot-violence-alerts-valid'
and sinks to Paimon 'security.violence_incidents'.

Run inside Flink JobManager:
    flink run -py /opt/flink/scripts/sink_to_paimon.py
"""
import os
from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment


def main():
    # Setup Stream Table Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # Paimon requires checkpointing to commit snapshots
    env.enable_checkpointing(30000)  # 30s interval
    t_env = StreamTableEnvironment.create(env)

    # JARs (Kafka + Paimon + S3 connectors) are pre-loaded in /opt/flink/lib/
    # No need to set pipeline.jars — avoids classloading conflicts

    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
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

    # 1b. Ensure Paimon database + table exist (idempotent after hard reset)
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
    print("[INFO] Paimon table violence_incidents ready.")

    # 2. Define Kafka Source Table
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE kafka_valid_alerts (
            event_id STRING,
            camera_id STRING,
            `timestamp` STRING,
            risk_score DOUBLE,
            confidence DOUBLE,
            is_violent BOOLEAN,
            event_type STRING,
            location STRING,
            metadata STRING,
            is_valid BOOLEAN,
            row_time AS TO_TIMESTAMP(SUBSTR(REPLACE(REPLACE(`timestamp`, 'T', ' '), 'Z', ''), 1, 23)),
            WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'hot-violence-alerts-valid',
            'properties.bootstrap.servers' = '{kafka_broker}',
            'properties.group.id' = 'paimon-sink-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # 3. Insert into Paimon Warm Table
    # Map 'event_id' → 'incident_id', add 'is_deleted' = false for soft-delete support
    # Frame columns (frame_url, thumbnail_b64, frame_capture_ts) are enriched by frame_extractor_sink.py
    print("[INFO] Starting Flink job: Kafka to Paimon Warm Sink...")
    t_env.execute_sql("""
        INSERT INTO paimon.security.violence_incidents
        SELECT
            event_id AS incident_id,
            camera_id,
            row_time AS `timestamp`,
            risk_score,
            confidence,
            is_violent,
            event_type,
            location,
            CAST(false AS BOOLEAN) AS is_deleted,
            CAST(null AS STRING) AS frame_url,
            CAST(null AS STRING) AS thumbnail_b64,
            CAST(null AS BIGINT) AS frame_capture_ts
        FROM kafka_valid_alerts
        WHERE is_valid = true
    """)


if __name__ == '__main__':
    main()