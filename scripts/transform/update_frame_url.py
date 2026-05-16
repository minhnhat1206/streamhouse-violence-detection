"""
Flink Streaming Job: hot-violence-frames-uploaded → Paimon frame_url UPSERT.

Pipeline context
----------------
sink_to_paimon.py writes incidents with frame_url=NULL immediately when a
validated event arrives.  frame_extractor_sink.py runs as a sidecar: it reads
the same validated event, uploads the JPEG thumbnail to MinIO, then publishes
the enriched record (with frame_url set) to 'hot-violence-frames-uploaded'.

This job reads that enriched topic and re-inserts into Paimon.
Because the table uses merge-engine='deduplicate' with incident_id as PRIMARY KEY,
the latest record wins → the NULL frame_url row is replaced by the one with the
real MinIO URL.

Run inside Flink JobManager:
    flink run -py /opt/flink/scripts/update_frame_url.py -d
"""

import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # Paimon requires checkpointing to commit snapshot files
    env.enable_checkpointing(30_000)  # 30 s
    t_env = StreamTableEnvironment.create(env)

    kafka_broker   = os.getenv("KAFKA_BROKER",    "kafka:9092")
    s3_endpoint    = os.getenv("S3_ENDPOINT",     "http://minio:9000")
    s3_access_key  = os.getenv("MINIO_ROOT_USER", "minio")
    s3_secret_key  = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")
    warehouse_path = os.getenv("PAIMON_WAREHOUSE", "s3://warehouse/paimon")

    # ── 1. Paimon catalog ─────────────────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE CATALOG paimon WITH (
            'type'                = 'paimon',
            'warehouse'           = '{warehouse_path}',
            's3.endpoint'         = '{s3_endpoint}',
            's3.access-key'       = '{s3_access_key}',
            's3.secret-key'       = '{s3_secret_key}',
            's3.path.style.access'= 'true'
        )
    """)

    # ── 2. Kafka source: enriched incidents with frame_url ────────────────────
    # Schema mirrors what frame_extractor_sink.py publishes to this topic:
    #   all original fields from hot-violence-alerts-valid
    #   + frame_url (string, HTTP URL to MinIO JPEG)
    #   + frame_capture_ts (bigint, epoch ms)
    #   + thumbnail_b64 (string, base64 JPEG — promoted from metadata.thumbnail)
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE kafka_frames_uploaded (
            event_id          STRING,
            camera_id         STRING,
            `timestamp`       STRING,
            risk_score        DOUBLE,
            confidence        DOUBLE,
            is_violent        BOOLEAN,
            event_type        STRING,
            location          STRING,
            metadata          STRING,
            is_valid          BOOLEAN,
            frame_url         STRING,
            frame_capture_ts  BIGINT,
            thumbnail_b64     STRING,
            row_time AS TO_TIMESTAMP(
                SUBSTR(REPLACE(REPLACE(`timestamp`, 'T', ' '), 'Z', ''), 1, 23)
            ),
            WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = 'hot-violence-frames-uploaded',
            'properties.bootstrap.servers'  = '{kafka_broker}',
            'properties.group.id'           = 'paimon-frame-update-group',
            'scan.startup.mode'             = 'latest-offset',
            'format'                        = 'json'
        )
    """)

    # ── 3. UPSERT into Paimon ─────────────────────────────────────────────────
    # merge-engine='deduplicate' → latest record per incident_id wins.
    # This INSERT replaces the earlier row (frame_url=NULL) written by
    # sink_to_paimon.py with this row that has the real MinIO URL.
    print("[INFO] Starting Flink job: frame_url UPSERT → Paimon...", flush=True)
    t_env.execute_sql("""
        INSERT INTO paimon.security.violence_incidents
        SELECT
            event_id            AS incident_id,
            camera_id,
            row_time            AS `timestamp`,
            risk_score,
            confidence,
            is_violent,
            event_type,
            location,
            CAST(false AS BOOLEAN) AS is_deleted,
            frame_url,
            thumbnail_b64,
            frame_capture_ts
        FROM kafka_frames_uploaded
        WHERE is_valid = true
          AND frame_url IS NOT NULL
    """)


if __name__ == "__main__":
    main()
