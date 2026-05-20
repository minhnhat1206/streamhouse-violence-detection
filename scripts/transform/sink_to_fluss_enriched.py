"""
Flink Streaming Job: Kafka → Temporal Join → Fluss HOT (Enriched).

Streamhouse write-once pattern:
  Kafka hot-violence-alerts-valid
    → PROCTIME() temporal join with fluss.security.dim_camera
    → INSERT INTO fluss.security.hot_violence_alerts (with location enrichment)

Replaces sink_to_fluss.py (no enrichment) + dual-write sink_to_paimon_star.py.
Data moves to Paimon WARM via tier_fluss_to_paimon.py (every TIERING_INTERVAL_MINS).

Run inside Flink JobManager (submitted by pipeline_manager.py, --detached):
    flink run --detached -py /opt/flink/scripts/sink_to_fluss_enriched.py
"""
import os
from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.restart_strategy import RestartStrategies


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(20, 15000))
    env.enable_checkpointing(30000)
    t_env = StreamTableEnvironment.create(env)

    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("table.exec.resource.default-parallelism", "1")

    kafka_broker      = os.getenv("KAFKA_BROKER",      "kafka:9092")
    fluss_coordinator = os.getenv("FLUSS_COORDINATOR", "fluss-coordinator:9123")

    # ── 1. Register Fluss Catalog ────────────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE CATALOG fluss WITH (
            'type'              = 'fluss',
            'bootstrap.servers' = '{fluss_coordinator}'
        )
    """)
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS `fluss`.`security`")

    # ── 2. Ensure hot_violence_alerts has enriched schema ────────────────────────
    # Extended schema adds location/ward_id/district vs the old sink_to_fluss.py.
    # IF the old table exists (no location columns), DROP it first — HOT data is
    # ephemeral (tiered every 30 min), so data loss during upgrade is acceptable.
    t_env.execute_sql("DROP TABLE IF EXISTS `fluss`.`security`.`hot_violence_alerts`")
    t_env.execute_sql("""
        CREATE TABLE `fluss`.`security`.`hot_violence_alerts` (
            incident_id STRING,
            camera_id   STRING,
            `timestamp` TIMESTAMP(3),
            risk_score  DOUBLE,
            confidence  DOUBLE,
            is_violent  BOOLEAN,
            event_type  STRING,
            location    STRING,
            ward_id     STRING,
            district    STRING,
            PRIMARY KEY (incident_id) NOT ENFORCED
        ) WITH (
            'bucket.num' = '3'
        )
    """)
    print("[INFO] Fluss table hot_violence_alerts ready (enriched schema: +location/ward_id/district).")

    # dim_camera — versioned primary key table for temporal joins (schema unchanged)
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
    print("[INFO] Fluss table dim_camera ready.")

    # ── 3. Kafka Source with PROCTIME for temporal join ──────────────────────────
    # proc_time AS PROCTIME() is required for temporal join with Fluss primary key table.
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE kafka_valid_alerts (
            event_id    STRING,
            camera_id   STRING,
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
            'connector'                    = 'kafka',
            'topic'                        = 'hot-violence-alerts-valid',
            'properties.bootstrap.servers' = '{kafka_broker}',
            'properties.group.id'          = 'fluss-enriched-sink-group',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'json'
        )
    """)
    print("[INFO] Kafka source table ready.")

    # ── 4. Insert into Fluss with temporal join enrichment ───────────────────────
    # Temporal join: enriches each event with dim_camera state AT event processing time.
    # COALESCE ensures fallback to Kafka's raw location if dim_camera lookup misses.
    print("[INFO] Starting Flink job: Kafka → Temporal Join dim_camera → Fluss HOT (enriched)...")
    t_env.execute_sql("""
        INSERT INTO `fluss`.`security`.`hot_violence_alerts`
        SELECT
            a.event_id                                  AS incident_id,
            a.camera_id,
            a.row_time                                  AS `timestamp`,
            a.risk_score,
            a.confidence,
            a.is_violent,
            a.event_type,
            COALESCE(c.location, a.location, 'Unknown') AS location,
            COALESCE(c.ward_id,  'Unknown')             AS ward_id,
            COALESCE(c.district, 'Unknown')             AS district
        FROM kafka_valid_alerts AS a
        LEFT JOIN `fluss`.`security`.`dim_camera`
            FOR SYSTEM_TIME AS OF a.proc_time AS c
        ON a.camera_id = c.camera_id
        WHERE a.is_valid = true
    """)


if __name__ == "__main__":
    main()
