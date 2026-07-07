"""
Flink Streaming Job: Kafka → Temporal Join → Fluss HOT (Enriched) v2.

Streamhouse write-once pattern:
  Kafka hot-violence-alerts-valid
    → PROCTIME() temporal join with fluss.security.dim_camera
    → INSERT INTO fluss.security.hot_violence_alerts   (event grain, enriched)
    → INSERT INTO fluss.security.hot_violence_incidents (grain = 1 VỤ,
      GROUP BY incident_uid — upsert liên tục khi vụ còn diễn ra)

Đếm số vụ realtime = COUNT trên hot_violence_incidents (KHÔNG đếm events —
events phát 0.5s/lần khi violent nên 1 vụ ~20s = ~40 event).

DDL dùng CREATE IF NOT EXISTS — schema migration làm ở init_star_schema_v2.py,
job streaming restart thường xuyên không được phép DROP dữ liệu HOT.

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

    # ── 2. Ensure HOT tables exist (schema v2 — migration ở init_star_schema_v2) ─
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `fluss`.`security`.`hot_violence_alerts` (
            incident_id  STRING,
            incident_uid STRING,
            camera_id    STRING,
            `timestamp`  TIMESTAMP(3),
            risk_score   DOUBLE,
            confidence   DOUBLE,
            is_violent   BOOLEAN,
            event_type   STRING,
            location     STRING,
            ward_id      STRING,
            district     STRING,
            people_count INT,
            PRIMARY KEY (incident_id) NOT ENFORCED
        ) WITH (
            'bucket.num' = '3'
        )
    """)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `fluss`.`security`.`hot_violence_incidents` (
            incident_uid   STRING,
            camera_id      STRING,
            start_ts       TIMESTAMP(3),
            last_ts        TIMESTAMP(3),
            event_count    BIGINT,
            max_risk_score DOUBLE,
            avg_confidence DOUBLE,
            event_type     STRING,
            location       STRING,
            ward_id        STRING,
            district       STRING,
            people_count   INT,
            PRIMARY KEY (incident_uid) NOT ENFORCED
        ) WITH (
            'bucket.num' = '3'
        )
    """)
    print("[INFO] Fluss tables ready: hot_violence_alerts (v2) + hot_violence_incidents.")

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
            event_id     STRING,
            incident_uid STRING,
            camera_id    STRING,
            `timestamp`  STRING,
            risk_score   DOUBLE,
            confidence   DOUBLE,
            is_violent   BOOLEAN,
            event_type   STRING,
            location     STRING,
            people_count INT,
            metadata     STRING,
            is_valid     BOOLEAN,
            row_time  AS TO_TIMESTAMP(SUBSTR(REPLACE(REPLACE(`timestamp`, 'T', ' '), 'Z', ''), 1, 23)),
            proc_time AS PROCTIME(),
            WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'hot-violence-alerts-valid',
            'properties.bootstrap.servers' = '{kafka_broker}',
            'properties.group.id'          = 'fluss-enriched-sink-group',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true'
        )
    """)
    print("[INFO] Kafka source table ready.")

    # ── 4. StatementSet: events + incidents trong 1 Flink job ────────────────────
    # (a) Event grain: temporal join dim_camera (location tại thời điểm xử lý),
    #     COALESCE fallback về location thô từ Kafka nếu dim_camera miss.
    # (b) Incident grain: GROUP BY incident_uid → upsert vào Fluss PK table;
    #     mỗi event mới của vụ cập nhật last_ts/event_count/max_risk (changelog upsert).
    stmt = t_env.create_statement_set()
    stmt.add_insert_sql("""
        INSERT INTO `fluss`.`security`.`hot_violence_alerts`
        SELECT
            a.event_id                                  AS incident_id,
            a.incident_uid,
            a.camera_id,
            a.row_time                                  AS `timestamp`,
            a.risk_score,
            a.confidence,
            a.is_violent,
            a.event_type,
            COALESCE(c.location, a.location, 'Unknown') AS location,
            COALESCE(c.ward_id,  'Unknown')             AS ward_id,
            COALESCE(c.district, 'Unknown')             AS district,
            COALESCE(a.people_count, 0)                 AS people_count
        FROM kafka_valid_alerts AS a
        LEFT JOIN `fluss`.`security`.`dim_camera`
            FOR SYSTEM_TIME AS OF a.proc_time AS c
        ON a.camera_id = c.camera_id
        WHERE a.is_valid = true
    """)
    stmt.add_insert_sql("""
        INSERT INTO `fluss`.`security`.`hot_violence_incidents`
        SELECT
            a.incident_uid,
            a.camera_id,
            MIN(a.row_time)                                   AS start_ts,
            MAX(a.row_time)                                   AS last_ts,
            COUNT(*)                                          AS event_count,
            MAX(a.risk_score)                                 AS max_risk_score,
            AVG(a.confidence)                                 AS avg_confidence,
            MAX(a.event_type)                                 AS event_type,
            COALESCE(MAX(c.location), MAX(a.location), 'Unknown') AS location,
            COALESCE(MAX(c.ward_id),  'Unknown')              AS ward_id,
            COALESCE(MAX(c.district), 'Unknown')              AS district,
            MAX(COALESCE(a.people_count, 0))                  AS people_count
        FROM kafka_valid_alerts AS a
        LEFT JOIN `fluss`.`security`.`dim_camera`
            FOR SYSTEM_TIME AS OF a.proc_time AS c
        ON a.camera_id = c.camera_id
        WHERE a.is_valid = true
          AND a.incident_uid IS NOT NULL
        GROUP BY a.incident_uid, a.camera_id
    """)

    print("[INFO] Starting Flink job: Kafka → Fluss HOT (events + incidents)...")
    stmt.execute()


if __name__ == "__main__":
    main()
