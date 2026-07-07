"""
Flink Streaming Job: Paimon Aggregation (Warm Gold Layer) v2.

Đọc CDC changelog từ fact_violence_incident (grain = 1 VỤ đã sessionize)
thay vì raw events → daily_incident_stats / camera_stats đếm ĐÚNG số vụ
(bản cũ COUNT(*) trên events: 1 vụ 20s = ~40 event vì producer gửi 0.5s/lần).

Outputs (giữ nguyên tên bảng để Grafana/chatbot không phải đổi):
  - daily_incident_stats: theo ngày × location (street từ dim_camera)
  - camera_stats:         theo ngày × camera

Run inside Flink JobManager:
    flink run -py /opt/flink/scripts/aggregate_paimon.py
"""
import os
from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.statement_set import StatementSet


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30000)  # 30s — Paimon requires checkpointing
    t_env = StreamTableEnvironment.create(env)

    # Force parallelism=1 for ALL table operations.
    # Without this, Paimon source inherits bucket count → starves task slots.
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("table.exec.resource.default-parallelism", "1")

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
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS `paimon`.`security`")

    # 1b. Gold tables (schema không đổi so với v1)
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

    # 2. Temporary source table trỏ thẳng path của fact với scan.parallelism=1
    #    (catalog connector gán source parallelism = bucket count, hint không override được).
    table_path = f"{warehouse_path}/security.db/fact_violence_incident"
    print("[INFO] Creating temporary source table fact_stream (scan.parallelism=1)...")
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE fact_stream (
            incident_id    STRING,
            camera_id      STRING,
            date_id        DATE,
            time_id        INT,
            event_type_id  INT,
            start_ts       TIMESTAMP(3),
            end_ts         TIMESTAMP(3),
            duration_sec   INT,
            event_count    BIGINT,
            max_risk_score DOUBLE,
            avg_confidence DOUBLE,
            is_violent     BOOLEAN,
            people_count   INT,
            frame_url      STRING,
            created_at     TIMESTAMP(3),
            PRIMARY KEY (incident_id) NOT ENFORCED
        ) WITH (
            'connector' = 'paimon',
            'path' = '{table_path}',
            's3.endpoint' = '{s3_endpoint}',
            's3.access-key' = '{s3_access_key}',
            's3.secret-key' = '{s3_secret_key}',
            's3.path.style.access' = 'true',
            'merge-engine' = 'partial-update',
            'changelog-producer' = 'lookup',
            'scan.parallelism' = '1'
        )
    """)

    stmt_set: StatementSet = t_env.create_statement_set()

    # 3. Daily stats theo location: join dim_camera (bản is_current) lấy street.
    #    dim nhỏ (vài chục dòng) → regular changelog join chấp nhận được.
    print("[INFO] Adding INSERT: daily_incident_stats (incident grain)...")
    stmt_set.add_insert_sql("""
        INSERT INTO paimon.security.daily_incident_stats
        SELECT
            f.date_id                                    AS stat_date,
            COALESCE(c.street, 'Unknown')                AS location,
            COUNT(*)                                     AS total_incidents,
            COUNT(*) FILTER (WHERE f.is_violent = true)  AS violent_incidents,
            AVG(f.max_risk_score)                        AS avg_risk_score,
            MAX(f.max_risk_score)                        AS max_risk_score
        FROM fact_stream f
        LEFT JOIN `paimon`.`security`.`dim_camera` c
            ON f.camera_id = c.camera_id AND c.is_current = true
        GROUP BY f.date_id, COALESCE(c.street, 'Unknown')
    """)

    # 4. Camera stats (incident grain)
    print("[INFO] Adding INSERT: camera_stats (incident grain)...")
    stmt_set.add_insert_sql("""
        INSERT INTO paimon.security.camera_stats
        SELECT
            date_id                                    AS stat_date,
            camera_id,
            COUNT(*)                                   AS total_incidents,
            COUNT(*) FILTER (WHERE is_violent = true)  AS violent_incidents,
            AVG(max_risk_score)                        AS avg_risk_score,
            AVG(avg_confidence)                        AS avg_confidence
        FROM fact_stream
        GROUP BY date_id, camera_id
    """)

    print("[INFO] Starting Flink job: Paimon Aggregation v2 (fact → gold)...")
    stmt_set.execute()


if __name__ == '__main__':
    main()
