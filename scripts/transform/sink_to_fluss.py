import os
from pyflink.table import EnvironmentSettings, TableEnvironment, StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.restart_strategy import RestartStrategies

def main():
    # Setup Stream Table Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(20, 15000))
    env.enable_checkpointing(30000)
    t_env = StreamTableEnvironment.create(env)
    
    # JARs (Kafka + Fluss connectors) are pre-loaded in /opt/flink/lib/ (system classpath)
    # No need to set pipeline.jars — avoids classloading conflicts between connectors
    
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    fluss_coordinator = os.getenv("FLUSS_COORDINATOR", "fluss-coordinator:9123")

    # 1. Register Fluss Catalog
    t_env.execute_sql(f"""
        CREATE CATALOG fluss WITH (
            'type' = 'fluss',
            'bootstrap.servers' = '{fluss_coordinator}'
        )
    """)

    # 1b. Ensure Fluss database + table exist (idempotent after hard reset)
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS `fluss`.`security`")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `fluss`.`security`.`hot_violence_alerts` (
            incident_id   STRING,
            camera_id     STRING,
            `timestamp`   TIMESTAMP(3),
            risk_score    DOUBLE,
            confidence    DOUBLE,
            is_violent    BOOLEAN,
            event_type    STRING,
            PRIMARY KEY (incident_id) NOT ENFORCED
        ) WITH (
            'bucket.num' = '3'
        )
    """)
    print("[INFO] Fluss table hot_violence_alerts ready.")

    # 2. Define Kafka Source Table
    # Note: We use the exact field names from the producer/validator
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
            'properties.group.id' = 'fluss-sink-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # 3. Insert into Fluss Table
    # We map 'event_id' to 'incident_id' as defined in Fluss schema
    print("[INFO] Starting Flink job: Kafka to Fluss Sink...")
    t_env.execute_sql("""
        INSERT INTO fluss.security.hot_violence_alerts
        SELECT 
            event_id as incident_id,
            camera_id,
            row_time as `timestamp`,
            risk_score,
            confidence,
            is_violent,
            event_type
        FROM kafka_valid_alerts
        WHERE is_valid = true
    """)

if __name__ == '__main__':
    main()
