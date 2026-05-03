import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # Setup Table Environment
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)
    
    # Fluss connector JAR is pre-loaded in /opt/flink/lib/ (system classpath)
    
    coordinator = os.getenv("FLUSS_COORDINATOR", "fluss-coordinator:9123")
    zookeeper = os.getenv("FLUSS_ZOOKEEPER", "fluss-zookeeper:2181")

    print("[INFO] Creating Fluss Catalog...")
    t_env.execute_sql(f"""
        CREATE CATALOG fluss WITH (
            'type' = 'fluss',
            'bootstrap.servers' = '{coordinator}'
        )
    """)

    t_env.execute_sql("USE CATALOG fluss")

    print("[INFO] Creating Database 'security'...")
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS security")
    t_env.execute_sql("USE security")

    print("[INFO] Creating Table 'hot_violence_alerts' in Fluss...")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS hot_violence_alerts (
            incident_id STRING,
            camera_id STRING,
            `timestamp` TIMESTAMP(3),
            risk_score DOUBLE,
            confidence DOUBLE,
            is_violent BOOLEAN,
            event_type STRING,
            PRIMARY KEY (incident_id) NOT ENFORCED
        ) WITH (
            'connector' = 'fluss'
        )
    """)
    print("[SUCCESS] Fluss Catalog and Tables initialized successfully.")

if __name__ == '__main__':
    main()
