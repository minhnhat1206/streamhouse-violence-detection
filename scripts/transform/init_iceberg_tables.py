"""
Initialize Iceberg Catalog and Tables (Cold Storage Layer).
Creates: iceberg.security.historical_violence_incidents (partitioned by incident_date)

Requires: Hive Metastore + MySQL running.
Run inside Flink JobManager:
    python /opt/flink/scripts/init_iceberg_tables.py
"""
import os
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    # Iceberg + Hadoop JARs are pre-loaded in /opt/flink/lib/ (system classpath)

    # Hive Metastore config
    metastore_uri = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
    warehouse_path = os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/iceberg_warehouse")

    # S3/MinIO config
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("MINIO_ROOT_USER", "minio")
    s3_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")

    # 1. Create Iceberg Catalog (Hive Metastore type + S3FileIO for MinIO)
    print("[INFO] Creating Iceberg Catalog (hive_catalog)...")
    t_env.execute_sql(f"""
        CREATE CATALOG iceberg WITH (
            'type' = 'iceberg',
            'catalog-type' = 'hive',
            'uri' = '{metastore_uri}',
            'warehouse' = '{warehouse_path}',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = '{s3_endpoint}',
            's3.access-key-id' = '{s3_access_key}',
            's3.secret-access-key' = '{s3_secret_key}',
            's3.path-style-access' = 'true',
            'client.region' = 'us-east-1'
        )
    """)

    t_env.execute_sql("USE CATALOG iceberg")

    # 2. Create Database
    print("[INFO] Creating Database 'security'...")
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS security")
    t_env.execute_sql("USE security")

    # 3. Create historical incidents table (partitioned by date for efficient pruning)
    print("[INFO] Creating Table 'historical_violence_incidents'...")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS historical_violence_incidents (
            incident_id STRING,
            camera_id STRING,
            `timestamp` TIMESTAMP(3),
            risk_score DOUBLE,
            confidence DOUBLE,
            is_violent BOOLEAN,
            event_type STRING,
            location STRING,
            incident_date DATE
        ) PARTITIONED BY (incident_date)
        WITH (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

    print("[SUCCESS] Iceberg Catalog and historical_violence_incidents table initialized.")
    print("  - Catalog: iceberg (Hive Metastore)")
    print(f"  - Warehouse: {warehouse_path}")
    print("  - Table: security.historical_violence_incidents (partitioned by incident_date)")


if __name__ == '__main__':
    main()
