"""
Initialize Fluss tables for the Streamhouse HOT layer.

Creates (or migrates) hot_violence_alerts with the enriched schema that
includes location/ward_id/district columns added for true tiering.

Schema migration: uses DROP TABLE IF EXISTS + CREATE TABLE because Fluss
primary key tables do not support ALTER COLUMN. HOT data is ephemeral
(tiered every 30 min), so data loss during upgrade is acceptable.

Run once before deploying sink_to_fluss_enriched.py:
    flink run -py /opt/flink/scripts/init_fluss_tables.py
"""
import os
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    coordinator = os.getenv("FLUSS_COORDINATOR", "fluss-coordinator:9123")

    print("[INFO] Creating Fluss Catalog...")
    t_env.execute_sql(f"""
        CREATE CATALOG fluss WITH (
            'type'              = 'fluss',
            'bootstrap.servers' = '{coordinator}'
        )
    """)

    t_env.execute_sql("USE CATALOG fluss")
    print("[INFO] Creating Database 'security'...")
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS security")
    t_env.execute_sql("USE security")

    # Schema migration: DROP + CREATE to add location/ward_id/district columns.
    # HOT data is ephemeral (tiered every 30 min); data loss during upgrade is acceptable.
    print("[INFO] Migrating hot_violence_alerts schema (adding location/ward_id/district)...")
    t_env.execute_sql("DROP TABLE IF EXISTS hot_violence_alerts")
    t_env.execute_sql("""
        CREATE TABLE hot_violence_alerts (
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
    print("[INFO] hot_violence_alerts ready (enriched schema: +location/ward_id/district).")

    # dim_camera: versioned primary key table for temporal joins.
    # Schema unchanged — CREATE TABLE IF NOT EXISTS (idempotent).
    print("[INFO] Ensuring dim_camera table exists...")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS dim_camera (
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
    print("[SUCCESS] Fluss tables initialized: hot_violence_alerts (enriched) + dim_camera.")


if __name__ == '__main__':
    main()
