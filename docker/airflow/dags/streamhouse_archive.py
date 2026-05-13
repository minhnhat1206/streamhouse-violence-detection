"""
DAG 2: Streamhouse Archive — Weekly maintenance pipeline
Runs every Sunday at 02:00 AM:
  1. Archive old Paimon WARM data → Iceberg COLD via Flink batch job
  2. Expire old Paimon snapshots (>30 days)
  3. Expire old Iceberg snapshots (>90 days)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="streamhouse_archive",
    description="Weekly archive: Paimon → Iceberg + snapshot cleanup",
    schedule_interval="0 2 * * 0",  # Sunday 02:00
    start_date=datetime(2026, 5, 7),
    catchup=False,
    tags=["streamhouse", "archive", "maintenance"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "owner": "vigilance-ai",
    },
) as dag:

    # Step 1: Archive old Paimon WARM data → Iceberg COLD via Flink batch
    archive_paimon = BashOperator(
        task_id="archive_paimon_to_iceberg",
        bash_command="""
        echo "=== [Step 1] Archiving Paimon WARM → Iceberg COLD ==="
        docker exec jobmanager \
          /opt/flink/bin/flink run \
          --python /opt/flink/scripts/archive_to_iceberg.py \
          --pyFiles /opt/flink/scripts/ \
          -Dexecution.runtime-mode=BATCH \
          -Dpipeline.name=weekly_archive_paimon_to_iceberg
        echo "=== Archive completed ==="
        """,
        execution_timeout=timedelta(hours=2),
    )

    # Step 2: Expire Paimon snapshots older than 30 days
    # Uses Trino to call Paimon system procedure
    expire_paimon = BashOperator(
        task_id="expire_paimon_snapshots",
        bash_command="""
        echo "=== [Step 2] Expiring Paimon snapshots > 30 days ==="
        docker exec trino-coordinator \
          trino --server localhost:8080 \
                --catalog paimon \
                --schema security \
                --execute "
            CALL paimon.system.expire_snapshots(
              table => 'security.violence_incidents',
              older_than => TIMESTAMP '{{ macros.ds_add(ds, -30) }} 00:00:00'
            )
          " || echo "[WARN] Paimon expire_snapshots failed (catalog may be disabled)"
        echo "=== Paimon cleanup done ==="
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # Step 3: Expire Iceberg snapshots older than 90 days
    expire_iceberg = BashOperator(
        task_id="expire_iceberg_snapshots",
        bash_command="""
        echo "=== [Step 3] Expiring Iceberg snapshots > 90 days ==="
        docker exec trino-coordinator \
          trino --server localhost:8080 \
                --catalog iceberg \
                --schema security \
                --execute "
            ALTER TABLE iceberg.security.historical_violence_incidents
            EXECUTE expire_snapshots(retention_threshold => '90d')
          " || echo "[WARN] Iceberg expire_snapshots failed"
        echo "=== Iceberg cleanup done ==="
        """,
        execution_timeout=timedelta(minutes=30),
    )

    archive_paimon >> expire_paimon >> expire_iceberg
