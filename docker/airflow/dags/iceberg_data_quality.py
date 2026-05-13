"""
DAG 3: Iceberg Data Quality — Daily checks at 06:00 AM
Validates:
  - Total row count
  - Null camera_id / null timestamp counts
  - Row ingestion rate (last 24h must have data)
  - Violent event ratio (last 7 days, alerts if > 80%)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

TRINO_HOST = "trino-coordinator"
TRINO_PORT = 8080  # internal port inside docker network
TRINO_USER = "airflow"


def run_quality_checks(**context) -> dict:
    """Run data quality checks against Iceberg via Trino."""
    log = logging.getLogger("iceberg_quality")

    try:
        import trino
    except ImportError:
        log.error("trino package not installed. Add trino to requirements.txt.")
        raise

    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog="iceberg",
        schema="security",
    )
    cur = conn.cursor()

    checks = {
        "total_rows": """
            SELECT COUNT(*) FROM iceberg.security.historical_violence_incidents
        """,
        "null_camera_id": """
            SELECT COUNT(*) FROM iceberg.security.historical_violence_incidents
            WHERE camera_id IS NULL
        """,
        "null_timestamp": """
            SELECT COUNT(*) FROM iceberg.security.historical_violence_incidents
            WHERE timestamp IS NULL
        """,
        "rows_last_24h": """
            SELECT COUNT(*) FROM iceberg.security.historical_violence_incidents
            WHERE timestamp >= NOW() - INTERVAL '24' HOUR
        """,
        "violent_ratio_pct": """
            SELECT ROUND(
                100.0 * SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2
            ) FROM iceberg.security.historical_violence_incidents
            WHERE timestamp >= NOW() - INTERVAL '7' DAY
        """,
    }

    results = {}
    for name, sql in checks.items():
        try:
            cur.execute(sql.strip())
            row = cur.fetchone()
            val = row[0] if row else None
            results[name] = val
            log.info("Quality check [%s]: %s", name, val)
        except Exception as exc:
            log.error("Quality check [%s] FAILED: %s", name, exc)
            results[name] = f"ERROR: {exc}"

    conn.close()

    # ── Alert conditions ──────────────────────────────────────────
    null_cam = results.get("null_camera_id", 0)
    if isinstance(null_cam, int) and null_cam > 0:
        log.error("DATA QUALITY ALERT: %d rows with null camera_id", null_cam)

    rows_24h = results.get("rows_last_24h", -1)
    if isinstance(rows_24h, int) and rows_24h == 0:
        log.warning("DATA QUALITY ALERT: No data ingested in last 24h — pipeline may be down")

    ratio = results.get("violent_ratio_pct")
    if isinstance(ratio, (int, float)) and ratio > 80:
        log.warning("DATA QUALITY ALERT: Violent ratio %.1f%% is unusually high", ratio)

    total = results.get("total_rows", 0)
    log.info("=== Quality summary: total=%s | last24h=%s | null_cam=%s | violent_ratio=%s%% ===",
             total, rows_24h, null_cam, ratio)

    return results


with DAG(
    dag_id="iceberg_data_quality",
    description="Daily data quality checks on Iceberg historical_violence_incidents",
    schedule_interval="0 6 * * *",  # Daily 06:00
    start_date=datetime(2026, 5, 7),
    catchup=False,
    tags=["streamhouse", "iceberg", "data-quality"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "owner": "vigilance-ai",
    },
) as dag:

    quality_checks = PythonOperator(
        task_id="iceberg_quality_checks",
        python_callable=run_quality_checks,
    )
