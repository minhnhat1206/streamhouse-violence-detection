"""
DAG 1: Flink Jobs Health Monitor
Runs every 15 minutes — checks Flink REST API and restarts failed streaming jobs.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

FLINK_API = "http://jobmanager:8081"

# Jobs that must always be running for the Streamhouse pipeline
REQUIRED_JOBS = [
    "sink_to_fluss",
    "sink_to_paimon",
    "archive_to_iceberg",
    "aggregate_paimon",
]

# Map job name → PyFlink script path inside jobmanager container
JOB_SCRIPTS = {
    "sink_to_fluss":      "/opt/flink/usrlib/sink_to_fluss.py",
    "sink_to_paimon":     "/opt/flink/usrlib/sink_to_paimon.py",
    "archive_to_iceberg": "/opt/flink/usrlib/archive_to_iceberg.py",
    "aggregate_paimon":   "/opt/flink/usrlib/aggregate_paimon.py",
}


def check_and_restart_flink_jobs(**context) -> dict:
    """Check running Flink jobs and restart any that are missing."""
    log = logging.getLogger("flink_monitor")

    # Fetch current job list from Flink REST API
    try:
        resp = requests.get(f"{FLINK_API}/jobs/overview", timeout=10)
        resp.raise_for_status()
        jobs_data = resp.json().get("jobs", [])
    except Exception as exc:
        log.error("Cannot reach Flink API at %s: %s", FLINK_API, exc)
        raise

    running = {j["name"] for j in jobs_data if j["state"] == "RUNNING"}
    log.info("Currently running Flink jobs: %s", running)

    missing = [name for name in REQUIRED_JOBS if name not in running]
    results = {"checked": REQUIRED_JOBS, "running": list(running), "missing": missing, "restarted": []}

    for job_name in missing:
        script = JOB_SCRIPTS.get(job_name)
        if not script:
            log.warning("No script mapped for job '%s' — skipping restart.", job_name)
            continue

        log.warning("Flink job '%s' NOT running — attempting restart via PyFlink.", job_name)
        try:
            cmd = [
                "docker", "exec", "flink-jobmanager",
                "/opt/flink/bin/flink", "run",
                "--python", script,
                "--pyFiles", "/opt/flink/usrlib/",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                log.info("Job '%s' restarted successfully.", job_name)
                results["restarted"].append(job_name)
            else:
                log.error("Failed to restart '%s': %s", job_name, result.stderr)
        except subprocess.TimeoutExpired:
            log.error("Timeout restarting job '%s'.", job_name)
        except Exception as exc:
            log.error("Error restarting '%s': %s", job_name, exc)

    if not missing:
        log.info("All required Flink jobs are RUNNING — pipeline healthy.")

    return results


with DAG(
    dag_id="flink_jobs_monitor",
    description="Monitor Flink streaming jobs and restart if not running",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 5, 7),
    catchup=False,
    tags=["streamhouse", "flink", "monitoring"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "owner": "vigilance-ai",
    },
) as dag:

    check_jobs = PythonOperator(
        task_id="check_flink_jobs",
        python_callable=check_and_restart_flink_jobs,
    )
