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

# Jobs identified by substring match against Flink job names.
# Flink names jobs after the sink table, e.g. "insert-into_fluss.security.hot_violence_alerts"
REQUIRED_JOBS = [
    "hot_violence_alerts",   # sink_to_fluss    → insert-into_fluss.security.hot_violence_alerts
    "violence_incidents",    # sink_to_paimon   → insert-into_paimon.security.violence_incidents
    "daily_incident_stats",  # aggregate_paimon → insert-into_paimon.security.daily_incident_stats
]

# Map job key → PyFlink script path inside jobmanager container
# scripts/transform/ is mounted at /opt/flink/scripts/ in the Flink containers
JOB_SCRIPTS = {
    "hot_violence_alerts":  "/opt/flink/scripts/sink_to_fluss.py",
    "violence_incidents":   "/opt/flink/scripts/sink_to_paimon.py",
    "daily_incident_stats": "/opt/flink/scripts/aggregate_paimon.py",
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

    running_names = {j["name"] for j in jobs_data if j["state"] == "RUNNING"}
    log.info("Currently running Flink jobs: %s", running_names)

    # Match by substring — Flink names jobs after the sink table
    def is_running(key):
        return any(key in name for name in running_names)

    missing = [key for key in REQUIRED_JOBS if not is_running(key)]
    results = {"checked": REQUIRED_JOBS, "running": list(running_names), "missing": missing, "restarted": []}

    for job_name in missing:
        script = JOB_SCRIPTS.get(job_name)
        if not script:
            log.warning("No script mapped for job '%s' — skipping restart.", job_name)
            continue

        log.warning("Flink job '%s' NOT running — attempting restart via PyFlink.", job_name)
        try:
            cmd = [
                "docker", "exec", "jobmanager",
                "/opt/flink/bin/flink", "run", "--detached",
                "--python", script,
                "--pyFiles", "/opt/flink/scripts/",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
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
