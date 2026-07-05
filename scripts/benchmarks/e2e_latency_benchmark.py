#!/usr/bin/env python3
"""
e2e_latency_benchmark.py — End-to-End Pipeline Latency Measurement Tool
========================================================================
Measures full pipeline latency from Kafka send → Fluss HOT write → Paimon WARM.

Usage (run on GCP VM or local with proper env vars):
    python e2e_latency_benchmark.py

Output: CSV + summary table printed to stdout.

Methodology:
- Subscribes to Kafka topic 'urban-safety-alerts'
- Records t1 = kafka_sent_at from message metadata
- Records t2 = NOW() when message is consumed by this script (Kafka consumer lag proxy)
- Polls Fluss HOT via SQL Gateway for the same event_id
- Records t3 = time event appears in Fluss HOT (hot_write_latency = t3 - t1)
- Polls Paimon WARM via Trino for the same event_id
- Records t4 = time event appears in WARM (warm_write_latency = t4 - t1)
- Computes inference_ms from embedded metadata.inference_ms

Results are written to e2e_latency_results.csv
"""

import csv
import json
import os
import sys
import time
import threading
import uuid
from collections import deque, defaultdict
from datetime import datetime, timezone
import urllib.request
import urllib.error

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER     = os.getenv("KAFKA_BROKER", "34.124.131.144:9093")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "urban-safety-alerts")
TRINO_HOST       = os.getenv("TRINO_HOST", "34.124.131.144")
TRINO_PORT       = int(os.getenv("TRINO_PORT", "8082"))
SQL_GATEWAY      = os.getenv("SQL_GATEWAY", "http://34.124.131.144:8083")
OUTPUT_CSV       = os.getenv("OUTPUT_CSV", "e2e_latency_results.csv")
N_SAMPLES        = int(os.getenv("N_SAMPLES", "50"))   # collect N events then summarize
POLL_TIMEOUT_S   = int(os.getenv("POLL_TIMEOUT_S", "30"))  # max wait for HOT/WARM to appear
VIOLENT_ONLY     = os.getenv("VIOLENT_ONLY", "true").lower() == "true"

# ── Shared state ──────────────────────────────────────────────────────────────
_results = []
_results_lock = threading.Lock()

# ── Trino HTTP helper ──────────────────────────────────────────────────────────

def trino_query(sql: str, timeout: int = 15) -> list:
    """Run SQL on Trino via REST API, return list of rows."""
    url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/statement"
    req = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        headers={
            "X-Trino-User": "admin",
            "X-Trino-Catalog": "iceberg",
            "X-Trino-Schema": "default",
            "Content-Type": "text/plain",
        },
        method="POST",
    )
    rows = []
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read())
            if body.get("data"):
                rows.extend(body["data"])
            next_uri = body.get("nextUri")
        while next_uri:
            with urllib.request.urlopen(next_uri, timeout=timeout) as resp:
                body = json.loads(resp.read())
                if body.get("data"):
                    rows.extend(body["data"])
                next_uri = body.get("nextUri")
    except Exception as e:
        print(f"[Trino] Query error: {e}")
    return rows


def wait_for_fluss_hot(event_id: str, deadline: float) -> float | None:
    """Poll Trino for event_id appearing in WARM (sessionized view). Return timestamp."""
    sql = f"""
        SELECT CAST(session_start AS VARCHAR)
        FROM iceberg.default.violence_incidents_sessionized
        WHERE incident_id = '{event_id}'
        LIMIT 1
    """
    while time.time() < deadline:
        rows = trino_query(sql, timeout=10)
        if rows:
            return time.time()
        time.sleep(2)
    return None


# ── Kafka consumer ─────────────────────────────────────────────────────────────

def collect_samples():
    """
    Subscribe to Kafka and collect N samples, then measure latency for each.
    Requires kafka-python installed: pip install kafka-python
    """
    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("ERROR: kafka-python not installed. Run: pip install kafka-python")
        sys.exit(1)

    print(f"[Benchmark] Connecting to Kafka {KAFKA_BROKER} topic={KAFKA_TOPIC}")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=f"e2e-benchmark-{uuid.uuid4().hex[:6]}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=60000,
    )

    collected = 0
    print(f"[Benchmark] Waiting for {N_SAMPLES} events (violent_only={VIOLENT_ONLY})...")

    for msg in consumer:
        event = msg.value
        is_violent = event.get("is_violent", False)
        if VIOLENT_ONLY and not is_violent:
            continue

        kafka_sent_at = None
        meta = event.get("metadata", {})
        if isinstance(meta, dict):
            kafka_sent_at = meta.get("kafka_sent_at")
        inference_ms = meta.get("inference_ms", 0) if isinstance(meta, dict) else 0

        t_consumed = time.time()
        kafka_consumer_lag_ms = (t_consumed - kafka_sent_at) * 1000 if kafka_sent_at else None

        event_id = event.get("event_id", "")
        camera_id = event.get("camera_id", "")
        risk_score = event.get("risk_score", 0)

        print(f"[{collected+1}/{N_SAMPLES}] event={event_id[:8]}... cam={camera_id} "
              f"violent={is_violent} risk={risk_score:.3f} "
              f"kafka_lag={kafka_consumer_lag_ms:.1f}ms" if kafka_consumer_lag_ms else
              f"[{collected+1}/{N_SAMPLES}] event={event_id[:8]}... cam={camera_id} violent={is_violent}")

        # Poll for appearance in WARM (sessionized view)
        deadline = time.time() + POLL_TIMEOUT_S
        t_warm_appeared = wait_for_fluss_hot(event_id, deadline)
        warm_e2e_ms = (t_warm_appeared - kafka_sent_at) * 1000 if (t_warm_appeared and kafka_sent_at) else None

        record = {
            "event_id": event_id,
            "camera_id": camera_id,
            "is_violent": is_violent,
            "risk_score": risk_score,
            "kafka_sent_at": kafka_sent_at,
            "t_consumed": t_consumed,
            "kafka_consumer_lag_ms": round(kafka_consumer_lag_ms, 2) if kafka_consumer_lag_ms else None,
            "inference_ms": inference_ms,
            "t_warm_appeared": t_warm_appeared,
            "warm_e2e_ms": round(warm_e2e_ms, 2) if warm_e2e_ms else None,
        }
        with _results_lock:
            _results.append(record)

        collected += 1
        if collected >= N_SAMPLES:
            break

    consumer.close()
    print(f"\n[Benchmark] Collected {collected} samples.")


# ── Summary ────────────────────────────────────────────────────────────────────

def summarize():
    with _results_lock:
        data = list(_results)

    if not data:
        print("[Benchmark] No data collected.")
        return

    # Write CSV
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"\n[Benchmark] Results written to: {OUTPUT_CSV}")

    # Stats
    def stats(values):
        vals = [v for v in values if v is not None]
        if not vals:
            return {"count": 0, "mean": None, "min": None, "max": None, "p50": None, "p95": None}
        vals.sort()
        n = len(vals)
        return {
            "count": n,
            "mean": round(sum(vals) / n, 2),
            "min": round(vals[0], 2),
            "max": round(vals[-1], 2),
            "p50": round(vals[n // 2], 2),
            "p95": round(vals[int(n * 0.95)], 2),
        }

    print("\n" + "=" * 70)
    print("  E2E PIPELINE LATENCY SUMMARY")
    print("=" * 70)

    kc_stats = stats([r["kafka_consumer_lag_ms"] for r in data])
    inf_stats = stats([r["inference_ms"] for r in data])
    warm_stats = stats([r["warm_e2e_ms"] for r in data])

    print(f"\n{'Metric':<35} {'Count':>6} {'Mean':>8} {'P50':>8} {'P95':>8} {'Min':>8} {'Max':>8}")
    print("-" * 70)
    for label, s in [
        ("Kafka consumer lag (ms)", kc_stats),
        ("Model inference latency (ms)", inf_stats),
        ("Full E2E to WARM (ms)", warm_stats),
    ]:
        print(f"{label:<35} {s['count']:>6} {str(s['mean']):>8} {str(s['p50']):>8} "
              f"{str(s['p95']):>8} {str(s['min']):>8} {str(s['max']):>8}")

    print("\n" + "=" * 70)
    print("  Per-camera breakdown (inference_ms)")
    print("-" * 70)
    cam_data = defaultdict(list)
    for r in data:
        if r["inference_ms"]:
            cam_data[r["camera_id"]].append(r["inference_ms"])
    for cam, vals in sorted(cam_data.items()):
        s = stats(vals)
        print(f"  {cam}: mean={s['mean']}ms  p95={s['p95']}ms  n={s['count']}")

    print("=" * 70)


if __name__ == "__main__":
    collect_samples()
    summarize()
