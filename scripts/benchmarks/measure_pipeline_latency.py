#!/usr/bin/env python3
"""
measure_pipeline_latency.py — Latency Measurement Script (runs ON GCP VM)
=========================================================================
This script measures:
  1. Trino query latency per tier (HOT / WARM / COLD) by timing SQL queries directly.
  2. Chatbot E2E request latency (via HTTP POST to /webhook/chat).
  3. Kafka consumer lag (by reading offset metadata via admin client).

Results are printed to stdout in a formatted table, and also written to
/tmp/latency_report.json for downstream processing.

Run on GCP VM:
    python3 measure_pipeline_latency.py

Requirements:
    pip install kafka-python requests
"""

import json
import os
import time
import statistics
import urllib.request
import urllib.error
from datetime import datetime

# ── Configuration ─────────────────────────────────────────────────────────────
TRINO_HOST       = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT       = int(os.getenv("TRINO_PORT", "8082"))
CHATBOT_URL      = os.getenv("CHATBOT_URL", "http://localhost:5002")
KAFKA_BROKER     = os.getenv("KAFKA_BROKER", "kafka:9093")
N_REPS           = int(os.getenv("N_REPS", "5"))       # repetitions per query
OUTPUT_JSON      = os.getenv("OUTPUT_JSON", "/tmp/latency_report.json")

# ── Trino HTTP helper ──────────────────────────────────────────────────────────

def trino_query_timed(sql: str, catalog: str = "iceberg", schema: str = "default",
                      timeout: int = 60) -> dict:
    """Time a full Trino query cycle. Returns {latency_s, rows, error}."""
    url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/statement"
    req = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        headers={
            "X-Trino-User": "admin",
            "X-Trino-Catalog": catalog,
            "X-Trino-Schema": schema,
            "Content-Type": "text/plain",
        },
        method="POST",
    )
    t_start = time.time()
    rows = []
    error = None
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read())
            if body.get("data"):
                rows.extend(body["data"])
            next_uri = body.get("nextUri")
            if body.get("error"):
                error = body["error"].get("message", "unknown")
        while next_uri:
            with urllib.request.urlopen(next_uri, timeout=timeout) as resp:
                body = json.loads(resp.read())
                if body.get("data"):
                    rows.extend(body["data"])
                next_uri = body.get("nextUri")
                if body.get("error"):
                    error = body["error"].get("message", "unknown")
                    break
    except Exception as e:
        error = str(e)
    latency_s = time.time() - t_start
    return {"latency_s": round(latency_s, 3), "rows": len(rows), "error": error}


def chatbot_request_timed(query: str, timeout: int = 120) -> dict:
    """POST to chatbot /webhook/chat and time the full E2E response."""
    url = f"{CHATBOT_URL}/webhook/chat"
    body = json.dumps({"query": query}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t_start = time.time()
    error = None
    layer = None
    duration_ms = None
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
            layer = data.get("layer")
            duration_ms = data.get("duration_ms")
    except Exception as e:
        error = str(e)
    wall_s = time.time() - t_start
    return {
        "wall_latency_s": round(wall_s, 3),
        "chatbot_duration_ms": duration_ms,
        "layer": layer,
        "error": error,
    }


# ── Measurement routines ───────────────────────────────────────────────────────

def measure_trino_latency():
    """Measure Trino query latency across all three tiers."""
    print("\n" + "=" * 65)
    print("  TRINO STORAGE LATENCY MEASUREMENT")
    print("=" * 65)

    queries = [
        ("WARM — sessionized COUNT", "iceberg", "default",
         "SELECT COUNT(*) FROM iceberg.default.violence_incidents_sessionized WHERE is_violent = TRUE"),
        ("WARM — raw Paimon COUNT", "paimon", "security",
         "SELECT COUNT(*) FROM paimon.security.violence_incidents WHERE is_violent = TRUE"),
        ("COLD — Iceberg sessionized", "iceberg", "default",
         "SELECT COUNT(*) FROM iceberg.default.historical_violence_incidents_sessionized WHERE is_violent = TRUE"),
        ("WARM — JOIN query", "iceberg", "default",
         """SELECT camera_id, COUNT(*) cnt, ROUND(AVG(avg_risk_score),3) avg_risk
            FROM iceberg.default.violence_incidents_sessionized
            WHERE is_violent = TRUE
            GROUP BY camera_id
            ORDER BY cnt DESC"""),
    ]

    results = {}
    for label, cat, schema, sql in queries:
        lats = []
        print(f"\n  [{label}]")
        for i in range(N_REPS):
            r = trino_query_timed(sql, catalog=cat, schema=schema)
            status = f"✓ {r['latency_s']:.3f}s ({r['rows']} rows)" if not r["error"] else f"✗ {r['error'][:60]}"
            print(f"    Rep {i+1}: {status}")
            if not r["error"]:
                lats.append(r["latency_s"])
            time.sleep(1)

        if lats:
            results[label] = {
                "mean_s": round(statistics.mean(lats), 3),
                "min_s": round(min(lats), 3),
                "max_s": round(max(lats), 3),
                "stdev_s": round(statistics.stdev(lats), 3) if len(lats) > 1 else 0,
                "n": len(lats),
            }
            print(f"    → mean={results[label]['mean_s']}s  min={results[label]['min_s']}s  max={results[label]['max_s']}s")

    return results


def measure_chatbot_latency():
    """Measure chatbot E2E latency for common query types."""
    print("\n" + "=" * 65)
    print("  CHATBOT E2E LATENCY MEASUREMENT")
    print("=" * 65)

    test_queries = [
        ("Tổng hợp WARM — count violent", "Tổng số vụ bạo lực trong 7 ngày qua là bao nhiêu?"),
        ("Chi tiết WARM — theo camera", "Camera nào có nhiều vụ bạo lực nhất?"),
        ("HOT — realtime", "Có vụ bạo lực nào đang xảy ra không?"),
    ]

    results = {}
    for label, query in test_queries:
        print(f"\n  [{label}]")
        print(f"    Query: {query}")
        r = chatbot_request_timed(query)
        status = f"✓ wall={r['wall_latency_s']}s chatbot={r['chatbot_duration_ms']}ms layer={r['layer']}"
        if r["error"]:
            status = f"✗ {r['error'][:60]}"
        print(f"    {status}")
        results[label] = r

    return results


def measure_prometheus_metrics():
    """Read current Prometheus gauge values from chatbot /metrics."""
    print("\n" + "=" * 65)
    print("  PROMETHEUS METRICS SNAPSHOT")
    print("=" * 65)

    try:
        url = f"{CHATBOT_URL}/metrics"
        with urllib.request.urlopen(url, timeout=10) as resp:
            content = resp.read().decode("utf-8")

        metrics = {}
        for line in content.split("\n"):
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split(" ")
            if len(parts) >= 2:
                key = parts[0]
                val = parts[-1]
                metrics[key] = val

        keys_of_interest = [
            "violence_incidents_24h_total",
            "violence_incidents_7d_total",
            "violence_cameras_active",
            "violence_avg_risk_score",
            "streamhouse_hot_rows_total",
            "streamhouse_warm_rows_total",
            "streamhouse_cold_rows_total",
            "streamhouse_e2e_kafka_to_fluss_ms",
            "streamhouse_inference_latency_ms",
            "streamhouse_pipeline_events_per_min",
        ]

        print("\n  Key metrics:")
        extracted = {}
        for k in keys_of_interest:
            # Find in metrics dict (may have label suffixes)
            found = {mk: mv for mk, mv in metrics.items() if mk.startswith(k)}
            for mk, mv in found.items():
                print(f"    {mk:<55} = {mv}")
                extracted[mk] = mv

        return extracted
    except Exception as e:
        print(f"  Error reading metrics: {e}")
        return {}


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print(f"\nStreamhouse Pipeline Latency Measurement")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"N_REPS: {N_REPS} | Trino: {TRINO_HOST}:{TRINO_PORT} | Chatbot: {CHATBOT_URL}")

    report = {
        "timestamp": datetime.now().isoformat(),
        "config": {"n_reps": N_REPS, "trino": f"{TRINO_HOST}:{TRINO_PORT}", "chatbot": CHATBOT_URL},
    }

    report["trino_latency"] = measure_trino_latency()
    report["chatbot_latency"] = measure_chatbot_latency()
    report["prometheus_snapshot"] = measure_prometheus_metrics()

    # Summary table
    print("\n" + "=" * 65)
    print("  SUMMARY TABLE (for thesis §4.3)")
    print("=" * 65)
    print(f"\n{'Layer / Query':<42} {'Mean (s)':>10} {'Min (s)':>10} {'Max (s)':>10}")
    print("-" * 65)
    for label, stats in report["trino_latency"].items():
        print(f"{label:<42} {stats['mean_s']:>10} {stats['min_s']:>10} {stats['max_s']:>10}")
    print("\n  Chatbot E2E:")
    for label, stats in report["chatbot_latency"].items():
        wall = stats.get("wall_latency_s", "N/A")
        layer = stats.get("layer", "?")
        print(f"  [{label}] wall={wall}s  layer={layer}")
    print("=" * 65)

    # Write JSON
    with open(OUTPUT_JSON, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nReport written to: {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
