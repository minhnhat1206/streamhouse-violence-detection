"""
Federated Query Interface — Streamhouse Architecture.

Routes queries across 3 storage layers:
  - Hot  (< 1 hour):   Flink SQL Gateway → Apache Fluss
  - Warm (1hr - 7d):   Trino → Apache Paimon
  - Cold (> 7 days):   Trino → Apache Iceberg
  - Federated:         Trino → JOIN Paimon + Iceberg

Usage:
    # Run full demo
    python federated_queries.py --demo

    # Single query with explicit layer
    python federated_queries.py --layer warm --sql "SELECT * FROM paimon.security.violence_incidents LIMIT 5"

Requirements:
    pip install trino requests
"""
import argparse
import json
import os
import sys
import time
from typing import Any

import requests

# ── Connection config ──────────────────────────────────────────────────────────
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8082"))
TRINO_USER = os.getenv("TRINO_USER", "trino")
FLINK_GATEWAY_URL = os.getenv("FLINK_GATEWAY_URL", "http://localhost:8083")


# ── Trino client (warm + cold + federated) ─────────────────────────────────────
def trino_query(sql: str, catalog: str = "paimon", schema: str = "security") -> list[dict[str, Any]]:
    """Execute SQL on Trino. Supports paimon and iceberg catalogs."""
    try:
        import trino as trino_client
    except ImportError:
        print("[ERROR] Package 'trino' not installed. Run: pip install trino")
        sys.exit(1)

    conn = trino_client.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=catalog,
        schema=schema,
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    if cursor.description is None:
        return []
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ── Flink SQL Gateway client (hot layer — Fluss) ───────────────────────────────
def fluss_query(sql: str) -> list[dict[str, Any]]:
    """
    Execute SQL targeting Fluss hot storage via Flink SQL Gateway.

    Requires flink-sql-gateway container running (--profile ui).
    API docs: http://localhost:8083/v1/
    """
    # 1. Open session
    resp = requests.post(
        f"{FLINK_GATEWAY_URL}/v1/sessions",
        json={"properties": {"execution.target": "remote"}},
        timeout=15,
    )
    resp.raise_for_status()
    session_id = resp.json()["sessionHandle"]

    try:
        # 2. Register Fluss catalog
        _gw_exec(session_id, """
            CREATE CATALOG IF NOT EXISTS fluss_hot WITH (
                'type'              = 'fluss',
                'bootstrap.servers' = 'fluss-coordinator:9123'
            )
        """)
        _gw_exec(session_id, "USE CATALOG fluss_hot")
        _gw_exec(session_id, "USE `security`")

        # 3. Run query
        return _gw_query(session_id, sql)
    finally:
        # 4. Close session
        requests.delete(f"{FLINK_GATEWAY_URL}/v1/sessions/{session_id}", timeout=10)


def _gw_exec(session_id: str, sql: str, timeout: int = 30) -> None:
    """Fire-and-wait a SQL statement via Flink SQL Gateway."""
    resp = requests.post(
        f"{FLINK_GATEWAY_URL}/v1/sessions/{session_id}/statements",
        json={"statement": sql.strip()},
        timeout=timeout,
    )
    resp.raise_for_status()
    op_id = resp.json()["operationHandle"]
    _gw_wait(session_id, op_id, timeout)


def _gw_query(session_id: str, sql: str, timeout: int = 60) -> list[dict[str, Any]]:
    """Execute a SELECT statement via Flink SQL Gateway and collect results."""
    resp = requests.post(
        f"{FLINK_GATEWAY_URL}/v1/sessions/{session_id}/statements",
        json={"statement": sql.strip()},
        timeout=timeout,
    )
    resp.raise_for_status()
    op_id = resp.json()["operationHandle"]
    _gw_wait(session_id, op_id, timeout)

    result_resp = requests.get(
        f"{FLINK_GATEWAY_URL}/v1/sessions/{session_id}/operations/{op_id}/result/0",
        timeout=30,
    )
    result_resp.raise_for_status()
    data = result_resp.json().get("results", {})
    columns = [col["name"] for col in data.get("columns", [])]
    return [dict(zip(columns, row["fields"])) for row in data.get("data", [])]


def _gw_wait(session_id: str, op_id: str, timeout: int) -> None:
    """Poll operation status until FINISHED or ERROR."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(
            f"{FLINK_GATEWAY_URL}/v1/sessions/{session_id}/operations/{op_id}/status",
            timeout=10,
        )
        resp.raise_for_status()
        status = resp.json().get("status", {}).get("statusCode", "")
        if status in ("FINISHED", "CLOSED"):
            return
        if status in ("ERROR", "CANCELED"):
            raise RuntimeError(f"Flink SQL Gateway operation failed: {status}")
        time.sleep(1)
    raise TimeoutError(f"Operation {op_id} timed out after {timeout}s")


# ── Query router ───────────────────────────────────────────────────────────────
def route_query(sql: str, layer: str) -> list[dict[str, Any]]:
    """
    Route SQL to the correct storage layer.

    Args:
        sql:   SQL query string
        layer: 'hot' | 'warm' | 'cold' | 'federated'
    """
    if layer == "hot":
        print("[ROUTER] hot → Flink SQL Gateway (Fluss)")
        return fluss_query(sql)
    elif layer in ("warm", "federated"):
        print(f"[ROUTER] {layer} → Trino (Paimon)")
        return trino_query(sql, catalog="paimon")
    elif layer == "cold":
        print("[ROUTER] cold → Trino (Iceberg)")
        return trino_query(sql, catalog="iceberg")
    else:
        raise ValueError(f"Unknown layer: {layer}. Choose: hot | warm | cold | federated")


# ── Demo queries ───────────────────────────────────────────────────────────────
DEMO_QUERIES = [
    {
        "name": "Hot: Recent alerts (last 30 min) via Fluss",
        "layer": "hot",
        "sql": """
            SELECT camera_id, risk_score, event_type, `timestamp`
            FROM hot_violence_alerts
            WHERE `timestamp` > NOW() - INTERVAL '30' MINUTE
            ORDER BY `timestamp` DESC
            LIMIT 10
        """,
    },
    {
        "name": "Warm: Today's incident count by location (Paimon)",
        "layer": "warm",
        "sql": """
            SELECT location,
                   COUNT(*) AS total_incidents,
                   ROUND(AVG(risk_score), 3) AS avg_risk
            FROM paimon.security.violence_incidents
            WHERE CAST("timestamp" AS DATE) = CURRENT_DATE
            GROUP BY location
            ORDER BY total_incidents DESC
        """,
    },
    {
        "name": "Warm: Camera performance stats (Paimon)",
        "layer": "warm",
        "sql": """
            SELECT camera_id,
                   total_incidents,
                   violent_incidents,
                   ROUND(avg_risk_score, 3) AS avg_risk
            FROM paimon.security.camera_stats
            WHERE stat_date = CURRENT_DATE
            ORDER BY violent_incidents DESC
            LIMIT 10
        """,
    },
    {
        "name": "Cold: 7-day incident trend (Iceberg)",
        "layer": "cold",
        "sql": """
            SELECT DATE_TRUNC('day', event_timestamp) AS day,
                   COUNT(*) AS incidents,
                   ROUND(AVG(risk_score), 3) AS avg_risk
            FROM iceberg.violence_db.violence_events_for_rag
            WHERE event_timestamp >= NOW() - INTERVAL '7' DAY
            GROUP BY 1
            ORDER BY 1
        """,
    },
    {
        "name": "Federated: Paimon (warm) JOIN Iceberg (cold) — Camera risk ranking",
        "layer": "federated",
        "sql": """
            WITH warm_data AS (
                SELECT camera_id,
                       COUNT(*) AS incidents_24h
                FROM paimon.security.violence_incidents
                WHERE "timestamp" >= NOW() - INTERVAL '24' HOUR
                GROUP BY camera_id
            ),
            cold_data AS (
                SELECT camera_id,
                       COUNT(*) AS incidents_30d,
                       ROUND(AVG(risk_score), 3) AS historical_avg_risk
                FROM iceberg.violence_db.violence_events_for_rag
                WHERE event_timestamp >= NOW() - INTERVAL '30' DAY
                GROUP BY camera_id
            )
            SELECT
                COALESCE(w.camera_id, c.camera_id) AS camera_id,
                COALESCE(w.incidents_24h, 0)       AS incidents_last_24h,
                COALESCE(c.incidents_30d, 0)        AS incidents_last_30d,
                c.historical_avg_risk
            FROM warm_data w
            FULL JOIN cold_data c ON w.camera_id = c.camera_id
            ORDER BY incidents_last_24h DESC
        """,
    },
]


def run_demo() -> None:
    """Run all demo queries and print results."""
    print("=" * 65)
    print("  FEDERATED QUERY DEMO — Violence Detection Streamhouse")
    print("  Hot=Fluss  |  Warm=Paimon  |  Cold=Iceberg  |  Fed=JOIN")
    print("=" * 65)

    passed = failed = 0
    for q in DEMO_QUERIES:
        print(f"\n[{q['layer'].upper()}] {q['name']}")
        print("-" * 50)
        try:
            t0 = time.time()
            rows = route_query(q["sql"], q["layer"])
            elapsed_ms = (time.time() - t0) * 1000
            print(f"  {len(rows)} rows  |  {elapsed_ms:.0f} ms")
            for row in rows[:5]:
                print(f"  {json.dumps(row, default=str)}")
            if len(rows) > 5:
                print(f"  ... ({len(rows) - 5} more rows)")
            passed += 1
        except Exception as exc:
            print(f"  FAIL: {exc}")
            failed += 1

    print("\n" + "=" * 65)
    print(f"  Results: {passed} passed, {failed} failed")
    print("=" * 65)


# ── CLI ────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Federated query router for Streamhouse")
    parser.add_argument("--demo", action="store_true", help="Run all demo queries")
    parser.add_argument("--layer", choices=["hot", "warm", "cold", "federated"],
                        help="Storage layer to query")
    parser.add_argument("--sql", help="SQL statement to execute")
    args = parser.parse_args()

    if args.demo:
        run_demo()
    elif args.layer and args.sql:
        rows = route_query(args.sql, args.layer)
        print(json.dumps(rows, indent=2, default=str))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
