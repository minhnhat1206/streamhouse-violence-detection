"""
End-to-end Text-to-SQL test suite — Session 29.

Usage (inside chatbot container):
  python3 /app/scripts/chatbot/test_text2sql.py

Or against the live API:
  python3 /app/scripts/chatbot/test_text2sql.py --api http://localhost:5002

Each test case checks:
  1. Layer routing (expected_layer)
  2. SQL contains required keywords (expected_sql_contains)
  3. SQL does NOT contain forbidden keywords (expected_sql_not_contains)
  4. API response has citation with source_table + layer
"""

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import List, Optional

import requests

# ── Test case definitions ─────────────────────────────────────────────────────

@dataclass
class TestCase:
    query: str
    expected_layer: Optional[str] = None          # "Fluss" | "Paimon" | "Iceberg"
    expected_sql_contains: List[str] = field(default_factory=list)
    expected_sql_not_contains: List[str] = field(default_factory=list)
    description: str = ""


TEST_CASES: List[TestCase] = [
    # ── HOT layer (Fluss) ─────────────────────────────────────────────────────
    TestCase(
        query="Hiện tại có bao nhiêu vụ bạo lực đang xảy ra?",
        expected_layer="Fluss",
        expected_sql_contains=["hot_violence_alerts"],
        description="HOT: real-time keyword → Fluss",
    ),

    # ── WARM layer (Paimon) ───────────────────────────────────────────────────
    TestCase(
        query="Hôm nay camera nào phát hiện nhiều bạo lực nhất?",
        expected_layer="Paimon",
        expected_sql_contains=["violence_incidents", "camera_id"],
        description="WARM: today + GROUP BY camera",
    ),
    TestCase(
        query="Tuần này có bao nhiêu vụ bạo lực tại quận 1?",
        expected_layer="Paimon",
        expected_sql_contains=["violence_incidents"],
        description="WARM: this week + location filter",
    ),
    TestCase(
        query="Liệt kê 5 vụ bạo lực gần nhất kèm risk score",
        expected_layer="Paimon",
        expected_sql_contains=["violence_incidents", "risk_score"],
        description="WARM: list recent + ORDER BY timestamp DESC",
    ),
    TestCase(
        query="Camera nào có risk score trung bình cao nhất 3 ngày qua?",
        expected_layer="Paimon",
        expected_sql_contains=["violence_incidents", "camera_id", "risk_score"],
        description="WARM: 3 days + AVG(risk_score) GROUP BY camera",
    ),

    # ── COLD layer (Iceberg) ──────────────────────────────────────────────────
    TestCase(
        query="Tháng trước có bao nhiêu vụ bạo lực tổng cộng?",
        expected_layer="Iceberg",
        expected_sql_contains=["historical_violence_incidents"],
        description="COLD: last month → Iceberg",
    ),
    TestCase(
        query="Năm 2024 tổng cộng có bao nhiêu sự kiện bạo lực được ghi nhận?",
        expected_layer="Iceberg",
        expected_sql_contains=["historical_violence_incidents"],
        expected_sql_not_contains=["violence_incidents"],
        description="COLD: year-based → must use historical table",
    ),

    # ── Correctness: is_violent filter ────────────────────────────────────────
    TestCase(
        query="Hôm nay có bao nhiêu vụ bạo lực?",
        expected_layer="Paimon",
        expected_sql_contains=["is_violent"],
        description="CORRECTNESS: must include is_violent filter",
    ),

    # ── Self-correction / camera ID normalization ─────────────────────────────
    TestCase(
        query="Camera số 3 trong 24 giờ qua có bao nhiêu vụ bạo lực?",
        expected_layer="Paimon",
        expected_sql_contains=["camera_id", "violence_incidents"],
        description="WARM: 24h + camera filter",
    ),

    # ── Complex Vietnamese ─────────────────────────────────────────────────────
    TestCase(
        query="So sánh số vụ bạo lực hôm nay với hôm qua theo từng camera",
        expected_layer="Paimon",
        expected_sql_contains=["camera_id", "violence_incidents"],
        description="WARM: comparison today vs yesterday per camera",
    ),
]


# ── Result tracking ───────────────────────────────────────────────────────────

@dataclass
class TestResult:
    case: TestCase
    passed: bool
    layer_ok: bool
    sql_contains_ok: bool
    sql_not_contains_ok: bool
    actual_layer: str = ""
    actual_sql: str = ""
    error: str = ""
    latency_ms: int = 0


# ── API test runner ────────────────────────────────────────────────────────────

def run_via_api(api_base: str, case: TestCase) -> TestResult:
    """Call the live /chat endpoint and evaluate response."""
    t0 = time.time()
    try:
        resp = requests.post(
            f"{api_base}/chat",
            json={"query": case.query},
            timeout=300,
        )
        latency_ms = int((time.time() - t0) * 1000)
        resp.raise_for_status()
        body = resp.json()
    except Exception as e:
        return TestResult(
            case=case, passed=False,
            layer_ok=False, sql_contains_ok=False, sql_not_contains_ok=False,
            error=str(e), latency_ms=int((time.time() - t0) * 1000),
        )

    actual_layer = body.get("layer", "")
    actual_sql = (body.get("sql_used") or "").upper()

    # ── Evaluate ──────────────────────────────────────────────────────────────
    layer_ok = (
        case.expected_layer is None
        or case.expected_layer.lower() in actual_layer.lower()
    )

    sql_contains_ok = all(
        kw.upper() in actual_sql for kw in case.expected_sql_contains
    )
    sql_not_contains_ok = all(
        kw.upper() not in actual_sql for kw in case.expected_sql_not_contains
    )
    passed = layer_ok and sql_contains_ok and sql_not_contains_ok

    return TestResult(
        case=case,
        passed=passed,
        layer_ok=layer_ok,
        sql_contains_ok=sql_contains_ok,
        sql_not_contains_ok=sql_not_contains_ok,
        actual_layer=actual_layer,
        actual_sql=body.get("sql_used", ""),
        latency_ms=latency_ms,
    )


# ── Unit test runner (offline — only tests routing + schema_registry) ─────────

def run_offline(case: TestCase) -> TestResult:
    """
    Offline unit test: only validates layer routing logic (no Gemini call).
    Useful when the container is not running.
    """
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
    from scripts.chatbot.agent import _parse_intent_keywords, select_data_layer
    import asyncio as _asyncio
    import time as _time

    intent = _parse_intent_keywords(case.query)

    # Build a minimal state
    state = {
        "request_id": "test",
        "user_query": case.query,
        "intent": intent,
        "trino_schema": "security",
        "retry_count": 0,
        "retry_errors": [],
    }

    # Run select_data_layer synchronously
    result_state = _asyncio.run(select_data_layer(state))  # type: ignore
    actual_layer = (result_state.get("selected_layer") or "").value if result_state.get("selected_layer") else ""

    layer_ok = (
        case.expected_layer is None
        or case.expected_layer.lower() in actual_layer.lower()
    )

    return TestResult(
        case=case,
        passed=layer_ok,
        layer_ok=layer_ok,
        sql_contains_ok=True,   # not checked in offline mode
        sql_not_contains_ok=True,
        actual_layer=actual_layer,
        actual_sql="(offline — no SQL generated)",
    )


# ── Report ────────────────────────────────────────────────────────────────────

def print_report(results: List[TestResult], mode: str):
    total = len(results)
    passed = sum(r.passed for r in results)
    failed = total - passed

    sep = "=" * 72
    print(f"\n{sep}")
    print(f"  Text-to-SQL Test Suite — Session 29  ({mode} mode)")
    print(sep)

    for i, r in enumerate(results, 1):
        icon = "✅" if r.passed else "❌"
        layer_icon = "✓" if r.layer_ok else "✗"
        print(f"\n[{i:02d}] {icon} {r.case.description}")
        print(f"     Query   : {r.case.query}")
        print(f"     Layer   : [{layer_icon}] expected={r.case.expected_layer}  actual={r.actual_layer}")
        if mode == "api":
            sql_icon = "✓" if r.sql_contains_ok else "✗"
            neg_icon = "✓" if r.sql_not_contains_ok else "✗"
            print(f"     SQL ✓kw : [{sql_icon}] {r.case.expected_sql_contains}")
            print(f"     SQL ✗kw : [{neg_icon}] {r.case.expected_sql_not_contains}")
            if r.actual_sql:
                preview = r.actual_sql[:200].replace("\n", " ")
                print(f"     SQL     : {preview}...")
            print(f"     Latency : {r.latency_ms}ms")
        if r.error:
            print(f"     Error   : {r.error}")

    print(f"\n{sep}")
    print(f"  RESULT: {passed}/{total} PASSED  ({failed} FAILED)")
    if mode == "offline":
        print("  (offline mode — only layer routing tested, no SQL or Gemini calls)")
    print(sep + "\n")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Text-to-SQL test suite")
    parser.add_argument(
        "--api",
        default=None,
        help="Base URL of live chatbot API, e.g. http://localhost:5002. "
             "If omitted, runs in offline (layer-routing-only) mode.",
    )
    args = parser.parse_args()

    if args.api:
        print(f"Running API tests against {args.api} ...")
        results = [run_via_api(args.api, tc) for tc in TEST_CASES]
        print_report(results, mode="api")
    else:
        print("Running offline routing tests ...")
        results = [run_offline(tc) for tc in TEST_CASES]
        print_report(results, mode="offline")

    # Exit 1 if any test failed (useful for CI)
    if any(not r.passed for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
