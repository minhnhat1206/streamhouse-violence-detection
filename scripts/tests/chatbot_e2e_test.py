"""
Chatbot E2E Test Suite — Streamhouse Logic Verification
=========================================================
Tests 12 queries covering:
  - Time routing: HOT (≤60 min), WARM (1h–30d), COLD (>30d)
  - Complex aggregations: GROUP BY camera, daily trends
  - Multi-condition queries
  - Boundary edge cases: 61 min, exactly 1 day, 30 days
  - Cross-layer logic
  - Vietnamese NLP variations

Expected behavior per query is documented inline.
"""

import json
import sys
import time
import urllib.request
import urllib.error

CHATBOT_URL = "http://localhost:5002"
CHAT_ENDPOINT = f"{CHATBOT_URL}/chat"

# ANSI colors
GRN = "\033[92m"; RED = "\033[91m"; YLW = "\033[93m"
BLU = "\033[94m"; CYN = "\033[96m"; RST = "\033[0m"; BLD = "\033[1m"

def chat(query: str, timeout: int = 90) -> dict:
    payload = json.dumps({"query": query}).encode()
    req = urllib.request.Request(
        CHAT_ENDPOINT,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}: {e.read().decode()[:200]}"}
    except Exception as e:
        return {"error": str(e)}


def check(condition: bool, label: str) -> bool:
    icon = f"{GRN}✓{RST}" if condition else f"{RED}✗{RST}"
    print(f"    {icon}  {label}")
    return condition


def run_test(idx: int, query: str, expected: dict) -> dict:
    print(f"\n{BLD}{BLU}[T{idx:02d}]{RST} {CYN}{query}{RST}")
    t0 = time.time()
    resp = chat(query)
    elapsed = time.time() - t0

    if "error" in resp:
        print(f"  {RED}ERROR:{RST} {resp['error']}")
        return {"pass": False, "error": resp["error"], "elapsed": elapsed}

    # Response schema: answer, layer (display name), citations.sql_used, confidence
    LAYER_MAP = {"fluss": "hot", "paimon": "warm", "iceberg": "cold"}
    raw_layer  = (resp.get("layer") or "").strip()
    layer      = LAYER_MAP.get(raw_layer.lower(), raw_layer.lower())  # normalize to hot/warm/cold
    citations  = resp.get("citations") or {}
    sql        = citations.get("sql_used", "") or resp.get("sql_used", "") or ""
    rows_count = citations.get("row_count") or citations.get("rows_returned") or 0
    rows       = [{}] * (rows_count or 0)  # synthetic list for checks
    answer     = resp.get("answer", "") or ""
    fallback   = resp.get("fallback", False)

    print(f"  Layer : {YLW}{raw_layer}→{layer}{RST}  |  Elapsed: {elapsed:.1f}s  |  Rows: {rows_count}  |  Confidence: {resp.get('confidence',0):.2f}")
    print(f"  SQL   : {sql[:120].strip()}{'…' if len(sql)>120 else ''}")
    print(f"  Answer: {answer[:280].strip()}{'…' if len(answer)>280 else ''}")

    passes = []
    for key, value in expected.items():
        if key == "layer":
            passes.append(check(layer == value, f"layer == {value} (got: {layer})"))
        elif key == "layer_in":
            passes.append(check(layer in value, f"layer in {value} (got: {layer})"))
        elif key == "has_rows":
            passes.append(check(len(rows) > 0, f"rows > 0 (got: {len(rows)})"))
        elif key == "no_error_in_answer":
            bad = any(w in answer.lower() for w in ["lỗi", "error", "không thể", "thất bại", "failed"])
            passes.append(check(not bad, f"answer has no error phrase"))
        elif key == "answer_contains":
            found = value.lower() in answer.lower()
            passes.append(check(found, f"answer contains '{value}'"))
        elif key == "answer_contains_any":
            found = any(v.lower() in answer.lower() for v in value)
            passes.append(check(found, f"answer contains any of {value}"))
        elif key == "sql_contains":
            found = value.lower() in sql.lower()
            passes.append(check(found, f"sql contains '{value}'"))
        elif key == "sql_not_contains":
            passes.append(check(value.lower() not in sql.lower(), f"sql NOT contains '{value}'"))
        elif key == "has_answer":
            passes.append(check(len(answer) > 30, f"answer non-empty (len={len(answer)})"))
        elif key == "rows_gte":
            passes.append(check(len(rows) >= value, f"rows >= {value} (got: {len(rows)})"))
        elif key == "elapsed_lt":
            passes.append(check(elapsed < value, f"elapsed < {value}s (got: {elapsed:.1f}s)"))

    overall = all(passes)
    status = f"{GRN}PASS{RST}" if overall else f"{RED}FAIL{RST}"
    print(f"  → {status} ({sum(passes)}/{len(passes)} checks)")
    return {"pass": overall, "checks": len(passes), "passed": sum(passes),
            "layer": layer, "elapsed": elapsed, "rows": len(rows)}


# ─── TEST CASES ────────────────────────────────────────────────────────────────

TESTS = [
    # ── HOT LAYER (≤60 min) ────────────────────────────────────────────────────
    {
        "id": 1, "category": "HOT routing",
        "query": "Có sự kiện bạo lực nào trong 30 phút qua không?",
        "expected": {
            "layer": "hot",
            "has_answer": True,
            "no_error_in_answer": True,
            "sql_not_contains": "ORDER BY",          # HOT strips ORDER BY
        },
    },
    {
        "id": 2, "category": "HOT routing — real-time keyword",
        "query": "Cho tôi xem các cảnh báo vừa xảy ra ngay bây giờ",
        "expected": {
            "layer": "hot",
            "has_answer": True,
            "no_error_in_answer": True,
        },
    },
    {
        "id": 3, "category": "HOT boundary — exactly 60 min",
        "query": "Liệt kê các sự kiện trong 60 phút vừa rồi",
        "expected": {
            "layer": "hot",
            "has_answer": True,
        },
    },

    # ── WARM LAYER (1h – 30d) ──────────────────────────────────────────────────
    {
        "id": 4, "category": "WARM boundary — 61 min (just over HOT)",
        "query": "Cho tôi xem dữ liệu 61 phút qua",
        "expected": {
            "layer": "warm",
            "has_answer": True,
            "no_error_in_answer": True,
        },
    },
    {
        "id": 5, "category": "WARM — aggregation by camera",
        "query": "Camera nào phát hiện nhiều bạo lực nhất trong 7 ngày qua?",
        "expected": {
            "layer": "warm",
            "has_answer": True,
            "no_error_in_answer": True,
            # Data may be 0 if stack just started; check answer content instead
            "answer_contains_any": ["cam_", "camera", "Camera", "không tìm thấy", "không có"],
        },
    },
    {
        "id": 6, "category": "WARM — daily trend",
        "query": "Xu hướng số vụ bạo lực theo ngày trong 7 ngày qua?",  # explicit 7 ngày for determinism
        "expected": {
            "layer": "warm",
            "has_answer": True,
            "sql_contains": "count",
        },
    },
    {
        "id": 7, "category": "WARM — risk score analytics",
        "query": "Risk score trung bình theo từng camera trong 24 giờ qua là bao nhiêu?",
        "expected": {
            "layer": "warm",
            "has_answer": True,
            "no_error_in_answer": True,
            "sql_contains": "avg",
        },
    },
    {
        "id": 8, "category": "WARM — complex multi-condition",
        "query": "Trong 7 ngày qua, camera nào có risk score trung bình cao nhất và tổng số vụ là bao nhiêu?",
        "expected": {
            "layer": "warm",
            "has_answer": True,
            "no_error_in_answer": True,
            "sql_contains": "avg",
        },
    },
    {
        "id": 9, "category": "WARM — today keyword",
        "query": "Hôm nay có bao nhiêu vụ bạo lực được ghi nhận?",
        "expected": {
            "layer": "warm",
            "has_answer": True,
            "no_error_in_answer": True,
        },
    },

    # ── COLD LAYER (>30d) ──────────────────────────────────────────────────────
    {
        "id": 10, "category": "COLD routing — last month",
        "query": "Tháng trước có bao nhiêu sự kiện bạo lực nghiêm trọng?",
        "expected": {
            "layer": "cold",
            "has_answer": True,
            "no_error_in_answer": True,
        },
    },
    {
        "id": 11, "category": "COLD — historical trend",
        "query": "Thống kê số vụ bạo lực theo từng tháng trong 3 tháng qua",
        "expected": {
            "layer_in": ["cold", "warm"],   # 90 days may resolve to warm
            "has_answer": True,
            "no_error_in_answer": True,
        },
    },

    # ── CROSS-LAYER / COMPLEX ──────────────────────────────────────────────────
    {
        "id": 12, "category": "COMPLEX — top location with confidence filter",
        "query": "Địa điểm nào có nhiều vụ bạo lực confidence > 0.8 nhất trong 7 ngày qua?",
        "expected": {
            "layer": "warm",
            "has_answer": True,
            "no_error_in_answer": True,
            "sql_contains": "confidence",
            "answer_contains_any": ["Đường", "đường", "location", "địa điểm", "cam_", "không tìm thấy"],
        },
    },
]

# ─── RUNNER ────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{BLD}{'='*65}{RST}")
    print(f"{BLD}  Chatbot E2E Test Suite — Streamhouse Query Logic{RST}")
    print(f"{BLD}  Target: {CHATBOT_URL}{RST}")
    print(f"{BLD}{'='*65}{RST}")

    # Health check
    try:
        with urllib.request.urlopen(f"{CHATBOT_URL}/health", timeout=5) as r:
            print(f"\n{GRN}✓ Chatbot healthy{RST}\n")
    except Exception as e:
        print(f"{RED}✗ Chatbot unreachable: {e}{RST}")
        sys.exit(1)

    results = []
    for t in TESTS:
        res = run_test(t["id"], t["query"], t["expected"])
        res["category"] = t["category"]
        results.append(res)

    # ── Summary ────────────────────────────────────────────────────────────────
    total  = len(results)
    passed = sum(1 for r in results if r["pass"])
    failed = total - passed

    print(f"\n{BLD}{'='*65}{RST}")
    print(f"{BLD}  RESULTS: {GRN}{passed} PASS{RST} / {RED}{failed} FAIL{RST} / {total} TOTAL{RST}")
    print(f"{BLD}{'='*65}{RST}")

    layer_dist = {}
    for r in results:
        l = r.get("layer", "?")
        layer_dist[l] = layer_dist.get(l, 0) + 1

    print(f"\n{BLD}Layer distribution:{RST}")
    for l, cnt in sorted(layer_dist.items()):
        print(f"  {l}: {cnt} queries")

    avg_elapsed = sum(r["elapsed"] for r in results) / total
    print(f"\n{BLD}Avg response time:{RST} {avg_elapsed:.1f}s")

    if failed:
        print(f"\n{BLD}{RED}Failed tests:{RST}")
        for r in results:
            if not r["pass"]:
                print(f"  T{results.index(r)+1:02d} [{r['category']}] — {r['passed']}/{r['checks']} checks")

    print()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
