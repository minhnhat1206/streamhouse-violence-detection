"""
Chatbot Advanced Query Test Suite
===================================
Kiểm tra khả năng Text-to-SQL phức tạp, KHÔNG CHỈ thời gian:
  - Multi-dimension GROUP BY (camera + location + event_type)
  - Window functions, ranking, percentile
  - Cross-condition filters (risk_score + confidence + event_type)
  - Trend analysis, so sánh giữa các periods
  - Aggregation cascade (top-N within group)
  - Geospatial / ward-level aggregation
  - Comparative analysis (camera vs camera)
  - Anomaly / outlier detection intent
  - Time-series hourly breakdown
  - Count với multiple event_type
"""

import json
import sys
import time
import urllib.request
import urllib.error

CHATBOT_URL = "http://localhost:5002"
CHAT_ENDPOINT = f"{CHATBOT_URL}/chat"

GRN = "\033[92m"; RED = "\033[91m"; YLW = "\033[93m"
BLU = "\033[94m"; CYN = "\033[96m"; MAG = "\033[95m"; RST = "\033[0m"; BLD = "\033[1m"

def chat(query: str, timeout: int = 90) -> dict:
    payload = json.dumps({"query": query}).encode()
    req = urllib.request.Request(
        CHAT_ENDPOINT, data=payload,
        headers={"Content-Type": "application/json"}, method="POST",
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


def run_test(idx: int, category: str, query: str, expected: dict) -> dict:
    cat_color = {
        "SQL complexity": BLU, "Multi-dim": MAG, "Comparative": YLW,
        "Anomaly": RED, "Geospatial": CYN, "HOT fix": GRN,
    }.get(category, RST)

    print(f"\n{BLD}{cat_color}[T{idx:02d}] [{category}]{RST}")
    print(f"  {CYN}{query}{RST}")
    t0 = time.time()
    resp = chat(query)
    elapsed = time.time() - t0

    if "error" in resp:
        print(f"  {RED}ERROR:{RST} {resp['error']}")
        return {"pass": False, "error": resp["error"], "elapsed": elapsed}

    LAYER_MAP = {"fluss": "hot", "paimon": "warm", "iceberg": "cold"}
    raw_layer = (resp.get("layer") or "").strip()
    layer      = LAYER_MAP.get(raw_layer.lower(), raw_layer.lower())
    citations  = resp.get("citations") or {}
    sql        = resp.get("sql_used") or citations.get("sql_used") or ""
    rows_count = citations.get("row_count") or 0
    answer     = resp.get("answer", "") or ""
    confidence = resp.get("confidence", 0)

    sql_lower  = sql.lower()
    ans_lower  = answer.lower()

    print(f"  Layer: {YLW}{raw_layer}→{layer}{RST} | {elapsed:.1f}s | rows={rows_count} | conf={confidence:.2f}")
    print(f"  SQL  : {sql[:160].strip()}{'…' if len(sql)>160 else ''}")
    print(f"  Ans  : {answer[:300].strip()}{'…' if len(answer)>300 else ''}")

    passes = []
    for key, value in expected.items():
        if key == "layer":
            passes.append(check(layer == value, f"layer=={value} (got:{layer})"))
        elif key == "layer_in":
            passes.append(check(layer in value, f"layer∈{value} (got:{layer})"))
        elif key == "sql_has_all":
            for kw in value:
                passes.append(check(kw.lower() in sql_lower, f"SQL has '{kw}'"))
        elif key == "sql_has_any":
            found = any(kw.lower() in sql_lower for kw in value)
            passes.append(check(found, f"SQL has any of {value}"))
        elif key == "answer_has_any":
            found = any(v.lower() in ans_lower for v in value)
            passes.append(check(found, f"answer has any of {value}"))
        elif key == "answer_has_all":
            for v in value:
                passes.append(check(v.lower() in ans_lower, f"answer has '{v}'"))
        elif key == "no_error":
            bad = any(w in ans_lower for w in ["lỗi", "error", "thất bại", "failed", "exception"])
            passes.append(check(not bad, "no error phrase in answer"))
        elif key == "has_answer":
            passes.append(check(len(answer) > 20, f"answer non-trivial (len={len(answer)})"))
        elif key == "sql_not_has":
            for kw in value:
                passes.append(check(kw.lower() not in sql_lower, f"SQL NOT has '{kw}'"))
        elif key == "elapsed_lt":
            passes.append(check(elapsed < value, f"elapsed<{value}s (got:{elapsed:.1f}s)"))

    overall = all(passes)
    status = f"{GRN}PASS{RST}" if overall else f"{RED}FAIL{RST}"
    print(f"  → {status} ({sum(passes)}/{len(passes)})")
    return {
        "pass": overall, "checks": len(passes), "passed": sum(passes),
        "layer": layer, "elapsed": elapsed, "rows": rows_count,
        "category": category, "query": query,
    }


TESTS = [
    # ── HOT ROUTING FIX ────────────────────────────────────────────────────────
    {
        "id": 1, "category": "HOT fix",
        "query": "Cho tôi xem các cảnh báo vừa xảy ra ngay bây giờ",
        "expected": {
            "layer": "hot",
            "has_answer": True,
            "no_error": True,
        },
    },
    {
        "id": 2, "category": "HOT fix",
        "query": "Liệt kê các sự kiện trong 60 phút vừa rồi",
        "expected": {
            "layer": "hot",
            "has_answer": True,
        },
    },

    # ── SQL COMPLEXITY: GROUP BY nhiều chiều ───────────────────────────────────
    {
        "id": 3, "category": "Multi-dim",
        "query": "Thống kê số vụ bạo lực theo từng loại sự kiện (event_type) trong 7 ngày qua",
        "expected": {
            "layer": "warm",
            "sql_has_all": ["event_type", "count"],
            "sql_has_any": ["group by", "GROUP BY"],
            "has_answer": True,
            "no_error": True,
        },
    },
    {
        "id": 4, "category": "Multi-dim",
        "query": "Mỗi camera ghi nhận bao nhiêu vụ FIGHTING và bao nhiêu vụ ASSAULT trong 7 ngày qua?",
        "expected": {
            "layer": "warm",
            "sql_has_any": ["fighting", "FIGHTING", "assault", "ASSAULT", "event_type"],
            "sql_has_all": ["camera_id", "count"],
            "has_answer": True,
            "no_error": True,
        },
    },
    {
        "id": 5, "category": "SQL complexity",
        "query": "Tỷ lệ phần trăm các vụ có risk_score > 0.7 trên tổng số vụ trong 7 ngày qua",
        "expected": {
            "layer": "warm",
            "sql_has_any": ["0.7", "risk_score", "/ count", "* 100", "percent", "ratio"],
            "has_answer": True,
            "no_error": True,
        },
    },
    {
        "id": 6, "category": "SQL complexity",
        "query": "Top 3 camera có số vụ bạo lực cao nhất và risk score trung bình theo thứ tự giảm dần trong 7 ngày qua",
        "expected": {
            "layer": "warm",
            "sql_has_all": ["avg", "count"],
            "sql_has_any": ["limit 3", "LIMIT 3", "order by", "ORDER BY"],
            "has_answer": True,
            "answer_has_any": ["cam_", "1.", "2.", "3."],
            "no_error": True,
        },
    },

    # ── COMPARATIVE ANALYSIS ───────────────────────────────────────────────────
    {
        "id": 7, "category": "Comparative",
        "query": "So sánh số vụ bạo lực giữa các phường (ward) trong 7 ngày qua, phường nào nguy hiểm nhất?",
        "expected": {
            "layer": "warm",
            "sql_has_any": ["ward", "ward_id", "location", "phường"],
            "sql_has_any": ["group by", "GROUP BY"],
            "has_answer": True,
            "no_error": True,
        },
    },
    {
        "id": 8, "category": "Comparative",
        "query": "Camera cam_03 và cam_05 cái nào có tổng risk score cao hơn trong 7 ngày qua?",
        "expected": {
            "layer": "warm",
            "sql_has_any": ["cam_03", "cam_05", "camera_id"],
            "has_answer": True,
            "answer_has_any": ["cam_03", "cam_05", "cao hơn", "lớn hơn"],
            "no_error": True,
        },
    },

    # ── TIME-SERIES BREAKDOWN ──────────────────────────────────────────────────
    {
        "id": 9, "category": "Multi-dim",
        "query": "Phân bổ số vụ bạo lực theo từng giờ trong ngày 26/05/2026",
        "expected": {
            "layer_in": ["warm", "cold"],
            "sql_has_any": ["hour", "extract", "date_part", "HOUR", "EXTRACT"],
            "sql_has_any": ["2026-05-26", "05-26", "26"],
            "has_answer": True,
            "no_error": True,
        },
    },
    {
        "id": 10, "category": "SQL complexity",
        "query": "Confidence score trung bình và độ lệch chuẩn của các vụ bạo lực trong 7 ngày qua",
        "expected": {
            "layer": "warm",
            "sql_has_any": ["avg", "AVG", "stddev", "STDDEV", "variance", "confidence"],
            "has_answer": True,
            "no_error": True,
        },
    },

    # ── ANOMALY / OUTLIER ──────────────────────────────────────────────────────
    {
        "id": 11, "category": "Anomaly",
        "query": "Camera nào có risk_score đột biến (cao hơn 2 lần trung bình tổng thể) trong 7 ngày qua?",
        "expected": {
            "layer": "warm",
            "sql_has_any": ["avg(", "AVG(", "having", "HAVING", "> 2"],
            "has_answer": True,
            "no_error": True,
        },
    },
    {
        "id": 12, "category": "Anomaly",
        "query": "Tìm các camera không hoạt động (0 sự kiện) trong 7 ngày qua nhưng có lịch sử trước đó",
        "expected": {
            "layer_in": ["warm", "cold"],
            "has_answer": True,
            "no_error": True,
        },
    },

    # ── GEOSPATIAL ────────────────────────────────────────────────────────────
    {
        "id": 13, "category": "Geospatial",
        "query": "Thống kê số vụ bạo lực theo từng quận (district) và phường trong 7 ngày qua",
        "expected": {
            "layer": "warm",
            "sql_has_any": ["district", "ward", "location"],
            "sql_has_any": ["group by", "GROUP BY"],
            "has_answer": True,
            "no_error": True,
        },
    },
    {
        "id": 14, "category": "Geospatial",
        "query": "Đường nào (street/location) có mật độ bạo lực cao nhất tính theo vụ/camera trong 7 ngày qua?",
        "expected": {
            "layer": "warm",
            "sql_has_any": ["location", "count", "COUNT"],
            "has_answer": True,
            "no_error": True,
        },
    },

    # ── COLD HISTORICAL ────────────────────────────────────────────────────────
    {
        "id": 15, "category": "SQL complexity",
        "query": "Trong lịch sử, tháng nào năm 2026 có tổng số vụ bạo lực nhiều nhất?",
        "expected": {
            "layer": "cold",
            "sql_has_any": ["month", "MONTH", "date_trunc", "DATE_TRUNC"],
            "has_answer": True,
            "no_error": True,
        },
    },
]


def main():
    print(f"\n{BLD}{'='*70}{RST}")
    print(f"{BLD}  Advanced Text-to-SQL Test Suite — Streamhouse Chatbot{RST}")
    print(f"{BLD}  15 queries: multi-dim, comparative, anomaly, geospatial, complex SQL{RST}")
    print(f"{BLD}{'='*70}{RST}")

    try:
        with urllib.request.urlopen(f"{CHATBOT_URL}/health", timeout=5) as r:
            print(f"\n{GRN}✓ Chatbot healthy{RST}\n")
    except Exception as e:
        print(f"{RED}✗ Chatbot unreachable: {e}{RST}")
        sys.exit(1)

    results = []
    for t in TESTS:
        res = run_test(t["id"], t["category"], t["query"], t["expected"])
        results.append(res)

    total  = len(results)
    passed = sum(1 for r in results if r["pass"])
    failed = total - passed

    # Category summary
    from collections import defaultdict
    by_cat = defaultdict(list)
    for r in results:
        by_cat[r["category"]].append(r)

    print(f"\n{BLD}{'='*70}{RST}")
    print(f"{BLD}  RESULTS: {GRN}{passed} PASS{RST} / {RED}{failed} FAIL{RST} / {total} TOTAL{RST}")
    print(f"{BLD}{'='*70}{RST}")
    print(f"\n{BLD}By category:{RST}")
    for cat, rs in sorted(by_cat.items()):
        p = sum(1 for r in rs if r["pass"])
        icon = GRN if p == len(rs) else YLW if p > 0 else RED
        print(f"  {icon}{cat:20s}{RST}: {p}/{len(rs)}")

    avg_elapsed = sum(r["elapsed"] for r in results) / total
    print(f"\n{BLD}Avg response time:{RST} {avg_elapsed:.1f}s")

    if failed:
        print(f"\n{BLD}{RED}Failed tests:{RST}")
        for r in results:
            if not r["pass"]:
                print(f"  T{results.index(r)+1:02d} [{r['category']}] {r['query'][:60]}…")
                print(f"       {r['passed']}/{r['checks']} checks, layer={r['layer']}")
    print()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
