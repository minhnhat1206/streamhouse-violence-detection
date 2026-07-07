"""
Schema Registry — nguồn schema cho True Text-to-SQL (schema v2).

2 lớp:
  1. STATIC_REGISTRY  — fallback khi chưa introspect được (mô tả + routing notes).
  2. refresh_from_trino() — introspect SHOW COLUMNS lúc startup để cột LUÔN khớp
     schema thật (không còn hardcode chết như bản v1 — thêm bảng/cột là chatbot tự thấy).

Quy ước bảng v2:
  - Đếm/thống kê SỐ VỤ  → *_incident(s) grain=1 vụ (đã sessionize theo incident_uid)
  - Chi tiết / evidence → bảng event grain (frame_url, people_json)
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

# ── Static definitions (fallback + notes cho prompt) ─────────────────────────

STATIC_REGISTRY: Dict[str, Dict[str, Any]] = {
    # ── HOT (Fluss) ──────────────────────────────────────────────────────────
    "hot_violence_incidents": {
        "layer": "fluss",
        "catalog": "fluss",
        "grain": "incident",
        "columns": {
            "incident_uid":   {"type": "VARCHAR",      "note": "Primary key — 1 dòng = 1 VỤ"},
            "camera_id":      {"type": "VARCHAR",      "note": "e.g. cam_01"},
            "start_ts":       {"type": "TIMESTAMP(3)", "note": "Vụ bắt đầu (UTC)"},
            "last_ts":        {"type": "TIMESTAMP(3)", "note": "Event cuối cùng của vụ"},
            "event_count":    {"type": "BIGINT",       "note": "Số event thô trong vụ"},
            "max_risk_score": {"type": "DOUBLE",       "note": "0.0 – 1.0"},
            "avg_confidence": {"type": "DOUBLE"},
            "event_type":     {"type": "VARCHAR",      "note": "FIGHTING/ASSAULT/..."},
            "location":       {"type": "VARCHAR",      "note": "Tên đường"},
            "ward_id":        {"type": "VARCHAR"},
            "district":       {"type": "VARCHAR"},
            "people_count":   {"type": "INT",          "note": "Số người (bbox)"},
        },
        "primary_key": "incident_uid",
        "retention": "1-2 hours",
        "note": "HOT — ĐẾM SỐ VỤ realtime dùng bảng này (1 dòng = 1 vụ). "
                "KHÔNG đếm hot_violence_alerts (event thô 0.5s/lần).",
        "timestamp_column": "start_ts",
    },
    "hot_violence_alerts": {
        "layer": "fluss",
        "catalog": "fluss",
        "grain": "event",
        "columns": {
            "incident_id":  {"type": "VARCHAR",      "note": "Primary key (event)"},
            "incident_uid": {"type": "VARCHAR",      "note": "Vụ chứa event này"},
            "camera_id":    {"type": "VARCHAR"},
            "timestamp":    {"type": "TIMESTAMP(3)", "note": "Event time (UTC)"},
            "risk_score":   {"type": "DOUBLE"},
            "confidence":   {"type": "DOUBLE"},
            "is_violent":   {"type": "BOOLEAN"},
            "event_type":   {"type": "VARCHAR"},
            "location":     {"type": "VARCHAR"},
            "ward_id":      {"type": "VARCHAR"},
            "district":     {"type": "VARCHAR"},
            "people_count": {"type": "INT"},
        },
        "primary_key": "incident_id",
        "retention": "1-2 hours",
        "note": "HOT event thô (0.5s/event khi violent) — chỉ dùng xem chi tiết, KHÔNG đếm vụ.",
        "timestamp_column": "timestamp",
    },
    # ── WARM (Paimon) ────────────────────────────────────────────────────────
    "fact_violence_incident": {
        "layer": "paimon",
        "catalog": "paimon",
        "grain": "incident",
        "columns": {
            "incident_id":    {"type": "VARCHAR",      "note": "Primary key — 1 dòng = 1 VỤ"},
            "camera_id":      {"type": "VARCHAR"},
            "date_id":        {"type": "DATE",         "note": "Ngày vụ bắt đầu"},
            "time_id":        {"type": "INT",          "note": "Giờ 0-23"},
            "event_type_id":  {"type": "INT",          "note": "join dim_event_type"},
            "start_ts":       {"type": "TIMESTAMP(3)"},
            "end_ts":         {"type": "TIMESTAMP(3)"},
            "duration_sec":   {"type": "INT"},
            "event_count":    {"type": "BIGINT"},
            "max_risk_score": {"type": "DOUBLE"},
            "avg_confidence": {"type": "DOUBLE"},
            "is_violent":     {"type": "BOOLEAN"},
            "people_count":   {"type": "INT"},
            "frame_url":      {"type": "VARCHAR",      "note": "Ảnh PEAK có bounding box (MinIO)"},
        },
        "primary_key": "incident_id",
        "retention": "7-30 days",
        "note": "WARM FACT (star schema v2) — ĐẾM/THỐNG KÊ SỐ VỤ hôm nay/tuần này dùng bảng này. "
                "Location lấy qua JOIN dim_camera ON camera_id (cột street/ward/district).",
        "timestamp_column": "start_ts",
    },
    "violence_incidents": {
        "layer": "paimon",
        "catalog": "paimon",
        "grain": "event",
        "columns": {
            "incident_id":  {"type": "VARCHAR",      "note": "Primary key (event)"},
            "incident_uid": {"type": "VARCHAR",      "note": "Vụ chứa event này"},
            "camera_id":    {"type": "VARCHAR"},
            "timestamp":    {"type": "TIMESTAMP(3)"},
            "risk_score":   {"type": "DOUBLE"},
            "confidence":   {"type": "DOUBLE"},
            "is_violent":   {"type": "BOOLEAN"},
            "event_type":   {"type": "VARCHAR"},
            "location":     {"type": "VARCHAR"},
            "frame_url":    {"type": "VARCHAR",      "note": "Ảnh evidence có bbox (MinIO)"},
            "people_json":  {"type": "VARCHAR",      "note": "Toạ độ bbox từng người"},
            "people_count": {"type": "INT"},
        },
        "primary_key": "incident_id",
        "retention": "7-30 days",
        "note": "WARM event thô — dùng cho câu hỏi evidence/ảnh/chi tiết. KHÔNG đếm vụ ở đây.",
        "timestamp_column": "timestamp",
    },
    "dim_camera": {
        "layer": "paimon",
        "catalog": "paimon",
        "grain": "dimension",
        "columns": {
            "camera_id":  {"type": "VARCHAR"},
            "street":     {"type": "VARCHAR", "note": "Tên đường"},
            "ward":       {"type": "VARCHAR"},
            "district":   {"type": "VARCHAR"},
            "city":       {"type": "VARCHAR"},
            "latitude":   {"type": "DOUBLE"},
            "longitude":  {"type": "DOUBLE"},
            "is_current": {"type": "BOOLEAN", "note": "SCD2 — lọc is_current=true"},
        },
        "primary_key": "camera_id, valid_from",
        "retention": "static",
        "note": "DIM camera (SCD2). JOIN với fact để lấy địa điểm.",
        "timestamp_column": None,
    },
    # ── COLD (Iceberg) ───────────────────────────────────────────────────────
    "historical_incident_facts": {
        "layer": "iceberg",
        "catalog": "iceberg",
        "grain": "incident",
        "columns": {
            "incident_id":    {"type": "VARCHAR", "note": "Primary key — 1 dòng = 1 VỤ"},
            "camera_id":      {"type": "VARCHAR"},
            "date_id":        {"type": "DATE"},
            "time_id":        {"type": "INT"},
            "start_ts":       {"type": "TIMESTAMP(6)"},
            "end_ts":         {"type": "TIMESTAMP(6)"},
            "duration_sec":   {"type": "INT"},
            "event_count":    {"type": "BIGINT"},
            "max_risk_score": {"type": "DOUBLE"},
            "is_violent":     {"type": "BOOLEAN"},
            "people_count":   {"type": "INT"},
            "frame_url":      {"type": "VARCHAR"},
        },
        "primary_key": "incident_id",
        "retention": "years",
        "note": "COLD FACT — đếm/thống kê SỐ VỤ lịch sử (tháng/năm) dùng bảng này.",
        "timestamp_column": "start_ts",
    },
    "historical_violence_incidents": {
        "layer": "iceberg",
        "catalog": "iceberg",
        "grain": "event",
        "columns": {
            "incident_id": {"type": "VARCHAR"},
            "camera_id":   {"type": "VARCHAR"},
            "timestamp":   {"type": "TIMESTAMP(6) WITH TIME ZONE"},
            "risk_score":  {"type": "DOUBLE"},
            "confidence":  {"type": "DOUBLE"},
            "is_violent":  {"type": "BOOLEAN"},
            "event_type":  {"type": "VARCHAR"},
            "location":    {"type": "VARCHAR"},
        },
        "primary_key": "incident_id",
        "retention": "years",
        "note": "COLD event thô — chỉ dùng chi tiết lịch sử, KHÔNG đếm vụ.",
        "timestamp_column": "timestamp",
    },
}

# Registry đang hoạt động (bắt đầu = static, được refresh_from_trino cập nhật cột)
SCHEMA_REGISTRY: Dict[str, Dict[str, Any]] = {
    k: {**v, "columns": dict(v["columns"])} for k, v in STATIC_REGISTRY.items()
}


# ── Table routing: chọn bảng theo (layer, mục đích) ──────────────────────────

def table_for(layer: str, purpose: str = "count") -> str:
    """Chọn bảng đúng grain: purpose='count'/'stats' → incident; 'detail'/'evidence' → event."""
    layer = (layer or "").lower()
    incident = purpose in ("count", "stats", "aggregate")
    if "fluss" in layer or layer == "hot":
        return "hot_violence_incidents" if incident else "hot_violence_alerts"
    if "iceberg" in layer or layer == "cold":
        return "historical_incident_facts" if incident else "historical_violence_incidents"
    # default WARM/Paimon
    return "fact_violence_incident" if incident else "violence_incidents"


# ── Dynamic introspection ─────────────────────────────────────────────────────

def refresh_from_trino(trino_query_fn) -> int:
    """Introspect cột thật từ Trino (SHOW COLUMNS) để registry không bao giờ lệch schema.

    Args:
        trino_query_fn: callable(sql: str) -> list[tuple] (rows).
    Returns: số bảng refresh thành công.
    """
    refreshed = 0
    for table, meta in SCHEMA_REGISTRY.items():
        if meta["catalog"] == "fluss":
            continue  # Fluss không qua Trino — giữ static (đã khớp DDL init v2)
        try:
            rows = trino_query_fn(
                f"SHOW COLUMNS FROM {meta['catalog']}.security.{table}"
            )
            if not rows:
                continue
            live_cols = {}
            for r in rows:
                col, ctype = str(r[0]), str(r[1]).upper()
                old = meta["columns"].get(col, {})
                live_cols[col] = {"type": ctype, **({"note": old["note"]} if old.get("note") else {})}
            meta["columns"] = live_cols
            refreshed += 1
        except Exception as e:
            logger.warning("schema refresh skipped for %s: %s", table, str(e)[:120])
    logger.info("Schema registry refreshed from Trino: %d tables", refreshed)
    return refreshed


# ── Prompt helpers ────────────────────────────────────────────────────────────

def get_schema_for_prompt(table_name: str) -> str:
    """Compact schema string cho prompt Gemini — cột lấy từ registry (đã introspect)."""
    schema = SCHEMA_REGISTRY.get(table_name)
    if not schema:
        return f"(no schema found for table '{table_name}')"

    lines = [
        f"Table: {table_name}",
        f"Layer: {schema['layer'].upper()} | Grain: {schema['grain']} | Retention: {schema['retention']}",
        f"Note: {schema['note']}",
        "",
        "Columns:",
    ]
    for col, meta in schema["columns"].items():
        note = f"  -- {meta['note']}" if meta.get("note") else ""
        lines.append(f"  {col}  {meta['type']}{note}")

    lines += ["", f"Primary key: {schema['primary_key']}"]
    if schema.get("timestamp_column"):
        lines.append(f"Time filter column: \"{schema['timestamp_column']}\"")
    return "\n".join(lines)


def get_all_schemas_for_prompt() -> str:
    """Toàn bộ schema (rút gọn) — dùng cho prompt 1-call (intent + layer + SQL)."""
    parts = []
    for name, meta in SCHEMA_REGISTRY.items():
        if meta["grain"] == "dimension":
            continue
        cols = ", ".join(meta["columns"].keys())
        parts.append(
            f"- {meta['catalog']}.security.{name} [{meta['layer'].upper()}, "
            f"grain={meta['grain']}, retention={meta['retention']}]\n"
            f"  cột: {cols}\n  {meta['note']}"
        )
    return "\n".join(parts)


def get_full_table_ref(table_name: str, schema: str = "security") -> str:
    """Return catalog.schema.table for use in SQL FROM clause."""
    reg = SCHEMA_REGISTRY.get(table_name)
    if not reg:
        return table_name
    return f"{reg['catalog']}.{schema}.{table_name}"


def column_names(table_name: str) -> list:
    """Return list of column names for a registered table."""
    reg = SCHEMA_REGISTRY.get(table_name)
    if not reg:
        return []
    return list(reg["columns"].keys())


def timestamp_column(table_name: str) -> str:
    reg = SCHEMA_REGISTRY.get(table_name) or {}
    return reg.get("timestamp_column") or "timestamp"
