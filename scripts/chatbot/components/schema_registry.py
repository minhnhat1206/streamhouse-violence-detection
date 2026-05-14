"""
Schema Registry — single source of truth for Streamhouse table schemas.

Used by the True Text-to-SQL generator to build accurate Gemini prompts
without hallucinating column names or catalog paths.
"""

from typing import Dict, Any

# ── Schema definitions ────────────────────────────────────────────────────────

SCHEMA_REGISTRY: Dict[str, Dict[str, Any]] = {
    "hot_violence_alerts": {
        "layer": "fluss",
        "catalog": "fluss",
        "flink_catalog": "fluss_hot",
        "columns": {
            "event_id":   {"type": "VARCHAR",       "nullable": False, "note": "Primary key"},
            "camera_id":  {"type": "VARCHAR",        "nullable": False, "note": "e.g. cam_01, cam_02"},
            "timestamp":  {"type": "TIMESTAMP(3)",   "nullable": False, "note": "Event time (UTC)"},
            "is_violent": {"type": "BOOLEAN",        "nullable": False, "note": "True = violence detected"},
            "risk_score": {"type": "DOUBLE",         "nullable": False, "note": "0.0 – 1.0"},
            "location":   {"type": "VARCHAR",        "nullable": True,  "note": "District / ward"},
            "event_type": {"type": "VARCHAR",        "nullable": True,  "note": "e.g. fighting, assault"},
        },
        "primary_key": "event_id",
        "retention": "1-2 hours",
        "note": "Real-time HOT layer. Only use for 'right now' / last-hour queries.",
        "timestamp_format": "TIMESTAMP '2026-05-14 10:00:00'",
    },
    "violence_incidents": {
        "layer": "paimon",
        "catalog": "paimon",
        "flink_catalog": "paimon_warm",
        "columns": {
            "incident_id": {"type": "VARCHAR",      "nullable": False, "note": "Primary key"},
            "camera_id":   {"type": "VARCHAR",      "nullable": False, "note": "e.g. cam_01, cam_02"},
            "timestamp":   {"type": "TIMESTAMP(3)", "nullable": False, "note": "Event time (UTC)"},
            "is_violent":  {"type": "BOOLEAN",      "nullable": False, "note": "True = violence detected"},
            "risk_score":  {"type": "DOUBLE",       "nullable": False, "note": "0.0 – 1.0"},
            "location":    {"type": "VARCHAR",      "nullable": True,  "note": "District / ward"},
            "event_type":  {"type": "VARCHAR",      "nullable": True,  "note": "e.g. fighting, assault"},
            "frame_url":   {"type": "VARCHAR",      "nullable": True,  "note": "MinIO evidence URL"},
        },
        "primary_key": "incident_id",
        "retention": "7-30 days",
        "note": "WARM layer — use for today/yesterday/this week queries. ACID + CDC via Flink.",
        "timestamp_format": "TIMESTAMP '2026-05-14 10:00:00'",
    },
    "historical_violence_incidents": {
        "layer": "iceberg",
        "catalog": "iceberg",
        "flink_catalog": None,
        "columns": {
            "incident_id": {"type": "VARCHAR",                  "nullable": False, "note": "Primary key"},
            "camera_id":   {"type": "VARCHAR",                  "nullable": False, "note": "e.g. cam_01"},
            "timestamp":   {"type": "TIMESTAMP(6) WITH TIME ZONE", "nullable": False, "note": "Event time (UTC)"},
            "is_violent":  {"type": "BOOLEAN",                  "nullable": False},
            "risk_score":  {"type": "DOUBLE",                   "nullable": False, "note": "0.0 – 1.0"},
            "location":    {"type": "VARCHAR",                  "nullable": True},
            "event_type":  {"type": "VARCHAR",                  "nullable": True},
        },
        "primary_key": "incident_id",
        "retention": "years",
        "note": "COLD layer — use for historical / monthly / yearly queries. Parquet, time-travel.",
        "timestamp_format": "TIMESTAMP '2026-05-14 10:00:00 UTC'",
    },
}


def get_schema_for_prompt(table_name: str) -> str:
    """Return a compact schema string ready to paste into a Gemini prompt.

    Includes column names, types, and notes — no hallucination needed.
    """
    schema = SCHEMA_REGISTRY.get(table_name)
    if not schema:
        return f"(no schema found for table '{table_name}')"

    lines = [
        f"Table: {table_name}",
        f"Layer: {schema['layer'].upper()} ({schema['note']})",
        f"Retention: {schema['retention']}",
        "",
        "Columns:",
    ]
    for col, meta in schema["columns"].items():
        nullable = "NULL" if meta.get("nullable") else "NOT NULL"
        note = f"  -- {meta['note']}" if meta.get("note") else ""
        lines.append(f"  {col}  {meta['type']}  {nullable}{note}")

    lines += [
        "",
        f"Primary key: {schema['primary_key']}",
        f"Timestamp literal format: {schema['timestamp_format']}",
    ]
    return "\n".join(lines)


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
