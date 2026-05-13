"""
Vigilance AI Chatbot Backend — FastAPI service (port 5002)  v2.0
Provides REST endpoints for the vigilance-ai dashboard:
  GET  /api/recent-incidents  — query Iceberg via Trino (includes frame_url from MinIO)
  GET  /api/evidence          — retrieve evidence images from MinIO by camera+date
  GET  /api/stats             — aggregated analytics from Iceberg via Trino
  POST /api/chat              — Agentic RAG: Text-to-SQL (Gemini) + 3-layer routing
                                  HOT  → Fluss via Flink SQL Gateway
                                  WARM → Paimon via Flink SQL Gateway
                                  COLD → Iceberg via Trino
"""

import os
import re
import json
import asyncio
import logging
import time
import xml.etree.ElementTree as ET
from typing import Optional

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Vigilance AI Chatbot", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Environment / connection config
# ---------------------------------------------------------------------------
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8082"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

MINIO_INTERNAL = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_EXTERNAL = os.getenv("MINIO_EXTERNAL_URL", "http://localhost:9000")
EVIDENCE_BUCKET = "evidence-frames"

TRINO_BASE = f"http://{TRINO_HOST}:{TRINO_PORT}"

# Flink SQL Gateway — HOT (Fluss) and WARM (Paimon) layers
FLINK_GATEWAY_HOST = os.getenv("FLINK_GATEWAY_HOST", "flink-sql-gateway")
FLINK_GATEWAY_PORT = int(os.getenv("FLINK_GATEWAY_PORT", "8083"))
FLINK_GATEWAY_BASE = f"http://{FLINK_GATEWAY_HOST}:{FLINK_GATEWAY_PORT}"

# MinIO / S3 credentials — needed for Paimon catalog DDL
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")
MINIO_S3_ENDPOINT = os.getenv("MINIO_S3_ENDPOINT", "http://minio:9000")

LAYER_TIMEOUT: dict[str, float] = {
    "hot":  60.0,   # Fluss — near-realtime (60s includes catalog init ~12s + query)
    "warm": 360.0,  # Paimon — Flink batch on MinIO ~3-5 min
    "cold": 30.0,   # Iceberg via Trino — fast
}

# Paimon catalog DDL — run once per Flink Gateway session before WARM queries
_PAIMON_CATALOG_DDL = """CREATE CATALOG paimon_warm WITH (
  'type'                  = 'paimon',
  'warehouse'             = 's3://warehouse/paimon',
  's3.endpoint'           = '{s3_endpoint}',
  's3.access-key'         = '{access_key}',
  's3.secret-key'         = '{secret_key}',
  's3.path.style.access'  = 'true'
)"""

# Fluss catalog DDL — run once per Flink Gateway session before HOT queries
_FLUSS_CATALOG_DDL = (
    "CREATE CATALOG fluss WITH ("
    "'type' = 'fluss', "
    "'bootstrap.servers' = 'fluss-coordinator:9123'"
    ")"
)

GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.5-flash:generateContent"
)


# ---------------------------------------------------------------------------
# Schema Registry — injected into Gemini Text-to-SQL prompt
# ---------------------------------------------------------------------------
SCHEMA_FOR_PROMPT = """
## Vigilance AI — Streamhouse Tables

### 1. iceberg.security.historical_violence_incidents  [ENGINE: Trino]
Use for: historical data, long-term analysis, queries > 30 days
Columns:
  incident_id STRING, camera_id STRING (cam_01..cam_15),
  timestamp TIMESTAMP, risk_score DOUBLE (0-1), confidence DOUBLE (0-1),
  is_violent BOOLEAN, event_type STRING, location VARCHAR, incident_date DATE
SQL dialect (Trino):
  - Time filter : timestamp >= NOW() - INTERVAL 'N' DAY
  - Partition   : incident_date >= CURRENT_DATE - INTERVAL 'N' DAY  ← REQUIRED for performance
  - Date format : format_datetime(timestamp, 'yyyy-MM-dd HH:mm')
  - Location    : json_extract_scalar(CAST(location AS VARCHAR), '$.street')
  - event_type  : FIGHTING, ASSAULT, STABBING, SHOOTING (null if is_violent=false)
Example:
  SELECT camera_id, COUNT(*) cnt
  FROM iceberg.security.historical_violence_incidents
  WHERE is_violent = true
    AND incident_date >= CURRENT_DATE - INTERVAL '30' DAY
  GROUP BY camera_id ORDER BY cnt DESC LIMIT 10

### 2. paimon.security.daily_incident_stats  [ENGINE: Flink SQL Gateway]
← PREFER THIS for "hôm nay", "tuần này", daily/weekly aggregate questions
Columns:
  stat_date DATE, location STRING,
  total_incidents BIGINT, violent_incidents BIGINT,
  avg_risk_score DOUBLE, max_risk_score DOUBLE
SQL dialect (Flink SQL):
  - Time filter : stat_date >= CURRENT_DATE - INTERVAL '7' DAY
Example:
  SELECT stat_date, SUM(violent_incidents) AS total_violent
  FROM paimon.security.daily_incident_stats
  WHERE stat_date >= CURRENT_DATE - INTERVAL '7' DAY
  GROUP BY stat_date ORDER BY stat_date DESC LIMIT 7

### 3. paimon.security.camera_stats  [ENGINE: Flink SQL Gateway]
← PREFER THIS for "camera nào nhiều nhất", per-camera ranking
Columns:
  stat_date DATE, camera_id STRING,
  total_incidents BIGINT, violent_incidents BIGINT,
  avg_risk_score DOUBLE, avg_confidence DOUBLE
Example:
  SELECT camera_id, SUM(violent_incidents) AS cnt
  FROM paimon.security.camera_stats
  WHERE stat_date >= CURRENT_DATE - INTERVAL '7' DAY
  GROUP BY camera_id ORDER BY cnt DESC LIMIT 5

### 4. paimon.security.violence_incidents  [ENGINE: Flink SQL Gateway]
Use for: recent incident details, frame_url access
Columns:
  incident_id STRING, camera_id STRING, timestamp TIMESTAMP,
  risk_score DOUBLE, confidence DOUBLE, is_violent BOOLEAN,
  event_type STRING, location STRING, is_deleted BOOLEAN,
  frame_url STRING, frame_capture_ts BIGINT
Note: always add: is_deleted = false OR is_deleted IS NULL
Example:
  SELECT incident_id, camera_id, timestamp, risk_score, event_type
  FROM paimon.security.violence_incidents
  WHERE is_violent = true AND (is_deleted = false OR is_deleted IS NULL)
    AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
  ORDER BY timestamp DESC LIMIT 10

### 5. fluss.security.hot_violence_alerts  [ENGINE: Flink SQL Gateway]
Use for: REAL-TIME last 1-2 hours ONLY
Columns:
  incident_id STRING, camera_id STRING, timestamp TIMESTAMP(3),
  risk_score DOUBLE, confidence DOUBLE, is_violent BOOLEAN, event_type STRING
⚠ NO location column — do NOT reference location here
Example:
  SELECT camera_id, COUNT(*) cnt, AVG(risk_score) avg_score
  FROM fluss.security.hot_violence_alerts
  WHERE is_violent = true
    AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '60' MINUTE
  GROUP BY camera_id ORDER BY cnt DESC LIMIT 5

## SQL Generation Rules
1. ONLY SELECT statements — never INSERT/UPDATE/DELETE/DROP/CREATE/ALTER
2. Always LIMIT (max 100)
3. Match dialect: Trino syntax for iceberg tables; Flink SQL for paimon/fluss tables
4. Violence queries → WHERE is_violent = true
5. Prefer daily_incident_stats / camera_stats over scanning violence_incidents
6. Iceberg queries → always include incident_date filter for partition pruning
7. Fluss queries → NEVER use location column
"""

# ---------------------------------------------------------------------------
# Fallback SQL (when Gemini fails or generates invalid SQL)
# ---------------------------------------------------------------------------
FALLBACK_SQL: dict[str, str] = {
    "recent_cold": """
        SELECT
            incident_id, camera_id,
            CAST(timestamp AS VARCHAR) AS ts,
            risk_score,
            COALESCE(event_type, 'Anomaly') AS event_type,
            json_extract_scalar(CAST(location AS VARCHAR), '$.street') AS street
        FROM iceberg.security.historical_violence_incidents
        WHERE is_violent = true
          AND incident_date >= CURRENT_DATE - INTERVAL '30' DAY
        ORDER BY timestamp DESC
        LIMIT 20
    """,
    "recent_hot": """
        SELECT
            incident_id, camera_id,
            CAST(timestamp AS VARCHAR) AS ts,
            risk_score, confidence, is_violent, event_type
        FROM fluss.security.hot_violence_alerts
        WHERE is_violent = true
        ORDER BY timestamp DESC
        LIMIT 10
    """,
    "count_today": """
        SELECT stat_date,
               SUM(total_incidents)   AS total,
               SUM(violent_incidents) AS violent,
               AVG(avg_risk_score)    AS avg_score
        FROM paimon.security.daily_incident_stats
        WHERE stat_date >= CURRENT_DATE - INTERVAL '7' DAY
        GROUP BY stat_date
        ORDER BY stat_date DESC
        LIMIT 7
    """,
    "top_cameras": """
        SELECT camera_id,
               SUM(violent_incidents) AS cnt,
               AVG(avg_risk_score)    AS avg_score
        FROM paimon.security.camera_stats
        WHERE stat_date >= CURRENT_DATE - INTERVAL '7' DAY
        GROUP BY camera_id
        ORDER BY cnt DESC
        LIMIT 10
    """,
}

# ---------------------------------------------------------------------------
# SQL Validation
# ---------------------------------------------------------------------------
ALLOWED_TABLES = {
    "iceberg.security.historical_violence_incidents",
    "paimon.security.violence_incidents",
    "paimon.security.daily_incident_stats",
    "paimon.security.camera_stats",
    "fluss.security.hot_violence_alerts",
}

_FORBIDDEN_KW = re.compile(
    r"\b(DROP|DELETE|INSERT|UPDATE|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|EXEC|EXECUTE)\b",
    re.IGNORECASE,
)


def _validate_sql(sql: str) -> tuple[bool, str]:
    """Return (is_valid, reason). Blocks dangerous SQL from Gemini hallucinations."""
    if not sql or not sql.strip():
        return False, "Empty SQL"

    sql_strip = sql.strip()
    if not re.match(r"^\s*SELECT\b", sql_strip, re.IGNORECASE):
        return False, "Only SELECT statements allowed"

    m = _FORBIDDEN_KW.search(sql_strip)
    if m:
        return False, f"Forbidden keyword: {m.group()}"

    limit_m = re.search(r"\bLIMIT\s+(\d+)", sql_strip, re.IGNORECASE)
    if limit_m and int(limit_m.group(1)) > 500:
        return False, "LIMIT exceeds 500"

    # Extract table references from FROM / JOIN
    from_tables = re.findall(r"\bFROM\s+([\w.]+)", sql_strip, re.IGNORECASE)
    join_tables = re.findall(r"\bJOIN\s+([\w.]+)", sql_strip, re.IGNORECASE)
    all_tables = from_tables + join_tables

    # Require at least one table reference
    if not all_tables:
        return False, "No table reference found"

    for tbl in all_tables:
        if tbl.lower() not in ALLOWED_TABLES:
            return False, f"Unknown table: {tbl}"

    return True, "OK"


def _adapt_sql_for_flink(sql: str, layer: str = "warm") -> str:
    """
    Adapt SQL for Flink SQL Gateway execution (WARM/HOT layers).

    Changes applied:
    1. Strip catalog prefixes — session runs USE CATALOG + USE <db>
    2. Remap table aliases:
       - WARM: hot_violence_alerts / historical_violence_incidents → violence_incidents (Paimon table)
       - HOT:  keep hot_violence_alerts as-is (Fluss table name); strip fluss. prefix
    3. Cast NOW()/CURRENT_TIMESTAMP → TIMESTAMP(3) to avoid TIMESTAMP_LTZ mismatch
    4. Replace double-quoted reserved keywords with backticks (Trino→Flink)
    5. Auto-backtick bare 'timestamp' column references (Flink reserved keyword)
    """
    adapted = sql

    if layer == "hot":
        # HOT: only strip fluss catalog prefix; keep table name hot_violence_alerts
        # Fluss catalog is registered as 'fluss' in the session init — keep that prefix
        # but strip 'fluss.security.' since we USE CATALOG fluss + USE security
        adapted = adapted.replace("fluss.security.", "")
        adapted = adapted.replace("fluss.", "")
        adapted = adapted.replace("historical_violence_incidents", "hot_violence_alerts")
    else:
        # 1. Strip catalog/schema prefixes (order matters: longer prefix first)
        for prefix in (
            "paimon.security.", "paimon.",
            "fluss.security.",  "fluss.",
            "iceberg.security.", "iceberg.",
        ):
            adapted = adapted.replace(prefix, "")

        # 2. Remap table aliases (WARM path)
        adapted = adapted.replace("hot_violence_alerts", "violence_incidents")
        adapted = adapted.replace("historical_violence_incidents", "violence_incidents")

    # 3. Fix TIMESTAMP_LTZ vs TIMESTAMP(3): replace with LOCALTIMESTAMP
    #    LOCALTIMESTAMP returns TIMESTAMP(3) in Flink (no LTZ) — safe for Fluss/Paimon comparisons
    adapted = re.sub(
        r"\bNOW\(\)",
        "LOCALTIMESTAMP",
        adapted, flags=re.IGNORECASE,
    )
    adapted = re.sub(
        r"\bCURRENT_TIMESTAMP\b",
        "LOCALTIMESTAMP",
        adapted, flags=re.IGNORECASE,
    )

    # 4. Trino double-quote reserved keywords → Flink backtick
    adapted = adapted.replace('"timestamp"', '`timestamp`')

    # 5. Auto-backtick bare 'timestamp' column references (reserved in Flink SQL).
    #    Guard: protect CURRENT_TIMESTAMP and TIMESTAMP(...) type tokens first.
    adapted = adapted.replace("CURRENT_TIMESTAMP", "__CURRENT_TS_PLACEHOLDER__")
    adapted = re.sub(r"\bTIMESTAMP\s*\(", "__TIMESTAMP_TYPE__(", adapted, flags=re.IGNORECASE)
    # Now quote any remaining bare `timestamp` token (column reference)
    adapted = re.sub(r"(?<!`)\btimestamp\b(?!`)", "`timestamp`", adapted, flags=re.IGNORECASE)
    # Restore guarded tokens
    adapted = adapted.replace("__CURRENT_TS_PLACEHOLDER__", "CURRENT_TIMESTAMP")
    adapted = adapted.replace("__TIMESTAMP_TYPE__(", "TIMESTAMP(")

    return adapted


def _adapt_sql_to_iceberg(sql: str) -> str:
    """
    Rewrite SQL targeting Paimon/Fluss tables to use Iceberg equivalents.
    Used as the COLD fallback when HOT/WARM layer is unavailable.

    Aggregate tables (daily_incident_stats, camera_stats) are rewritten
    as inline aggregations over iceberg.security.historical_violence_incidents.
    """
    RAW = "iceberg.security.historical_violence_incidents"
    adapted = sql

    # Map all table references to the Iceberg raw table
    for old, new in [
        ("paimon.security.violence_incidents",    RAW),
        ("paimon.security.daily_incident_stats",  RAW),
        ("paimon.security.camera_stats",          RAW),
        ("fluss.security.hot_violence_alerts",    RAW),
        ("fluss.security.violence_incidents",     RAW),
        # bare prefixes last
        ("paimon.security.", "iceberg.security."),
        ("paimon.",          "iceberg."),
        ("fluss.security.",  "iceberg.security."),
        ("fluss.",           "iceberg."),
    ]:
        adapted = adapted.replace(old, new)

    # Unqualified standalone table names
    for pattern, replacement in [
        (r"(?<![.\w])violence_incidents\b",      RAW),
        (r"(?<![.\w])hot_violence_alerts\b",     RAW),
        (r"(?<![.\w])daily_incident_stats\b",    RAW),
        (r"(?<![.\w])camera_stats\b",            RAW),
    ]:
        adapted = re.sub(pattern, replacement, adapted)

    # Rewrite queries that reference aggregate tables → inline aggregation
    for agg_table in ("daily_incident_stats", "camera_stats"):
        if not re.search(rf"\b{agg_table}\b", sql, re.IGNORECASE):
            continue
        interval_m = re.search(
            r"INTERVAL\s+'?(\d+)'?\s+(HOUR|DAY|MINUTE|MONTH|YEAR)",
            sql, re.IGNORECASE,
        )
        if interval_m:
            qty, unit = interval_m.group(1), interval_m.group(2).upper()
            time_filter = (
                f"timestamp >= NOW() - INTERVAL '{qty}' {unit}\n"
                f"  AND incident_date >= CURRENT_DATE - INTERVAL '{qty}' {unit}"
            )
        else:
            time_filter = "1=1"

        if agg_table == "camera_stats" or re.search(r"\bcamera_id\b", sql, re.IGNORECASE):
            adapted = (
                f"SELECT camera_id,\n"
                f"       COUNT(*) AS total_incidents,\n"
                f"       SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) AS violent_incidents,\n"
                f"       AVG(risk_score) AS avg_risk_score\n"
                f"FROM {RAW}\n"
                f"WHERE {time_filter}\n"
                f"GROUP BY camera_id\n"
                f"ORDER BY violent_incidents DESC\n"
                f"LIMIT 20"
            )
        else:
            adapted = (
                f"SELECT CAST(timestamp AS DATE) AS stat_date,\n"
                f"       COUNT(*) AS total_incidents,\n"
                f"       SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) AS violent_incidents,\n"
                f"       AVG(risk_score) AS avg_risk_score\n"
                f"FROM {RAW}\n"
                f"WHERE {time_filter}\n"
                f"GROUP BY CAST(timestamp AS DATE)\n"
                f"ORDER BY stat_date DESC\n"
                f"LIMIT 30"
            )
        break

    # Convert Flink-style backtick identifiers → Trino double-quote style
    # (Gemini may generate backtick-quoted columns based on schema hints)
    adapted = re.sub(r'`([^`]+)`', r'"\1"', adapted)

    return adapted


# ---------------------------------------------------------------------------
# MinIO helpers
# ---------------------------------------------------------------------------

async def _minio_list_objects(prefix: str, max_keys: int = 1) -> list[str]:
    """List object keys in evidence-frames bucket using S3 XML API."""
    url = f"http://{MINIO_INTERNAL}/{EVIDENCE_BUCKET}"
    params = {"list-type": "2", "prefix": prefix, "max-keys": str(max_keys)}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                return []
        root = ET.fromstring(resp.text)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        keys = [
            c.findtext("s3:Key", namespaces=ns) or c.findtext("Key", "")
            for c in root.findall(".//s3:Contents", ns) or root.findall(".//Contents")
        ]
        return [k for k in keys if k]
    except Exception as exc:
        logger.warning("MinIO list failed for prefix=%s: %s", prefix, exc)
        return []


async def _get_frame_url(camera_id: str, incident_date: str, incident_id: str) -> str | None:
    """Return a public MinIO URL for the evidence frame (3-level fallback)."""
    direct_key = f"{camera_id}/{incident_date}/{incident_id}.jpg"
    direct_url = f"{MINIO_EXTERNAL}/{EVIDENCE_BUCKET}/{direct_key}"
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            head = await client.head(direct_url)
            if head.status_code == 200:
                return direct_url
    except Exception:
        pass

    prefix = f"{camera_id}/{incident_date}/"
    keys = await _minio_list_objects(prefix, max_keys=1)
    if keys:
        return f"{MINIO_EXTERNAL}/{EVIDENCE_BUCKET}/{keys[0]}"

    prefix_any = f"{camera_id}/"
    keys_any = await _minio_list_objects(prefix_any, max_keys=1)
    if keys_any:
        img_keys = [k for k in keys_any if not k.endswith("/") and k.endswith(".jpg")]
        if img_keys:
            return f"{MINIO_EXTERNAL}/{EVIDENCE_BUCKET}/{img_keys[0]}"

    return None


# ---------------------------------------------------------------------------
# Trino query client (COLD layer)
# ---------------------------------------------------------------------------

async def _trino_query(sql: str, timeout: float = 30.0) -> list[list]:
    """Execute a Trino SQL statement and return all rows."""
    headers = {
        "X-Trino-User": TRINO_USER,
        "X-Trino-Catalog": "iceberg",
        "X-Trino-Schema": "security",
        "Content-Type": "text/plain",
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(f"{TRINO_BASE}/v1/statement", content=sql, headers=headers)
        resp.raise_for_status()
        body = resp.json()

        rows: list[list] = []
        next_uri = body.get("nextUri")
        if body.get("data"):
            rows.extend(body["data"])

        while next_uri:
            resp = await client.get(next_uri, headers=headers)
            resp.raise_for_status()
            body = resp.json()
            if body.get("data"):
                rows.extend(body["data"])
            next_uri = body.get("nextUri")
            if body.get("stats", {}).get("state") in ("FAILED", "CANCELED"):
                raise RuntimeError(
                    f"Trino query failed: {body.get('error', {}).get('message')}"
                )
    return rows


# ---------------------------------------------------------------------------
# Flink SQL Gateway client (HOT / WARM layers)
# ---------------------------------------------------------------------------

async def _exec_flink_statement(
    client: httpx.AsyncClient,
    session_id: str,
    sql: str,
    deadline: float,
) -> list[list]:
    """
    Submit one SQL statement to Flink Gateway and collect all rows.

    Poll strategy (from trino_client.py reference implementation):
    - Token-based pagination: result/0 → result/1 → ...
    - nextResultUri advances token when new data is available
    - nextResultUri stays at same token when query is still running (empty page)
    - resultType == 'EOS' signals bounded query finished
    - Streaming aggregates: 3 consecutive empty pages → values have converged
    - Collect INSERT + UPDATE_AFTER rows; UPDATE_AFTER overwrites the running total
    """
    resp = await client.post(
        f"{FLINK_GATEWAY_BASE}/v1/sessions/{session_id}/statements",
        json={"statement": sql},
    )
    resp.raise_for_status()
    op_handle = resp.json()["operationHandle"]

    result_token = 0
    all_rows: list[list] = []
    latest_agg_rows: list[list] = []
    columns: list[dict] = []
    stable_polls = 0
    exited_eos = False  # True = bounded query; False = streaming aggregate

    while time.time() < deadline:
        result_resp = await client.get(
            f"{FLINK_GATEWAY_BASE}/v1/sessions/{session_id}"
            f"/operations/{op_handle}/result/{result_token}"
        )
        result_resp.raise_for_status()
        data = result_resp.json()

        result_type = data.get("resultType", "NOT_READY")
        results_block = data.get("results", {})

        # Capture column metadata on first response
        if not columns and results_block.get("columns"):
            columns = results_block["columns"]

        page_rows: list[list] = []
        for raw_row in results_block.get("data", []):
            fields = raw_row.get("fields", []) if isinstance(raw_row, dict) else raw_row
            kind   = raw_row.get("kind", "INSERT") if isinstance(raw_row, dict) else "INSERT"
            if isinstance(fields, (list, tuple)) and kind in ("INSERT", "UPDATE_AFTER"):
                page_rows.append(list(fields))

        if page_rows:
            stable_polls = 0
            all_rows.extend(page_rows)
            latest_agg_rows = page_rows  # latest stable snapshot for streaming aggregates

        else:
            stable_polls += 1

        if result_type == "EOS":
            exited_eos = True
            break  # bounded query finished normally

        next_uri = data.get("nextResultUri")
        if next_uri:
            try:
                result_token = int(next_uri.rstrip("/").split("/")[-1])
            except (ValueError, IndexError):
                break
            continue  # fetch next page immediately (no sleep)

        if not data.get("isQueryRunning", result_type == "NOT_READY"):
            exited_eos = True
            break

        # 3 consecutive empty polls → streaming aggregate has converged
        if stable_polls >= 3 and latest_agg_rows:
            logger.info("Flink Gateway: aggregate stable after %d empty polls", stable_polls)
            break

        await asyncio.sleep(2)

    # For bounded queries (EOS): all_rows is correct (no duplicates).
    # For streaming aggregates: latest_agg_rows is the final stable snapshot —
    # using all_rows would include intermediate UPDATE_AFTER states per key.
    if exited_eos:
        return all_rows
    return latest_agg_rows if latest_agg_rows else all_rows


async def _flink_gateway_query(
    sql: str,
    timeout: float = 360.0,
    layer: str = "warm",
) -> list[list]:
    """
    Execute SQL via Flink SQL Gateway and return rows as list[list].

    For WARM (Paimon) queries, runs 3 init statements before the query:
      1. CREATE CATALOG paimon_warm  (register Paimon S3 catalog)
      2. USE CATALOG paimon_warm
      3. USE `security`

    For HOT (Fluss) queries, runs 3 init statements:
      1. CREATE CATALOG fluss  (register Fluss coordinator)
      2. USE CATALOG fluss
      3. USE `security`
    """
    adapted_sql = _adapt_sql_for_flink(sql, layer=layer)
    logger.info("Flink Gateway [%s] SQL: %s", layer.upper(), adapted_sql[:200])

    session_id: str | None = None
    deadline = time.time() + timeout

    # Layer-specific init statements (catalog registration per session)
    paimon_init: list[str] = []
    if layer == "warm":
        paimon_init = [
            _PAIMON_CATALOG_DDL.format(
                s3_endpoint=MINIO_S3_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
            ),
            "USE CATALOG paimon_warm",
            "USE `security`",
        ]
    elif layer == "hot":
        paimon_init = [
            _FLUSS_CATALOG_DDL,
            "USE CATALOG fluss",
            "USE `security`",
        ]

    try:
        async with httpx.AsyncClient(timeout=min(timeout, 60.0)) as client:

            # 1. Create session (always STREAMING — never BATCH)
            resp = await client.post(f"{FLINK_GATEWAY_BASE}/v1/sessions", json={})
            resp.raise_for_status()
            session_id = resp.json()["sessionHandle"]
            logger.info("Flink Gateway session: %s", session_id)

            # 2. Run init statements (catalog DDL, USE)
            # Use a simple fire-and-wait approach for DDL — avoids 500 errors
            # from rapid-fire complex polling on short-lived operations.
            for init_sql in paimon_init:
                r = await client.post(
                    f"{FLINK_GATEWAY_BASE}/v1/sessions/{session_id}/statements",
                    json={"statement": init_sql},
                )
                r.raise_for_status()
                op = r.json()["operationHandle"]
                # Poll result/0 until EOS or NOT_READY settles (max 30s)
                for _ in range(30):
                    r2 = await client.get(
                        f"{FLINK_GATEWAY_BASE}/v1/sessions/{session_id}"
                        f"/operations/{op}/result/0"
                    )
                    if r2.status_code == 200:
                        body2 = r2.json()
                        rtype = body2.get("resultType", "NOT_READY")
                        if rtype in ("EOS", "PAYLOAD") and not body2.get("nextResultUri"):
                            break
                        if rtype == "PAYLOAD" and body2.get("nextResultUri"):
                            break
                    await asyncio.sleep(1)
                await asyncio.sleep(0.5)  # brief settle between statements

            # 3. Run the actual query
            rows = await _exec_flink_statement(client, session_id, adapted_sql, deadline)

        # Deduplicate streaming aggregate rows: Flink emits multiple UPDATE_AFTER
        # per key as the aggregate value changes. Keep only the LAST value per key
        # (first column = group-by key).  Bounded SELECT queries are unaffected
        # because each key appears exactly once.
        if rows and len(rows[0]) >= 2:
            seen: dict = {}
            for row in rows:
                key = row[0]
                seen[key] = row        # last occurrence wins (highest/final value)
            rows = list(seen.values())

        logger.info("Flink Gateway returned %d rows (after dedup)", len(rows))
        return rows

    finally:
        if session_id:
            try:
                async with httpx.AsyncClient(timeout=5.0) as cl:
                    await cl.delete(f"{FLINK_GATEWAY_BASE}/v1/sessions/{session_id}")
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Time context extraction (replaces naive keyword matching)
# ---------------------------------------------------------------------------

def _unit_label(mult: int) -> str:
    return {1: "minute", 60: "hour", 1440: "day", 10080: "week", 43200: "month"}.get(
        mult, "unit"
    )


def _extract_time_context(query: str) -> dict:
    """
    Extract time window from query using regex, return layer + SQL interval.
    Regex runs BEFORE named-window checks to avoid substring false matches.
    """
    q = query.lower()

    # ── Numeric patterns first (prevents "24 giờ" matching "hôm nay" branch) ──
    # Each pattern covers both Unicode diacritic and ASCII no-diacritic forms.
    numeric_patterns = [
        (r"(\d+)\s*ph[uú]t",        1),      # phút / phut
        (r"(\d+)\s*minute",         1),
        (r"(\d+)\s*min\b",          1),
        (r"(\d+)\s*gi[oờ]\b",       60),     # giờ / gio
        (r"(\d+)\s*ti[eế]ng\b",     60),     # tiếng (1 tiếng = 1 hour)
        (r"(\d+)\s*hour",           60),
        (r"(\d+)\s*ng[aà]y",        1440),   # ngày / ngay
        (r"(\d+)\s*day",            1440),
        (r"(\d+)\s*tu[aầ]n",        10080),  # tuần / tuan
        (r"(\d+)\s*week",           10080),
        (r"(\d+)\s*th[aá]ng",       43200),  # tháng / thang
        (r"(\d+)\s*month",          43200),
    ]

    for pattern, mult in numeric_patterns:
        m = re.search(pattern, q)
        if m:
            n = int(m.group(1))
            window = n * mult
            # HOT: ≤60 min (Fluss retains 1-2hr; only route here when < 1 hour)
            # WARM: 61 min–30 days; COLD: >30 days
            if window <= 60:
                layer, interval = "hot", f"{window} MINUTE"
            elif window <= 43200:   # ≤ 30 days → warm
                if window < 1440:
                    interval = f"{window // 60 or 1} HOUR"
                else:
                    interval = f"{window // 1440} DAY"
                layer = "warm"
            else:
                layer, interval = "cold", f"{window // 1440} DAY"
            return {
                "window_minutes": window,
                "layer": layer,
                "sql_interval": interval,
                "description": f"last {n} {_unit_label(mult)}(s)",
            }

    # ── Named windows (Unicode + ASCII no-diacritic forms) ──
    named = [
        # HOT: realtime keywords
        (["vừa", "vua", "live", "realtime", "real-time",
          "ngay bây giờ", "ngay bay gio", "hiện tại", "hien tai"],
         60, "hot", "60 MINUTE", "real-time (last 60 min)"),
        # WARM: today
        (["hôm nay", "hom nay", "today", "trong ngày", "trong ngay"],
         1440, "warm", "1 DAY", "today (last 24 hours)"),
        # WARM: yesterday
        (["hôm qua", "hom qua", "yesterday"],
         2880, "warm", "2 DAY", "yesterday"),
        # WARM: this week
        (["tuần này", "tuan nay", "this week", "7 ngày qua", "7 ngay qua"],
         10080, "warm", "7 DAY", "this week"),
        # WARM: this month
        (["tháng này", "thang nay", "this month", "30 ngày", "30 ngay"],
         43200, "warm", "30 DAY", "this month"),
        # COLD: last month / historical
        (["tháng trước", "thang truoc", "last month"],
         86400, "cold", "60 DAY", "last month (historical)"),
    ]
    for keywords, window, layer, interval, desc in named:
        if any(k in q for k in keywords):
            return {
                "window_minutes": window,
                "layer": layer,
                "sql_interval": interval,
                "description": desc,
            }

    # ── Default: cold (most historical data lives there) ──
    return {
        "window_minutes": None,
        "layer": "cold",
        "sql_interval": "30 DAY",
        "description": "historical (no specific time range)",
    }


# ---------------------------------------------------------------------------
# Multi-turn conversation helper
# ---------------------------------------------------------------------------

def _format_history_for_gemini(history: list[dict]) -> list[dict]:
    """Convert chat history to Gemini contents format (last 6 messages = 3 turns)."""
    contents = []
    for msg in (history or [])[-6:]:
        role = "model" if msg.get("role") == "model" else "user"
        text = str(msg.get("content") or "").strip()
        if text:
            contents.append({"role": role, "parts": [{"text": text}]})
    return contents


async def _gemini_call(contents: list[dict], timeout: float = 30.0) -> str:
    """
    Call Gemini generateContent API with automatic 429 retry + backoff.
    Returns the raw text from the first candidate.
    Raises on non-retryable errors after max_retries.
    """
    max_retries = 3
    backoff_secs = [5, 15, 30]   # wait 5s, 15s, 30s on consecutive 429s

    for attempt in range(max_retries):
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                f"{GEMINI_URL}?key={GEMINI_API_KEY}",
                json={"contents": contents},
            )

        if resp.status_code == 429:
            wait = backoff_secs[min(attempt, len(backoff_secs) - 1)]
            logger.warning(
                "Gemini 429 rate-limit (attempt %d/%d) — retrying in %ds",
                attempt + 1, max_retries, wait,
            )
            await asyncio.sleep(wait)
            continue

        resp.raise_for_status()
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"]

    raise RuntimeError("Gemini API: exceeded max retries due to rate limiting (429)")


# ---------------------------------------------------------------------------
# Text-to-SQL: Stage 1 — Generate SQL with Gemini
# ---------------------------------------------------------------------------

async def _generate_sql(
    query: str,
    layer: str,
    time_ctx: dict,
    history: list[dict],
) -> dict:
    """
    Ask Gemini to generate a SQL query from natural language.
    Returns dict: { sql, target_table, layer, confidence, explanation }
    """
    if not GEMINI_API_KEY:
        return {
            "sql": None, "target_table": None, "layer": layer,
            "confidence": 0, "explanation": "No Gemini API key",
        }

    layer_hint = {
        "hot":  "Use fluss.security.hot_violence_alerts (Flink SQL Gateway, last 1-2h)",
        "warm": "Prefer paimon.security.daily_incident_stats or camera_stats for aggregates; "
                "paimon.security.violence_incidents for row-level details (Flink SQL Gateway)",
        "cold": "Use iceberg.security.historical_violence_incidents (Trino engine)",
    }.get(layer, "Use iceberg.security.historical_violence_incidents (Trino engine)")

    time_hint = (
        f"Time window: {time_ctx['description']} → SQL interval: {time_ctx['sql_interval']}"
        if time_ctx.get("sql_interval")
        else "No specific time filter — use last 30 days as default"
    )

    system_prompt = f"""{SCHEMA_FOR_PROMPT}

## Current Request Context
Detected layer : {layer.upper()}
Layer guidance : {layer_hint}
Time context   : {time_hint}

## Your Task
Generate a SQL query to answer the user's question.
Return ONLY valid JSON — no markdown, no explanation outside the JSON:
{{
  "sql": "SELECT ...",
  "target_table": "schema.table_name",
  "layer": "{layer}",
  "confidence": 0.0,
  "explanation": "one-line description of what this query does"
}}
"""

    contents = _format_history_for_gemini(history) + [
        {"role": "user", "parts": [{"text": f"{system_prompt}\n\nUser question: {query}"}]}
    ]

    try:
        raw = await _gemini_call(contents, timeout=30.0)

        # Strip markdown code fences if Gemini wrapped the JSON
        clean = raw.strip()
        clean = re.sub(r"^```\w*\s*", "", clean)
        clean = re.sub(r"\s*```$", "", clean)

        result = json.loads(clean)
        logger.info(
            "Generated SQL (conf=%.2f): %s",
            result.get("confidence", 0),
            str(result.get("sql", ""))[:120],
        )
        return result

    except Exception as e:
        logger.error("_generate_sql error: %s", e)
        return {
            "sql": None, "target_table": None, "layer": layer,
            "confidence": 0, "explanation": str(e),
        }


# ---------------------------------------------------------------------------
# Response Synthesizer: Stage 2 — Natural-language answer from query results
# ---------------------------------------------------------------------------

async def _synthesize_response(
    query: str,
    sql: str,
    rows: list[list],
    layer: str,
    history: list[dict],
) -> tuple[str, float]:
    """
    Ask Gemini to interpret query results into a Vietnamese answer.
    Returns (answer_markdown, confidence_float).
    """
    if not GEMINI_API_KEY:
        if rows:
            lines = "\n".join(f"- {r}" for r in rows[:10])
            return f"Kết quả truy vấn ({len(rows)} dòng):\n{lines}", 0.6
        return "Không có dữ liệu phù hợp.", 0.4

    if not rows:
        rows_text = "_(Không có dữ liệu — 0 rows)_"
    else:
        rows_text = "\n".join(f"  Row {i+1}: {r}" for i, r in enumerate(rows[:25]))
        if len(rows) > 25:
            rows_text += f"\n  ... và {len(rows) - 25} rows khác"

    prompt = f"""Bạn là **Vigilance AI** — trợ lý phân tích an ninh thông minh cho hệ thống giám sát đô thị Việt Nam.

Người dùng hỏi: "{query}"

SQL đã thực thi (layer: **{layer.upper()}**):
```sql
{sql.strip()}
```

Kết quả ({len(rows)} rows):
{rows_text}

**Nhiệm vụ**: Trả lời câu hỏi bằng tiếng Việt, ngắn gọn, chính xác dựa trên dữ liệu thực tế.
- Nêu **số liệu cụ thể** từ kết quả (không bịa số ngoài dữ liệu)
- Dùng Markdown: **in đậm** số quan trọng, bullet list nếu có nhiều mục
- Nếu 0 rows: nói rõ không có dữ liệu trong khoảng thời gian đó
- Kết thúc với 1 nhận xét ngắn mang tính phân tích (xu hướng, camera đáng chú ý, v.v.)

Trả về JSON (không có markdown wrapper):
{{"answer": "...", "confidence": 0.0}}"""

    contents = _format_history_for_gemini(history) + [
        {"role": "user", "parts": [{"text": prompt}]}
    ]

    try:
        raw = await _gemini_call(contents, timeout=30.0)

        clean = raw.strip()
        clean = re.sub(r"^```\w*\s*", "", clean)
        clean = re.sub(r"\s*```$", "", clean)

        result = json.loads(clean)
        return result.get("answer", raw), float(result.get("confidence", 0.8))

    except Exception as e:
        logger.error("_synthesize_response error: %s", e)
        # Graceful fallback: present raw rows
        if rows:
            lines = "\n".join(f"- {r}" for r in rows[:10])
            return (
                f"Kết quả truy vấn ({len(rows)} dòng từ layer {layer.upper()}):\n{lines}",
                0.5,
            )
        return "Không có dữ liệu phù hợp với câu hỏi.", 0.4


# ---------------------------------------------------------------------------
# GET /api/recent-incidents
# ---------------------------------------------------------------------------

@app.get("/api/recent-incidents")
async def get_recent_incidents(limit: int = Query(50, ge=1, le=500)):
    sql = f"""
    SELECT
        incident_id,
        camera_id,
        CAST(timestamp AS VARCHAR)  AS timestamp,
        risk_score,
        COALESCE(event_type, 'Anomaly') AS label,
        location,
        'VioMobileNet-v2.1'         AS model_version,
        'Unreviewed'                AS status,
        CAST(incident_date AS VARCHAR) AS incident_date
    FROM iceberg.security.historical_violence_incidents
    ORDER BY timestamp DESC
    LIMIT {limit}
    """
    try:
        rows = await _trino_query(sql)
    except Exception as e:
        logger.error("Trino error (recent-incidents): %s", e)
        raise HTTPException(status_code=503, detail=f"Trino unavailable: {e}")

    _UUID_RE = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.I,
    )

    def _extract_location(raw) -> str:
        if not raw:
            return "Unknown"
        s = str(raw)
        if s.startswith("{"):
            try:
                obj = json.loads(s)
                return obj.get("street") or obj.get("district") or obj.get("city") or s
            except Exception:
                pass
        return s

    async def build_row(r: list) -> dict:
        incident_id = r[0] or ""
        camera_id = r[1] or ""
        incident_date = r[8] or ""
        frame_url = (
            None
            if _UUID_RE.match(incident_id)
            else await _get_frame_url(camera_id, incident_date, incident_id)
        )
        return {
            "event_id":       incident_id,
            "camera_id":      camera_id,
            "timestamp":      r[2],
            "violence_score": float(r[3]) if r[3] is not None else 0.0,
            "label":          r[4] or "Anomaly",
            "location":       _extract_location(r[5]) or camera_id,
            "model_version":  r[6] or "VioMobileNet-v2.1",
            "status":         r[7],
            "frame_url":      frame_url,
        }

    results = await asyncio.gather(*[build_row(r) for r in rows])
    return list(results)


# ---------------------------------------------------------------------------
# GET /api/evidence
# ---------------------------------------------------------------------------

@app.get("/api/evidence")
async def get_evidence(
    camera_id: str = Query(..., description="Camera ID, e.g. cam_01"),
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    limit: int = Query(10, ge=1, le=50),
):
    """Return public MinIO URLs for evidence images of a given camera and date."""
    prefix = f"{camera_id}/{date}/"
    keys = await _minio_list_objects(prefix, max_keys=limit)
    if not keys:
        return {"camera_id": camera_id, "date": date, "images": []}

    images = [
        {"key": k, "url": f"{MINIO_EXTERNAL}/{EVIDENCE_BUCKET}/{k}"}
        for k in keys
    ]
    return {"camera_id": camera_id, "date": date, "images": images}


# ---------------------------------------------------------------------------
# GET /api/stats
# ---------------------------------------------------------------------------

@app.get("/api/stats")
async def get_stats():
    alerts_per_hour_sql = """
    SELECT
        format_datetime(date_trunc('hour', timestamp), 'HH:mm') AS hour_label,
        COUNT(*) AS alert_count
    FROM iceberg.security.historical_violence_incidents
    WHERE is_violent = true
      AND timestamp >= NOW() - INTERVAL '40' DAY
    GROUP BY date_trunc('hour', timestamp)
    ORDER BY date_trunc('hour', timestamp)
    LIMIT 24
    """

    top_locations_sql = """
    SELECT
        COALESCE(
            json_extract_scalar(CAST(location AS VARCHAR), '$.street'),
            json_extract_scalar(CAST(location AS VARCHAR), '$.district'),
            CAST(location AS VARCHAR)
        ) AS loc_name,
        COUNT(*) AS cnt
    FROM iceberg.security.historical_violence_incidents
    WHERE is_violent = true
      AND timestamp >= NOW() - INTERVAL '40' DAY
    GROUP BY location
    ORDER BY cnt DESC
    LIMIT 5
    """

    alert_types_sql = """
    SELECT COALESCE(event_type, 'Unknown') AS etype, COUNT(*) AS cnt
    FROM iceberg.security.historical_violence_incidents
    WHERE is_violent = true
      AND timestamp >= NOW() - INTERVAL '40' DAY
    GROUP BY event_type
    ORDER BY cnt DESC
    """

    avg_score_sql = """
    SELECT
        format_datetime(CAST(timestamp AS DATE), 'MMM dd') AS day_label,
        CAST(timestamp AS DATE) AS day_date,
        AVG(risk_score) AS avg_score
    FROM iceberg.security.historical_violence_incidents
    WHERE timestamp >= NOW() - INTERVAL '40' DAY
    GROUP BY CAST(timestamp AS DATE)
    ORDER BY CAST(timestamp AS DATE)
    LIMIT 30
    """

    try:
        hours_rows, loc_rows, type_rows, score_rows = await asyncio.gather(
            _trino_query(alerts_per_hour_sql),
            _trino_query(top_locations_sql),
            _trino_query(alert_types_sql),
            _trino_query(avg_score_sql),
            return_exceptions=True,
        )
    except Exception as e:
        logger.error("Trino error (stats): %s", e)
        raise HTTPException(status_code=503, detail=f"Trino unavailable: {e}")

    def safe_rows(r):
        return r if isinstance(r, list) else []

    return {
        "alertsPerHour": [
            {"name": row[0], "alerts": int(row[1])} for row in safe_rows(hours_rows)
        ],
        "topLocations": [
            {"name": row[0] or "Unknown", "alerts": int(row[1])}
            for row in safe_rows(loc_rows)
        ],
        "alertTypes": [
            {"name": row[0] or "Unknown", "value": int(row[1])}
            for row in safe_rows(type_rows)
        ],
        "avgScore": [
            {"name": row[0], "score": round(float(row[2]), 3) if row[2] else 0}
            for row in safe_rows(score_rows)
        ],
    }


# ---------------------------------------------------------------------------
# Evidence image helpers (used by /api/chat image path)
# ---------------------------------------------------------------------------

_IMAGE_KEYWORDS = [
    "hình ảnh", "bằng chứng", "ảnh", "xem ảnh", "hiển thị ảnh",
    "hinh anh", "bang chung", "xem anh", "hien thi anh",
    "evidence", "image", "show image", "picture", "photo", "frame",
]


def _wants_images(query: str) -> bool:
    import unicodedata
    q = query.lower()
    q_ascii = "".join(
        c for c in unicodedata.normalize("NFD", q)
        if unicodedata.category(c) != "Mn"
    )
    return any(k in q for k in _IMAGE_KEYWORDS) or any(
        k in q_ascii for k in _IMAGE_KEYWORDS
    )


async def _fetch_evidence_incidents(limit: int = 6) -> list[dict]:
    """Fetch recent incidents with frame_url from Iceberg via Trino."""
    sql = f"""
    SELECT
        incident_id, camera_id,
        CAST(timestamp AS VARCHAR) AS ts,
        risk_score,
        COALESCE(event_type, 'Anomaly') AS label,
        location,
        CAST(incident_date AS VARCHAR) AS incident_date
    FROM iceberg.security.historical_violence_incidents
    ORDER BY timestamp DESC
    LIMIT {limit}
    """
    try:
        rows = await _trino_query(sql, timeout=15.0)
    except Exception as e:
        logger.warning("Could not fetch incidents for evidence: %s", e)
        return []

    _UUID_RE2 = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
    )

    def _loc(raw) -> str:
        if not raw:
            return "Unknown"
        s = str(raw)
        if s.startswith("{"):
            try:
                obj = json.loads(s)
                return obj.get("street") or obj.get("district") or obj.get("city") or s
            except Exception:
                pass
        return s

    results = []
    for r in rows:
        if len(r) < 7:
            continue
        incident_id = r[0] or ""
        camera_id = r[1] or ""
        incident_date = r[6] or ""
        frame_url = (
            None
            if _UUID_RE2.match(incident_id)
            else await _get_frame_url(camera_id, incident_date, incident_id)
        )
        results.append({
            "incident_id":    incident_id,
            "camera_id":      camera_id,
            "timestamp":      r[2],
            "violence_score": float(r[3]) if r[3] is not None else 0.0,
            "label":          r[4],
            "location":       _loc(r[5]) or camera_id,
            "frame_url":      frame_url,
        })
    return results


def _build_evidence_markdown(incidents: list[dict]) -> str:
    if not incidents:
        return "Không tìm thấy hình ảnh bằng chứng nào trong hệ thống."
    lines = ["### 📸 Hình ảnh bằng chứng các sự cố gần đây\n"]
    for inc in incidents:
        score_pct = f"{inc['violence_score'] * 100:.1f}%"
        ts = inc["timestamp"][:19] if inc["timestamp"] else "N/A"
        lines.append(
            f"**{inc['label']}** | `{inc['camera_id']}` | {inc['location']}"
            f" | Score: {score_pct} | {ts}"
        )
        if inc["frame_url"]:
            lines.append(f"![Evidence {inc['camera_id']} - {inc['label']}]({inc['frame_url']})")
        else:
            lines.append("_Không có hình ảnh cho sự cố này._")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# POST /api/chat  — Main chat endpoint (v2: Text-to-SQL + 3-layer routing)
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    query: str
    history: Optional[list[dict]] = None


@app.post("/api/chat")
async def chat(req: ChatRequest):
    start = time.time()
    history = req.history or []

    # ── Fast path: evidence image request ──
    if _wants_images(req.query):
        incidents = await _fetch_evidence_incidents(limit=5)
        answer = _build_evidence_markdown(incidents)
        return {
            "answer": answer,
            "layer": "cold",
            "citations": {
                "source_table": "iceberg.security.historical_violence_incidents",
                "data_layer":   "cold",
                "time_period":  "recent",
                "query_engine": "Trino",
                "rows_returned": len(incidents),
            },
            "confidence":   0.95,
            "duration_ms":  int((time.time() - start) * 1000),
            "images": [inc["frame_url"] for inc in incidents if inc.get("frame_url")],
        }

    # ── Step 1: Extract time context (regex-based, not keyword hacks) ──
    time_ctx = _extract_time_context(req.query)
    layer = time_ctx["layer"]
    logger.info("Layer routing: %s | %s", layer, time_ctx["description"])

    # ── Step 2: Text-to-SQL via Gemini ──
    sql_result = await _generate_sql(req.query, layer, time_ctx, history)
    generated_sql: str | None = sql_result.get("sql")
    target_table: str | None  = sql_result.get("target_table")

    # ── Step 3: Validate SQL ──
    sql_to_run: str
    used_fallback = False

    if generated_sql:
        valid, reason = _validate_sql(generated_sql)
        if valid:
            sql_to_run = generated_sql
        else:
            logger.warning("Generated SQL invalid (%s) — falling back", reason)
            used_fallback = True
    else:
        used_fallback = True

    if used_fallback:
        fallback_key = {"hot": "recent_hot", "warm": "count_today", "cold": "recent_cold"}.get(
            layer, "recent_cold"
        )
        sql_to_run = FALLBACK_SQL[fallback_key]
        target_table = {
            "hot":  "fluss.security.hot_violence_alerts",
            "warm": "paimon.security.daily_incident_stats",
            "cold": "iceberg.security.historical_violence_incidents",
        }.get(layer)

    # ── Step 4: Execute query on the correct layer ──
    rows: list[list] = []
    actual_layer = layer
    query_engine = "Trino" if layer == "cold" else "Flink SQL Gateway"
    fallback_to_cold = False

    try:
        if layer == "cold":
            rows = await _trino_query(sql_to_run, timeout=LAYER_TIMEOUT["cold"])
        else:
            rows = await _flink_gateway_query(
                sql_to_run, timeout=LAYER_TIMEOUT[layer], layer=layer
            )

    except Exception as exc:
        logger.error(
            "Query failed layer=%s: %s — falling back to cold (Trino)", layer, exc
        )
        fallback_to_cold = True
        actual_layer = "cold"
        query_engine = "Trino (cold fallback)"
        # Rewrite warm/hot SQL to Iceberg-compatible SQL
        cold_sql = _adapt_sql_to_iceberg(sql_to_run)
        # Validate rewritten SQL; if still invalid use safe fallback
        cold_valid, _ = _validate_sql(cold_sql)
        if not cold_valid:
            cold_sql = FALLBACK_SQL["recent_cold"]
        try:
            rows = await _trino_query(cold_sql, timeout=30.0)
            sql_to_run = cold_sql
            target_table = "iceberg.security.historical_violence_incidents"
        except Exception as exc2:
            logger.error("Cold fallback also failed: %s", exc2)
            rows = []

    # ── Step 5: Synthesize natural-language response ──
    answer, resp_conf = await _synthesize_response(
        req.query, sql_to_run, rows, actual_layer, history
    )

    # ── Step 6: Compute final confidence score ──
    confidence = 1.0
    if used_fallback:
        confidence -= 0.2
    elif sql_result.get("confidence", 1.0) < 0.7:
        confidence -= 0.1
    if len(rows) == 0:
        confidence -= 0.1
    if fallback_to_cold:
        confidence -= 0.05
    confidence = round(max(0.1, min(1.0, confidence)), 2)

    # Truncate SQL for citation display (max 300 chars)
    sql_display = sql_to_run.strip()
    if len(sql_display) > 300:
        sql_display = sql_display[:300] + "…"

    return {
        "answer": answer,
        "layer":  actual_layer,
        "citations": {
            "source_table":  target_table or "iceberg.security.historical_violence_incidents",
            "data_layer":    actual_layer,
            "time_period":   time_ctx.get("description", "recent"),
            "sql_used":      sql_display,
            "rows_returned": len(rows),
            "query_engine":  query_engine,
        },
        "confidence":  confidence,
        "duration_ms": int((time.time() - start) * 1000),
    }


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok", "service": "vigilance-ai-chatbot", "version": "2.0.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5002, log_level="info")
