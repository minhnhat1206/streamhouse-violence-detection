"""
Trino Client - Multi-Layer Query Routing

Manages PyTrino connections and Flink SQL Gateway calls for layer-based routing.

Layer routing:
  HOT   (Fluss)   → Flink SQL Gateway REST API (port 8083)
  WARM  (Paimon)  → Trino native paimon catalog (paimon-trino-440 plugin)
  COLD  (Iceberg) → Trino via PyTrino (catalog: iceberg)
"""

import logging
import os
import json
import requests
from typing import List, Dict, Optional, Any
from enum import Enum
import time

try:
    from trino.dbapi import connect as trino_connect
    from trino.exceptions import TrinoQueryError, TrinoConnectionError
    TrinoException = TrinoQueryError
except ImportError:
    trino_connect = None
    TrinoException = Exception
    TrinoQueryError = Exception
    TrinoConnectionError = Exception

logger = logging.getLogger(__name__)


class DataLayer(str, Enum):
    FLUSS = "FLUSS"
    PAIMON = "PAIMON"
    ICEBERG = "ICEBERG"


# ── Fluss catalog DDL used per-session in Flink SQL Gateway ───────────────────
_FLUSS_CATALOG_DDL = """CREATE CATALOG fluss_hot WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'fluss-coordinator:9123'
)"""


class TrinoClient:
    """Trino client with connection pooling and layer routing."""

    def __init__(
        self,
        trino_host: str = "localhost",
        trino_port: int = 8082,
        flink_gateway_host: str = "localhost",
        flink_gateway_port: int = 8083,
        pool_size: int = 10
    ):
        self.trino_host = trino_host
        self.trino_port = trino_port
        self.flink_gateway_host = flink_gateway_host
        self.flink_gateway_port = flink_gateway_port
        self.pool_size = pool_size

        # Cached Flink SQL Gateway session for HOT (Fluss) queries.
        # Reusing the session avoids 3 DDL roundtrips (CREATE CATALOG + USE + USE)
        # that add ~5-10s latency on every HOT query.
        self._fluss_session_id: Optional[str] = None
        self._fluss_session_ts: float = 0.0
        self._FLUSS_SESSION_TTL = 1800  # 30 min — Flink SQL Gateway sessions expire ~1h

        logger.info(
            f"Initialized TrinoClient: "
            f"Trino {trino_host}:{trino_port}, "
            f"Flink Gateway {flink_gateway_host}:{flink_gateway_port}"
        )

    # ── Low-level: Flink SQL Gateway ──────────────────────────────────────────

    def _ensure_fluss_session(self, init_timeout: int = 60) -> str:
        """Get or create a pre-warmed Flink SQL Gateway session for Fluss.

        Caches the session across HOT queries. First call initializes the session
        with catalog DDLs (adds ~5s); subsequent calls reuse it instantly.
        Session is recreated if it's older than _FLUSS_SESSION_TTL or dead.
        """
        gateway_url = f"http://{self.flink_gateway_host}:{self.flink_gateway_port}"
        now = time.time()

        # Try to reuse existing session
        if self._fluss_session_id and (now - self._fluss_session_ts) < self._FLUSS_SESSION_TTL:
            try:
                resp = requests.get(
                    f"{gateway_url}/v1/sessions/{self._fluss_session_id}",
                    timeout=5
                )
                if resp.status_code == 200:
                    logger.debug(f"Reusing Fluss session: {self._fluss_session_id}")
                    return self._fluss_session_id
            except Exception:
                pass
            logger.info("Fluss session dead, recreating...")
            self._fluss_session_id = None

        # Create new session
        logger.info("Creating new Fluss SQL Gateway session...")
        sess_resp = requests.post(f"{gateway_url}/v1/sessions", json={}, timeout=init_timeout)
        sess_resp.raise_for_status()
        session_id = sess_resp.json()["sessionHandle"]

        # Initialize with Fluss catalog DDLs (each DDL is fast, ~1-5s)
        for stmt in [_FLUSS_CATALOG_DDL, "USE CATALOG fluss_hot", "USE `security`"]:
            self._exec_flink_statement(session_id, stmt, gateway_url, init_timeout)

        self._fluss_session_id = session_id
        self._fluss_session_ts = time.time()
        logger.info(f"Fluss session ready: {session_id}")
        return session_id

    def _cancel_operation(self, session_id: str, op_handle: str, gateway_url: str) -> None:
        """Cancel a Flink SQL Gateway operation.

        Called after collecting results. Also triggers cleanup of zombie collect jobs
        via the Flink REST API (SQL Gateway cancel alone doesn't stop the Flink job).
        """
        # Cancel the SQL Gateway operation
        try:
            requests.delete(
                f"{gateway_url}/v1/sessions/{session_id}/operations/{op_handle}",
                timeout=5
            )
            logger.debug(f"Cancelled operation {op_handle[:8]}...")
        except Exception as e:
            logger.debug(f"Could not cancel operation {op_handle[:8]}: {e}")
        # Cancel zombie Flink 'collect' jobs via REST (streaming HOT queries never stop)
        self._cleanup_collect_jobs()

    def _cleanup_collect_jobs(self) -> None:
        """Cancel all RUNNING 'collect' jobs in Flink to free task slots.

        Flink SQL Gateway streaming queries leave zombie 'collect' jobs running
        indefinitely. Cancel them via Flink REST API before each HOT query so
        the new query can acquire task slots.
        """
        try:
            flink_url = "http://jobmanager:8081"
            r = requests.get(f"{flink_url}/jobs/overview", timeout=5)
            if r.status_code != 200:
                return
            for job in r.json().get("jobs", []):
                if job.get("name") == "collect" and job.get("state") == "RUNNING":
                    jid = job.get("jid", "")
                    try:
                        requests.patch(f"{flink_url}/jobs/{jid}", timeout=5)
                        logger.info(f"Cancelled zombie collect job {jid[:8]}...")
                    except Exception as e:
                        logger.debug(f"Could not cancel job {jid[:8]}: {e}")
        except Exception as e:
            logger.debug(f"Could not clean up collect jobs: {e}")

    def _exec_flink_statement(
        self,
        session_id: str,
        sql: str,
        gateway_url: str,
        timeout: int
    ) -> List[Dict[str, Any]]:
        """Execute one SQL statement in an existing Flink SQL Gateway session."""
        exec_resp = requests.post(
            f"{gateway_url}/v1/sessions/{session_id}/statements",
            json={"statement": sql},
            timeout=timeout
        )
        exec_resp.raise_for_status()
        op_handle = exec_resp.json()["operationHandle"]

        # Poll result pages with token-based pagination.
        # Flink SQL Gateway only supports tokens 0 and 1 (token 2+ → HTTP 500).
        # Token advancement rules (CRITICAL — see token behavior notes below):
        #   - Advance 0→1 ONLY when: page has rows OR resultType=="PAYLOAD" (not NOT_READY)
        #   - NOT_READY with empty page = job still running, data not buffered yet → stay at 0
        #   - PAYLOAD with empty page = buffer consumed / job done → advance once to check token 1
        #   - isQueryRunning is ABSENT in all Flink 1.18 SQL Gateway responses
        #     (do NOT use it for state detection; rely on resultType and EOS instead)
        HTTP_TIMEOUT = min(timeout, 30)
        TOTAL_DEADLINE = time.time() + max(timeout, 30)  # respect caller's timeout
        result_token = 0
        all_rows: List[Dict[str, Any]] = []
        latest_agg_rows: List[Dict[str, Any]] = []
        columns: List[Dict] = []
        stable_polls = 0
        consecutive_500s = 0
        MAX_TOKEN = 1          # Gateway supports tokens 0 and 1 only
        MAX_CONSEC_500 = 5     # Give up after 5 consecutive HTTP 500s (stale session)

        while time.time() < TOTAL_DEADLINE:
            result_resp = requests.get(
                f"{gateway_url}/v1/sessions/{session_id}/operations/{op_handle}/result/{result_token}",
                timeout=HTTP_TIMEOUT
            )
            if result_resp.status_code != 200:
                consecutive_500s += 1
                logger.warning(
                    f"Gateway result/{result_token} returned {result_resp.status_code} "
                    f"(attempt {consecutive_500s}/{MAX_CONSEC_500})"
                )
                if consecutive_500s >= MAX_CONSEC_500:
                    logger.error("Too many HTTP errors from Gateway — aborting poll")
                    break
                result_token = 0  # reset to start position
                time.sleep(2)
                continue
            consecutive_500s = 0
            data = result_resp.json()

            result_type = data.get("resultType", "NOT_READY")

            results_block = data.get("results", {})
            if not columns and results_block.get("columns"):
                columns = results_block["columns"]

            page_rows = []
            page_raw = results_block.get("data", [])
            for raw_row in page_raw:
                fields = raw_row.get("fields", []) if isinstance(raw_row, dict) else raw_row
                row_kind = raw_row.get("kind", "INSERT") if isinstance(raw_row, dict) else "INSERT"
                if isinstance(fields, (list, tuple)):
                    row = {
                        col.get("name", f"col_{i}"): fields[i]
                        for i, col in enumerate(columns)
                        if i < len(fields)
                    }
                    if row_kind in ("UPDATE_AFTER", "INSERT"):
                        page_rows.append(row)

            if page_rows:
                stable_polls = 0
                all_rows.extend(page_rows)
                # For streaming aggregates (e.g. Fluss GROUP BY), the same group keys
                # are emitted repeatedly as UPDATE_AFTER events with updated aggregates.
                # Keep only the LATEST value per group-by key to avoid returning thousands
                # of intermediate streaming updates.
                # Dedup heuristic:
                #   - 2+ columns: use all-but-last as key (last col = aggregate value)
                #   - 1 column: use that column as key (SELECT col FROM table → unique values)
                #   - 0 columns: no dedup (return all_rows as fallback)
                if columns:
                    n = len(columns)
                    key_cols = columns[:n - 1] if n > 1 else columns
                    dedup_key_names = [c.get("name") for c in key_cols]
                    dedup_dict: Dict[tuple, Dict] = {}
                    for row in all_rows:
                        k = tuple(row.get(cn) for cn in dedup_key_names)
                        dedup_dict[k] = row
                    latest_agg_rows = list(dedup_dict.values())
                else:
                    latest_agg_rows = list(all_rows)
            else:
                stable_polls += 1

            if result_type == "EOS":
                return latest_agg_rows if latest_agg_rows else all_rows

            next_uri = data.get("nextResultUri")
            if next_uri and result_token < MAX_TOKEN:
                # Advance ONLY when safe: page had rows (consume batch), or
                # resultType is PAYLOAD (buffer acknowledged, data may be at next token).
                # Do NOT advance on NOT_READY (job running, data not yet in buffer) —
                # that would strand the data at the current token.
                if page_rows or result_type == "PAYLOAD":
                    try:
                        next_token = int(next_uri.rstrip("/").split("/")[-1])
                        if next_token <= MAX_TOKEN:
                            result_token = next_token
                    except (ValueError, IndexError):
                        pass
                    continue  # fetch next page immediately (no sleep)

            # 2 consecutive empty polls → streaming aggregate has converged
            # (lowered from 3 to return faster; 4s gap between data pages is enough
            # for Fluss streaming aggregate to reach stable state)
            if stable_polls >= 2 and latest_agg_rows:
                logger.info(f"Streaming aggregate stabilized after {stable_polls} empty polls")
                self._cancel_operation(session_id, op_handle, gateway_url)
                return latest_agg_rows

            time.sleep(2)

        elapsed = time.time() - (TOTAL_DEADLINE - max(timeout, 30))
        logger.info(f"Flink statement polling ended after {elapsed:.0f}s: "
                    f"{len(latest_agg_rows)} deduped rows (from {len(all_rows)} total)")
        self._cancel_operation(session_id, op_handle, gateway_url)
        return latest_agg_rows if latest_agg_rows else all_rows

    def _query_flink_gateway(
        self,
        sql: str,
        init_statements: Optional[List[str]] = None,
        timeout: int = 30
    ) -> List[Dict[str, Any]]:
        """Execute SQL via Flink SQL Gateway REST API."""
        gateway_url = f"http://{self.flink_gateway_host}:{self.flink_gateway_port}"

        sess_resp = requests.post(f"{gateway_url}/v1/sessions", json={}, timeout=timeout)
        sess_resp.raise_for_status()
        session_id = sess_resp.json()["sessionHandle"]
        logger.info(f"Flink SQL Gateway session: {session_id}")

        try:
            for stmt in (init_statements or []):
                self._exec_flink_statement(session_id, stmt, gateway_url, timeout)
            return self._exec_flink_statement(session_id, sql, gateway_url, timeout)
        finally:
            try:
                requests.delete(f"{gateway_url}/v1/sessions/{session_id}", timeout=5)
            except Exception:
                pass

    # ── Low-level: Trino (PyTrino) ────────────────────────────────────────────

    def _query_trino(
        self,
        sql: str,
        catalog: str = "iceberg",
        schema: str = "security",
        timeout: int = 30
    ) -> List[Dict[str, Any]]:
        """Execute SQL via PyTrino."""
        if trino_connect is None:
            raise ImportError("PyTrino not installed")

        # v2: KHÔNG còn rewrite sang view *_sessionized — sessionization giờ nằm
        # trong pipeline (incident_uid + fact_violence_incident), agent chọn đúng
        # bảng grain=incident khi đếm vụ ngay từ lúc sinh SQL.

        conn = trino_connect(
            host=self.trino_host,
            port=self.trino_port,
            user="admin",
            catalog=catalog,
            schema=schema,
        )
        cursor = conn.cursor()
        try:
            t0 = time.time()
            cursor.execute(sql)
            col_names = [d[0] for d in cursor.description] if cursor.description else []
            rows = [{c: v for c, v in zip(col_names, row)} for row in cursor.fetchall()]
            logger.info(
                f"Trino {catalog}.{schema}: {len(rows)} rows in {time.time()-t0:.2f}s"
            )
            return rows
        except TrinoException as e:
            logger.error(f"Trino error: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    # ── SQL adaptation helpers ─────────────────────────────────────────────────

    @staticmethod
    def _adapt_sql_for_flink_hot(sql: str) -> str:
        """Convert SQL to Flink SQL for HOT (Fluss) queries.

        Key behaviors:
        1. Strips fluss.security. prefix (session handles USE CATALOG + USE)
        2. Quotes the reserved keyword `timestamp`
        3. REMOVES time-based WHERE clauses — Fluss primary key table uses snapshot
           scan ONLY when there is no non-PK WHERE filter. Adding a timestamp filter
           forces Flink to use unbounded streaming scan (sees only future records).
           Since Fluss has ~1-2hr retention, all data in the table is already "hot"
           — time filtering is unnecessary and breaks snapshot reads.
        4. Ensures a LIMIT exists to make the scan bounded.
        """
        import re
        result = sql
        result = result.replace("fluss.security.", "")
        result = result.replace("fluss.", "")
        result = result.replace("historical_violence_incidents", "hot_violence_alerts")
        result = result.replace('"timestamp"', '`timestamp`')
        # Strip DISTINCT — causes streaming log scan (misses historical data).
        # KV snapshot scan (plain SELECT) returns all existing records; Python deduplicates.
        result = re.sub(r'\bSELECT\s+DISTINCT\b', 'SELECT', result, flags=re.IGNORECASE)
        # Map non-existent column aliases that Gemini tends to generate
        # hot_violence_alerts schema (enriched): incident_id, camera_id, timestamp,
        #   risk_score, confidence, is_violent, event_type, location, ward_id, district
        #   (NO event_id, id, alert_id, frame_url, frame_data, date_id)
        result = re.sub(r'\bevent_id\b', 'incident_id', result, flags=re.IGNORECASE)
        result = re.sub(r'\balert_id\b', 'incident_id', result, flags=re.IGNORECASE)
        result = re.sub(r'(?<!\w)\bid\b(?!\w)', 'incident_id', result, flags=re.IGNORECASE)
        result = re.sub(r'\bNOW\(\)', "LOCALTIMESTAMP", result, flags=re.IGNORECASE)
        result = re.sub(r'\bCURRENT_TIMESTAMP\b', "LOCALTIMESTAMP", result, flags=re.IGNORECASE)
        # Strip columns that don't exist in hot_violence_alerts
        # location, ward_id, district now exist (true tiering enrichment) — only strip old columns
        for _col in ("frame_url", "frame_data", "date_id"):
            result = re.sub(rf',\s*\b{_col}\b', '', result, flags=re.IGNORECASE)
            result = re.sub(rf'\b{_col}\b\s*,\s*', '', result, flags=re.IGNORECASE)
            result = re.sub(rf'\bWHERE\s+{_col}\b', 'WHERE 1=1', result, flags=re.IGNORECASE)

        # Strip time-based WHERE clauses (backtick-quoted first, then plain).
        # v2: bảng hot_violence_incidents dùng start_ts/last_ts — phải strip cả 2,
        # nếu giữ filter thời gian Fluss chuyển sang unbounded streaming scan
        # (chỉ thấy record TƯƠNG LAI) → trả 0 rows + treo tới timeout.
        _TS_COLS = r"(?:`timestamp`|\"timestamp\"|timestamp|`start_ts`|\"start_ts\"|start_ts|`last_ts`|\"last_ts\"|last_ts)"
        _time_filter = re.compile(
            r"(?:WHERE\s+|AND\s+)"
            + _TS_COLS + r"\s*[><=!]+\s*"
            r"(?:LOCALTIMESTAMP|NOW\(\)|CURRENT_TIMESTAMP)"
            r"(?:\s*[-+]\s*INTERVAL\s*'[^']+'\s*\w+)?",
            re.IGNORECASE,
        )
        result = _time_filter.sub("", result).strip()

        # Also strip standalone TIMESTAMP literal comparisons, including arithmetic expressions:
        # e.g. AND `timestamp` >= TIMESTAMP '2026-05-18 13:23:36'
        # e.g. AND start_ts >= (TIMESTAMP '2026-05-18 13:23:36' - INTERVAL '30' MINUTE)
        _ts_literal = re.compile(
            r"(?:WHERE\s+|AND\s+)"
            + _TS_COLS + r"\s*[><=!]+\s*"
            r"\(?"                                          # optional opening paren
            r"\s*TIMESTAMP\s*'[^']+'"                       # TIMESTAMP 'literal'
            r"(?:\s*[-+]\s*INTERVAL\s*'[^']+'\s*\w+)?"     # optional - INTERVAL 'N' UNIT
            r"\s*\)?",                                      # optional closing paren
            re.IGNORECASE,
        )
        result = _ts_literal.sub("", result).strip()

        # Strip BETWEEN timestamp range filters (e.g. WHERE timestamp BETWEEN TIMESTAMP '...' AND TIMESTAMP '...')
        _ts_between = re.compile(
            r"(?:WHERE\s+|AND\s+)"
            + _TS_COLS + r"\s+BETWEEN\s+"
            r"\(?\s*TIMESTAMP\s*'[^']+'\s*\)?"
            r"\s+AND\s+"
            r"\(?\s*TIMESTAMP\s*'[^']+'\s*\)?",
            re.IGNORECASE,
        )
        result = _ts_between.sub("", result).strip()

        # Fix SQL structure after timestamp removal:
        # 1. "WHERE [whitespace] AND x" → "WHERE x"
        #    Handles: timestamp was the FIRST WHERE condition; WHERE keyword survived
        #    but next condition is orphaned with AND.
        #    \s+ covers both "WHERE AND" (single-line) and "WHERE\n    AND" (multi-line)
        result = re.sub(r'\bWHERE\s+AND\b', 'WHERE', result, flags=re.IGNORECASE)
        # 2. No WHERE left + orphaned AND lines (timestamp WAS the entire WHERE clause;
        #    the regex strips "WHERE\s+<condition>" taking the WHERE keyword with it).
        #    Gemini often formats FROM/table on separate lines so simple FROM+table regex fails.
        #    Solution: if WHERE is completely gone, find the first line-initial AND and make
        #    it a WHERE. multiline mode: ^ matches start of each line.
        if not re.search(r'\bWHERE\b', result, re.IGNORECASE):
            result = re.sub(r'(?m)^(\s*)AND\b', r'\1WHERE ', result, count=1)
        # 3. Trailing WHERE with no conditions (before GROUP BY / ORDER BY / LIMIT / end)
        result = re.sub(
            r'\bWHERE\s+(?=GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING|$)',
            '', result, flags=re.IGNORECASE
        )
        result = result.strip()

        # Remove is_violent = TRUE filter (HOT layer stores all detection events;
        # only ~2% are violent on fresh data → this filter returns 0 rows).
        result = re.sub(r'\bWHERE\s+is_violent\s*=\s*TRUE\s+AND\s+', 'WHERE ', result, flags=re.IGNORECASE)
        result = re.sub(r'\bAND\s+is_violent\s*=\s*TRUE\b', '', result, flags=re.IGNORECASE)
        result = re.sub(r'\bWHERE\s+is_violent\s*=\s*TRUE\b', 'WHERE 1=1', result, flags=re.IGNORECASE)
        result = re.sub(r'\bWHERE\s+1=1\b\s*(?=GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING|\s*$)', '', result, flags=re.IGNORECASE)
        result = result.strip()

        # Strip ORDER BY from streaming HOT queries.
        # Flink streaming aggregates (GROUP BY) don't support ORDER BY on aggregate columns —
        # only time-attribute-based sorting is allowed on unbounded streams.
        # With ORDER BY present, Flink raises an internal server error on result fetch.
        # We remove ORDER BY entirely and let Python post-process sort if needed.
        # Case 1: ORDER BY ... LIMIT N → keep only LIMIT N
        result = re.sub(
            r'\bORDER\s+BY\b.*?\bLIMIT\b',
            'LIMIT',
            result, flags=re.IGNORECASE | re.DOTALL
        )
        # Case 2: trailing ORDER BY (no LIMIT after it)
        result = re.sub(
            r'\bORDER\s+BY\b[^;]*$',
            '',
            result, flags=re.IGNORECASE
        )
        result = result.strip()

        # Normalize LIMIT: Gemini may generate LIMIT 1 for "top camera" queries,
        # but without ORDER BY this returns an arbitrary row. Use LIMIT 100 instead.
        result = re.sub(r'\bLIMIT\s+\d+\b', 'LIMIT 100', result, flags=re.IGNORECASE)

        # Ensure LIMIT exists (bounds the Flink snapshot scan)
        if not re.search(r'\bLIMIT\b', result, re.IGNORECASE):
            result = result.rstrip(";").strip() + " LIMIT 100"

        # Quote reserved keyword `timestamp` (after WHERE removal to avoid partial matches)
        result = re.sub(r"(?<!`)\btimestamp\b(?!`\s*\()", "`timestamp`", result, flags=re.IGNORECASE)
        return result

    @staticmethod
    def _adapt_sql_to_iceberg(sql: str) -> str:
        """Rewrite SQL targeting Paimon/Fluss tables to use Iceberg equivalents."""
        import re

        RAW = "iceberg.security.historical_violence_incidents"
        result = sql

        for old, new in [
            ("paimon.security.daily_incident_stats",  RAW),
            ("fluss.security.daily_incident_stats",   RAW),
            ("iceberg.security.daily_incident_stats", RAW),
            ("paimon.security.camera_stats",          RAW),
            ("fluss.security.camera_stats",           RAW),
            ("iceberg.security.camera_stats",         RAW),
            ("paimon.security.violence_incidents",    RAW),
            ("fluss.security.hot_violence_alerts",    RAW),
            ("fluss.security.violence_incidents",     RAW),
            ("iceberg.security.violence_incidents",   RAW),
            ("paimon.security.", "iceberg.security."),
            ("paimon.",          "iceberg."),
            ("fluss.security.",  "iceberg.security."),
            ("fluss.",           "iceberg."),
        ]:
            result = result.replace(old, new)

        for pattern, replacement in [
            (r"(?<![.\w])violence_incidents\b",   RAW),
            (r"(?<![.\w])hot_violence_alerts\b",  RAW),
        ]:
            result = re.sub(pattern, replacement, result)

        for agg_table in ("daily_incident_stats", "camera_stats"):
            if not re.search(rf"\b{agg_table}\b", result, re.IGNORECASE):
                continue

            interval_match = re.search(
                r"INTERVAL\s+'?(\d+)'?\s+(HOUR|DAY|MINUTE|MONTH|YEAR)",
                result, re.IGNORECASE
            )
            if interval_match:
                qty, unit = interval_match.group(1), interval_match.group(2).upper()
                time_filter = f"timestamp >= NOW() - INTERVAL '{qty}' {unit}"
            else:
                time_filter = "1=1"

            if agg_table == "camera_stats" or re.search(r"\bcamera_id\b", result, re.IGNORECASE):
                result = (
                    f"SELECT camera_id, COUNT(*) AS total_incidents,\n"
                    f"       SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) AS violent_incidents,\n"
                    f"       AVG(risk_score) AS avg_risk_score\n"
                    f"FROM {RAW}\n"
                    f"WHERE {time_filter}\n"
                    f"GROUP BY camera_id\n"
                    f"ORDER BY violent_incidents DESC\n"
                    f"LIMIT 20"
                )
            else:
                result = (
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

        return result

    # ── Public layer query methods ─────────────────────────────────────────────

    def query_fluss(self, sql: str, timeout: int = 30) -> List[Dict[str, Any]]:
        """Query HOT layer (Fluss) via Flink SQL Gateway.

        Uses a cached session to avoid 3 DDL roundtrips (CREATE CATALOG + USE + USE)
        on every call. Falls back to fresh session on catalog/session errors.

        Fluss queries run as unbounded streaming aggregations (BATCH mode unsupported
        for full table scans). We cap the polling deadline at FLUSS_MAX_TIMEOUT so the
        caller gets the latest streaming aggregate rather than waiting indefinitely.
        _exec_flink_statement returns `latest_agg_rows` when the deadline expires.
        """
        # Fluss streaming aggregates stabilise in ~10s; 45s cap keeps latency reasonable
        FLUSS_MAX_TIMEOUT = 45
        fluss_timeout = min(timeout, FLUSS_MAX_TIMEOUT)

        # Cancel zombie 'collect' jobs before submitting new query to ensure task slots
        self._cleanup_collect_jobs()

        logger.info(f"Querying Fluss (HOT): {sql[:100]}... (timeout={fluss_timeout}s)")

        import re as _re
        flink_sql = self._adapt_sql_for_flink_hot(sql)
        flink_sql = _re.sub(r'\bviolence_incidents\b', 'hot_violence_alerts', flink_sql)

        gateway_url = f"http://{self.flink_gateway_host}:{self.flink_gateway_port}"
        _session_errors = (
            "catalog_not_found", "does not exist", "session", "not found",
            "no resource", "noresource", "could not acquire",
        )

        for attempt in range(2):
            try:
                session_id = self._ensure_fluss_session(init_timeout=60)
                return self._exec_flink_statement(session_id, flink_sql, gateway_url, fluss_timeout)
            except Exception as e:
                err_lower = str(e).lower()
                if attempt == 0 and any(x in err_lower for x in _session_errors):
                    logger.warning(
                        f"Fluss query failed (attempt {attempt+1}), invalidating session: {e}"
                    )
                    # Invalidate cached session so next attempt creates fresh one
                    self._fluss_session_id = None
                    time.sleep(2)
                    continue
                raise

    def query_paimon(self, sql: str, timeout: int = 30) -> List[Dict[str, Any]]:
        """Query WARM layer (Paimon) via Trino native paimon catalog.

        Uses paimon-trino-440 connector. Table refs must be fully-qualified:
        paimon.security.violence_incidents, paimon.security.daily_incident_stats, etc.
        """
        import re as _re
        # Trino uses ANSI double-quotes for identifiers; convert MySQL/Flink-style backticks
        sql = _re.sub(r'`([^`]+)`', r'"\1"', sql)
        logger.info(f"Querying Paimon (WARM) via Trino: {sql[:100]}...")
        return self._query_trino(sql, catalog="paimon", schema="security", timeout=timeout)

    def query_iceberg(self, sql: str, timeout: int = 30) -> List[Dict[str, Any]]:
        """Query COLD layer (Iceberg) via Trino."""
        import re as _re
        logger.info(f"Querying Iceberg (COLD): {sql[:100]}...")
        sql = self._adapt_sql_to_iceberg(sql)
        # Trino uses ANSI double-quotes; convert backtick identifiers
        sql = _re.sub(r'`([^`]+)`', r'"\1"', sql)
        return self._query_trino(sql, catalog="iceberg", schema="security", timeout=timeout)

    def route_query(
        self,
        sql: str,
        layer: DataLayer,
        timeout: int = 30
    ) -> List[Dict[str, Any]]:
        """Route query to the correct layer with Iceberg fallback."""
        layer_str = str(layer).upper() if layer else ""
        _infra_errors = (
            "catalog_not_found", "catalog", "connection refused",
            "connection timeout", "timeout", "unable to connect",
            "refused", "connect timed out", "table_not_found",
            "does not exist", "no such table",
            "server error", "500", "internal server error", "not found",
        )

        if "FLUSS" in layer_str or layer == DataLayer.FLUSS:
            try:
                return self.query_fluss(sql, timeout)
            except Exception as e:
                if any(x in str(e).lower() for x in _infra_errors):
                    logger.warning(f"Fluss unavailable → falling back to Iceberg")
                    return self.query_iceberg(self._adapt_sql_to_iceberg(sql), timeout)
                raise

        elif "PAIMON" in layer_str or layer == DataLayer.PAIMON:
            try:
                return self.query_paimon(sql, timeout)
            except Exception as e:
                if any(x in str(e).lower() for x in _infra_errors):
                    logger.warning(f"Paimon unavailable → falling back to Iceberg")
                    return self.query_iceberg(self._adapt_sql_to_iceberg(sql), timeout)
                raise

        elif "ICEBERG" in layer_str or layer == DataLayer.ICEBERG:
            return self.query_iceberg(sql, timeout)

        else:
            raise ValueError(f"Invalid data layer: {layer}")

    def query_union_all_layers(self) -> List[Dict[str, Any]]:
        """Federated read across all 3 layers, merged and sorted by time.

        HOT  → last 1h from Fluss via Flink SQL Gateway
        WARM → 1h–7d from Paimon via Trino
        COLD → 7d+ from Iceberg via Trino (last 20 rows)

        Returns combined list sorted by timestamp descending, max 20 rows total.
        """
        HOT_SQL = """
            SELECT 'HOT' AS layer, camera_id,
                   CAST(risk_score AS VARCHAR) AS score,
                   CAST(`timestamp` AS VARCHAR) AS event_time
            FROM hot_violence_alerts
            WHERE `timestamp` > LOCALTIMESTAMP - INTERVAL '1' HOUR
            ORDER BY `timestamp` DESC
            LIMIT 10
        """
        WARM_SQL = """
            SELECT 'WARM' AS layer, camera_id,
                   CAST(risk_score AS VARCHAR) AS score,
                   CAST(timestamp AS VARCHAR) AS event_time
            FROM paimon.security.violence_incidents
            WHERE timestamp >= NOW() - INTERVAL '7' DAY
            ORDER BY timestamp DESC
            LIMIT 10
        """
        COLD_SQL = """
            SELECT 'COLD' AS layer, camera_id,
                   CAST(risk_score AS VARCHAR) AS score,
                   CAST(timestamp AS VARCHAR) AS event_time
            FROM iceberg.security.historical_violence_incidents
            ORDER BY timestamp DESC
            LIMIT 10
        """

        results: List[Dict[str, Any]] = []

        for label, query_fn, sql in [
            ("HOT",  self.query_fluss,   HOT_SQL),
            ("WARM", self.query_paimon,  WARM_SQL),
            ("COLD", self.query_iceberg, COLD_SQL),
        ]:
            try:
                rows = query_fn(sql, timeout=60)
                results.extend(rows)
                logger.info(f"Union {label}: {len(rows)} rows")
            except Exception as e:
                logger.warning(f"Union {label} layer failed (skipped): {e}")

        results.sort(key=lambda r: str(r.get("event_time", "")), reverse=True)
        return results[:20]

    def health_check(self) -> Dict[str, bool]:
        """Check health of all data layers."""
        health = {"fluss": False, "paimon": False, "iceberg": False}

        try:
            self.query_fluss("SELECT 1", timeout=10)
            health["fluss"] = True
        except Exception as e:
            logger.warning(f"Fluss health check failed: {e}")

        try:
            self._query_trino(
                "SELECT incident_id FROM paimon.security.violence_incidents LIMIT 1",
                catalog="paimon", schema="security", timeout=15,
            )
            health["paimon"] = True
        except Exception as e:
            logger.warning(f"Paimon health check failed: {e}")

        try:
            self.query_iceberg(
                "SELECT COUNT(*) FROM iceberg.security.historical_violence_incidents LIMIT 1",
                timeout=10
            )
            health["iceberg"] = True
        except Exception as e:
            logger.warning(f"Iceberg health check failed: {e}")

        return health
