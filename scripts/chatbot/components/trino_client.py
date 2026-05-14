"""
Trino Client - Multi-Layer Query Routing

Manages PyTrino connections and Flink SQL Gateway calls for layer-based routing.

Layer routing:
  HOT   (Fluss)   → Flink SQL Gateway REST API (port 8083)
  WARM  (Paimon)  → Flink SQL Gateway REST API with per-session Paimon catalog DDL
  COLD  (Iceberg) → Trino via PyTrino (catalog: iceberg)

Paimon-Trino connector note: paimon-trino-476 has no pre-built release JAR and
cannot be compiled inside Docker due to network restrictions on repository.apache.org.
Paimon warm queries are therefore routed through the Flink SQL Gateway, which already
has paimon-flink-1.18-0.8.2.jar installed and can issue CREATE CATALOG DDL per session.
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


# ── Paimon catalog DDL used per-session in Flink SQL Gateway ──────────────────
_PAIMON_CATALOG_DDL = """CREATE CATALOG paimon_warm WITH (
  'type' = 'paimon',
  'warehouse' = 's3://warehouse/paimon',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key' = '{access_key}',
  's3.secret-key' = '{secret_key}',
  's3.path.style.access' = 'true'
)"""

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

        # S3 credentials for Paimon catalog DDL (read from env, default to dev values)
        self._s3_access_key = os.getenv("MINIO_ROOT_USER", "minio")
        self._s3_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")

        logger.info(
            f"Initialized TrinoClient: "
            f"Trino {trino_host}:{trino_port}, "
            f"Flink Gateway {flink_gateway_host}:{flink_gateway_port}"
        )

    # ── Low-level: Flink SQL Gateway ──────────────────────────────────────────

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
        # Two execution modes via the gateway:
        #   Bounded (SELECT ... LIMIT N): ends with EOS → return all accumulated rows.
        #   Streaming aggregate (COUNT(*), SUM ...): emits UPDATE_AFTER pages indefinitely;
        #     we track the latest "snapshot" (last page of UPDATE_AFTER rows) and return
        #     it on timeout or when values stop changing.
        # Paimon jobs take 30-150s to produce first result; total wall-clock budget = 240s.
        HTTP_TIMEOUT = min(timeout, 30)  # per-request HTTP timeout (fast round-trips)
        TOTAL_DEADLINE = time.time() + 240   # 4 minutes max per statement
        result_token = 0
        all_rows: List[Dict[str, Any]] = []
        latest_agg_rows: List[Dict[str, Any]] = []  # for streaming aggregates
        columns: List[Dict] = []
        stable_polls = 0  # consecutive polls with no new data → detect stable aggregate

        while time.time() < TOTAL_DEADLINE:
            result_resp = requests.get(
                f"{gateway_url}/v1/sessions/{session_id}/operations/{op_handle}/result/{result_token}",
                timeout=HTTP_TIMEOUT
            )
            result_resp.raise_for_status()
            data = result_resp.json()

            result_type = data.get("resultType", "NOT_READY")
            is_running = data.get("isQueryRunning", result_type == "NOT_READY")

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
                    # For streaming aggregates: UPDATE_AFTER replaces the running total
                    if row_kind in ("UPDATE_AFTER", "INSERT"):
                        page_rows.append(row)

            if page_rows:
                stable_polls = 0
                # For bounded queries: accumulate rows
                all_rows.extend(page_rows)
                # For streaming aggregates: replace with latest snapshot
                latest_agg_rows = page_rows
            else:
                stable_polls += 1

            if result_type == "EOS":
                # Bounded query finished normally
                return all_rows if all_rows else latest_agg_rows

            next_uri = data.get("nextResultUri")
            if next_uri:
                try:
                    result_token = int(next_uri.rstrip("/").split("/")[-1])
                except (ValueError, IndexError):
                    break
                continue  # fetch next page immediately

            if not is_running:
                break

            # Stable aggregate: 3 consecutive empty polls → values have converged
            if stable_polls >= 3 and latest_agg_rows:
                logger.info(f"Streaming aggregate stabilized after {stable_polls} empty polls")
                return latest_agg_rows

            time.sleep(2)

        elapsed = 240 - (TOTAL_DEADLINE - time.time())
        logger.info(f"Flink statement polling ended after {elapsed:.0f}s: "
                    f"{len(all_rows)} direct rows, {len(latest_agg_rows)} agg rows")
        # Return whatever we have: bounded rows or last streaming aggregate snapshot
        return all_rows if all_rows else latest_agg_rows

    def _query_flink_gateway(
        self,
        sql: str,
        init_statements: Optional[List[str]] = None,
        timeout: int = 30
    ) -> List[Dict[str, Any]]:
        """Execute SQL via Flink SQL Gateway REST API.

        Args:
            sql: Main query SQL
            init_statements: Optional DDL/USE statements to run before the query
            timeout: Per-request timeout in seconds
        """
        gateway_url = f"http://{self.flink_gateway_host}:{self.flink_gateway_port}"

        # Create session
        sess_resp = requests.post(f"{gateway_url}/v1/sessions", json={}, timeout=timeout)
        sess_resp.raise_for_status()
        session_id = sess_resp.json()["sessionHandle"]
        logger.info(f"Flink SQL Gateway session: {session_id}")

        try:
            # Run optional init statements (catalog DDL, USE, etc.)
            for stmt in (init_statements or []):
                self._exec_flink_statement(session_id, stmt, gateway_url, timeout)

            # Run main query
            return self._exec_flink_statement(session_id, sql, gateway_url, timeout)
        finally:
            # Best-effort session cleanup
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
    def _adapt_sql_for_flink(sql: str) -> str:
        """Convert Trino-dialect SQL to Flink SQL for Paimon queries.

        Differences handled:
        - Table references: strip 'paimon.security.' prefix (handled by USE CATALOG + USE)
        - Reserved keyword quoting: "timestamp" (Trino) → `timestamp` (Flink)
        - Timestamp type mismatch: NOW()/CURRENT_TIMESTAMP return TIMESTAMP_LTZ(3) but
          Paimon stores TIMESTAMP(3) — wrap with CAST to avoid CodeGenException.
        """
        import re
        result = sql
        # Strip any catalog.schema prefix — the gateway session runs USE paimon_warm + security.
        # Gemini may emit any catalog prefix (paimon/fluss/iceberg) regardless of routed layer.
        for prefix in ("paimon.security.", "paimon.", "fluss.security.", "fluss.",
                       "iceberg.security.", "iceberg."):
            result = result.replace(prefix, "")
        # Remap Fluss/Iceberg table aliases to Paimon warm table names
        result = result.replace("hot_violence_alerts", "violence_incidents")
        result = result.replace("historical_violence_incidents", "violence_incidents")
        result = result.replace("historical_daily_stats", "daily_incident_stats")
        result = result.replace("historical_camera_stats", "camera_stats")
        # Trino uses double-quotes for reserved keywords; Flink uses backticks
        result = result.replace('"timestamp"', '`timestamp`')
        # TIMESTAMP_LTZ vs TIMESTAMP(3) fix: wrap NOW() and CURRENT_TIMESTAMP
        result = re.sub(r'\bNOW\(\)', "CAST(NOW() AS TIMESTAMP(3))", result, flags=re.IGNORECASE)
        result = re.sub(r'\bCURRENT_TIMESTAMP\b', "CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))", result, flags=re.IGNORECASE)
        return result

    @staticmethod
    def _adapt_sql_to_iceberg(sql: str) -> str:
        """Rewrite SQL targeting Paimon/Fluss tables to use Iceberg equivalents.

        Only iceberg.security.historical_violence_incidents actually exists.
        Queries against aggregate tables (daily_incident_stats, camera_stats)
        are rewritten to inline aggregations over historical_violence_incidents.
        """
        import re

        RAW = "iceberg.security.historical_violence_incidents"
        result = sql

        # ── Step 1: Strip catalog/schema prefixes from aggregate table refs ──────
        for old, new in [
            # daily_incident_stats → always rewrite to raw table
            ("paimon.security.daily_incident_stats",  "daily_incident_stats"),
            ("fluss.security.daily_incident_stats",   "daily_incident_stats"),
            ("iceberg.security.daily_incident_stats", "daily_incident_stats"),
            ("iceberg.security.historical_daily_stats", "daily_incident_stats"),
            # camera_stats → always rewrite to raw table
            ("paimon.security.camera_stats",          "camera_stats"),
            ("fluss.security.camera_stats",           "camera_stats"),
            ("iceberg.security.camera_stats",         "camera_stats"),
            ("iceberg.security.historical_camera_stats", "camera_stats"),
            # violence_incidents → map to the real Iceberg table
            ("paimon.security.violence_incidents",    RAW),
            ("fluss.security.hot_violence_alerts",    RAW),
            ("fluss.security.violence_incidents",     RAW),
            ("iceberg.security.violence_incidents",   RAW),
            # Bare catalog prefixes (catch remaining paimon./fluss. refs)
            ("paimon.security.", "iceberg.security."),
            ("paimon.",          "iceberg."),
            ("fluss.security.",  "iceberg.security."),
            ("fluss.",           "iceberg."),
        ]:
            result = result.replace(old, new)

        # ── Step 2: Unqualified standalone table names ───────────────────────────
        for pattern, replacement in [
            (r"(?<![.\w])violence_incidents\b",   RAW),
            (r"(?<![.\w])hot_violence_alerts\b",  RAW),
        ]:
            result = re.sub(pattern, replacement, result)

        # ── Step 3: Rewrite queries against non-existent aggregate tables ──────────
        # daily_incident_stats and camera_stats only exist in Paimon, not Iceberg.
        # Replace the entire SQL with an equivalent query over the raw incidents table.
        for agg_table in ("daily_incident_stats", "camera_stats"):
            if not re.search(rf"\b{agg_table}\b", result, re.IGNORECASE):
                continue

            # Extract time filter interval if present (e.g. INTERVAL '24' HOUR, INTERVAL '7' DAY)
            interval_match = re.search(
                r"INTERVAL\s+'?(\d+)'?\s+(HOUR|DAY|MINUTE|MONTH|YEAR)",
                result, re.IGNORECASE
            )
            if interval_match:
                qty, unit = interval_match.group(1), interval_match.group(2).upper()
                time_filter = f"timestamp >= NOW() - INTERVAL '{qty}' {unit}"
            else:
                time_filter = "1=1"  # no time restriction

            # Detect GROUP BY camera — use camera-level aggregation
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
                # Generic daily aggregation
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
            break  # only one rewrite needed

        return result

    # ── Public layer query methods ─────────────────────────────────────────────

    def query_fluss(self, sql: str, timeout: int = 30) -> List[Dict[str, Any]]:
        """Query HOT layer (Fluss) via Flink SQL Gateway.

        Sets up the fluss_hot catalog per-session before executing the query.
        Table reference: fluss_hot.security.hot_violence_alerts
        """
        logger.info(f"Querying Fluss (HOT): {sql[:100]}...")

        # Strip any catalog/schema prefix — the gateway session runs USE CATALOG + USE
        import re as _re
        flink_sql = sql
        for prefix in ("fluss.security.", "fluss.",
                       "paimon.security.", "paimon.",
                       "iceberg.security.", "iceberg."):
            flink_sql = flink_sql.replace(prefix, "")
        # Normalise table name: violence_incidents → hot_violence_alerts for Fluss
        flink_sql = _re.sub(r'\bviolence_incidents\b', 'hot_violence_alerts', flink_sql)
        # Quote reserved keyword `timestamp` if used bare
        flink_sql = _re.sub(r'(?<![`"\w])timestamp(?![`"\w(])', '`timestamp`', flink_sql, flags=_re.IGNORECASE)

        init = [
            _FLUSS_CATALOG_DDL,
            "USE CATALOG fluss_hot",
            "USE `security`",
        ]

        return self._query_flink_gateway(flink_sql, init_statements=init, timeout=timeout)

    def query_paimon(self, sql: str, timeout: int = 30) -> List[Dict[str, Any]]:
        """Query WARM layer (Paimon) via Flink SQL Gateway with per-session catalog.

        Falls back to Trino paimon catalog if the gateway is unavailable,
        then raises so route_query can cascade to Iceberg.
        """
        logger.info(f"Querying Paimon (WARM): {sql[:100]}...")

        flink_sql = self._adapt_sql_for_flink(sql)
        catalog_ddl = _PAIMON_CATALOG_DDL.format(
            access_key=self._s3_access_key,
            secret_key=self._s3_secret_key,
        )
        init = [
            catalog_ddl,
            "USE CATALOG paimon_warm",
            "USE `security`",
        ]

        try:
            return self._query_flink_gateway(flink_sql, init_statements=init, timeout=timeout)
        except Exception as gw_err:
            logger.warning(
                f"Flink SQL Gateway unavailable for Paimon ({gw_err.__class__.__name__}), "
                "trying Trino paimon catalog..."
            )

        # Trino paimon catalog fallback (requires paimon-trino JAR — may not be installed)
        adapted = sql
        for prefix in ("fluss.security.", "fluss."):
            adapted = adapted.replace(prefix, "paimon.security.")
        return self._query_trino(adapted, catalog="paimon", schema="security", timeout=timeout)

    def query_iceberg(self, sql: str, timeout: int = 30) -> List[Dict[str, Any]]:
        """Query COLD layer (Iceberg) via Trino."""
        logger.info(f"Querying Iceberg (COLD): {sql[:100]}...")
        # Always normalise table references — handles both direct Iceberg routing
        # and Paimon/Fluss fallback paths
        sql = self._adapt_sql_to_iceberg(sql)
        return self._query_trino(sql, catalog="iceberg", schema="security", timeout=timeout)

    def route_query(
        self,
        sql: str,
        layer: DataLayer,
        timeout: int = 30
    ) -> List[Dict[str, Any]]:
        """Route query to the correct layer with Iceberg fallback.

        Fallback triggers on: catalog not found, connection refused/timeout,
        unable to connect — any infrastructure-level failure.
        """
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

    def health_check(self) -> Dict[str, bool]:
        """Check health of all data layers."""
        health = {"fluss": False, "paimon": False, "iceberg": False}

        try:
            self.query_fluss("SELECT 1", timeout=10)
            health["fluss"] = True
        except Exception as e:
            logger.warning(f"Fluss health check failed: {e}")

        try:
            catalog_ddl = _PAIMON_CATALOG_DDL.format(
                access_key=self._s3_access_key,
                secret_key=self._s3_secret_key,
            )
            self._query_flink_gateway(
                "SELECT incident_id FROM violence_incidents LIMIT 1",
                init_statements=[
                    catalog_ddl,
                    "USE CATALOG paimon_warm",
                    "USE `security`",
                ],
                timeout=60,
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
