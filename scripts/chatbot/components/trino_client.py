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
        HTTP_TIMEOUT = min(timeout, 30)
        TOTAL_DEADLINE = time.time() + 240
        result_token = 0
        all_rows: List[Dict[str, Any]] = []
        latest_agg_rows: List[Dict[str, Any]] = []
        columns: List[Dict] = []
        stable_polls = 0

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
                    if row_kind in ("UPDATE_AFTER", "INSERT"):
                        page_rows.append(row)

            if page_rows:
                stable_polls = 0
                all_rows.extend(page_rows)
                latest_agg_rows = page_rows
            else:
                stable_polls += 1

            if result_type == "EOS":
                return all_rows if all_rows else latest_agg_rows

            next_uri = data.get("nextResultUri")
            if next_uri:
                try:
                    result_token = int(next_uri.rstrip("/").split("/")[-1])
                except (ValueError, IndexError):
                    break
                continue

            if not is_running:
                break

            if stable_polls >= 3 and latest_agg_rows:
                logger.info(f"Streaming aggregate stabilized after {stable_polls} empty polls")
                return latest_agg_rows

            time.sleep(2)

        elapsed = 240 - (TOTAL_DEADLINE - time.time())
        logger.info(f"Flink statement polling ended after {elapsed:.0f}s: "
                    f"{len(all_rows)} direct rows, {len(latest_agg_rows)} agg rows")
        return all_rows if all_rows else latest_agg_rows

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

        Strips fluss.security. prefix (handled by USE CATALOG + USE in session init),
        fixes TIMESTAMP_LTZ vs TIMESTAMP(3) mismatch, and quotes reserved keywords.
        """
        import re
        result = sql
        result = result.replace("fluss.security.", "")
        result = result.replace("fluss.", "")
        result = result.replace("historical_violence_incidents", "hot_violence_alerts")
        result = result.replace('"timestamp"', '`timestamp`')
        result = re.sub(r'\bNOW\(\)', "LOCALTIMESTAMP", result, flags=re.IGNORECASE)
        result = re.sub(r'\bCURRENT_TIMESTAMP\b', "LOCALTIMESTAMP", result, flags=re.IGNORECASE)
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

        Retries session setup once on catalog_not_found before falling back.
        """
        logger.info(f"Querying Fluss (HOT): {sql[:100]}...")

        flink_sql = self._adapt_sql_for_flink_hot(sql)
        import re as _re
        flink_sql = _re.sub(r'\bviolence_incidents\b', 'hot_violence_alerts', flink_sql)

        init = [
            _FLUSS_CATALOG_DDL,
            "USE CATALOG fluss_hot",
            "USE `security`",
        ]

        _catalog_errors = ("catalog_not_found", "catalog", "does not exist", "not found")

        for attempt in range(2):
            try:
                return self._query_flink_gateway(flink_sql, init_statements=init, timeout=timeout)
            except Exception as e:
                err_lower = str(e).lower()
                if attempt == 0 and any(x in err_lower for x in _catalog_errors):
                    logger.warning(
                        f"Fluss catalog init failed (attempt {attempt+1}), retrying: {e}"
                    )
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
            WHERE timestamp BETWEEN NOW() - INTERVAL '7' DAY AND NOW() - INTERVAL '1' HOUR
            ORDER BY timestamp DESC
            LIMIT 10
        """
        COLD_SQL = """
            SELECT 'COLD' AS layer, camera_id,
                   CAST(risk_score AS VARCHAR) AS score,
                   CAST(timestamp AS VARCHAR) AS event_time
            FROM iceberg.security.historical_violence_incidents
            WHERE timestamp < NOW() - INTERVAL '7' DAY
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
