"""
LangGraph Agent Framework

6-node agentic RAG agent for violence detection chatbot:
1. understand_query - Parse Vietnamese intent
2. select_data_layer - Route to Fluss/Paimon/Iceberg
3. generate_sql - Create Trino SQL
4. execute_query - Run query
5. self_correct - Retry on failure (max 3x)
6. generate_response - Vietnamese answer with citations
"""

from typing import Any, Dict, List, Optional, TypedDict, Annotated, Union
from enum import Enum
from datetime import datetime, timedelta
import json
import re
import time

from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, END

# CompiledStateGraph type varies by langgraph version — use Any as fallback
try:
    from langgraph.graph.state import CompiledStateGraph
except ImportError:
    try:
        from langgraph.types import CompiledStateGraph
    except ImportError:
        CompiledStateGraph = Any  # type: ignore

try:
    import google.generativeai as genai
except ImportError:
    genai = None

from .logger import setup_logger, log_agent_node
from .components.chromadb_wrapper import ChromaDBWrapper
from .components.trino_client import TrinoClient, DataLayer
from .components.sql_generator import SQLGenerator
from .components.evidence_service import EvidenceService
from .components.schema_registry import get_schema_for_prompt, get_full_table_ref

logger = setup_logger(__name__)

# Global component instances (initialized in main.py)
_chromadb: Optional[ChromaDBWrapper] = None
_trino_client: Optional[TrinoClient] = None
_sql_generator: Optional[SQLGenerator] = None
_evidence_service: Optional[EvidenceService] = None


def set_components(
    chromadb: ChromaDBWrapper,
    trino_client: TrinoClient,
    sql_generator: SQLGenerator,
    evidence_service: EvidenceService
) -> None:
    """Set global component instances."""
    global _chromadb, _trino_client, _sql_generator, _evidence_service
    _chromadb = chromadb
    _trino_client = trino_client
    _sql_generator = sql_generator
    _evidence_service = evidence_service
    logger.info("Agent components initialized")


# ============================================================================
# Data Models
# ============================================================================

class LayerChoice(str, Enum):
    """Data layer options."""
    FLUSS = "Fluss"
    PAIMON = "Paimon"
    ICEBERG = "Iceberg"


class IntentSchema(BaseModel):
    """Extracted user intent."""
    time_period: str = Field(..., description="Time period in natural language (e.g., '1 day ago')")
    location: Optional[str] = Field(None, description="Location filter (district, ward, etc.)")
    metric: str = Field(default="count", description="Aggregation metric (count, avg, sum, max)")
    intent_type: str = Field(..., description="Intent type (aggregate, trend, comparison, etc.)")
    filter_camera: Optional[str] = Field(None, description="Specific camera ID if mentioned")
    query_confidence: float = Field(ge=0.0, le=1.0, description="Confidence score of intent extraction")


class QueryResult(BaseModel):
    """Result from query execution."""
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    row_count: Optional[int] = None


class AgentState(TypedDict):
    """State dictionary for LangGraph agent."""
    # Input
    user_query: str
    context: str
    options: Dict[str, Any]
    request_id: str

    # Intent extraction
    intent: Optional[IntentSchema]

    # Layer selection
    selected_layer: Optional[LayerChoice]
    trino_catalog: Optional[str]
    trino_schema: str
    table_name: Optional[str]

    # SQL generation
    generated_sql: Optional[str]

    # Query execution
    query_result: Optional[QueryResult]

    # Retry logic
    retry_count: int
    retry_errors: list[str]

    # Response generation
    final_answer: Optional[str]
    response_confidence: float
    source_table: Optional[str]
    data_layer: Optional[str]
    time_period: Optional[str]
    row_count: Optional[int]

    # Frame evidence (for single-result queries)
    incident_id: Optional[str]
    camera_id: Optional[str]
    incident_date: Optional[str]
    frame_url: Optional[str]
    frame_base64: Optional[str]

    # Metadata
    start_time: float
    duration_ms: Optional[int]


# ============================================================================
# Node Functions (Stubs - To Be Implemented)
# ============================================================================

async def understand_query(state: AgentState) -> AgentState:
    """
    Node 1: Extract intent from Vietnamese natural language query.

    Parses user question to extract:
    - time_period: "hôm nay", "tuần trước", "1 tháng trước"
    - location: "quận 1", "phường X", "đường Y"
    - metric: "count", "average", "sum"
    - intent_type: "aggregate_count", "time_series", "comparison"
    """
    log_agent_node(logger, state["request_id"], "understand_query", "started")

    try:
        user_query = state["user_query"]

        if not genai:
            logger.warning("Gemini not available - using fallback intent parsing")
            state["intent"] = IntentSchema(
                time_period="today",
                location=None,
                metric="count",
                intent_type="statistics",
                query_confidence=0.5,
            )
            return state

        # Use Gemini to parse Vietnamese intent
        model = genai.GenerativeModel("gemini-2.5-flash")

        prompt = f"""
Phân tích câu hỏi tiếng Việt này và trích xuất ý định người dùng.
Trả về JSON với 5 trường: time_period, location, metric, intent_type, query_confidence

Câu hỏi: "{user_query}"

Hướng dẫn:
- time_period: "hôm nay", "hôm qua", "tuần trước", "tháng trước", "7 ngày", hoặc kiểu thời gian khác
- location: Tên quận/huyện hoặc null nếu toàn thành phố
- metric: "count", "average", "max", "min", "list"
- intent_type: "statistics", "query_recent", "trend", "comparison"
- query_confidence: [0.0-1.0] độ tin cậy trong việc hiểu ý định

Ví dụ JSON:
{{"time_period": "hôm nay", "location": "quận 1", "metric": "count", "intent_type": "statistics", "query_confidence": 0.95}}

Trả về CHỈ JSON, không có giải thích.
        """.strip()

        response = model.generate_content(prompt)
        response_text = response.text.strip()

        # Parse JSON from response
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            intent_dict = json.loads(json_match.group())
            state["intent"] = IntentSchema(
                time_period=intent_dict.get("time_period", "today"),
                location=intent_dict.get("location"),
                metric=intent_dict.get("metric", "count"),
                intent_type=intent_dict.get("intent_type", "statistics"),
                query_confidence=float(intent_dict.get("query_confidence", 0.8)),
            )
        else:
            # Fallback if JSON parsing fails
            logger.warning(f"Failed to parse Gemini response: {response_text[:100]}")
            state["intent"] = IntentSchema(
                time_period="today",
                location=None,
                metric="count",
                intent_type="statistics",
                query_confidence=0.5,
            )

        log_agent_node(
            logger,
            state["request_id"],
            "understand_query",
            "completed",
            {
                "intent_type": state["intent"].intent_type,
                "confidence": state["intent"].query_confidence,
                "location": state["intent"].location
            }
        )

        return state

    except Exception as e:
        logger.error(f"Intent extraction failed: {e}")
        # Keyword-based Vietnamese fallback parser (works without Gemini)
        state["intent"] = _parse_intent_keywords(state["user_query"])
        return state


def _parse_intent_keywords(query: str) -> IntentSchema:
    """Vietnamese keyword-based intent parser (fallback when Gemini unavailable).

    Detects time period and routes correctly without LLM.
    """
    q = query.lower()

    # Time period detection (order matters: most specific first)
    # Step 1: Numeric patterns — "N phút/giờ/ngày qua" (handles sub-hour HOT routing)
    _num_match = re.search(
        r'(\d+)\s*(phút|phut|minute|min|giờ|gio|hour|ngày|ngay|day|tuần|tuan|week|tháng|thang|month)',
        q
    )
    if _num_match:
        _n = int(_num_match.group(1))
        _unit = _num_match.group(2).lower()
        if _unit in ("phút", "phut", "minute", "min"):
            time_period = f"{_n} phút qua"   # <1hr → FLUSS (handled by select_data_layer regex)
        elif _unit in ("giờ", "gio", "hour"):
            time_period = f"{_n} giờ qua"    # hour-based → PAIMON/FLUSS per routing
        elif _unit in ("ngày", "ngay", "day"):
            time_period = f"{_n} ngày qua"
        elif _unit in ("tuần", "tuan", "week"):
            time_period = f"{_n} tuần qua"
        elif _unit in ("tháng", "thang", "month"):
            time_period = f"{_n} tháng qua"
        else:
            time_period = "hôm nay"
    # Historical year-based query → COLD (Iceberg)
    elif re.search(r'\b(năm|nam|year)\s*(20\d{2}|19\d{2})\b|\b(20\d{2}|19\d{2})\b', q):
        time_period = "năm trước"   # select_data_layer: no numeric match, keyword "năm" → ICEBERG
    elif any(w in q for w in ["tháng trước", "thang truoc", "tháng qua", "thang qua",
                              "30 ngày", "30 ngay", "tháng này", "thang nay", "month"]):
        time_period = "tháng trước"
    elif any(w in q for w in ["tuần trước", "tuan truoc", "tuần qua", "tuan qua",
                               "7 ngày", "7 ngay", "week", "tuần này", "tuan nay"]):
        time_period = "tuần trước"
    elif any(w in q for w in ["hôm qua", "hom qua", "ngày qua", "ngay qua", "yesterday"]):
        time_period = "hôm qua"
    elif any(w in q for w in ["hôm nay", "hom nay", "today", "ngay hom nay", "trong ngày"]):
        time_period = "hôm nay"
    elif any(w in q for w in ["giờ trước", "gio truoc", "1 giờ", "gần đây", "real-time",
                               "trực tiếp", "hot", "mới nhất", "hien tai", "hiện tại",
                               "ngay bay gio", "ngay bây giờ", "vua roi", "vừa rồi",
                               "bay gio", "bây giờ", "now", "real time"]):
        time_period = "mới nhất"  # HOT keyword → select_data_layer routes to FLUSS
    else:
        # Default to "today" if no keyword matched
        time_period = "hôm nay"

    # Metric detection
    if any(w in q for w in ["bao nhiêu", "bao nhieu", "tổng", "tong", "count", "số lượng"]):
        metric = "count"
    elif any(w in q for w in ["trung bình", "trung binh", "average", "avg"]):
        metric = "average"
    elif any(w in q for w in ["cao nhất", "cao nhat", "max", "nguy hiểm nhất"]):
        metric = "max"
    elif any(w in q for w in ["danh sách", "danh sach", "list", "liệt kê"]):
        metric = "list"
    else:
        metric = "count"

    # Location detection (simple — look for "quận", "phường", "camera")
    location = None
    import re as _re
    loc_match = _re.search(
        r'(quận|quan|phường|phuong|camera|cam_)\s*(\w+)', q
    )
    if loc_match:
        location = loc_match.group(0)

    return IntentSchema(
        time_period=time_period,
        location=location,
        metric=metric,
        intent_type="statistics",
        query_confidence=0.6,
    )


async def select_data_layer(state: AgentState) -> AgentState:
    """
    Node 2: Route to appropriate data layer based on time period.

    Routing logic:
    - < 1 hour → Fluss (HOT, <100ms)
    - 1hr-7 days → Paimon (WARM, 1-10min)
    - > 7 days → Iceberg (COLD, 10+min)
    """
    log_agent_node(logger, state["request_id"], "select_data_layer", "started")

    try:
        intent = state["intent"]
        time_period_str = intent.time_period.lower()

        # Parse time period to determine layer.
        # Routing: <1hr → Fluss, 1hr-7d → Paimon, >7d → Iceberg
        # Strategy: numeric regex FIRST (most reliable), then keyword patterns.
        selected_layer = LayerChoice.PAIMON  # default

        # Step 1: Try numeric regex for "N minutes/hours/days/weeks/months" (EN + VI)
        # Vietnamese: phút/phut=minute, giờ=hour, ngày=day, tuần=week, tháng=month
        match = re.search(
            r'(\d+)\s*(minute|phút|phut|min|hour|giờ|gio|day|ngày|ngay|week|tuần|tuan|month|tháng|thang)s?',
            time_period_str
        )
        if match:
            num = int(match.group(1))
            unit = match.group(2).lower()
            if unit in ("minute", "phút", "phut", "min"):
                days = num / 24 / 60  # convert minutes to days
            elif unit in ("hour", "giờ", "gio"):
                days = num / 24
            elif unit in ("day", "ngày", "ngay"):
                days = num
            elif unit in ("week", "tuần", "tuan"):
                days = num * 7
            elif unit in ("month", "tháng", "thang"):
                days = num * 30
            else:
                days = 1  # fallback to warm
            if days < 1 / 24:  # < 1 hour → HOT (Fluss)
                selected_layer = LayerChoice.FLUSS
            elif days < 1:  # 1hr–1day → WARM (Paimon)
                selected_layer = LayerChoice.PAIMON
            elif days <= 7:
                selected_layer = LayerChoice.PAIMON
            else:
                selected_layer = LayerChoice.ICEBERG

        # Step 2: Keyword patterns for non-numeric expressions
        elif any(x in time_period_str for x in ["tháng trước", "thang truoc", "tháng qua", "thang qua",
                                                  "tháng này", "thang nay", "month",
                                                  "năm", "quý", "year", "quarter",
                                                  "30 ngày", "30 ngay", ">7", "hơn 7"]):
            selected_layer = LayerChoice.ICEBERG  # COLD - historical

        elif any(x in time_period_str for x in ["hôm qua", "hom qua", "yesterday",
                                                  "tuần trước", "tuan truoc", "tuần qua", "tuan qua",
                                                  "tuần này", "tuan nay", "week",
                                                  "7 ngày", "7 ngay", "7 day",
                                                  "hôm nay", "hom nay", "today"]):
            selected_layer = LayerChoice.PAIMON  # WARM

        # HOT: only explicit "right now" / "last hour" — NOT "24 giờ qua" (that's warm)
        elif any(x == time_period_str.strip() or
                 x in time_period_str and not re.search(r'\d+\s*' + re.escape(x), time_period_str)
                 for x in ["vừa rồi", "bây giờ", "real-time", "trực tiếp", "mới nhất", "now"]):
            selected_layer = LayerChoice.FLUSS  # HOT - real-time

        # Default: stay PAIMON (warm)

        # EXPLICIT routing log (visible without JSONFormatter extension)
        logger.info(
            f"[ROUTING] time_period='{time_period_str}' → layer={selected_layer.value}",
            extra={"request_id": state["request_id"], "action": "routing_decision"}
        )

        # Set catalog and table based on selected layer
        if selected_layer == LayerChoice.FLUSS:
            state["trino_catalog"] = "fluss"
            state["table_name"] = "hot_violence_alerts"
        elif selected_layer == LayerChoice.PAIMON:
            state["trino_catalog"] = "paimon"
            state["table_name"] = "violence_incidents"
        else:  # Iceberg
            state["trino_catalog"] = "iceberg"
            state["table_name"] = "historical_violence_incidents"

        state["selected_layer"] = selected_layer
        state["data_layer"] = selected_layer.value

        log_agent_node(
            logger,
            state["request_id"],
            "select_data_layer",
            "completed",
            {
                "layer": state["selected_layer"].value,
                "time_period": state["intent"].time_period,
                "table": state["table_name"]
            }
        )

        return state

    except Exception as e:
        logger.error(f"Layer selection failed: {e}")
        # Default to Paimon
        state["selected_layer"] = LayerChoice.PAIMON
        state["trino_catalog"] = "paimon"
        state["table_name"] = "violence_incidents"
        state["data_layer"] = LayerChoice.PAIMON.value
        return state


def _clean_sql(raw: str) -> str:
    """Strip markdown fences and leading/trailing whitespace from Gemini output."""
    sql = raw.strip()
    if sql.startswith("```"):
        parts = sql.split("```")
        # parts[1] is the content inside the fences
        sql = parts[1].lstrip("sql").strip() if len(parts) > 1 else sql
    # Remove trailing semicolons (Trino/Flink don't need them via API)
    sql = sql.rstrip(";").strip()
    return sql


async def generate_sql(state: AgentState) -> AgentState:
    """
    Node 3: True Text-to-SQL — Gemini generates SQL from real schema + user question.

    Uses schema_registry (ground truth) instead of ChromaDB templates to eliminate
    hallucinated column names.  Falls back to a safe template if Gemini unavailable.
    """
    log_agent_node(logger, state["request_id"], "generate_sql", "started")

    try:
        catalog = state["trino_catalog"]
        schema = state["trino_schema"]
        table = state["table_name"]
        layer = state["selected_layer"]
        full_ref = get_full_table_ref(table, schema)
        schema_str = get_schema_for_prompt(table)

        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # ── True Text-to-SQL via Gemini ──────────────────────────────────────
        if genai:
            try:
                model = genai.GenerativeModel("gemini-2.5-flash")

                # Layer-specific SQL dialect hints
                if layer and "FLUSS" in str(layer).upper():
                    dialect_hint = (
                        "Syntax: Flink SQL (backtick `timestamp`, no TIMESTAMP WITH TIME ZONE).\n"
                        "Use: `timestamp` >= TIMESTAMP '...' for time filters."
                    )
                elif layer and "ICEBERG" in str(layer).upper():
                    dialect_hint = (
                        "Syntax: Trino SQL (double-quote reserved words like \"timestamp\").\n"
                        "Use: timestamp >= TIMESTAMP '...' for time filters.\n"
                        "Iceberg timestamp is TIMESTAMP(6) WITH TIME ZONE — cast with AT TIME ZONE if needed."
                    )
                else:
                    dialect_hint = (
                        "Syntax: Flink SQL (backtick `timestamp`, no TIMESTAMP WITH TIME ZONE).\n"
                        "Use: `timestamp` >= TIMESTAMP '...' for time filters."
                    )

                prompt = f"""Bạn là chuyên gia SQL cho hệ thống giám sát bạo lực đô thị.
Viết MỘT câu truy vấn SQL CHÍNH XÁC cho câu hỏi tiếng Việt dưới đây.

## Câu hỏi người dùng
"{state['user_query']}"

## Schema của bảng `{full_ref}`
{schema_str}

## Thông tin thời gian hiện tại
- Hôm nay: {today_str}
- Bây giờ (UTC): {now_str}

## Dialect & quy tắc SQL
{dialect_hint}

## Quy tắc BẮT BUỘC
1. Chỉ truy vấn bảng `{full_ref}` — KHÔNG dùng bảng khác
2. Chỉ dùng các cột có trong schema ở trên — KHÔNG tự thêm cột không có
3. Thêm `is_violent = TRUE` trừ khi câu hỏi hỏi "tất cả sự kiện" hoặc "bình thường"
4. Thêm bộ lọc thời gian phù hợp với câu hỏi (dùng TIMESTAMP literal)
5. Giới hạn: LIMIT 50 cho SELECT *, không cần LIMIT cho COUNT/SUM/AVG
6. Kết quả: trả về CHỈ SQL, không có giải thích, không có markdown fence

## SQL:""".strip()

                response = model.generate_content(prompt)
                generated_sql = _clean_sql(response.text)
                logger.info(f"[True Text-to-SQL] Generated: {generated_sql[:120]}...")

            except Exception as e:
                logger.error(f"Gemini SQL generation failed: {e}")
                generated_sql = None
        else:
            generated_sql = None

        # ── Template fallback (Gemini unavailable or failed) ─────────────────
        if not generated_sql:
            logger.warning("Using template SQL fallback")
            generated_sql = (
                f"SELECT COUNT(*) AS incident_count\n"
                f"FROM {full_ref}\n"
                f"WHERE is_violent = TRUE\n"
                f"  AND `timestamp` >= TIMESTAMP '{today_str} 00:00:00'"
            )

        state["generated_sql"] = generated_sql

        # Basic validation
        if _sql_generator:
            if not _sql_generator.validate_sql(generated_sql):
                logger.warning(f"SQL failed basic validation: {generated_sql[:100]}")

        log_agent_node(
            logger,
            state["request_id"],
            "generate_sql",
            "completed",
            {"sql_length": len(generated_sql), "catalog": catalog, "table": table},
        )
        return state

    except Exception as e:
        logger.error(f"generate_sql node failed: {e}")
        schema = state.get("trino_schema", "security")
        state["generated_sql"] = (
            f"SELECT COUNT(*) AS incident_count "
            f"FROM {state['trino_catalog']}.{schema}.{state['table_name']}"
        )
        return state


async def execute_query(state: AgentState) -> AgentState:
    """
    Node 4: Execute generated SQL against Trino.

    Executes the SQL and captures:
    - Query results (data)
    - Row count
    - Execution errors
    """
    log_agent_node(logger, state["request_id"], "execute_query", "started")

    try:
        sql = state["generated_sql"]
        layer = state["selected_layer"]

        if not _trino_client:
            logger.error("Trino client not initialized")
            state["query_result"] = QueryResult(
                success=False,
                error="Trino client not available",
            )
            return state

        # Execute query on appropriate layer
        try:
            results = _trino_client.route_query(
                sql=sql,
                layer=layer,
                timeout=180
            )

            row_count = len(results) if results else 0
            state["query_result"] = QueryResult(
                success=True,
                data=results,
                row_count=row_count,
                error=None,
            )

            # Extract frame metadata if single result
            if row_count == 1 and results:
                first_row = results[0]
                state["incident_id"] = first_row.get("incident_id")
                state["camera_id"] = first_row.get("camera_id")
                state["incident_date"] = first_row.get("timestamp") or first_row.get("incident_date")
                state["frame_url"] = first_row.get("frame_url")
                logger.info(f"Frame metadata extracted: incident_id={state['incident_id']}, camera_id={state['camera_id']}")

            logger.info(f"Query executed successfully: {row_count} rows")

        except TimeoutError as e:
            logger.warning(f"Query timeout: {e}")
            state["query_result"] = QueryResult(
                success=False,
                error=f"Query timeout: {str(e)}",
            )

        except Exception as e:
            logger.error(f"Query execution error: {e}")
            state["query_result"] = QueryResult(
                success=False,
                error=str(e),
            )

        log_agent_node(
            logger,
            state["request_id"],
            "execute_query",
            "completed",
            {
                "success": state["query_result"].success,
                "rows": state["query_result"].row_count,
                "error": state["query_result"].error[:50] if state["query_result"].error else None
            }
        )

        return state

    except Exception as e:
        logger.error(f"Execute query node failed: {e}")
        state["query_result"] = QueryResult(
            success=False,
            error=str(e),
        )
        return state


async def self_correct(state: AgentState) -> Optional[AgentState]:
    """
    Node 5: Retry failed queries with error analysis.

    Conditional node - only executes if query failed.
    - Max 3 retries
    - Uses Gemini to analyze error and regenerate SQL
    - Logs each retry attempt
    """
    if state["query_result"].success or state["retry_count"] >= 3:
        # No error or max retries exceeded
        log_agent_node(
            logger,
            state["request_id"],
            "self_correct",
            "skipped",
            {"reason": "no_error" if state["query_result"].success else "max_retries"}
        )
        return None

    state["retry_count"] += 1
    log_agent_node(logger, state["request_id"], "self_correct", "started")

    try:
        error_msg = state["query_result"].error
        current_sql = state["generated_sql"]

        state["retry_errors"].append(error_msg)

        logger.info(f"Self-correcting SQL (attempt {state['retry_count']}/3): {error_msg[:100]}")

        # Use SQL generator to fix SQL based on error
        if _sql_generator:
            try:
                # Get fresh schema context
                schema_context = []
                if _chromadb:
                    schema_context = _chromadb.search_schema(state["user_query"], top_k=3)

                # Fix SQL using Gemini
                fixed_sql = _sql_generator.fix_sql_error(
                    sql=current_sql,
                    error_msg=error_msg,
                    schema_context=schema_context,
                    retry_count=state["retry_count"]
                )

                state["generated_sql"] = fixed_sql
                logger.info(f"Fixed SQL: {fixed_sql[:100]}...")

            except Exception as e:
                logger.error(f"Failed to fix SQL: {e}")
                # Try simpler approach: remove problematic parts
                if "column" in error_msg.lower():
                    state["generated_sql"] = current_sql.replace("ORDER BY", "-- ORDER BY")
                elif "timeout" in error_msg.lower():
                    state["generated_sql"] = current_sql.replace("LIMIT 100", "LIMIT 10")
                else:
                    # Last resort: simple count query
                    state["generated_sql"] = f"SELECT COUNT(*) FROM {state['trino_catalog']}.{state['trino_schema']}.{state['table_name']}"

        log_agent_node(
            logger,
            state["request_id"],
            "self_correct",
            "completed",
            {
                "retry": state["retry_count"],
                "error": error_msg[:50],
                "sql_modified": state["generated_sql"] != current_sql
            }
        )

        # Re-execute with corrected SQL
        return await execute_query(state)

    except Exception as e:
        logger.error(f"Self-correction failed: {e}")
        return state


async def generate_response(state: AgentState) -> AgentState:
    """
    Node 6: Generate Vietnamese response with citations.

    Creates final response with:
    - Vietnamese natural language answer
    - SQL query used
    - Mandatory citations (source table, layer, time period)
    - Confidence score
    """
    log_agent_node(logger, state["request_id"], "generate_response", "started")

    try:
        state["source_table"] = state["table_name"]
        state["data_layer"] = state["selected_layer"].value
        state["time_period"] = state["intent"].time_period if state["intent"] else "unknown"

        if state["query_result"].success:
            # Query succeeded (may have 0 or more rows)
            results = state["query_result"].data or []
            row_count = len(results)
            state["row_count"] = row_count

            if row_count == 0:
                # Successful query but no data found
                state["final_answer"] = (
                    f"Không tìm thấy dữ liệu cho câu hỏi của bạn trong khoảng thời gian "
                    f"'{state['intent'].time_period if state['intent'] else 'đã chọn'}'.\n"
                    f"Vui lòng thử mở rộng phạm vi thời gian hoặc điều chỉnh bộ lọc.\n\n"
                    f"Nguồn: {state['source_table']} ({state['data_layer']})"
                )
                state["response_confidence"] = 0.5
            else:
                # Format data as Vietnamese text
                formatted_data = _safe_json_dumps(results[:5])

                if genai:
                    try:
                        model = genai.GenerativeModel("gemini-2.5-flash")
                        src = state['source_table']
                        dlayer = state['data_layer']

                        prompt = f"""
Hãy tổng hợp kết quả truy vấn dưới đây thành một câu trả lời tự nhiên bằng tiếng Việt.

**Câu hỏi gốc:** "{state['user_query']}"

**Dữ liệu kết quả (JSON):**
{formatted_data}

**Tổng số hàng:** {row_count}
**Bảng nguồn:** {src}
**Lớp dữ liệu:** {dlayer}
**Thời gian:** {state['time_period']}

**Yêu cầu:**
1. Viết câu trả lời tự nhiên bằng tiếng Việt dựa trên dữ liệu
2. Nêu các con số cụ thể từ kết quả
3. Cuối cùng, thêm dòng citation: "Nguồn: {src} ({dlayer}), {row_count} hàng"
4. Không bịa dữ liệu, chỉ sử dụng những gì có trong kết quả
5. Trả về CHỈ câu trả lời, không có giải thích thêm

**Câu trả lời:**
                        """.strip()

                        response = model.generate_content(prompt)
                        state["final_answer"] = response.text.strip()
                        state["response_confidence"] = state["intent"].query_confidence * 0.95

                    except Exception as e:
                        logger.error(f"Gemini synthesis failed: {e}")
                        # Fallback: format data manually
                        state["final_answer"] = _format_response_fallback(
                            results, state, row_count
                        )
                        state["response_confidence"] = 0.7
                else:
                    # No Gemini: format manually
                    state["final_answer"] = _format_response_fallback(
                        results, state, row_count
                    )
                    state["response_confidence"] = 0.7

                # Fetch frame evidence for single-result queries
                if row_count == 1 and state["incident_id"] and _evidence_service:
                    try:
                        logger.info(f"Fetching frame evidence for incident: {state['incident_id']}")
                        frame_b64 = _evidence_service.get_frame(
                            incident_id=state["incident_id"],
                            camera_id=state.get("camera_id", "unknown"),
                            incident_date=state.get("incident_date")
                        )
                        if frame_b64:
                            state["frame_base64"] = frame_b64
                            logger.info("Frame evidence retrieved successfully")
                        else:
                            logger.warning(f"Frame not found for incident: {state['incident_id']}")
                    except Exception as e:
                        logger.warning(f"Failed to fetch frame evidence: {e}")
                        # Don't fail response if frame fetch fails

        else:
            # Query failed
            error_msg = state["query_result"].error if state["query_result"].error else "Lỗi không xác định"
            state["final_answer"] = f"Lỗi: Không thể truy vấn dữ liệu. {error_msg}"
            state["response_confidence"] = 0.0
            state["row_count"] = 0

        log_agent_node(
            logger,
            state["request_id"],
            "generate_response",
            "completed",
            {
                "confidence": state["response_confidence"],
                "rows": state["row_count"],
                "success": state["query_result"].success
            }
        )

        return state

    except Exception as e:
        logger.error(f"Response generation failed: {e}")
        state["final_answer"] = "Lỗi: Không thể xử lý câu hỏi của bạn. Vui lòng thử lại."
        state["response_confidence"] = 0.0
        return state


def _safe_json_dumps(obj) -> str:
    """JSON serialize with datetime/Decimal fallback."""
    def default(v):
        if hasattr(v, 'isoformat'):  # datetime, date, time
            return v.isoformat()
        return str(v)
    return json.dumps(obj, ensure_ascii=False, default=default)


def _format_response_fallback(results: List[Dict], state: Dict, row_count: int) -> str:
    """Fallback response formatting without Gemini."""
    time_period = state.get('time_period', 'unknown')
    source_table = state.get('source_table', 'unknown')
    data_layer = state.get('data_layer', 'unknown')

    if not results or row_count == 0:
        return f"Không tìm thấy dữ liệu cho thời gian '{time_period}'."

    response = f"Tìm thấy {row_count} kết quả:\n\n"

    for row in results[:3]:
        response += f"  • {_safe_json_dumps(row)}\n"

    if row_count > 3:
        response += f"  ... và {row_count - 3} kết quả khác\n"

    response += f"\nNguồn: {source_table} ({data_layer})"

    return response


# ============================================================================
# Graph Construction
# ============================================================================

def create_agent_graph() -> CompiledStateGraph:
    """
    Create and compile the LangGraph agent.

    Returns:
        Compiled state graph ready for invocation
    """
    graph = StateGraph(AgentState)

    # Add nodes
    graph.add_node("understand_query", understand_query)
    graph.add_node("select_data_layer", select_data_layer)
    graph.add_node("generate_sql", generate_sql)
    graph.add_node("execute_query", execute_query)
    graph.add_node("self_correct", self_correct)
    graph.add_node("generate_response", generate_response)

    # Add edges
    graph.add_edge("understand_query", "select_data_layer")
    graph.add_edge("select_data_layer", "generate_sql")
    graph.add_edge("generate_sql", "execute_query")

    # Conditional edge: if query failed and retries < 3, go to self_correct; else go to generate_response
    def should_retry(state: AgentState):
        if not state["query_result"].success and state["retry_count"] < 3:
            return "self_correct"
        return "generate_response"

    graph.add_conditional_edges(
        "execute_query",
        should_retry,
        {
            "self_correct": "self_correct",
            "generate_response": "generate_response",
        }
    )

    # After self_correct, go back to execute_query
    graph.add_edge("self_correct", "execute_query")

    # Final edge
    graph.add_edge("generate_response", END)

    # Set entry point
    graph.set_entry_point("understand_query")

    # Compile
    compiled_graph = graph.compile()

    logger.info("✓ LangGraph agent created successfully")

    return compiled_graph


# ============================================================================
# Initialization
# ============================================================================

# Create agent on module import
try:
    agent_graph = create_agent_graph()
except Exception as e:
    logger.error(f"Failed to create agent graph: {e}")
    agent_graph = None


if __name__ == "__main__":
    """Test agent when run directly."""
    import asyncio
    import time
    from datetime import datetime

    async def test_agent():
        """Test agent with sample query."""
        if not agent_graph:
            logger.error("Agent graph not initialized")
            return

        initial_state = AgentState(
            user_query="Hôm nay có bao nhiêu vụ bạo lực?",
            context="",
            options={},
            request_id="test-001",
            intent=None,
            selected_layer=None,
            trino_catalog=None,
            trino_schema="security",
            table_name=None,
            generated_sql=None,
            query_result=None,
            retry_count=0,
            retry_errors=[],
            final_answer=None,
            response_confidence=0.0,
            source_table=None,
            data_layer=None,
            time_period=None,
            row_count=None,
            start_time=time.time(),
            duration_ms=None,
        )

        logger.info("Starting test agent run...")
        result = agent_graph.invoke(initial_state)

        logger.info(f"Test completed: {result['final_answer']}")
        print(json.dumps({
            "answer": result["final_answer"],
            "layer": result["data_layer"],
            "confidence": result["response_confidence"],
        }, indent=2, ensure_ascii=False))

    asyncio.run(test_agent())
