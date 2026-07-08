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
import os
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
from .components.trino_client import TrinoClient, DataLayer
from .components.sql_generator import SQLGenerator
from .components.evidence_service import EvidenceService
from .components.schema_registry import (
    get_schema_for_prompt, get_full_table_ref, get_all_schemas_for_prompt, table_for,
)

logger = setup_logger(__name__)

# Global component instances (initialized in main.py)
_trino_client: Optional[TrinoClient] = None
_sql_generator: Optional[SQLGenerator] = None
_evidence_service: Optional[EvidenceService] = None


def set_components(
    trino_client: TrinoClient,
    sql_generator: SQLGenerator,
    evidence_service: EvidenceService
) -> None:
    """Set global component instances."""
    global _trino_client, _sql_generator, _evidence_service
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
    wants_evidence: bool = Field(default=False, description="User wants to see frame evidence images")


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

    # Frame evidence (single-result)
    incident_id: Optional[str]
    camera_id: Optional[str]
    incident_date: Optional[str]
    frame_url: Optional[str]
    frame_base64: Optional[str]

    # Frame evidence (multi-result — evidence image queries)
    frame_urls: Optional[List[str]]

    # Dual-layer query support ("hôm nay" spans HOT last-2h + WARM 2h-24h)
    also_query_hot: bool
    hot_query_result: Optional[QueryResult]

    # Metadata
    start_time: float
    duration_ms: Optional[int]


# ============================================================================
# Node Functions (Stubs - To Be Implemented)
# ============================================================================

# Vietnamese keywords that signal the user wants to see evidence images.
# NOTE: short tokens like "anh" (photo without diacritic) are NOT here because
# they appear as substrings in unrelated words, e.g. "canh" in "cảnh báo".
# Use _EVIDENCE_WORD_TOKENS + word-boundary regex for those.
_EVIDENCE_KEYWORDS = (
    "ảnh bằng chứng", "bang chung",
    "bằng chứng", "xem ảnh", "xem hinh", "ảnh chụp", "screenshot",
    "frame", "clip", "video", "chứng cứ", "chung cu", "hình ảnh",
    "cho xem", "xem được không", "có ảnh không", "có hình không",
    # phrases thường dùng nhưng chưa có:
    "hình ảnh bằng chứng", "bằng chứng gần đây", "xem bằng chứng",
    "bằng chứng hình ảnh", "ảnh gần đây", "hình gần đây",
    "cho tôi xem", "cho mình xem",
)

# Short tokens that need whole-word matching to avoid false-positives:
# "anh" alone means "photo" (without diacritic) but appears inside "canh" (cảnh/canh).
_EVIDENCE_WORD_TOKENS = ("anh", "ảnh", "hình",)


def _detect_evidence_intent(query: str) -> bool:
    """Return True if the user is asking to see evidence/frame images."""
    q = query.lower()
    if any(kw in q for kw in _EVIDENCE_KEYWORDS):
        return True
    # Word-boundary check for short ambiguous tokens
    return any(re.search(r'\b' + re.escape(tok) + r'\b', q) for tok in _EVIDENCE_WORD_TOKENS)


async def understand_query(state: AgentState) -> AgentState:
    """
    Node 1: MỘT lần gọi Gemini cho cả intent + table + SQL (thay vì 3 call/câu hỏi
    như bản cũ: intent → SQL → tổng hợp). Giảm latency ~2/3.

    Gemini nhận TOÀN BỘ schema registry (đã introspect từ Trino lúc startup) +
    quy tắc routing 3 tầng, trả JSON:
      {time_period, location, metric, intent_type, query_confidence, table, sql}

    select_data_layer sau đó verify table bằng rule tất định — nếu lệch thì
    generate_sql sẽ sinh lại SQL cho đúng bảng (hiếm khi xảy ra).
    """
    log_agent_node(logger, state["request_id"], "understand_query", "started")

    try:
        user_query = state["user_query"]
        wants_evidence = _detect_evidence_intent(user_query)

        if not genai:
            logger.warning("Gemini not available - using fallback intent parsing")
            intent = _parse_intent_keywords(user_query)
            intent.wants_evidence = wants_evidence
            state["intent"] = intent
            return state

        model = genai.GenerativeModel(os.getenv("GEMINI_MODEL", "gemini-2.5-flash"))

        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        prompt = f"""Bạn là chuyên gia SQL cho hệ thống giám sát bạo lực đô thị (Streamhouse 3 tầng).
Phân tích câu hỏi tiếng Việt và trả về MỘT JSON duy nhất gồm intent + bảng + SQL.

## Câu hỏi
"{user_query}"

## Các bảng có sẵn
{get_all_schemas_for_prompt()}

## Quy tắc routing (BẮT BUỘC)
- Thời gian < 1 giờ / "bây giờ" / "hiện tại" → tầng HOT (fluss.*)
- 1 giờ – 7 ngày (hôm nay/hôm qua/tuần này) → tầng WARM (paimon.*)
- > 7 ngày / tháng / năm → tầng COLD (iceberg.*)
- Câu hỏi ĐẾM SỐ VỤ / thống kê → bảng grain=incident (1 dòng = 1 vụ)
- Câu hỏi xem ảnh/bằng chứng/chi tiết event → bảng grain=event (có frame_url)

## Quy tắc SQL (Trino dialect; với bảng fluss.* dùng Flink dialect)
- Chỉ dùng cột có trong schema ở trên. Reserved word bọc double-quote: "timestamp".
- Hôm nay: {today_str} | Bây giờ (UTC): {now_str}. Time filter dùng TIMESTAMP literal.
- Với bảng fluss.*: KHÔNG dùng COUNT()/SUM() — SELECT các cột với LIMIT 200 (đếm ở client).
- LIMIT 50 cho SELECT chi tiết; COUNT/AVG không cần LIMIT.
- Aggregate LUÔN đặt alias rõ nghĩa: COUNT(*) AS incident_count, AVG(x) AS avg_risk_score.

## JSON trả về (CHỈ JSON, không giải thích, không markdown fence)
{{"time_period": "...", "location": null, "metric": "count|average|max|min|list",
  "intent_type": "statistics|query_recent|trend|comparison|evidence_lookup",
  "query_confidence": 0.95, "table": "<tên bảng không kèm catalog>", "sql": "SELECT ..."}}"""

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
                wants_evidence=wants_evidence,
            )
            # SQL + table từ cùng 1 call — select_data_layer sẽ verify
            llm_sql = (intent_dict.get("sql") or "").strip()
            if llm_sql:
                state["generated_sql"] = _clean_sql(llm_sql)
            state["options"] = {**(state.get("options") or {}),
                                "llm_table": intent_dict.get("table")}
        else:
            logger.warning(f"Failed to parse Gemini response: {response_text[:100]}")
            intent = _parse_intent_keywords(user_query)
            intent.wants_evidence = wants_evidence
            state["intent"] = intent

        log_agent_node(
            logger,
            state["request_id"],
            "understand_query",
            "completed",
            {
                "intent_type": state["intent"].intent_type,
                "confidence": state["intent"].query_confidence,
                "wants_evidence": state["intent"].wants_evidence,
                "has_sql": bool(state.get("generated_sql")),
            }
        )

        return state

    except Exception as e:
        logger.error(f"Intent extraction failed: {e}")
        intent = _parse_intent_keywords(state["user_query"])
        intent.wants_evidence = _detect_evidence_intent(state["user_query"])
        state["intent"] = intent
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
        wants_evidence=False,  # caller sets this after _detect_evidence_intent()
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
        import unicodedata as _ud; time_period_str = _ud.normalize("NFC", intent.time_period.lower())

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
            if days <= 1 / 24:  # <= 1 hour → HOT (Fluss)
                selected_layer = LayerChoice.FLUSS
            elif days < 1:  # 1hr–1day → WARM (Paimon)
                selected_layer = LayerChoice.PAIMON
            elif days <= 7:
                selected_layer = LayerChoice.PAIMON
            else:
                selected_layer = LayerChoice.ICEBERG

        # Step 2: Keyword patterns for non-numeric expressions
        elif (
            any(x in time_period_str for x in ["tháng trước", "thang truoc", "tháng qua", "thang qua",
                                                "tháng này", "thang nay", "month",
                                                "năm", "nam ngoai", "quý", "year", "quarter",
                                                "30 ngày", "30 ngay", ">7", "hơn 7"])
            # Bare 4-digit year (e.g. Gemini returns "2025" without "năm" keyword)
            or re.search(r'\b(20\d{2}|19\d{2})\b', time_period_str)
        ):
            selected_layer = LayerChoice.ICEBERG  # COLD - historical

        elif any(x in time_period_str for x in ["hôm qua", "hom qua", "yesterday",
                                                  "tuần trước", "tuan truoc", "tuần qua", "tuan qua",
                                                  "tuần này", "tuan nay", "week",
                                                  "7 ngày", "7 ngay", "7 day",
                                                  "hôm nay", "hom nay", "today"]):
            selected_layer = LayerChoice.PAIMON  # WARM

        # HOT: explicit "right now" signals — Gemini often returns diacritical forms
        # NOTE: NOT "24 giờ qua" (that's warm) — numeric regex above handles hours
        elif any(x in time_period_str for x in [
            "vừa rồi", "vua roi", "bây giờ", "bay gio", "real-time", "real time",
            "trực tiếp", "truc tiep", "mới nhất", "moi nhat",
            "gần đây", "gan day", "hiện tại", "hien tai",
            "now", "ngay bây giờ", "ngay bay gio", "realtime", "vừa", "vua", "hiện giờ", "hien gio",
        ]):
            selected_layer = LayerChoice.FLUSS  # HOT - real-time

        # Default: stay PAIMON (warm)

        # Evidence queries MUST use PAIMON — only layer with frame_url column.
        # Fluss (HOT) and Iceberg (COLD) have no frame_url; override if needed.
        if _detect_evidence_intent(state.get("user_query", "")):
            if selected_layer != LayerChoice.PAIMON:
                logger.info(
                    f"[ROUTING] Evidence query: overriding {selected_layer.value} → PAIMON (frame_url)"
                )
                selected_layer = LayerChoice.PAIMON

        # "hôm nay" (today) spans two layers:
        #   - Fluss HOT:  last 2h  (data not yet tiered to Paimon)
        #   - Paimon WARM: 2h–24h (already tiered from HOT every 30min)
        # → run PAIMON as primary, then do a supplementary HOT scan and merge.
        # Evidence queries skip this (PAIMON already has frame_url for both windows).
        # IMPORTANT: do NOT match "hôm qua" — use exact phrase "hôm nay" only.
        wants_evidence = state.get("intent") and state["intent"].wants_evidence
        state["also_query_hot"] = (
            selected_layer == LayerChoice.PAIMON
            and any(x in time_period_str for x in [
                "hôm nay", "hom nay", "today", "trong ngày", "trong ngay"
            ])
            # "hôm qua" / "hom qua" = yesterday → fully in WARM, no HOT supplement needed
            and not any(x in time_period_str for x in ["hôm qua", "hom qua", "yesterday"])
            and not wants_evidence
        )
        if state.get("also_query_hot"):
            logger.info(
                "[ROUTING] 'hôm nay' → PAIMON primary + FLUSS HOT supplementary (dual-layer)"
            )

        # Post-routing override: raw query HOT signals take priority over Gemini time_period
        # (Gemini may return inconsistent time_period strings for realtime queries)
        import unicodedata as _ud2
        _uq = state["user_query"] if "user_query" in state else (state.user_query if hasattr(state, "user_query") else "")
        _raw_q = _ud2.normalize("NFC", _uq.lower())
        logger.info(f"[HOT_RAW_CHECK] user_query={repr(_uq[:30])}, raw_q={repr(_raw_q[:30])}")
        _HOT_RAW = ["vừa", "vua", "bây giờ", "bay gio", "ngay bây giờ",
                    "ngay bay gio", "trực tiếp", "hiện tại", "hien tai"]
        if any(_sig in _raw_q for _sig in _HOT_RAW) and not (state.get("intent") and state["intent"].wants_evidence):
            selected_layer = LayerChoice.FLUSS
            state["also_query_hot"] = False  # clear hom-nay dual-layer flag if HOT override fires

        # EXPLICIT routing log (visible without JSONFormatter extension)
        logger.info(
            f"[ROUTING] time_period='{time_period_str}' → layer={selected_layer.value}",
            extra={"request_id": state["request_id"], "action": "routing_decision"}
        )

        # Chọn bảng theo GRAIN (v2): đếm/thống kê SỐ VỤ → bảng incident (1 dòng = 1 vụ,
        # đã sessionize); evidence/ảnh/chi tiết → bảng event (có frame_url/people_json).
        intent_obj = state.get("intent")
        purpose = "evidence" if (wants_evidence or (
            intent_obj and intent_obj.intent_type == "evidence_lookup")) else "count"
        state["trino_catalog"] = (
            "fluss" if selected_layer == LayerChoice.FLUSS
            else "paimon" if selected_layer == LayerChoice.PAIMON
            else "iceberg"
        )
        state["table_name"] = table_for(state["trino_catalog"], purpose)

        # Verify SQL sinh sẵn từ understand_query (1-call): nếu Gemini chọn bảng khác
        # với routing tất định → bỏ SQL đó, generate_sql sẽ sinh lại cho đúng bảng.
        pre_sql = state.get("generated_sql")
        if pre_sql and state["table_name"] not in pre_sql:
            logger.info(
                f"[ROUTING] Discarding pre-generated SQL (targets wrong table; "
                f"expected {state['table_name']})"
            )
            state["generated_sql"] = None

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

        # SQL đã sinh từ understand_query (1-call) và trúng bảng routing → dùng luôn,
        # tiết kiệm 1 lần gọi Gemini. (Bị xoá ở select_data_layer nếu lệch bảng.)
        if state.get("generated_sql") and state["retry_count"] == 0:
            if not _sql_generator or _sql_generator.validate_sql(state["generated_sql"]):
                log_agent_node(
                    logger, state["request_id"], "generate_sql", "completed",
                    {"source": "one-call", "catalog": catalog, "table": table},
                )
                return state
            state["generated_sql"] = None  # fail validation → sinh lại bên dưới

        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        wants_evidence = state.get("intent") and state["intent"].wants_evidence

        # ── True Text-to-SQL via Gemini ──────────────────────────────────────
        if genai:
            try:
                model = genai.GenerativeModel("gemini-2.5-flash")

                # Always generate Trino SQL dialect (double-quote for reserved words).
                # _adapt_sql_for_flink() in trino_client.py converts "timestamp" → `timestamp`
                # automatically when the query is routed through Flink SQL Gateway.
                if layer and "ICEBERG" in str(layer).upper():
                    dialect_hint = (
                        'Syntax: Trino SQL. Use double-quote for reserved keyword: "timestamp".\n'
                        "Iceberg timestamp column is TIMESTAMP(6) WITH TIME ZONE.\n"
                        "Time filter example: \"timestamp\" >= TIMESTAMP '2026-05-14 00:00:00'"
                    )
                elif layer and "FLUSS" in str(layer).upper():
                    # HOT layer: Fluss snapshot scan — use LIMIT instead of COUNT(*)
                    # Streaming COUNT(*) only counts future events (starts at 0); LIMIT reads snapshot
                    dialect_hint = (
                        'Syntax: Flink SQL. Use double-quote for reserved keyword: "timestamp".\n'
                        'QUAN TRỌNG: KHÔNG dùng COUNT(*) hay SUM() — hãy dùng SELECT với LIMIT.\n'
                        'Ví dụ tốt: SELECT incident_id, camera_id, "timestamp", risk_score, confidence, is_violent, event_type, location, ward_id, district FROM hot_violence_alerts LIMIT 50\n'
                        'BẮT BUỘC: LUÔN LUÔN include các cột location, ward_id, district trong SELECT — đây là tên đường/phường/quận của camera.\n'
                        'Nếu câu hỏi hỏi số lượng: dùng LIMIT 200 và đếm ở Python — KHÔNG dùng COUNT().'
                    )
                else:
                    dialect_hint = (
                        'Syntax: Trino SQL. Use double-quote for reserved keyword: "timestamp".\n'
                        'Time filter example: "timestamp" >= TIMESTAMP \'2026-05-14 00:00:00\'\n'
                        "Do NOT use backtick — use double-quote only."
                    )

                evidence_note = ""
                if wants_evidence and "frame_url" in schema_str:
                    evidence_note = (
                        "\n7. BẮTBUỘC: Luôn SELECT cột `frame_url` — người dùng muốn xem ảnh bằng chứng\n"
                        "8. Ưu tiên ORDER BY \"timestamp\" DESC LIMIT 10 để lấy ảnh mới nhất\n"
                        "9. Chỉ lấy các dòng có frame_url IS NOT NULL"
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
6. Kết quả: trả về CHỈ SQL, không có giải thích, không có markdown fence{evidence_note}

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

            # Extract frame metadata for single result (existing behaviour)
            if row_count == 1 and results:
                first_row = results[0]
                state["incident_id"] = first_row.get("incident_id")
                state["camera_id"] = first_row.get("camera_id")
                state["incident_date"] = first_row.get("timestamp") or first_row.get("incident_date")
                state["frame_url"] = first_row.get("frame_url")

            # Collect frame_urls from ALL results when evidence was requested
            # Prefer stored frame_url column; fall back to building URL from metadata
            wants_evidence = (
                state.get("intent") and state["intent"].wants_evidence
            )
            # --- Collect frame_urls from SQL results ---
            if results and (wants_evidence or any(r.get("frame_url") for r in results)):
                minio_base = os.getenv("MINIO_PUBLIC_URL", "http://localhost:9000")
                bucket = os.getenv("S3_BUCKET", "evidence-frames")
                collected: List[str] = []
                for row in results:
                    url = row.get("frame_url") or ""
                    if url and not url.startswith("http"):
                        url = f"{minio_base}/{bucket}/{url.lstrip('/')}"
                    if url:
                        collected.append(url)
                if collected:
                    state["frame_urls"] = collected
                    logger.info(f"Collected {len(collected)} frame URLs from SQL results")

            # --- Evidence fallback: list actual MinIO files (camera_id + date từ Paimon) ---
            # Paimon incident_id ≠ MinIO filename (2 pipelines khác nhau).
            # Fix: lấy camera_id + date từ Paimon, rồi list actual files trong MinIO bucket.
            if wants_evidence and not state.get("frame_urls") and _trino_client and _evidence_service:
                try:
                    import re as _re
                    q_lower = (state.get("user_query") or "").lower()

                    # Count limit
                    count_m = _re.search(r'(\d+)\s*(ảnh|hình|frame|image)', q_lower)
                    limit = min(int(count_m.group(1)) if count_m else 10, 20)

                    # Location filter
                    loc_m = _re.search(
                        r'(?:đường|phường|quận)\s+([^\s,]+(?:\s+[^\s,]+){0,3})',
                        q_lower
                    )
                    loc_where = ""
                    if loc_m:
                        loc_val = loc_m.group(1).strip().replace("'", "''")
                        loc_where = f"AND LOWER(location) LIKE '%{loc_val.lower()}%'"

                    # Query Paimon — chỉ cần camera_id + date (không cần incident_id)
                    sql = (
                        f"SELECT DISTINCT camera_id, CAST(DATE(timestamp) AS VARCHAR) AS dt "
                        f"FROM paimon.security.violence_incidents "
                        f"WHERE is_violent = TRUE {loc_where} "
                        f"ORDER BY 2 DESC LIMIT 5"
                    )
                    logger.info("[EVIDENCE FALLBACK] SQL: %s", sql[:150])
                    rows = _trino_client.query_paimon(sql, timeout=15) if hasattr(_trino_client, 'query_paimon') else []

                    minio_pub = os.getenv("MINIO_EXTERNAL_URL", "http://localhost:9000")
                    bucket = os.getenv("S3_BUCKET", "evidence-frames")
                    s3 = _evidence_service.client if hasattr(_evidence_service, "client") else None

                    urls = []
                    for r in (rows or []):
                        if len(urls) >= limit:
                            break
                        cid = (r.get("camera_id") if isinstance(r, dict) else r[0]) or ""
                        dt  = (r.get("dt") if isinstance(r, dict) else r[1]) or ""
                        dt  = str(dt).split(" ")[0].split("T")[0]
                        if not (cid and dt):
                            continue
                        # List actual files from MinIO for this camera/date prefix
                        if s3:
                            prefix = f"{cid}/{dt}/"
                            try:
                                objs = list(s3.list_objects(
                                    bucket_name=bucket, prefix=prefix, recursive=False
                                ))
                                for obj in objs:
                                    key = getattr(obj, "object_name", "")
                                    size = getattr(obj, "size", 0)
                                    # Skip corrupt/empty files (< 500 bytes)
                                    if key and size > 500:
                                        urls.append(f"{minio_pub}/{bucket}/{key}")
                                        if len(urls) >= limit:
                                            break
                            except Exception:
                                pass

                    if urls:
                        state["frame_urls"] = urls
                        state["row_count"] = len(urls)
                        logger.info("[EVIDENCE FALLBACK] %d real MinIO files (loc=%s)", len(urls),
                                    loc_m.group(1) if loc_m else "any")
                    else:
                        logger.info("[EVIDENCE FALLBACK] No MinIO files found")
                except Exception as ev_err:
                    logger.warning("[EVIDENCE FALLBACK] Failed: %s", ev_err, exc_info=True)

            # ── Dual-layer: supplementary HOT scan for "hôm nay" queries ──────────────
            # "today" = Paimon WARM (2h–24h old) + Fluss HOT (last 2h, not yet tiered).
            # Run a simple HOT scan and merge results so the answer covers the full day.
            if state.get("also_query_hot") and _trino_client:
                try:
                    # v2: quét bảng INCIDENT (1 dòng = 1 vụ) — không cộng raw events
                    # (0.5s/event) vào số vụ như bản cũ.
                    hot_sql = (
                        "SELECT incident_uid AS incident_id, camera_id, "
                        "start_ts, last_ts, event_count, max_risk_score AS risk_score, "
                        "event_type, location "
                        "FROM hot_violence_incidents "
                        "LIMIT 100"
                    )
                    logger.info("[DUAL-LAYER] Running supplementary HOT incident scan...")
                    hot_results = _trino_client.route_query(
                        sql=hot_sql,
                        layer=LayerChoice.FLUSS,
                        timeout=45,
                    )
                    violent_hot = list(hot_results or [])
                    state["hot_query_result"] = QueryResult(
                        success=True,
                        data=violent_hot,
                        row_count=len(violent_hot),
                    )
                    logger.info(f"[DUAL-LAYER] HOT supplementary: {len(violent_hot)} incidents")

                    if violent_hot and state["query_result"].success:
                        primary_data = state["query_result"].data or []
                        primary_ids = {r.get("incident_id") for r in primary_data}
                        new_hot_rows = [r for r in violent_hot if r.get("incident_id") not in primary_ids]

                        if (
                            len(primary_data) == 1
                            and "incident_count" in (primary_data[0] or {})
                        ):
                            # COUNT query: add HOT violent count to PAIMON count
                            warm_count = int(primary_data[0].get("incident_count") or 0)
                            hot_count = len(violent_hot)
                            merged_data = [{"incident_count": warm_count + hot_count}]
                            logger.info(
                                f"[DUAL-LAYER] COUNT merge: WARM={warm_count} + HOT={hot_count} = {warm_count + hot_count}"
                            )
                        else:
                            # LIST query: combine and deduplicate by incident_id
                            merged_data = list(primary_data) + new_hot_rows
                            logger.info(
                                f"[DUAL-LAYER] LIST merge: WARM={len(primary_data)} + HOT_new={len(new_hot_rows)} = {len(merged_data)}"
                            )

                        state["query_result"] = QueryResult(
                            success=True,
                            data=merged_data,
                            row_count=len(merged_data),
                        )
                        state["data_layer"] = "Paimon (WARM) + Fluss (HOT)"

                except Exception as _hot_err:
                    logger.warning(f"[DUAL-LAYER] HOT supplementary failed (non-fatal): {_hot_err}")
            # ─────────────────────────────────────────────────────────────────────────

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
                schema_context = []

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

        # Return updated state — graph edge self_correct → execute_query handles re-execution
        return state

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

            # Nếu có frame_urls từ evidence fallback → không báo "no data"
            frame_urls_from_fallback = state.get("frame_urls") or []
            if row_count == 0 and frame_urls_from_fallback:
                # Evidence được tìm theo context (location/camera/time) — tạo answer phù hợp
                n = len(frame_urls_from_fallback)
                user_q = state.get("user_query", "")
                state["final_answer"] = (
                    f"Đã tìm thấy {n} hình ảnh bằng chứng phù hợp với yêu cầu của bạn.\n"
                    f"Nguồn: violence_incidents (Paimon), {n} ảnh"
                )
                state["response_confidence"] = 0.85
                row_count = n  # Update để gallery rendering hoạt động
            elif row_count == 0:
                state["final_answer"] = (
                    f"Không tìm thấy dữ liệu cho câu hỏi của bạn trong khoảng thời gian "
                    f"'{state['intent'].time_period if state['intent'] else 'đã chọn'}'.\n"
                    f"Vui lòng thử mở rộng phạm vi thời gian hoặc điều chỉnh bộ lọc.\n\n"
                    f"Nguồn: {state['source_table']} ({state['data_layer']})"
                )
                state["response_confidence"] = 0.5
            else:
                # Format data as Vietnamese text
                # For HOT layer queries, show violent events first so Gemini sees location data
                # is_violent can be Python bool True, int 1, or string "true"/"1" from Flink
                violent_rows = [
                    r for r in results
                    if str(r.get("is_violent", "false")).lower() in ("true", "1")
                ]
                sample_rows = violent_rows[:10] if violent_rows else results[:10]
                formatted_data = _safe_json_dumps(sample_rows)

                frame_urls = state.get("frame_urls") or []
                evidence_note_for_gemini = ""
                if frame_urls:
                    evidence_note_for_gemini = (
                        f"\n6. Người dùng yêu cầu xem ảnh bằng chứng. "
                        f"Đã tìm thấy {len(frame_urls)} ảnh. "
                        f"Hãy thông báo ngắn gọn có {len(frame_urls)} ảnh và sẽ hiển thị bên dưới."
                    )

                # Kết quả aggregate 1 dòng toàn số (COUNT/AVG/MAX...) → trả lời bằng
                # template, KHÔNG tốn thêm 1 call Gemini (call thứ 3 của bản cũ).
                _first = results[0] if results else {}
                simple_aggregate = (
                    row_count == 1
                    and isinstance(_first, dict)
                    and not frame_urls
                    and all(not isinstance(v, (list, dict)) for v in _first.values())
                    and any(any(t in k.lower() for t in
                                ("count", "total", "avg", "max", "min", "sum"))
                            for k in _first)
                )
                if simple_aggregate:
                    src = state['source_table']
                    dlayer = state['data_layer']
                    count_key = next(
                        (k for k in _first if "count" in k.lower() or "total" in k.lower()),
                        None,
                    )
                    lines = []
                    if count_key is not None and len(_first) == 1:
                        lines.append(
                            f"Ghi nhận {_first[count_key]} vụ trong khoảng thời gian "
                            f"'{state['time_period']}'."
                        )
                    else:
                        pretty = ", ".join(
                            f"{k} = {round(v, 4) if isinstance(v, float) else v}"
                            for k, v in _first.items() if v is not None
                        )
                        lines.append(
                            f"Kết quả cho khoảng thời gian '{state['time_period']}': {pretty}."
                        )
                    lines.append(f"Nguồn: {src} ({dlayer}), {row_count} hàng")
                    state["final_answer"] = "\n".join(lines)
                    state["response_confidence"] = (
                        state["intent"].query_confidence if state["intent"] else 0.8
                    )
                elif genai:
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
**SQL đã dùng (tóm tắt):** {state.get('sql_used', '')[:120]}

**Yêu cầu:**
1. Viết câu trả lời tự nhiên bằng tiếng Việt dựa trên dữ liệu
2. Nêu các con số cụ thể từ kết quả
3. Cuối cùng, thêm dòng citation: "Nguồn: {src} ({dlayer}), {row_count} hàng"
4. Không bịa dữ liệu, chỉ sử dụng những gì có trong kết quả
5. Trả về CHỈ câu trả lời, không có giải thích thêm
6. KHI đề cập địa điểm xảy ra sự kiện: PHẢI dùng giá trị cột `location` (ví dụ: "tại Đường Nguyễn Huệ", "tại Đường Võ Văn Kiệt"), KHÔNG dùng "tại camera cam_XX". Nếu có cột `ward_id`/`district` thì thêm thông tin phường/quận.
7. QUAN TRỌNG: Nếu dữ liệu JSON là rỗng [] hoặc không có hàng nào, PHẢI trả lời "Không tìm thấy dữ liệu" — KHÔNG được tạo số liệu.{evidence_note_for_gemini}

**Câu trả lời:**
                        """.strip()

                        response = model.generate_content(prompt)
                        answer = response.text.strip()

                        # Anti-hallucination guard: nếu DB trả 0 rows nhưng Gemini
                        # vẫn tạo ra số liệu cụ thể → override bằng "không có dữ liệu"
                        if row_count == 0:
                            import re as _re
                            if _re.search(r'\d+\s*(vụ|alert|sự cố|camera|lần|incident)', answer, _re.IGNORECASE):
                                logger.warning("Hallucination detected (row_count=0 but Gemini returned numbers) — overriding")
                                answer = (
                                    f"Không tìm thấy dữ liệu cho câu hỏi của bạn trong khoảng thời gian "
                                    f"'{state['time_period']}'.\n"
                                    f"Vui lòng thử mở rộng phạm vi thời gian hoặc điều chỉnh bộ lọc.\n\n"
                                    f"Nguồn: {src} ({dlayer})"
                                )
                                state["response_confidence"] = 0.0

                        state["final_answer"] = answer
                        state["response_confidence"] = state["intent"].query_confidence * 0.95 if row_count > 0 else 0.0

                    except Exception as e:
                        logger.error(f"Gemini synthesis failed: {e}")
                        state["final_answer"] = _format_response_fallback(
                            results, state, row_count
                        )
                        state["response_confidence"] = 0.7
                else:
                    state["final_answer"] = _format_response_fallback(
                        results, state, row_count
                    )
                    state["response_confidence"] = 0.7

                # Append markdown image gallery for evidence queries
                if frame_urls:
                    display_urls = frame_urls[:20]
                    remaining = len(frame_urls) - len(display_urls)
                    gallery_lines = ["\n\n---\n### Ảnh bằng chứng"]
                    for i, url in enumerate(display_urls, 1):
                        cam = "evidence"
                        gallery_lines.append(f"![Ảnh {i} — {cam}]({url})")
                    if remaining > 0:
                        gallery_lines.append(f"\n*...và {remaining} ảnh khác*")
                    state["final_answer"] += "\n".join(gallery_lines)

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
            frame_urls_fallback = state.get("frame_urls") or []
            if frame_urls_fallback:
                # SQL failed but we have MinIO images — show them with partial response
                state["final_answer"] = (
                    f"Không thể truy vấn metadata sự kiện, nhưng đã tìm thấy "
                    f"{len(frame_urls_fallback)} ảnh bằng chứng gần đây từ MinIO."
                )
                display_urls = frame_urls_fallback[:20]
                remaining = len(frame_urls_fallback) - len(display_urls)
                gallery_lines = ["\n\n---\n### Ảnh bằng chứng (từ MinIO)"]
                for i, url in enumerate(display_urls, 1):
                    gallery_lines.append(f"![Ảnh {i}]({url})")
                if remaining > 0:
                    gallery_lines.append(f"\n*...và {remaining} ảnh khác*")
                state["final_answer"] += "\n".join(gallery_lines)
                state["response_confidence"] = 0.5
            else:
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
