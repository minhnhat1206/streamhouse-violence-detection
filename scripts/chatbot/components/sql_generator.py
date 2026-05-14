"""
SQL Generator - Template SQL + Gemini Synthesis

Generates Trino SQL from user intent using templates and Gemini.
Validates SQL syntax and handles error-based SQL rewrites.
"""

import logging
import re
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta

try:
    import google.generativeai as genai
except ImportError:
    genai = None

logger = logging.getLogger(__name__)


class SQLGenerator:
    """Generate Trino SQL from intent using Gemini."""

    def __init__(self, gemini_api_key: Optional[str] = None, model: str = "gemini-2.5-flash"):
        """Initialize SQL generator with Gemini.

        Args:
            gemini_api_key: Google Gemini API key
            model: Gemini model name
        """
        self.model_name = model

        if genai and gemini_api_key:
            genai.configure(api_key=gemini_api_key)
            self.model = genai.GenerativeModel(model)
            logger.info(f"Initialized Gemini SQL generator with model: {model}")
        else:
            self.model = None
            logger.warning("Gemini not configured - SQL generation will use templates only")

    @staticmethod
    def _sanitize_value(value: str, max_length: int = 100) -> str:
        """Escape string value for safe embedding in SQL literals."""
        if not value:
            return ""
        value = str(value)[:max_length]
        value = value.replace("'", "''")
        value = value.replace("--", "").replace("/*", "").replace("*/", "")
        return value

    def _parse_time_period(self, time_period_str: str) -> Dict[str, Any]:
        """Parse time period string to SQL WHERE clause.

        Args:
            time_period_str: Time period in natural language
                (e.g., "today", "yesterday", "7 days", "2 weeks ago")

        Returns:
            Dict with 'start_datetime' and 'where_clause' for SQL
        """
        now = datetime.utcnow()

        # Handle various time period formats
        time_period_str = time_period_str.lower().strip()

        # Use variable for quoted identifier (Trino requires double-quotes for reserved words)
        TS = '"timestamp"'

        def ts_fmt(dt):
            """Format datetime as Trino TIMESTAMP literal."""
            return "TIMESTAMP '" + dt.strftime('%Y-%m-%d %H:%M:%S') + "'"

        if time_period_str in ["hôm nay", "today", "hom nay"]:
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            where_clause = f"WHERE {TS} >= {ts_fmt(start)}"

        elif time_period_str in ["hôm qua", "yesterday", "hom qua"]:
            start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            end = now.replace(hour=0, minute=0, second=0, microsecond=0)
            where_clause = f"WHERE {TS} BETWEEN {ts_fmt(start)} AND {ts_fmt(end)}"

        elif time_period_str in ["tuần này", "this week", "tuan nay"]:
            days_since_monday = now.weekday()
            start = (now - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
            where_clause = f"WHERE {TS} >= {ts_fmt(start)}"

        elif time_period_str in ["tuần trước", "last week", "tuan truoc"]:
            days_since_monday = now.weekday()
            start = (now - timedelta(days=days_since_monday+7)).replace(hour=0, minute=0, second=0, microsecond=0)
            end = (now - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
            where_clause = f"WHERE {TS} BETWEEN {ts_fmt(start)} AND {ts_fmt(end)}"

        elif time_period_str in ["tháng này", "this month", "thang nay", "tháng trước", "thang truoc", "last month"]:
            start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
            start = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            where_clause = f"WHERE {TS} >= {ts_fmt(start)}"

        else:
            # Try to parse "N <unit>" patterns — includes Vietnamese units
            _UNIT_MAP = {
                # minutes
                "minute": "minute", "minutes": "minute", "min": "minute",
                "phút": "minute", "phut": "minute",
                # hours
                "hour": "hour", "hours": "hour",
                "giờ": "hour", "gio": "hour",
                # days
                "day": "day", "days": "day",
                "ngày": "day", "ngay": "day",
                # weeks
                "week": "week", "weeks": "week",
                "tuần": "week", "tuan": "week",
                # months
                "month": "month", "months": "month",
                "tháng": "month", "thang": "month",
            }
            match = re.search(
                r'(\d+)\s*(phút|phut|minute|min|giờ|gio|hour|ngày|ngay|day|tuần|tuan|week|tháng|thang|month)s?',
                time_period_str
            )
            if match:
                num = int(match.group(1))
                unit = _UNIT_MAP.get(match.group(2).lower(), "day")

                if unit == "minute":
                    delta = timedelta(minutes=num)
                elif unit == "hour":
                    delta = timedelta(hours=num)
                elif unit == "day":
                    delta = timedelta(days=num)
                elif unit == "week":
                    delta = timedelta(weeks=num)
                elif unit == "month":
                    delta = timedelta(days=num * 30)
                else:
                    delta = timedelta(days=7)

                start = now - delta
                where_clause = f"WHERE {TS} >= {ts_fmt(start)}"
            else:
                # Default: last 30 days
                start = now - timedelta(days=30)
                where_clause = f"WHERE {TS} >= {ts_fmt(start)}"

        return {
            "start_datetime": start,
            "where_clause": where_clause
        }

    def _build_template_sql(
        self,
        intent: Any,  # IntentSchema
        schema_context: List[Dict[str, Any]],
        table_name: str = "violence_incidents"
    ) -> str:
        """Build template SQL from intent.

        Args:
            intent: IntentSchema with time_period, location, metric, intent_type
            schema_context: Schema metadata from ChromaDB
            table_name: Table to query from

        Returns:
            Template SQL string (will be refined by Gemini)
        """
        # Parse time period
        time_info = self._parse_time_period(intent.time_period)
        where_clause = time_info["where_clause"]

        # Add location filter if specified
        if intent.location:
            location_filter = "AND location = '" + self._sanitize_value(intent.location) + "'"
            where_clause = where_clause + " " + location_filter

        # Add camera filter if specified
        if hasattr(intent, 'filter_camera') and intent.filter_camera:
            camera_filter = "AND camera_id = '" + self._sanitize_value(intent.filter_camera) + "'"
            where_clause = where_clause + " " + camera_filter

        # Build SQL based on metric and intent type
        if intent.metric == "count":
            sql = f"""
SELECT
    location,
    COUNT(*) as total_incidents,
    COUNT(CASE WHEN is_violent = true THEN 1 END) as violent_count,
    AVG(risk_score) as avg_risk_score
FROM {table_name}
{where_clause}
GROUP BY location
ORDER BY violent_count DESC
LIMIT 100
            """.strip()

        elif intent.metric == "average":
            sql = f"""
SELECT
    camera_id,
    COUNT(*) as incident_count,
    AVG(risk_score) as avg_risk,
    MAX(risk_score) as max_risk
FROM {table_name}
{where_clause}
GROUP BY camera_id
ORDER BY avg_risk DESC
LIMIT 100
            """.strip()

        elif intent.metric == "max":
            sql = f"""
SELECT
    incident_id,
    camera_id,
    location,
    "timestamp",
    risk_score,
    event_type
FROM {table_name}
{where_clause}
ORDER BY risk_score DESC
LIMIT 10
            """.strip()

        elif intent.metric == "list":
            sql = f"""
SELECT
    incident_id,
    camera_id,
    location,
    "timestamp",
    risk_score,
    event_type,
    is_violent
FROM {table_name}
{where_clause}
ORDER BY "timestamp" DESC
LIMIT 100
            """.strip()

        else:
            # Default to count
            sql = f"""
SELECT
    COUNT(*) as total_incidents,
    COUNT(DISTINCT camera_id) as unique_cameras,
    AVG(risk_score) as avg_risk
FROM {table_name}
{where_clause}
            """.strip()

        return sql

    def generate_from_intent(
        self,
        intent: Any,  # IntentSchema
        schema_context: List[Dict[str, Any]],
        table_name: str = "violence_incidents"
    ) -> str:
        """Generate SQL from intent using Gemini.

        Args:
            intent: IntentSchema with time_period, location, metric, intent_type
            schema_context: Schema metadata from ChromaDB
            table_name: Table to query from

        Returns:
            Generated SQL query

        Raises:
            ValueError: If SQL generation fails
        """
        try:
            # Build template SQL first
            template_sql = self._build_template_sql(intent, schema_context, table_name)

            # If Gemini not available, return template
            if not self.model:
                logger.warning("Gemini not available - using template SQL")
                return template_sql

            # Use Gemini to refine SQL
            prompt = f"""
Bạn là một chuyên gia Trino SQL. Hãy cải thiện và hoàn thiện query SQL dưới đây:

**Yêu cầu người dùng:**
- Time period: {intent.time_period}
- Location: {intent.location if hasattr(intent, 'location') and intent.location else 'All'}
- Metric type: {intent.metric}
- Query type: {intent.intent_type}

**Schema có sẵn:**
{json.dumps(schema_context, ensure_ascii=False, indent=2)}

**Template SQL:**
{template_sql}

**Yêu cầu cho SQL cải thiện:**
1. BẮTBUỘC: Giữ nguyên tên table `{table_name}` - KHÔNG được đổi sang table khác
2. Chỉ sử dụng các column có trong template SQL (không tự thêm column mới như is_violent, label, etc.)
3. Thêm các filter thích hợp dựa trên thời gian và location (dùng `timestamp` column)
4. Đảm bảo syntax Trino đúng
5. Giới hạn kết quả: LIMIT 100
6. Trả về CHỈ câu query SQL, không có giải thích

**SQL cải thiện:**
            """.strip()

            response = self.model.generate_content(prompt)
            generated_sql = response.text.strip()

            # Clean up the response (remove markdown code blocks if present)
            if generated_sql.startswith("```"):
                generated_sql = generated_sql.split("```")[1]
                if generated_sql.startswith("sql"):
                    generated_sql = generated_sql[3:]
            generated_sql = generated_sql.strip()

            logger.info(f"Generated SQL from intent: {generated_sql[:100]}...")

            return generated_sql

        except Exception as e:
            logger.error(f"Failed to generate SQL: {e}")
            # Fall back to template
            return self._build_template_sql(intent, schema_context, table_name)

    def validate_sql(self, sql: str) -> bool:
        """Validate SQL syntax (basic checks).

        Args:
            sql: SQL query to validate

        Returns:
            True if SQL appears valid
        """
        sql = sql.strip().upper()

        # Check for balanced parentheses
        if sql.count('(') != sql.count(')'):
            logger.warning("SQL syntax error: unbalanced parentheses")
            return False

        # Check for SELECT keyword
        if "SELECT" not in sql:
            logger.warning("SQL syntax error: missing SELECT")
            return False

        # Check for FROM keyword (unless it's a simple SELECT 1)
        if "FROM" not in sql and "SELECT 1" not in sql and "SELECT COUNT" not in sql:
            logger.warning("SQL syntax error: missing FROM")
            return False

        # Check for multiple semicolons (potential injection)
        if sql.count(';') > 1:
            logger.warning("SQL syntax error: multiple statements detected")
            return False

        # Block comment sequences (injection pattern)
        if "--" in sql or "/*" in sql:
            logger.warning("SQL syntax error: comment sequence detected")
            return False

        logger.info("SQL syntax validation passed")
        return True

    def fix_sql_error(
        self,
        sql: str,
        error_msg: str,
        schema_context: List[Dict[str, Any]],
        retry_count: int = 1
    ) -> str:
        """Fix SQL based on error message using Gemini.

        Args:
            sql: Original SQL query that failed
            error_msg: Error message from query execution
            schema_context: Schema metadata from ChromaDB
            retry_count: Current retry count

        Returns:
            Fixed SQL query
        """
        if not self.model or retry_count > 3:
            logger.warning(f"Cannot fix SQL error (retry {retry_count}): {error_msg}")
            return sql

        try:
            prompt = f"""
Hãy sửa câu Trino SQL này dựa trên lỗi được báo:

**SQL lỗi:**
{sql}

**Lỗi:**
{error_msg}

**Schema có sẵn:**
{json.dumps(schema_context, ensure_ascii=False, indent=2)}

**Hướng sửa:**
1. BẮTBUỘC: Giữ nguyên tên TABLE - KHÔNG được đổi table name sang bảng khác
2. Nếu lỗi về column không tồn tại: LOẠI BỎ column đó khỏi query (dùng COUNT(*) hoặc SELECT * thay thế)
3. Nếu lỗi về timestamp format: dùng CURRENT_DATE hoặc timestamp cast
4. Nếu lỗi về timeout: thêm LIMIT 10 hoặc giảm date range
5. Trả về CHỈ câu query SQL sửa, không có giải thích

**SQL sửa:**
            """.strip()

            response = self.model.generate_content(prompt)
            fixed_sql = response.text.strip()

            # Clean up
            if fixed_sql.startswith("```"):
                fixed_sql = fixed_sql.split("```")[1]
                if fixed_sql.startswith("sql"):
                    fixed_sql = fixed_sql[3:]
            fixed_sql = fixed_sql.strip()

            logger.info(f"Fixed SQL: {fixed_sql[:100]}...")
            return fixed_sql

        except Exception as e:
            logger.error(f"Failed to fix SQL: {e}")
            return sql


# Import json for schema context formatting
import json
