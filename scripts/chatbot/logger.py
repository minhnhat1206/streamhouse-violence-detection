"""
Structured Logging Setup

JSON-based logging with request tracing for chatbot service.
"""

import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional

from .config import settings


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_obj = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_obj["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info),
            }

        # Add custom attributes
        if hasattr(record, "request_id"):
            log_obj["request_id"] = record.request_id
        if hasattr(record, "user_id"):
            log_obj["user_id"] = record.user_id
        if hasattr(record, "action"):
            log_obj["action"] = record.action
        if hasattr(record, "duration_ms"):
            log_obj["duration_ms"] = record.duration_ms
        if hasattr(record, "status_code"):
            log_obj["status_code"] = record.status_code

        return json.dumps(log_obj, ensure_ascii=False)


class PlainFormatter(logging.Formatter):
    """Plain text formatter for human-readable logs."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as plain text."""
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname

        # Add request ID if available
        request_id = getattr(record, "request_id", None)
        request_part = f"[{request_id}] " if request_id else ""

        # Base message
        msg = f"{timestamp} [{level}] {request_part}{record.getMessage()}"

        # Add exception info
        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)

        return msg


def setup_logger(name: str) -> logging.Logger:
    """
    Setup structured logger for a module.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(settings.LOG_LEVEL)

    # Remove existing handlers
    logger.handlers = []

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(settings.LOG_LEVEL)

    # Choose formatter based on LOG_FORMAT setting
    if settings.LOG_FORMAT == "json":
        formatter = JSONFormatter()
    else:
        formatter = PlainFormatter()

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


# Global logger
logger = setup_logger(__name__)


# ============================================================================
# Logging Helper Functions
# ============================================================================

def log_request(
    logger: logging.Logger,
    request_id: str,
    method: str,
    path: str,
    user_id: Optional[str] = None,
) -> None:
    """
    Log incoming HTTP request.

    Args:
        logger: Logger instance
        request_id: Unique request identifier
        method: HTTP method
        path: Request path
        user_id: Optional user identifier
    """
    extra = {
        "request_id": request_id,
        "action": f"{method} {path}",
    }
    if user_id:
        extra["user_id"] = user_id

    logger.info(f"→ {method} {path}", extra=extra)


def log_response(
    logger: logging.Logger,
    request_id: str,
    status_code: int,
    duration_ms: Optional[int] = None,
) -> None:
    """
    Log HTTP response.

    Args:
        logger: Logger instance
        request_id: Unique request identifier
        status_code: HTTP status code
        duration_ms: Response duration in milliseconds
    """
    extra = {
        "request_id": request_id,
        "status_code": status_code,
    }
    if duration_ms:
        extra["duration_ms"] = duration_ms

    status_emoji = "✓" if 200 <= status_code < 300 else "✗" if status_code >= 400 else "→"
    logger.info(f"{status_emoji} Response {status_code}", extra=extra)


def log_agent_node(
    logger: logging.Logger,
    request_id: str,
    node_name: str,
    status: str = "started",
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Log agent node execution.

    Args:
        logger: Logger instance
        request_id: Unique request identifier
        node_name: Name of the agent node
        status: Node status (started, completed, failed)
        details: Optional additional details
    """
    extra = {
        "request_id": request_id,
        "action": f"agent_node.{node_name}",
    }

    if details:
        for key, value in details.items():
            if isinstance(value, (str, int, float, bool)):
                extra[key] = value

    emoji = "→" if status == "started" else "✓" if status == "completed" else "✗"
    logger.info(f"{emoji} [{node_name}] {status}", extra=extra)


def log_query_execution(
    logger: logging.Logger,
    request_id: str,
    sql: str,
    layer: str,
    duration_ms: int,
    row_count: Optional[int] = None,
    error: Optional[str] = None,
) -> None:
    """
    Log SQL query execution.

    Args:
        logger: Logger instance
        request_id: Unique request identifier
        sql: SQL query
        layer: Data layer (Fluss/Paimon/Iceberg)
        duration_ms: Execution duration
        row_count: Number of rows returned
        error: Optional error message
    """
    extra = {
        "request_id": request_id,
        "action": f"query.{layer}",
        "duration_ms": duration_ms,
    }

    if row_count is not None:
        extra["row_count"] = row_count

    if error:
        logger.error(f"Query failed on {layer}: {error}\nSQL: {sql}", extra=extra)
    else:
        logger.info(f"Query executed on {layer} in {duration_ms}ms", extra=extra)


def log_retry_attempt(
    logger: logging.Logger,
    request_id: str,
    attempt: int,
    max_attempts: int,
    error: str,
    action: str = "query",
) -> None:
    """
    Log retry attempt.

    Args:
        logger: Logger instance
        request_id: Unique request identifier
        attempt: Current attempt number
        max_attempts: Maximum attempts
        error: Error message that triggered retry
        action: Action being retried (query, parse, etc.)
    """
    extra = {
        "request_id": request_id,
        "action": f"retry.{action}",
    }

    logger.warning(
        f"Retry {attempt}/{max_attempts} after {action} failure: {error}",
        extra=extra,
    )


if __name__ == "__main__":
    """Test logger when run directly."""
    test_logger = setup_logger(__name__)

    test_logger.info("This is an info message")
    test_logger.warning("This is a warning")
    test_logger.error("This is an error")

    try:
        1 / 0
    except Exception:
        test_logger.exception("An exception occurred")
