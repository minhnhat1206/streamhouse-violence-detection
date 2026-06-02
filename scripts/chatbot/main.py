"""
Chatbot API - FastAPI Application Entry Point

Agentic RAG system for Violence Detection with LangGraph orchestration.
- 6-node LangGraph agent: understand → select_layer → generate_sql → execute → correct → respond
- Layer-aware routing: Fluss (hot), Paimon (warm), Iceberg (cold)
- Self-correction with max 3 retries
- Anti-hallucination guards with mandatory citations
"""

import json
import logging
import os
import traceback
import time as time_module
from contextlib import asynccontextmanager
from typing import List, Optional
from uuid import uuid4

try:
    from prometheus_client import Histogram, Counter, generate_latest, CONTENT_TYPE_LATEST
    _PROM_ENABLED = True
    _query_duration = Histogram(
        "chatbot_query_duration_seconds",
        "Query duration by layer",
        labelnames=["layer"],
        buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300],
    )
    _query_total = Counter(
        "chatbot_queries_total",
        "Total queries by layer",
        labelnames=["layer"],
    )
except ImportError:
    _PROM_ENABLED = False

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .config import settings, validate_config
from .logger import setup_logger, log_request, log_response
from .agent import (
    create_agent_graph, AgentState, set_components, LayerChoice, IntentSchema,
    QueryResult
)
from .components.trino_client import TrinoClient
from .components.sql_generator import SQLGenerator
from .components.evidence_service import EvidenceService

# Initialize logger
logger = setup_logger(__name__)

# Global references for lifecycle management
agent_graph = None
app_state = {
    "initialized": False,
    "trino_client": None,
    "sql_generator": None,
    "evidence_service": None,
}


# ============================================================================
# Pydantic Models for Request/Response
# ============================================================================

class ChatRequest(BaseModel):
    """User chat request."""
    query: str = Field(..., min_length=1, max_length=1000, description="User question in Vietnamese")
    context: Optional[str] = Field(None, description="Additional context or previous conversation")
    options: Optional[dict] = Field(None, description="Query options (future use)")

    class Config:
        example = {
            "query": "Hôm qua quận 1 có bao nhiêu vụ bạo lực?",
            "context": None,
            "options": {}
        }


class Citation(BaseModel):
    """Source attribution for response."""
    source_table: str = Field(..., description="Table name (e.g., violence_incidents)")
    data_layer: str = Field(..., description="Storage layer (Fluss/Paimon/Iceberg)")
    time_period: str = Field(..., description="Time range of data")
    row_count: Optional[int] = Field(None, description="Number of records used")


class ChatResponse(BaseModel):
    """Chatbot response with citations."""
    answer: str = Field(..., description="Response in Vietnamese")
    sql_used: Optional[str] = Field(None, description="SQL query executed")
    citations: Citation = Field(..., description="Source attribution")
    layer: str = Field(..., description="Storage layer used")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Response confidence (0-1)")
    duration_ms: int = Field(..., description="Total execution time in milliseconds")
    frame_base64: Optional[str] = Field(None, description="Base64-encoded JPEG frame (if single incident)")
    frame_url: Optional[str] = Field(None, description="S3 path to evidence frame")
    frame_urls: Optional[List[str]] = Field(None, description="Public HTTP URLs for evidence frames (evidence queries)")
    incident_id: Optional[str] = Field(None, description="Incident ID for frame reference")

    class Config:
        example = {
            "answer": "Hôm qua, khu vực quận 1 ghi nhận 42 vụ bạo lực.",
            "sql_used": "SELECT COUNT(*) FROM paimon.security.violence_incidents WHERE ...",
            "citations": {
                "source_table": "violence_incidents",
                "data_layer": "Paimon",
                "time_period": "2026-04-27",
                "row_count": 42
            },
            "layer": "Paimon",
            "confidence": 0.92,
            "duration_ms": 3420
        }


class ErrorResponse(BaseModel):
    """Error response structure."""
    error: str = Field(..., description="Error message in Vietnamese")
    error_code: str = Field(..., description="Error code (e.g., QUERY_FAILED)")
    details: Optional[str] = Field(None, description="Additional error details")
    timestamp: str = Field(..., description="ISO 8601 timestamp")

    class Config:
        example = {
            "error": "Không thể truy vấn cơ sở dữ liệu sau 3 lần thử.",
            "error_code": "QUERY_FAILED",
            "details": "Column 'district' not found in table",
            "timestamp": "2026-04-28T14:30:45.123Z"
        }


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Status: ok or degraded")
    services: dict = Field(..., description="Status of each service")
    version: str = Field(..., description="API version")


# ============================================================================
# Startup & Shutdown
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan context manager for startup/shutdown."""

    # Startup
    logger.info("🚀 Chatbot API starting up...")

    try:
        # Validate configuration
        validate_config()
        logger.info("✓ Configuration validated")

        # Initialize Trino Client
        logger.info("Initializing Trino Client...")
        trino_client = TrinoClient(
            trino_host=settings.TRINO_HOST,
            trino_port=settings.TRINO_PORT,
            flink_gateway_host=settings.FLINK_GATEWAY_HOST,
            flink_gateway_port=settings.FLINK_GATEWAY_PORT,
        )
        app_state["trino_client"] = trino_client
        logger.info("✓ Trino Client initialized")

        # Initialize SQL Generator
        logger.info("Initializing SQL Generator...")
        sql_generator = SQLGenerator(
            gemini_api_key=settings.GEMINI_API_KEY,
            model="gemini-2.5-flash"
        )
        app_state["sql_generator"] = sql_generator
        logger.info("✓ SQL Generator initialized")

        # Initialize Evidence Service
        logger.info("Initializing Evidence Service...")
        minio_endpoint = settings.S3_ENDPOINT.replace("http://", "").replace("https://", "")
        use_ssl = settings.S3_ENDPOINT.startswith("https://")
        evidence_service = EvidenceService(
            minio_endpoint=minio_endpoint,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            bucket_name=settings.S3_BUCKET,
            cache_size=100,
            use_ssl=use_ssl
        )
        app_state["evidence_service"] = evidence_service
        logger.info("✓ Evidence Service initialized")

        # Set components in agent module
        set_components(trino_client, sql_generator, evidence_service)

        # Pre-warm Flink SQL Gateway session so the first HOT query doesn't
        # spend its entire timeout budget on DDL (CREATE CATALOG + USE + USE).
        try:
            logger.info("Pre-warming Fluss SQL Gateway session...")
            trino_client._ensure_fluss_session(init_timeout=60)
            logger.info("✓ Fluss session warmed")
        except Exception as e:
            logger.warning(f"Fluss session pre-warm failed (non-fatal): {e}")

        # Create LangGraph agent
        logger.info("Creating LangGraph agent...")
        global agent_graph
        agent_graph = create_agent_graph()
        logger.info("✓ LangGraph agent initialized")

        # Mark as initialized
        app_state["initialized"] = True
        logger.info("✓ Chatbot API ready")

    except Exception as e:
        logger.error(f"❌ Startup failed: {e}", exc_info=True)
        raise

    yield

    # Shutdown
    logger.info("🛑 Chatbot API shutting down...")
    app_state["initialized"] = False

    logger.info("✓ Shutdown complete")


# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="Chatbot API - Agentic RAG",
    description="Vietnamese language violence detection chatbot with LangGraph orchestration",
    version="2.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# Custom middleware for request/response logging
@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    """Log all HTTP requests and responses."""
    request_id = request.headers.get("X-Request-ID", "unknown")

    # Log request
    log_request(logger, request_id, request.method, request.url.path)

    try:
        response = await call_next(request)

        # Log response
        log_response(logger, request_id, response.status_code)

        return response
    except Exception as e:
        logger.error(f"[{request_id}] Unhandled exception: {e}", exc_info=True)
        raise


# ============================================================================
# Routes
# ============================================================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    services = {
        "api": "ok",
        "agent_initialized": app_state["initialized"],
        "config_valid": True,
    }

    status = "ok" if all(v in (True, "ok") for v in services.values()) else "degraded"

    return HealthResponse(
        status=status,
        services=services,
        version="2.0.0",
    )


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Main chat endpoint - Process Vietnamese natural language query.

    Returns structured response with answer, SQL used, and citations.
    """
    start_time = time_module.time()
    request_id = str(uuid4())[:8]

    if not app_state["initialized"]:
        raise HTTPException(
            status_code=503,
            detail="Chatbot chưa khởi tạo. Vui lòng thử lại sau."
        )

    try:
        logger.info(f"[{request_id}] Processing query: {request.query[:100]}...")

        # Prepare complete AgentState with all required fields
        agent_input = AgentState(
            # Input
            user_query=request.query,
            context=request.context or "",
            options=request.options or {},
            request_id=request_id,
            # Intent extraction
            intent=None,
            # Layer selection
            selected_layer=None,
            trino_catalog=None,
            trino_schema="security",
            table_name=None,
            # SQL generation
            generated_sql=None,
            # Query execution
            query_result=None,
            # Retry logic
            retry_count=0,
            retry_errors=[],
            # Response generation
            final_answer=None,
            response_confidence=0.0,
            source_table=None,
            data_layer=None,
            time_period=None,
            row_count=None,
            # Frame evidence
            incident_id=None,
            camera_id=None,
            incident_date=None,
            frame_url=None,
            frame_base64=None,
            frame_urls=None,
            # Metadata
            start_time=start_time,
            duration_ms=None,
        )

        # Run agent graph (async nodes require ainvoke)
        result = await agent_graph.ainvoke(agent_input)

        # Calculate execution time
        duration_ms = int((time_module.time() - start_time) * 1000)

        # Record Prometheus metrics
        if _PROM_ENABLED:
            used_layer = result.get("data_layer", result.get("selected_layer", "unknown")) or "unknown"
            # Map technology name → architectural layer name for consistent dashboard labels
            _layer_map = {"paimon": "warm", "iceberg": "cold", "fluss": "hot"}
            metric_layer = _layer_map.get(str(used_layer).lower(), str(used_layer).lower())
            _query_duration.labels(layer=metric_layer).observe(duration_ms / 1000)
            _query_total.labels(layer=metric_layer).inc()

        logger.info(f"[{request_id}] Query processed successfully in {duration_ms}ms")

        # Extract response components
        response = ChatResponse(
            answer=result.get("final_answer", "Lỗi: Không có câu trả lời"),
            sql_used=result.get("generated_sql"),
            citations=Citation(
                source_table=result.get("source_table", "unknown"),
                data_layer=result.get("data_layer", result.get("selected_layer", "unknown")),
                time_period=result.get("time_period", "unknown"),
                row_count=result.get("row_count"),
            ),
            layer=result.get("data_layer", result.get("selected_layer", "unknown")),
            confidence=result.get("response_confidence", 0.0),
            duration_ms=duration_ms,
            frame_base64=result.get("frame_base64"),
            frame_url=result.get("frame_url"),
            frame_urls=result.get("frame_urls"),
            incident_id=result.get("incident_id"),
        )

        return response

    except Exception as e:
        logger.error(f"[{request_id}] Query processing failed: {e}", exc_info=True)

        # Return structured error
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Không thể xử lý câu hỏi. Vui lòng thử lại.",
                "error_code": "QUERY_PROCESSING_FAILED",
                "details": str(e) if settings.DEBUG else None,
            }
        )


@app.post("/webhook/chat", response_model=ChatResponse)
async def webhook_chat(request: ChatRequest):
    """
    Webhook endpoint for n8n integration.

    Same as /chat but designed for automated workflow triggering.
    """
    return await chat(request)


@app.get("/api/evidence/{incident_id}/frame")
async def get_evidence_frame(
    incident_id: str,
    format: str = "url",
    camera_id: Optional[str] = None,
    incident_date: Optional[str] = None
):
    """
    Retrieve evidence frame for an incident.

    Query parameters:
    - format: 'url' (returns JSON with S3 path) or 'base64' (returns base64 JPEG)
    - camera_id: Optional camera ID (for frame lookup)
    - incident_date: Optional incident date (format: YYYY-MM-DD)
    """
    try:
        if not app_state["initialized"]:
            raise HTTPException(status_code=503, detail="Service not initialized")

        evidence_service = app_state.get("evidence_service")
        if not evidence_service:
            raise HTTPException(status_code=503, detail="Evidence service not available")

        logger.info(f"Retrieving frame for incident: {incident_id}")

        if format == "url":
            # Return S3 URL without downloading
            frame_url = evidence_service.get_frame_url(
                incident_id=incident_id,
                camera_id=camera_id or "unknown",
                incident_date=incident_date or time_module.strftime("%Y-%m-%d")
            )
            return {
                "incident_id": incident_id,
                "frame_url": frame_url,
                "s3_endpoint": settings.S3_ENDPOINT,
                "bucket": settings.S3_BUCKET,
            }

        elif format == "base64":
            # Download and return as base64
            frame_b64 = evidence_service.get_frame(
                incident_id=incident_id,
                camera_id=camera_id,
                incident_date=incident_date
            )

            if not frame_b64:
                raise HTTPException(
                    status_code=404,
                    detail=f"Frame not found for incident: {incident_id}"
                )

            return {
                "incident_id": incident_id,
                "frame_base64": frame_b64,
                "content_type": "image/jpeg",
            }

        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid format: {format}. Use 'url' or 'base64'."
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Frame retrieval failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Không thể lấy ảnh chứng cứ"
        )


# ============================================================================
# Exception Handlers
# ============================================================================

# ============================================================================
# Dashboard data endpoints — Trino Iceberg + Kafka
# ============================================================================

async def _trino_query(sql: str, timeout: float = 20.0) -> list:
    """Execute Trino SQL and return all rows via nextUri chain."""
    import httpx
    trino_host = os.getenv("TRINO_HOST", "trino-coordinator")
    trino_port = os.getenv("TRINO_PORT", "8080")
    trino_user = os.getenv("TRINO_USER", "trino")
    base = f"http://{trino_host}:{trino_port}"
    headers = {
        "X-Trino-User": trino_user,
        "X-Trino-Catalog": "iceberg",
        "X-Trino-Schema": "security",
        "Content-Type": "text/plain",
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(f"{base}/v1/statement", content=sql, headers=headers)
        resp.raise_for_status()
        body = resp.json()
        rows: list = []
        if body.get("data"):
            rows.extend(body["data"])
        next_uri = body.get("nextUri")
        while next_uri:
            resp = await client.get(next_uri, headers=headers)
            resp.raise_for_status()
            body = resp.json()
            if body.get("data"):
                rows.extend(body["data"])
            next_uri = body.get("nextUri")
            state = body.get("stats", {}).get("state", "")
            if state in ("FAILED", "CANCELED"):
                raise RuntimeError(body.get("error", {}).get("message", "Trino query failed"))
    return rows


@app.get("/api/evidence/frames")
async def get_evidence_frames(
    camera_id: Optional[str] = None,
    date: Optional[str] = None,
    limit: int = 20,
):
    """
    Return public MinIO HTTP URLs for evidence frames.

    Query params:
    - camera_id: filter by camera (e.g. cam_01)
    - date: filter by date YYYY-MM-DD (defaults to today)
    - limit: max number of frames (default 20)

    Each frame is accessible at:
      http://<minio-host>:9000/evidence-frames/{camera_id}/{date}/{uuid}.jpg
    """
    import datetime as _dt
    minio_host = os.getenv("MINIO_ENDPOINT", "minio:9000")
    bucket = os.getenv("S3_BUCKET", "evidence-frames")
    public_base = os.getenv("MINIO_PUBLIC_URL", f"http://{minio_host}")

    evidence_svc = app_state.get("evidence_service")
    if not evidence_svc or not evidence_svc.client:
        raise HTTPException(status_code=503, detail="Evidence service unavailable")

    target_date = date or _dt.datetime.utcnow().strftime("%Y-%m-%d")
    prefix = f"{camera_id}/{target_date}/" if camera_id else f""

    try:
        objects = evidence_svc.client.list_objects(
            bucket_name=bucket,
            prefix=prefix,
            recursive=True,
        )
        frames = []
        for obj in objects:
            if not obj.object_name.endswith(".jpg"):
                continue
            frames.append({
                "url": f"{public_base}/{bucket}/{obj.object_name}",
                "object_key": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
            })
            if len(frames) >= limit:
                break

        return {
            "total": len(frames),
            "camera_id": camera_id,
            "date": target_date,
            "frames": frames,
        }
    except Exception as e:
        logger.error(f"Evidence frames listing failed: {e}")
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}")


@app.get("/api/recent-incidents")
async def get_recent_incidents(limit: int = 50):
    """Latest incidents from Iceberg cold layer via Trino, with frame_url from MinIO."""
    sql = f"""
    SELECT
        incident_id, camera_id,
        CAST(timestamp AS VARCHAR) AS timestamp,
        risk_score, event_type, location,
        is_violent,
        CAST(timestamp AS VARCHAR) AS incident_date
    FROM paimon.security.violence_incidents
    WHERE is_violent = TRUE
    ORDER BY timestamp DESC
    LIMIT {limit}
    """
    try:
        rows = await _trino_query(sql)
    except Exception as e:
        logger.error(f"Trino error (recent-incidents): {e}")
        raise HTTPException(status_code=503, detail=f"Trino unavailable: {e}")

    minio_external = os.getenv("MINIO_EXTERNAL_URL", "http://localhost:9000")
    evidence_bucket = os.getenv("S3_BUCKET", "evidence-frames")

    def build_frame_url(incident_id: str, camera_id: str, incident_date: str) -> str:
        """Build HTTP URL for evidence frame in MinIO."""
        return f"{minio_external}/{evidence_bucket}/{camera_id}/{incident_date}/{incident_id}.jpg"

    return [
        {
            "event_id": r[0], "camera_id": r[1], "timestamp": r[2],
            "violence_score": float(r[3]) if r[3] is not None else 0.0,
            "label": r[4] or "Anomaly", "location": r[5] or r[1],
            "model_version": "VioMobileNet v2.1", "clip_link": "#",
            "status": "Unreviewed" if r[6] else "False Alarm",
            "frame_url": build_frame_url(r[0], r[1], r[7]) if r[7] else None,
        }
        for r in rows
    ]


@app.get("/api/stats")
async def get_stats():
    """Aggregated analytics from Iceberg for the dashboard."""
    hours_sql = """
    SELECT
        CAST(CAST(timestamp AS DATE) AS VARCHAR) AS hour_label,
        COUNT(*) AS alert_count
    FROM paimon.security.violence_incidents
    WHERE is_violent = TRUE AND timestamp >= NOW() - INTERVAL '7' DAY
    GROUP BY CAST(timestamp AS DATE)
    ORDER BY CAST(timestamp AS DATE)
    """
    loc_sql = """
    SELECT location, COUNT(*) AS cnt
    FROM paimon.security.violence_incidents
    WHERE is_violent = TRUE
    GROUP BY location ORDER BY cnt DESC LIMIT 5
    """
    type_sql = """
    SELECT event_type, COUNT(*) AS cnt
    FROM paimon.security.violence_incidents
    WHERE is_violent = TRUE
    GROUP BY event_type
    """
    score_sql = """
    SELECT
        date_format(CAST(timestamp AS DATE), '%b %d') AS day_label,
        AVG(risk_score) AS avg_score
    FROM paimon.security.violence_incidents
    WHERE is_violent = TRUE
    GROUP BY CAST(timestamp AS DATE)
    ORDER BY CAST(timestamp AS DATE)
    """
    import asyncio as _asyncio
    results = await _asyncio.gather(
        _trino_query(hours_sql), _trino_query(loc_sql),
        _trino_query(type_sql), _trino_query(score_sql),
        return_exceptions=True,
    )
    def safe(r): return r if isinstance(r, list) else []


    def _parse_loc(loc):
        try:
            d = __import__("json").loads(loc)
            return d.get("street") or d.get("ward") or str(loc)[:30]
        except Exception:
            return (loc or "Unknown")[:40]
    return {
        "alertsPerHour": [{"name": r[0], "alerts": int(r[1])} for r in safe(results[0])],
        "topLocations":  [{"name": _parse_loc(r[0]), "alerts": int(r[1])} for r in safe(results[1])],
        "alertTypes":    [{"name": r[0] or "Unknown", "value": int(r[1])} for r in safe(results[2])],
        "avgScore":      [{"name": r[0], "score": round(float(r[1]), 3) if r[1] else 0} for r in safe(results[3])],
    }


@app.get("/api/camera-status")
async def get_camera_status():
    """
    Real-time camera violence status from Kafka.
    Runs blocking Kafka IO in a thread executor to avoid blocking the event loop.
    """
    import asyncio as _asyncio

    def _read_kafka_sync() -> dict:
        import json as _json
        from datetime import datetime, timezone
        from kafka import KafkaConsumer, TopicPartition

        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        topics = ["hot-violence-alerts-valid"]
        cutoff_ms = int((datetime.now(timezone.utc).timestamp() - 300) * 1000)
        status_map: dict = {}
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=kafka_servers,
                value_deserializer=lambda v: _json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                consumer_timeout_ms=1000,
                group_id=None,
            )
            all_tps = []
            for topic in topics:
                partitions = consumer.partitions_for_topic(topic) or set()
                all_tps.extend([TopicPartition(topic, p) for p in partitions])
            if all_tps:
                consumer.assign(all_tps)
                # Snapshot end offsets BEFORE seeking — read only up to this point
                end_offsets = consumer.end_offsets(all_tps)
                read_targets: dict = {}
                for tp in all_tps:
                    end = end_offsets[tp]
                    if end == 0:
                        continue
                    start = max(0, end - 50)
                    consumer.seek(tp, start)
                    read_targets[tp] = end  # stop reading at this offset
                remaining = set(read_targets.keys())
                for msg in consumer:
                    if not remaining:
                        break
                    tp = TopicPartition(msg.topic, msg.partition)
                    # Stop reading this partition once we've reached end
                    if msg.offset >= read_targets.get(tp, 0) - 1:
                        remaining.discard(tp)
                    ts = msg.timestamp or 0
                    if ts < cutoff_ms:
                        continue
                    val = msg.value or {}
                    cam = val.get("camera_id") or val.get("cam_id")
                    if not cam:
                        continue
                    is_violent = val.get("is_violent", False) or val.get("violence_detected", False)
                    score = float(val.get("risk_score") or val.get("violence_score") or 0)
                    if is_violent or score >= 0.5:
                        status_map[cam] = "VIOLENCE_DETECTED"
                    elif cam not in status_map:
                        status_map[cam] = "NORMAL"
            consumer.close()
        except Exception as e:
            logger.warning(f"Kafka camera-status error: {e}")
        return status_map

    loop = _asyncio.get_event_loop()
    status_map = await loop.run_in_executor(None, _read_kafka_sync)
    return {"cameras": status_map, "window_seconds": 300}



@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint (chatbot_query_duration_seconds, chatbot_queries_total)."""
    from fastapi.responses import Response
    if not _PROM_ENABLED:
        return Response(content="# prometheus_client not installed\n", media_type="text/plain")
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/union-read")
async def union_read():
    """Federated read: HOT (Fluss) + WARM (Paimon) + COLD (Iceberg), merged by time."""
    import asyncio as _asyncio
    start = time_module.time()

    trino_client = app_state.get("trino_client")
    if not trino_client:
        raise HTTPException(status_code=503, detail="Trino client not initialized")

    def _sync_union():
        return trino_client.query_union_all_layers()

    loop = _asyncio.get_event_loop()
    try:
        rows = await loop.run_in_executor(None, _sync_union)
    except Exception as e:
        logger.error(f"union-read failed: {e}")
        rows = []

    return {
        "rows": rows,
        "total": len(rows),
        "duration_ms": int((time_module.time() - start) * 1000),
    }


@app.get("/api/layer-counts")
async def get_layer_counts():
    """Approximate record counts from all 3 Streamhouse storage layers."""
    import asyncio as _asyncio
    import httpx

    async def _count_hot() -> int | None:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.get("http://jobmanager:8081/jobs/overview")
                if r.status_code != 200:
                    return None
                for job in r.json().get("jobs", []):
                    if "hot_violence_alerts" in job.get("name", "") and job.get("state") == "RUNNING":
                        jid = job["jid"]
                        r2 = await client.get(f"http://jobmanager:8081/jobs/{jid}")
                        if r2.status_code != 200:
                            break
                        for v in r2.json().get("vertices", []):
                            vname = v.get("name", "").lower()
                            if "sink" in vname or "fluss" in vname:
                                mr = await client.get(
                                    f"http://jobmanager:8081/jobs/{jid}/vertices/{v['id']}/metrics",
                                    params={"get": "0.numRecordsIn"},
                                )
                                if mr.status_code == 200:
                                    for m in mr.json():
                                        mid = m.get("id", "")
                                        if mid in ("numRecordsIn", "0.numRecordsIn"):
                                            val = int(m.get("value", 0))
                                            if val > 0:
                                                return val
        except Exception as exc:
            logger.debug("count_hot via Flink metrics failed: %s", exc)
        return None

    async def _count_warm() -> int | None:
        try:
            rows = await _trino_query(
                "SELECT COUNT(*) FROM paimon.security.violence_incidents",
                timeout=15.0,
            )
            return int(rows[0][0]) if rows and rows[0] else 0
        except Exception:
            return None

    async def _count_cold() -> int | None:
        try:
            rows = await _trino_query(
                "SELECT COUNT(*) FROM iceberg.security.historical_violence_incidents",
                timeout=15.0,
            )
            return int(rows[0][0]) if rows and rows[0] else 0
        except Exception:
            return None

    t0 = time_module.time()
    hot, warm, cold = await _asyncio.gather(_count_hot(), _count_warm(), _count_cold())
    return {
        "hot":         hot,
        "warm":        warm,
        "cold":        cold,
        "duration_ms": round((time_module.time() - t0) * 1000),
    }


@app.get("/api/grafana/stats")
async def grafana_stats():
    """Grafana Infinity-compatible endpoint — returns array of stats for dashboard panels.
    Wraps layer-counts + violence stats into [{...}] array format required by Infinity datasource.
    """
    import asyncio as _asyncio

    async def _violence_counts():
        try:
            rows_24h = await _trino_query(
                "SELECT COUNT(*) FROM paimon.security.violence_incidents "
                "WHERE is_violent = TRUE AND timestamp >= NOW() - INTERVAL '1' DAY",
                timeout=15.0,
            )
            rows_7d = await _trino_query(
                "SELECT COUNT(*) FROM paimon.security.violence_incidents "
                "WHERE is_violent = TRUE AND timestamp >= NOW() - INTERVAL '7' DAY",
                timeout=15.0,
            )
            cameras = await _trino_query(
                "SELECT COUNT(DISTINCT camera_id) FROM paimon.security.violence_incidents "
                "WHERE timestamp >= NOW() - INTERVAL '1' DAY",
                timeout=15.0,
            )
            avg_score = await _trino_query(
                "SELECT ROUND(AVG(risk_score), 3) FROM paimon.security.violence_incidents "
                "WHERE is_violent = TRUE AND timestamp >= NOW() - INTERVAL '1' DAY",
                timeout=15.0,
            )
            return {
                "violent_24h": int(rows_24h[0][0]) if rows_24h and rows_24h[0][0] else 0,
                "violent_7d":  int(rows_7d[0][0])  if rows_7d  and rows_7d[0][0]  else 0,
                "cameras_24h": int(cameras[0][0])   if cameras   and cameras[0][0]   else 0,
                "avg_risk_score": float(avg_score[0][0]) if avg_score and avg_score[0][0] else 0.0,
            }
        except Exception as e:
            logger.warning("grafana_stats violence counts failed: %s", e)
            return {"violent_24h": 0, "violent_7d": 0, "cameras_24h": 0, "avg_risk_score": 0.0}

    # Get layer counts
    layer_task = get_layer_counts()
    violence_task = _violence_counts()
    layers, violence = await _asyncio.gather(layer_task, violence_task)

    # Return as array (Infinity datasource requires array at root)
    return [{
        "hot_rows":       layers.get("hot") or 0,
        "warm_rows":      layers.get("warm") or 0,
        "cold_rows":      layers.get("cold") or 0,
        "query_latency_ms": layers.get("duration_ms") or 0,
        "violent_24h":    violence["violent_24h"],
        "violent_7d":     violence["violent_7d"],
        "cameras_active": violence["cameras_24h"],
        "avg_risk_score": violence["avg_risk_score"],
    }]


@app.get("/api/latency")
async def get_latency():
    """Measure round-trip query latency for each Streamhouse storage layer."""
    import asyncio as _asyncio

    flink_gw_host = os.getenv("FLINK_GATEWAY_HOST", "flink-sql-gateway")
    flink_gw_port = os.getenv("FLINK_GATEWAY_PORT", "8083")
    flink_gw_base = f"http://{flink_gw_host}:{flink_gw_port}"

    async def _probe_hot() -> dict:
        import httpx
        t0 = time_module.time()
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(f"{flink_gw_base}/v1/info")
                ok = r.status_code == 200
            return {"latency_ms": round((time_module.time() - t0) * 1000), "ok": ok, "error": None}
        except Exception as exc:
            return {"latency_ms": round((time_module.time() - t0) * 1000), "ok": False, "error": str(exc)[:120]}

    async def _probe_warm() -> dict:
        t0 = time_module.time()
        try:
            await _trino_query(
                "SELECT incident_id FROM paimon.security.violence_incidents LIMIT 1",
                timeout=15.0,
            )
            return {"latency_ms": round((time_module.time() - t0) * 1000), "ok": True, "error": None}
        except Exception as exc:
            return {"latency_ms": round((time_module.time() - t0) * 1000), "ok": False, "error": str(exc)[:120]}

    async def _probe_cold() -> dict:
        t0 = time_module.time()
        try:
            await _trino_query(
                "SELECT incident_id FROM iceberg.security.historical_violence_incidents LIMIT 1",
                timeout=15.0,
            )
            return {"latency_ms": round((time_module.time() - t0) * 1000), "ok": True, "error": None}
        except Exception as exc:
            return {"latency_ms": round((time_module.time() - t0) * 1000), "ok": False, "error": str(exc)[:120]}

    hot, warm, cold = await _asyncio.gather(_probe_hot(), _probe_warm(), _probe_cold())
    return {
        "hot":  {**hot,  "target_ms": 100,   "layer_name": "Fluss"},
        "warm": {**warm, "target_ms": 10000, "layer_name": "Paimon"},
        "cold": {**cold, "target_ms": 30000, "layer_name": "Iceberg"},
    }


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions."""
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail if isinstance(exc.detail, str) else str(exc.detail),
            "error_code": f"HTTP_{exc.status_code}",
        },
    )


# ============================================================================
# Root endpoint
# ============================================================================

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "Chatbot API - Agentic RAG",
        "version": "2.0.0",
        "status": "running" if app_state["initialized"] else "initializing",
        "endpoints": {
            "health": "/health",
            "chat": "/chat (POST)",
            "webhook": "/webhook/chat (POST)",
            "evidence": "/api/evidence/{incident_id}/frame (GET)",
        }
    }


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.API_PORT,
        reload=settings.DEBUG,
        log_level="debug" if settings.DEBUG else "info",
    )
