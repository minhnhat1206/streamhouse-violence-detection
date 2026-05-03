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
import traceback
import time as time_module
from contextlib import asynccontextmanager
from typing import Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from config import settings, validate_config
from logger import setup_logger, log_request, log_response
from agent import (
    create_agent_graph, AgentState, set_components, LayerChoice, IntentSchema,
    QueryResult
)
from components.chromadb_wrapper import ChromaDBWrapper, get_default_schemas
from components.trino_client import TrinoClient
from components.sql_generator import SQLGenerator
from components.evidence_service import EvidenceService
from components.data_ingest import DataIngestor

# Initialize logger
logger = setup_logger(__name__)

# Global references for lifecycle management
agent_graph = None
app_state = {
    "initialized": False,
    "chromadb": None,
    "trino_client": None,
    "sql_generator": None,
    "evidence_service": None,
    "data_ingestor": None,
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

        # Initialize ChromaDB
        logger.info("Initializing ChromaDB...")
        chromadb = ChromaDBWrapper(persist_dir=settings.CHROMA_DIR)
        app_state["chromadb"] = chromadb

        # Ingest schemas
        try:
            schemas = get_default_schemas()
            for table_name, schema_def in schemas.items():
                chromadb.ingest_schema(
                    table_name=table_name,
                    columns=schema_def["columns"],
                    description=schema_def.get("description")
                )
            logger.info("✓ ChromaDB initialized with schemas")
        except Exception as e:
            logger.warning(f"ChromaDB ingest failed: {e}")

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

        # Initialize Data Ingestor
        logger.info("Initializing Data Ingestor...")
        data_ingestor = DataIngestor(chromadb, ingest_interval_seconds=300)
        data_ingestor.start()
        app_state["data_ingestor"] = data_ingestor
        logger.info("✓ Data Ingestor started")

        # Set components in agent module
        set_components(chromadb, trino_client, sql_generator, evidence_service)

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

    # Stop data ingestor
    if app_state.get("data_ingestor"):
        try:
            import asyncio
            asyncio.run(app_state["data_ingestor"].stop())
        except Exception as e:
            logger.warning(f"Error stopping data ingestor: {e}")

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
            # Metadata
            start_time=start_time,
            duration_ms=None,
        )

        # Run agent graph (async nodes require ainvoke)
        result = await agent_graph.ainvoke(agent_input)

        # Calculate execution time
        duration_ms = int((time_module.time() - start_time) * 1000)

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

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions."""
    return {
        "error": exc.detail if isinstance(exc.detail, str) else str(exc.detail),
        "error_code": f"HTTP_{exc.status_code}",
        "timestamp": __import__('datetime').datetime.utcnow().isoformat() + "Z",
    }


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
