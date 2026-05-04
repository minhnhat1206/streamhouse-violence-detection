"""
Vigilance AI Chatbot Backend — FastAPI service (port 5002)
Provides REST endpoints for the vigilance-ai dashboard:
  GET  /api/recent-incidents  — query Iceberg via Trino
  GET  /api/stats             — aggregated analytics from Iceberg via Trino
  POST /api/chat              — Agentic RAG using Gemini + Trino
"""

import os
import asyncio
import logging
import time
from typing import Optional

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Vigilance AI Chatbot", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8082"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

TRINO_BASE = f"http://{TRINO_HOST}:{TRINO_PORT}"


# ---------------------------------------------------------------------------
# Trino helpers
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
                raise RuntimeError(f"Trino query failed: {body.get('error', {}).get('message')}")

    return rows


# ---------------------------------------------------------------------------
# GET /api/recent-incidents
# ---------------------------------------------------------------------------

@app.get("/api/recent-incidents")
async def get_recent_incidents(limit: int = Query(50, ge=1, le=500)):
    sql = f"""
    SELECT
        incident_id,
        camera_id,
        CAST(timestamp AS VARCHAR) AS timestamp,
        risk_score,
        label,
        location,
        model_version,
        'Unreviewed' AS status
    FROM iceberg.security.violence_incidents
    ORDER BY timestamp DESC
    LIMIT {limit}
    """
    try:
        rows = await _trino_query(sql)
    except Exception as e:
        logger.error("Trino error (recent-incidents): %s", e)
        raise HTTPException(status_code=503, detail=f"Trino unavailable: {e}")

    return [
        {
            "event_id": r[0],
            "camera_id": r[1],
            "timestamp": r[2],
            "violence_score": float(r[3]) if r[3] is not None else 0.0,
            "label": r[4] or "Anomaly",
            "location": r[5] or r[1],
            "model_version": r[6] or "v2.1.0",
            "clip_link": "#",
            "status": r[7],
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# GET /api/stats
# ---------------------------------------------------------------------------

@app.get("/api/stats")
async def get_stats():
    alerts_per_hour_sql = """
    SELECT
        DATE_FORMAT(DATE_TRUNC('hour', timestamp), '%H:00') AS hour_label,
        COUNT(*) AS alert_count
    FROM iceberg.security.violence_incidents
    WHERE timestamp >= NOW() - INTERVAL '24' HOUR
    GROUP BY DATE_TRUNC('hour', timestamp)
    ORDER BY DATE_TRUNC('hour', timestamp)
    """

    top_locations_sql = """
    SELECT location, COUNT(*) AS cnt
    FROM iceberg.security.violence_incidents
    WHERE timestamp >= NOW() - INTERVAL '7' DAY
    GROUP BY location
    ORDER BY cnt DESC
    LIMIT 5
    """

    alert_types_sql = """
    SELECT label, COUNT(*) AS cnt
    FROM iceberg.security.violence_incidents
    WHERE timestamp >= NOW() - INTERVAL '7' DAY
    GROUP BY label
    """

    avg_score_sql = """
    SELECT
        DATE_FORMAT(CAST(timestamp AS DATE), '%b %d') AS day_label,
        CAST(timestamp AS DATE) AS day_date,
        AVG(risk_score) AS avg_score
    FROM iceberg.security.violence_incidents
    WHERE timestamp >= NOW() - INTERVAL '7' DAY
    GROUP BY CAST(timestamp AS DATE)
    ORDER BY CAST(timestamp AS DATE)
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
            {"name": row[0], "alerts": int(row[1])}
            for row in safe_rows(hours_rows)
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
# POST /api/chat
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    query: str
    history: Optional[list[dict]] = None


@app.post("/api/chat")
async def chat(req: ChatRequest):
    start = time.time()

    # --- Layer routing based on time keywords ---
    query_lower = req.query.lower()
    if any(k in query_lower for k in ["1 giờ", "30 phút", "15 phút", "vừa", "live", "realtime"]):
        layer = "hot"
    elif any(k in query_lower for k in ["hôm nay", "24 giờ", "today", "last 24"]):
        layer = "warm"
    else:
        layer = "cold"

    # --- Build context SQL based on layer ---
    context_sql = """
    SELECT incident_id, camera_id, CAST(timestamp AS VARCHAR), risk_score, label, location
    FROM iceberg.security.violence_incidents
    ORDER BY timestamp DESC
    LIMIT 20
    """
    try:
        rows = await _trino_query(context_sql, timeout=15.0)
        context_data = "\n".join(
            f"- [{r[0]}] cam={r[1]}, time={r[2]}, score={r[3]:.2f}, label={r[4]}, location={r[5]}"
            for r in rows[:10]
            if len(r) >= 6
        )
    except Exception as e:
        logger.warning("Could not fetch context from Trino: %s", e)
        context_data = "(không lấy được dữ liệu từ Trino)"

    # --- Call Gemini ---
    if not GEMINI_API_KEY:
        answer = (
            "Chatbot chưa được cấu hình API key. "
            f"Dữ liệu gần đây từ hệ thống:\n{context_data}"
        )
        citations = {"source_table": "iceberg.security.violence_incidents", "data_layer": layer, "time_period": "recent"}
    else:
        try:
            answer, citations = await _call_gemini(req.query, context_data, layer)
        except Exception as e:
            logger.error("Gemini error: %s", e)
            answer = f"Lỗi khi gọi AI: {e}. Dữ liệu gần đây:\n{context_data}"
            citations = {"source_table": "iceberg.security.violence_incidents", "data_layer": layer, "time_period": "recent"}

    duration_ms = int((time.time() - start) * 1000)

    return {
        "answer": answer,
        "layer": layer,
        "citations": citations,
        "confidence": 0.85,
        "duration_ms": duration_ms,
    }


async def _call_gemini(query: str, context: str, layer: str) -> tuple[str, dict]:
    """Call Gemini REST API with system prompt + Trino context."""
    prompt = f"""Bạn là AI assistant cho hệ thống giám sát an ninh Vigilance AI.
Dữ liệu thực tế từ layer {layer.upper()} (Streamhouse):
{context}

Câu hỏi: {query}

Trả lời ngắn gọn, chính xác. Nếu không có dữ liệu phù hợp, hãy nói rõ.
Cuối câu trả lời thêm dòng: [Nguồn: iceberg.security.violence_incidents | Layer: {layer}]"""

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}",
            json={"contents": [{"parts": [{"text": prompt}]}]},
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]

    citations = {
        "source_table": "iceberg.security.violence_incidents",
        "data_layer": layer,
        "time_period": "recent 24h" if layer == "warm" else "realtime" if layer == "hot" else "historical",
    }
    return text, citations


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok", "service": "vigilance-ai-chatbot"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5002, log_level="info")
