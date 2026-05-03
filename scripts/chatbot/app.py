import os
import re
import boto3
from flask import Flask, request, jsonify, send_file
from dotenv import load_dotenv
import google.generativeai as genai
from rag_store import RAGStore
from ingest import run_ingest
import json
from flask_cors import CORS
from io import BytesIO

load_dotenv()

CHROMA_DIR = os.getenv("CHROMA_DIR", "/data/chroma")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# S3/MinIO configuration for frame retrieval
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = "evidence-frames"
S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")

app = Flask(__name__)
CORS(app)
rag = RAGStore(CHROMA_DIR)

# Initialize S3 client for frame retrieval
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name="us-east-1",
)

def extract_filters(question: str):
    """Helper function to extract metadata from question for Hybrid Search"""
    filters = {}
    
    # 1. Extract camera_id (e.g., cam01, cam02...)
    cam_match = re.search(r'cam\d+', question.lower())
    if cam_match:
        filters["camera_id"] = cam_match.group()
        
    # 2. Extract date (format YYYY-MM-DD)
    date_match = re.search(r'\d{4}-\d{2}-\d{2}', question)
    if date_match:
        filters["date"] = date_match.group()
        
    return filters if filters else None

def init_data_incremental():
    """Startup: Only ingest new events from MinIO not yet in database"""
    print("[app] Checking for new data in MinIO...")
    try:
        # Get list of existing event_ids to avoid duplication
        current_data = rag.collection.get(include=['metadatas'])
        existing_ids = set()
        if current_data and current_data['metadatas']:
            existing_ids = {str(m.get('event_id')) for m in current_data['metadatas'] if m.get('event_id')}
        
        print(f"[app] Found {len(existing_ids)} events already in database.")

        # Ingest only missing data
        new_docs = run_ingest(existing_ids=existing_ids)
        
        if new_docs:
            rag.upsert_documents(new_docs)
            print(f"[app] Successfully indexed {len(new_docs)} NEW documents.")
        else:
            print("[app] Database is up-to-date. No new events added.")
            
    except Exception as e:
        import traceback
        print("[app] ERROR during startup ingest:", e)
        traceback.print_exc()

# Perform data ingestion immediately upon Server startup
init_data_incremental()

# Configure Gemini Client
model = None
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

def synthesize_answer(question: str, hits: list) -> str:
    if not hits:
        return "Hệ thống không tìm thấy dữ liệu nào phù hợp với yêu cầu của bạn."
    
    context = "\n\n".join([f"- {h['text']}" for h in hits])
    if model:
        prompt = f"""Bạn là Trợ lý Giám sát An ninh. Hãy trả lời câu hỏi dựa trên các ngữ cảnh sau.
Nếu dữ liệu có số liệu cụ thể (score, camera ID, thời gian), hãy trích dẫn chính xác.
Câu hỏi: {question}

Ngữ cảnh:
{context}

Trả lời chi tiết bằng tiếng Việt, tập trung vào các sự kiện bạo lực được ghi nhận.  Nếu câu hỏi không liên quan đến dữ liệu, hãy trả lời rằng bạn chỉ có thể hỗ trợ các câu hỏi liên quan đến giám sát an ninh."""
        try:
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            return f"Lỗi gọi LLM: {e}\n\nDữ liệu tìm thấy:\n{context}"
    return f"Kết quả truy xuất:\n{context}"

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "docs_indexed": rag.collection.count()}), 200

@app.route("/chat", methods=["POST"])
def chat():
    try:
        raw = request.data.decode("utf-8")
        if not raw:
            return jsonify({"error": "Request body trống"}), 400

        payload = json.loads(raw)

        question = payload.get("question", "").strip()
        if not question:
            return jsonify({"error": "Vui lòng nhập câu hỏi"}), 400

        metadata_filters = extract_filters(question)
        hits = rag.query(question, k=6, filter_metadata=metadata_filters)
        answer = synthesize_answer(question, hits)

        return jsonify({
            "question": question,
            "answer": answer,
            "filters_applied": metadata_filters,
            "data_sources": hits
        }), 200

    except json.JSONDecodeError as e:
        return jsonify({
            "error": "JSON không hợp lệ",
            "raw_body": raw,
            "detail": str(e)
        }), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/evidence/<incident_id>/frame", methods=["GET"])
def get_evidence_frame(incident_id: str):
    """
    Retrieve evidence frame for an incident from S3.

    Query params:
      - format: 'image' (default) or 'url' (return S3 URL only)

    Returns:
        - format=image: JPEG image file
        - format=url: JSON with frame_url
    """
    try:
        format_type = request.args.get("format", "image").lower()

        # Query Trino/Paimon to find frame location
        # For now, we'll construct S3 path based on incident_id
        # In production, query should be:
        # SELECT frame_url, camera_id, timestamp FROM paimon.security.violence_incidents
        # WHERE incident_id = ?

        # Fallback: Try common S3 path patterns
        # Pattern: evidence-frames/{camera_id}/{YYYY-MM-DD}/{incident_id}.jpg
        # We can query the RAG store for metadata

        query_result = rag.collection.get(
            where={"incident_id": incident_id},
            include=["metadatas"]
        )

        if not query_result or not query_result.get("metadatas"):
            return jsonify({
                "error": f"No incident found with ID: {incident_id}",
                "hint": "Ensure incident_id is correct and has been processed"
            }), 404

        metadata = query_result["metadatas"][0] if query_result["metadatas"] else {}
        camera_id = metadata.get("camera_id", "unknown")
        incident_date = metadata.get("date", "2026-04-28")

        s3_key = f"{camera_id}/{incident_date}/{incident_id}.jpg"

        # Return based on requested format
        if format_type == "url":
            return jsonify({
                "incident_id": incident_id,
                "camera_id": camera_id,
                "incident_date": incident_date,
                "frame_url": f"s3://{S3_BUCKET}/{s3_key}",
                "s3_endpoint": S3_ENDPOINT
            }), 200

        # Default: return actual image
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
            image_data = response["Body"].read()
            return send_file(
                BytesIO(image_data),
                mimetype="image/jpeg",
                as_attachment=False,
                download_name=f"{incident_id}.jpg"
            )
        except s3_client.exceptions.NoSuchKey:
            return jsonify({
                "error": f"Frame not found in S3: {s3_key}",
                "s3_bucket": S3_BUCKET,
                "s3_key": s3_key
            }), 404
        except Exception as e:
            return jsonify({
                "error": f"Failed to retrieve frame from S3: {str(e)}"
            }), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Server runs on port 5002 as configured in Docker
    app.run(host="0.0.0.0", port=5002, debug=False)