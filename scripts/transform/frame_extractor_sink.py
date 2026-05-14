"""
Frame Evidence Extraction Service
==================================
Sidecar service that reads validated incidents from Kafka,
extracts base64 thumbnails, uploads to MinIO S3, and publishes
enriched records with frame URLs.

Pipeline:
  1. Subscribe to Kafka: hot-violence-alerts-valid
  2. Extract metadata.thumbnail (base64)
  3. Upload to S3: evidence-frames/{camera_id}/{date}/{incident_id}.jpg
  4. Publish enriched record: hot-violence-frames-uploaded topic
  5. Failures → frame-extraction-dlq dead-letter topic

Graceful stop:
  touch /app/tmp/STOP

Run standalone:
  python frame_extractor_sink.py
"""
import base64
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Optional

import boto3
from kafka import KafkaConsumer, KafkaProducer

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = "evidence-frames"
S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")
# Public-facing base URL for stored frame_url (browser-accessible)
# Inside Docker: http://minio:9000   |   From host browser: http://localhost:9000
S3_PUBLIC_URL = os.getenv("S3_PUBLIC_URL", "http://localhost:9000").rstrip("/")
STOP_FILE = os.getenv("STOP_FILE", "/app/tmp/STOP")
MAX_RETRIES = 3


def get_s3_client():
    """Create S3 client."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )


def upload_frame(
    s3_client,
    thumbnail_b64: str,
    camera_id: str,
    incident_date: str,
    incident_id: str,
    risk_score: float,
) -> Optional[str]:
    """
    Upload base64 frame to S3.

    Returns:
        S3 URL if successful, None otherwise
    """
    s3_key = f"{camera_id}/{incident_date}/{incident_id}.jpg"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            frame_bytes = base64.b64decode(thumbnail_b64)
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=frame_bytes,
                ContentType="image/jpeg",
                Metadata={
                    "incident_id": incident_id,
                    "camera_id": camera_id,
                    "risk_score": str(risk_score),
                    "capture_date": incident_date,
                },
            )
            # Return public HTTP URL (browser-accessible via MinIO API port 9000)
            return f"{S3_PUBLIC_URL}/{S3_BUCKET}/{s3_key}"

        except Exception as e:
            logger.warning(
                f"[S3] Upload attempt {attempt}/{MAX_RETRIES} failed for {incident_id}: {e}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
            else:
                logger.error(f"[S3] Final failure for {incident_id}: {e}")
                return None

    return None


def process_record(
    record: dict,
    s3_client,
    producer,
) -> bool:
    """
    Process single Kafka record.

    Returns:
        True if successfully processed, False otherwise
    """
    try:
        incident_id = record.get("event_id", "unknown")
        camera_id = record.get("camera_id", "unknown")
        risk_score = record.get("risk_score", 0.0)
        timestamp_str = record.get("timestamp", "")

        # Extract thumbnail from metadata
        metadata = record.get("metadata", {})
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}

        thumbnail_b64 = metadata.get("thumbnail", "")

        if not thumbnail_b64:
            logger.warning(f"[FRAME] No thumbnail for {incident_id}")
            producer.send(
                "frame-extraction-dlq",
                value={
                    "original": record,
                    "error": "No thumbnail found",
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )
            return False

        # Parse date from timestamp
        try:
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            incident_date = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            incident_date = datetime.utcnow().strftime("%Y-%m-%d")
            logger.warning(f"[FRAME] Failed to parse timestamp {timestamp_str}")

        # Upload to S3
        frame_url = upload_frame(
            s3_client,
            thumbnail_b64,
            camera_id,
            incident_date,
            incident_id,
            risk_score,
        )

        if not frame_url:
            logger.error(f"[FRAME] Failed to upload {incident_id} after retries")
            producer.send(
                "frame-extraction-dlq",
                value={
                    "original": record,
                    "error": "S3 upload failed after retries",
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )
            return False

        # Enrich and publish to downstream topic
        # Promote thumbnail_b64 to a top-level field so Flink job can map it directly
        enriched = dict(record)
        enriched["frame_url"] = frame_url
        enriched["frame_capture_ts"] = int(datetime.utcnow().timestamp() * 1000)
        enriched["thumbnail_b64"] = thumbnail_b64  # promote from metadata

        producer.send("hot-violence-frames-uploaded", value=enriched)
        logger.info(f"[FRAME] Processed {incident_id} → {frame_url}")
        return True

    except Exception as e:
        logger.error(f"[FRAME] Unexpected error: {e}", exc_info=True)
        return False


def main():
    logger.info("[MAIN] Starting Frame Evidence Extractor...")
    logger.info(f"  Kafka: {KAFKA_BROKER}")
    logger.info(f"  S3: {S3_BUCKET} @ {S3_ENDPOINT}")
    logger.info(f"  Stop file: {STOP_FILE}")

    # Create clients
    s3_client = get_s3_client()

    consumer = KafkaConsumer(
        "hot-violence-alerts-valid",
        bootstrap_servers=KAFKA_BROKER,
        group_id="frame-extractor-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else {},
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Test S3 connectivity
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        logger.info(f"[S3] Connected to bucket: {S3_BUCKET}")
    except s3_client.exceptions.NoSuchBucket:
        logger.error(f"[S3] Bucket {S3_BUCKET} does not exist!")
        sys.exit(1)
    except Exception as e:
        logger.error(f"[S3] Connection error: {e}")
        sys.exit(1)

    # Main loop
    try:
        logger.info("[MAIN] Listening for incidents...")
        for msg in consumer:
            # Check for stop signal
            if os.path.exists(STOP_FILE):
                logger.info("[MAIN] Stop signal detected. Exiting...")
                break

            if msg.value:
                process_record(msg.value, s3_client, producer)

    except Exception as e:
        logger.error(f"[KAFKA] Error: {e}", exc_info=True)
    except KeyboardInterrupt:
        logger.info("[MAIN] Interrupted by user")
    finally:
        consumer.close()
        producer.close()
        logger.info("[MAIN] Service stopped")


if __name__ == "__main__":
    main()
