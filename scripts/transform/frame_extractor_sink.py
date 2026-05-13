"""
Frame Extractor Sink
Consumes frame metadata from Kafka hot-violence-frames-uploaded,
downloads frames from MinIO (or in-memory mock), and re-saves to MinIO
under a structured path for downstream processing.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Optional MinIO (minio package may not be installed)
try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

# ── CONFIG ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",   "kafka:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC",    "hot-violence-frames-uploaded")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "frame-extractor-group")
S3_ENDPOINT    = os.getenv("S3_ENDPOINT",    "http://minio:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER",     "minio")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")
DEST_BUCKET    = os.getenv("DEST_BUCKET",    "rtsp-frames")
STOP_FILE      = os.getenv("STOP_FILE",      "/app/tmp/STOP")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("frame-extractor")


# ── MINIO CLIENT ───────────────────────────────────────────────────────────────
def _build_minio() -> "Minio | None":
    if not MINIO_AVAILABLE:
        log.warning("minio package not installed — frame storage disabled.")
        return None
    endpoint = S3_ENDPOINT.replace("http://", "").replace("https://", "")
    secure   = S3_ENDPOINT.startswith("https://")
    try:
        client = Minio(endpoint, access_key=MINIO_USER, secret_key=MINIO_PASSWORD, secure=secure)
        # Ensure bucket exists
        if not client.bucket_exists(DEST_BUCKET):
            client.make_bucket(DEST_BUCKET)
            log.info("Created bucket: %s", DEST_BUCKET)
        return client
    except Exception as e:
        log.error("MinIO connection failed: %s", e)
        return None


# ── KAFKA CONSUMER ─────────────────────────────────────────────────────────────
def _build_consumer(max_retries: int = 5) -> KafkaConsumer:
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                group_id=CONSUMER_GROUP,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                consumer_timeout_ms=5000,
            )
            log.info("Kafka consumer connected to topic: %s", KAFKA_TOPIC)
            return consumer
        except NoBrokersAvailable as e:
            log.warning("Kafka not ready (attempt %d/%d): %s", attempt, max_retries, e)
            if attempt < max_retries:
                time.sleep(4)
    log.error("Could not connect to Kafka. Exiting.")
    sys.exit(1)


# ── FRAME PROCESSING ───────────────────────────────────────────────────────────
def process_frame_event(event: dict, minio: "Minio | None") -> None:
    """Process a frame upload event and log/store metadata."""
    camera_id  = event.get("camera_id", "unknown")
    frame_path = event.get("frame_path", "")
    timestamp  = event.get("timestamp", datetime.utcnow().isoformat())
    label      = event.get("label", "unknown")
    score      = event.get("risk_score", 0.0)

    log.info(
        "Frame event: cam=%s label=%s score=%.3f path=%s",
        camera_id, label, score, frame_path,
    )

    if minio and frame_path:
        # Build structured destination path: year/month/day/camera/frame.jpg
        dt = datetime.utcnow()
        dest_key = f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/{camera_id}/{Path(frame_path).name}"
        try:
            # In production: download from source bucket, re-upload to dest
            # Here: write a metadata JSON as a placeholder
            import io
            meta_json = json.dumps(event, indent=2).encode("utf-8")
            minio.put_object(
                DEST_BUCKET,
                dest_key.replace(".jpg", ".meta.json"),
                io.BytesIO(meta_json),
                length=len(meta_json),
                content_type="application/json",
            )
            log.debug("Metadata written to %s/%s", DEST_BUCKET, dest_key)
        except Exception as e:
            log.error("MinIO write error: %s", e)


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    minio    = _build_minio()
    consumer = _build_consumer()

    log.info("Frame Extractor Sink running. Listening on %s ...", KAFKA_TOPIC)
    processed = 0

    try:
        while True:
            if Path(STOP_FILE).exists():
                log.info("STOP file detected. Exiting.")
                break

            for message in consumer:
                if Path(STOP_FILE).exists():
                    break
                try:
                    process_frame_event(message.value, minio)
                    processed += 1
                except Exception as e:
                    log.error("Error processing message offset=%d: %s", message.offset, e)

    except KeyboardInterrupt:
        log.info("KeyboardInterrupt — stopping.")
    finally:
        consumer.close()
        log.info("Processed %d frame events total.", processed)


if __name__ == "__main__":
    main()
