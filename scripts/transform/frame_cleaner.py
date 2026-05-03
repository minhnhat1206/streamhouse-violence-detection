"""
Frame Evidence Cleanup Job
===========================
Batch job to delete evidence frames older than retention window (30 days).
Runs weekly to maintain bounded storage on MinIO S3.

Lifecycle Policy:
  - Frames created with incident timestamp
  - Automatic cleanup when older than 30 days
  - Metadata preserved in Paimon for forensic analysis

Run manually:
    python frame_cleaner.py

Or schedule via cron:
    0 2 * * 0  cd /app/scripts/transform && python frame_cleaner.py
"""
import json
import logging
import os
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)

# Configuration
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = "evidence-frames"
S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")
RETENTION_DAYS = int(os.getenv("FRAME_RETENTION_DAYS", "30"))

# Kafka for reporting
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")


def get_s3_client():
    """Create S3 client."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )


def get_kafka_producer():
    """Create Kafka producer for cleanup events."""
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def cleanup_old_frames(s3_client, cutoff_date: str) -> dict:
    """
    Delete frames older than cutoff date.

    Args:
        s3_client: boto3 S3 client
        cutoff_date: Date string (YYYY-MM-DD) to delete frames before

    Returns:
        Dict with cleanup stats: {deleted_count, deleted_size_mb, errors}
    """
    stats = {
        "deleted_count": 0,
        "deleted_size_bytes": 0,
        "errors": 0,
        "scanned_count": 0,
    }

    try:
        logger.info(f"[CLEANUP] Starting cleanup for frames before {cutoff_date}")
        logger.info(f"[CLEANUP] Bucket: {S3_BUCKET}, Retention: {RETENTION_DAYS} days")

        # List all objects in bucket
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=S3_BUCKET)

        delete_list = []

        for page in pages:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                stats["scanned_count"] += 1
                key = obj["Key"]
                last_modified = obj["LastModified"]

                # Extract date from S3 key path: {camera_id}/{YYYY-MM-DD}/{incident_id}.jpg
                parts = key.split("/")
                if len(parts) >= 2:
                    try:
                        object_date = parts[1]  # YYYY-MM-DD format
                        if object_date < cutoff_date:
                            delete_list.append(
                                {
                                    "Key": key,
                                    "LastModified": last_modified,
                                    "Size": obj.get("Size", 0),
                                }
                            )
                    except (ValueError, IndexError):
                        logger.warning(f"[CLEANUP] Skipping invalid key format: {key}")
                        continue

        logger.info(f"[CLEANUP] Found {len(delete_list)} frames to delete")

        # Delete in batches (S3 allows up to 1000 objects per request)
        batch_size = 100
        for i in range(0, len(delete_list), batch_size):
            batch = delete_list[i : i + batch_size]

            try:
                delete_request = {
                    "Objects": [{"Key": obj["Key"]} for obj in batch]
                }
                response = s3_client.delete_objects(
                    Bucket=S3_BUCKET, Delete=delete_request
                )

                deleted = response.get("Deleted", [])
                stats["deleted_count"] += len(deleted)
                for deleted_obj in deleted:
                    obj_info = next(
                        (o for o in batch if o["Key"] == deleted_obj["Key"]), None
                    )
                    if obj_info:
                        stats["deleted_size_bytes"] += obj_info.get("Size", 0)

                logger.info(
                    f"[CLEANUP] Deleted batch {i // batch_size + 1}: "
                    f"{len(deleted)} objects"
                )

            except ClientError as e:
                stats["errors"] += len(batch)
                logger.error(f"[CLEANUP] Batch delete failed: {e}")

    except ClientError as e:
        logger.error(f"[CLEANUP] Error during cleanup: {e}")
        stats["errors"] += 1

    return stats


def publish_cleanup_event(stats: dict):
    """Publish cleanup event to Kafka for monitoring."""
    try:
        producer = get_kafka_producer()

        event = {
            "event_type": "frame_cleanup",
            "timestamp": datetime.utcnow().isoformat(),
            "retention_days": RETENTION_DAYS,
            "deleted_count": stats["deleted_count"],
            "deleted_size_mb": round(stats["deleted_size_bytes"] / (1024 * 1024), 2),
            "scanned_count": stats["scanned_count"],
            "errors": stats["errors"],
        }

        producer.send("frame-cleanup-events", value=event)
        producer.flush()
        producer.close()

        logger.info(f"[KAFKA] Published cleanup event: {event}")

    except Exception as e:
        logger.error(f"[KAFKA] Failed to publish cleanup event: {e}")


def main():
    logger.info("[MAIN] Frame Evidence Cleanup Job")
    logger.info(f"  Bucket: {S3_BUCKET}")
    logger.info(f"  Endpoint: {S3_ENDPOINT}")
    logger.info(f"  Retention: {RETENTION_DAYS} days")

    # Calculate cutoff date
    cutoff_date = (datetime.utcnow() - timedelta(days=RETENTION_DAYS)).strftime(
        "%Y-%m-%d"
    )
    logger.info(f"  Cutoff date (delete before): {cutoff_date}")

    # Get S3 client
    s3_client = get_s3_client()

    # Verify bucket exists
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        logger.info(f"[S3] Connected to bucket: {S3_BUCKET}")
    except s3_client.exceptions.NoSuchBucket:
        logger.error(f"[S3] Bucket {S3_BUCKET} does not exist!")
        return
    except Exception as e:
        logger.error(f"[S3] Connection error: {e}")
        return

    # Run cleanup
    stats = cleanup_old_frames(s3_client, cutoff_date)

    # Log results
    logger.info("[RESULTS]")
    logger.info(f"  Scanned: {stats['scanned_count']} frames")
    logger.info(f"  Deleted: {stats['deleted_count']} frames")
    logger.info(f"  Size freed: {stats['deleted_size_bytes'] / (1024 * 1024):.2f} MB")
    logger.info(f"  Errors: {stats['errors']}")

    # Publish cleanup event to Kafka
    publish_cleanup_event(stats)

    logger.info("[MAIN] Cleanup job completed")


if __name__ == "__main__":
    main()
