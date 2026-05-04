"""
Inference Mock — simple data generator (no RTSP dependency).
Generates mock violence detection events and publishes to Kafka.
Use when RTSP streams are unavailable or for baseline throughput testing.
"""

import csv
import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── CONFIG ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",  "urban-safety-alerts")
METADATA_FILE = os.getenv("METADATA_FILE", "/app/data/metadata/camera_registry.csv")
INTERVAL_S    = float(os.getenv("INTERVAL_S", "2.0"))
STOP_FILE     = os.getenv("STOP_FILE", "/app/tmp/STOP")
MODEL_VERSION = "VioMobileNet-mock-v2.1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("inference-mock")


# ── KAFKA INIT ─────────────────────────────────────────────────────────────────
def _connect_kafka(max_retries: int = 5) -> KafkaProducer:
    for attempt in range(1, max_retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
        except NoBrokersAvailable as e:
            log.warning("Waiting for Kafka... (%d retries left): %s", max_retries - attempt, e)
            if attempt < max_retries:
                time.sleep(4)
    log.error("Could not connect to Kafka. Exiting.")
    sys.exit(1)


# ── CAMERA REGISTRY ────────────────────────────────────────────────────────────
def load_cameras() -> list[dict]:
    try:
        with open(METADATA_FILE, newline="", encoding="utf-8") as f:
            cameras = list(csv.DictReader(f))
        log.info("Loaded %d cameras from registry.", len(cameras))
        return cameras
    except FileNotFoundError:
        log.error("Registry not found: %s — generating synthetic cameras.", METADATA_FILE)
        return [{"camera_id": f"cam_{i:02d}", "street": f"Street {i}", "district": "Quận 1",
                 "city": "TP. Hồ Chí Minh", "latitude": "10.77", "longitude": "106.70",
                 "ward": ""} for i in range(1, 16)]


# ── EVENT GENERATOR ────────────────────────────────────────────────────────────
_LABELS_WEIGHTS = [("normal", 0.70), ("violence", 0.20), ("crowd", 0.07), ("anomaly", 0.03)]
_LABELS, _WEIGHTS = zip(*_LABELS_WEIGHTS)


def generate_event(camera: dict) -> dict:
    label = random.choices(_LABELS, weights=_WEIGHTS, k=1)[0]
    if label == "normal":
        score = random.uniform(0.02, 0.45)
    elif label == "violence":
        score = random.uniform(0.75, 0.99)
    elif label == "crowd":
        score = random.uniform(0.50, 0.80)
    else:
        score = random.uniform(0.40, 0.65)

    return {
        "incident_id":   str(uuid.uuid4()),
        "camera_id":     camera["camera_id"],
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "risk_score":    round(score, 4),
        "label":         label,
        "location":      camera.get("street", camera.get("ward", "Unknown")),
        "district":      camera.get("district", ""),
        "city":          camera.get("city", "TP. Hồ Chí Minh"),
        "latitude":      float(camera.get("latitude", 0) or 0),
        "longitude":     float(camera.get("longitude", 0) or 0),
        "model_version": MODEL_VERSION,
        "frame_path":    f"mock-frames/{camera['camera_id']}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg",
        "source":        "inference_mock",
    }


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    cameras  = load_cameras()
    producer = _connect_kafka()
    log.info("Inference Mock started. Publishing to %s every %.1fs per camera.", KAFKA_TOPIC, INTERVAL_S)

    counter = 0
    try:
        while True:
            if Path(STOP_FILE).exists():
                log.info("STOP file found. Exiting.")
                break

            camera = random.choice(cameras)
            event  = generate_event(camera)
            producer.send(KAFKA_TOPIC, value=event, key=camera["camera_id"])
            counter += 1

            if event["label"] != "normal":
                log.info("[%d] %s label=%s score=%.3f", counter, camera["camera_id"],
                         event["label"], event["risk_score"])
            elif counter % 50 == 0:
                log.info("[%d] heartbeat — last: %s normal %.3f",
                         counter, camera["camera_id"], event["risk_score"])

            time.sleep(INTERVAL_S)

    except KeyboardInterrupt:
        log.info("KeyboardInterrupt — stopping.")
    finally:
        producer.flush()
        producer.close()
        log.info("Sent %d events total.", counter)


if __name__ == "__main__":
    main()
