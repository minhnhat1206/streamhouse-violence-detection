"""
Inference Mock (standalone — no RTSP required)
================================================
Generates mock violence-detection events and publishes to Kafka.
Acts as a drop-in data source when the streaming profile is not active.

Stop gracefully:
    touch /app/tmp/STOP
"""

import csv
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "urban-safety-alerts")
METADATA_FILE = os.getenv("METADATA_FILE", "/app/data/metadata/camera_registry.csv")
STOP_FILE     = os.getenv("STOP_FILE", "/app/tmp/STOP")

INTERVAL_S          = float(os.getenv("INTERVAL_S", "1.0"))   # seconds between events
VIOLENCE_PROB       = float(os.getenv("VIOLENCE_PROB", "0.05"))
EVENT_TYPES         = ["FIGHTING", "ASSAULT", "STABBING", "SHOOTING", "NONE"]


def load_cameras(path: str) -> list[dict]:
    cameras = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cameras.append(row)
    except FileNotFoundError:
        # Fallback: 5 built-in cameras
        for i in range(1, 6):
            cameras.append({"camera_id": f"cam_{i:02d}", "location": f"Location {i}"})
    return cameras


def make_event(camera: dict, is_violent: bool) -> dict:
    risk = round(random.uniform(0.70, 0.99) if is_violent else random.uniform(0.01, 0.40), 4)
    return {
        "event_id":   str(uuid.uuid4()),
        "camera_id":  camera.get("camera_id", "cam_01"),
        "timestamp":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "risk_score": risk,
        "is_violent": is_violent,
        "event_type": random.choice(EVENT_TYPES[:4]) if is_violent else "NONE",
        "confidence": round(random.uniform(0.80, 0.99) if is_violent else random.uniform(0.50, 0.85), 4),
        "location":   camera.get("location", "Unknown"),
        "frame_data": None,
        "is_valid":   True,
    }


def main():
    cameras = load_cameras(METADATA_FILE)
    print(f"[inference-mock] Loaded {len(cameras)} cameras", flush=True)

    producer = None
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            print(f"[inference-mock] Connected to Kafka at {KAFKA_BROKER}", flush=True)
            break
        except KafkaError as exc:
            print(f"[inference-mock] Kafka not ready ({exc}), retry {attempt+1}/10…", flush=True)
            time.sleep(5)
    else:
        print("[inference-mock] Could not connect to Kafka after 10 attempts — exiting", flush=True)
        sys.exit(1)

    count = 0
    try:
        while True:
            if os.path.exists(STOP_FILE):
                print("[inference-mock] STOP file detected — exiting gracefully", flush=True)
                break

            camera    = random.choice(cameras)
            is_violent = random.random() < VIOLENCE_PROB
            event     = make_event(camera, is_violent)

            producer.send(KAFKA_TOPIC, key=event["camera_id"], value=event)
            count += 1
            if count % 50 == 0:
                producer.flush()
                print(f"[inference-mock] {count} events sent", flush=True)

            time.sleep(INTERVAL_S)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print(f"[inference-mock] Stopped. Total events sent: {count}", flush=True)


if __name__ == "__main__":
    main()
