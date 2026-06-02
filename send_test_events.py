#!/usr/bin/env python3
"""Send test events to GCP Kafka to verify data pipeline."""
import json, uuid, time, random
from datetime import datetime, timezone
from kafka import KafkaProducer

GCP_KAFKA = "34.124.131.144:9093"
TOPIC = "urban-safety-alerts"

cameras = [f"cam_{i:02d}" for i in range(1, 16)]

producer = KafkaProducer(
    bootstrap_servers=GCP_KAFKA,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    request_timeout_ms=10000,
)

print(f"Sending 20 test events to {GCP_KAFKA} -> {TOPIC}")
sent = 0
for i in range(20):
    cam = random.choice(cameras)
    risk = round(random.uniform(0.6, 0.99), 3)
    event = {
        "event_id":   str(uuid.uuid4()),
        "camera_id":  cam,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "risk_score": risk,
        "confidence": round(random.uniform(0.7, 0.99), 3),
        "is_violent": True,
        "event_type": "FIGHT",
        "frame_data": None,
        "frame_url":  f"http://minio:9000/evidence-frames/{cam}/test/{i}.jpg",
    }
    future = producer.send(TOPIC, key=cam, value=event)
    try:
        meta = future.get(timeout=5)
        sent += 1
        print(f"  [{i+1:2d}] cam={cam} risk={risk:.3f} p={meta.partition} off={meta.offset}")
    except Exception as e:
        print(f"  [{i+1:2d}] FAILED: {e}")
    time.sleep(0.1)

producer.flush()
print(f"\nSent {sent}/20 events OK.")
