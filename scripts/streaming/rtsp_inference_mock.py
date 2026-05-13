"""
RTSP Inference Mock (ffmpeg backend)
Connects to each camera's RTSP stream, captures 1 frame/sec,
runs mock AI inference, publishes results to Kafka urban-safety-alerts.
"""

import csv
import json
import logging
import os
import random
import socket
import sys
import time
import threading
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── CONFIG ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",  "urban-safety-alerts")
METADATA_FILE   = os.getenv("METADATA_FILE", "/app/data/metadata/camera_registry.csv")
RTSP_FPS        = float(os.getenv("RTSP_FPS", "1"))
RTSP_TIMEOUT_S  = int(os.getenv("RTSP_TIMEOUT_S", "5"))
RECONNECT_DELAY = float(os.getenv("RECONNECT_DELAY_S", "2"))
STOP_FILE       = os.getenv("STOP_FILE", "/app/tmp/STOP")
MODEL_VERSION   = "VioMobileNet-mock-v2.1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("rtsp-mock")

# ── BANNER ─────────────────────────────────────────────────────────────────────
print("=" * 55)
print("  RTSP Inference Mock (ffmpeg backend)")
print("=" * 55)
print(f"  Kafka:        {KAFKA_BROKER} → {KAFKA_TOPIC}")
print(f"  Registry:     {METADATA_FILE}")
print(f"  Capture FPS:  {RTSP_FPS}")
print(f"  Stop file:    {STOP_FILE}")
print("=" * 55)


# ── KAFKA INIT ─────────────────────────────────────────────────────────────────
def _connect_kafka(max_retries: int = 5) -> KafkaProducer:
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            log.info("Kafka producer connected.")
            return producer
        except NoBrokersAvailable as e:
            log.warning("Kafka not ready (attempt %d/%d): %s", attempt, max_retries, e)
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
        log.error("Camera registry not found: %s", METADATA_FILE)
        sys.exit(1)


# ── MOCK INFERENCE ─────────────────────────────────────────────────────────────
_LABELS_WEIGHTS = [("normal", 0.70), ("violence", 0.20), ("crowd", 0.07), ("anomaly", 0.03)]
_LABELS, _WEIGHTS = zip(*_LABELS_WEIGHTS)


def mock_inference(camera: dict) -> dict:
    """Generate realistic mock inference result for a camera frame."""
    label = random.choices(_LABELS, weights=_WEIGHTS, k=1)[0]
    # Higher score variance for violence/crowd labels
    if label == "normal":
        score = random.uniform(0.02, 0.45)
    elif label == "violence":
        score = random.uniform(0.75, 0.99)
    elif label == "crowd":
        score = random.uniform(0.50, 0.80)
    else:
        score = random.uniform(0.40, 0.65)

    return {
        "incident_id":    str(uuid.uuid4()),
        "camera_id":      camera["camera_id"],
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "risk_score":     round(score, 4),
        "label":          label,
        "location":       camera.get("street", camera.get("ward", "Unknown")),
        "district":       camera.get("district", ""),
        "city":           camera.get("city", "TP. Hồ Chí Minh"),
        "latitude":       float(camera.get("latitude", 0)),
        "longitude":      float(camera.get("longitude", 0)),
        "model_version":  MODEL_VERSION,
        "frame_path":     f"rtsp-frames/{camera['camera_id']}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg",
        "source":         "rtsp_inference_mock",
    }


# ── STREAM AVAILABILITY CHECK ─────────────────────────────────────────────────
_MEDIAMTX_HOST = None
_MEDIAMTX_HLS_PORT = 8888


def _resolve_mediamtx_host(rtsp_url: str) -> str:
    """Extract host from rtsp://host:port/path."""
    try:
        return rtsp_url.split("://")[1].split(":")[0].split("/")[0]
    except Exception:
        return "mediamtx"


def _hls_stream_alive(cam_id: str, rtsp_url: str) -> bool:
    """
    Check stream availability via HLS playlist (HTTP, more reliable than ffprobe).
    MediaMTX serves HLS at http://<host>:8888/<path>/index.m3u8
    """
    global _MEDIAMTX_HOST
    if _MEDIAMTX_HOST is None:
        _MEDIAMTX_HOST = _resolve_mediamtx_host(rtsp_url)

    hls_url = f"http://{_MEDIAMTX_HOST}:{_MEDIAMTX_HLS_PORT}/{cam_id}/index.m3u8"
    try:
        req = urllib.request.urlopen(hls_url, timeout=RTSP_TIMEOUT_S)
        return req.status == 200
    except Exception:
        pass

    # Fallback: TCP socket check on RTSP port 8554
    try:
        with socket.create_connection((_MEDIAMTX_HOST, 8554), timeout=RTSP_TIMEOUT_S):
            return True
    except OSError:
        return False


# ── PER-CAMERA THREAD ──────────────────────────────────────────────────────────
def _camera_worker(camera: dict, producer: KafkaProducer, stop_event: threading.Event):
    cam_id   = camera["camera_id"]
    rtsp_url = camera.get("rtsp_url", f"rtsp://mediamtx:8554/{cam_id}")
    interval = 1.0 / RTSP_FPS

    logger = logging.getLogger(f"cam.{cam_id}")
    logger.info("Starting — RTSP: %s", rtsp_url)

    consecutive_failures = 0
    while not stop_event.is_set():
        # ── Check stream availability via HLS ───────────────────────────────
        if not _hls_stream_alive(cam_id, rtsp_url):
            consecutive_failures += 1
            logger.warning(
                "RTSP not reachable (attempt %d). Retrying in %.1fs",
                consecutive_failures, RECONNECT_DELAY,
            )
            stop_event.wait(RECONNECT_DELAY)
            continue

        consecutive_failures = 0

        # ── Run mock inference ───────────────────────────────────────────────
        result = mock_inference(camera)

        try:
            producer.send(KAFKA_TOPIC, value=result, key=cam_id)
            if result["label"] != "normal":
                logger.info(
                    "⚠  %s label=%s score=%.3f → %s",
                    cam_id, result["label"], result["risk_score"], KAFKA_TOPIC,
                )
            else:
                logger.debug("✓  %s label=normal score=%.3f", cam_id, result["risk_score"])
        except Exception as e:
            logger.error("Kafka publish error: %s", e)

        stop_event.wait(interval)

    logger.info("Stopped.")


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    cameras  = load_cameras()
    producer = _connect_kafka()

    stop_event = threading.Event()
    threads: list[threading.Thread] = []

    for camera in cameras:
        t = threading.Thread(
            target=_camera_worker,
            args=(camera, producer, stop_event),
            name=f"cam-{camera['camera_id']}",
            daemon=True,
        )
        t.start()
        threads.append(t)
        time.sleep(0.2)  # stagger starts slightly

    log.info("All %d camera threads started. Monitoring stop file: %s", len(cameras), STOP_FILE)

    try:
        while True:
            if Path(STOP_FILE).exists():
                log.info("STOP file detected. Shutting down gracefully.")
                stop_event.set()
                break
            time.sleep(2)
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt — shutting down.")
        stop_event.set()

    for t in threads:
        t.join(timeout=5)

    producer.flush()
    producer.close()
    log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
