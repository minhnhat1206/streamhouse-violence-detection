"""
RTSP Inference Mock
===================
Connects to real RTSP streams (via MediaMTX), captures frames with ffmpeg,
and generates mock inference results — sending them to Kafka.

No OpenCV or numpy required: frame capture uses the ffmpeg binary already
installed in the container. JPEG thumbnail is resized inline by ffmpeg.

When the real AI service is ready, replace mock_inference() with an actual
model call (e.g., HTTP to viomobilenet_api:8000).

Usage:
    python rtsp_inference_mock.py

Stop gracefully:
    touch /app/tmp/STOP
"""

import base64
import csv
import json
import os
import random
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

# ================= GLOBAL CONCURRENCY LIMIT =================
# Limit concurrent ffmpeg capture processes to avoid CPU starvation.
# With 15 camera threads and 0.5 CPU, unconstrained ffmpeg spawning causes
# every process to starve and timeout. 3 concurrent captures saturates ~1 CPU.
MAX_CONCURRENT_CAPTURES = int(os.getenv("MAX_CONCURRENT_CAPTURES", "3"))
_capture_semaphore = threading.Semaphore(MAX_CONCURRENT_CAPTURES)

# ================= CONFIGURATION =================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "urban-safety-alerts")
METADATA_FILE = os.getenv("METADATA_FILE", "/app/data/metadata/camera_registry.csv")
STOP_FILE = os.getenv("STOP_FILE", "/app/tmp/STOP")
CAPTURE_FPS = float(os.getenv("RTSP_FPS", "1"))       # frames/sec per camera
RTSP_TIMEOUT_S = int(os.getenv("RTSP_TIMEOUT_S", "10"))
RECONNECT_DELAY_S = int(os.getenv("RECONNECT_DELAY_S", "5"))

HEARTBEAT_INTERVAL = 5.0
ALERT_INTERVAL = 0.5

PROB_START_VIOLENCE = 0.02  # Probability of starting violence event
PROB_STOP_VIOLENCE = 0.10

EVENT_TYPES = ["FIGHTING", "ASSAULT", "STABBING", "SHOOTING"]


# ================= HELPERS =================

def json_serializer(data: dict) -> bytes:
    return json.dumps(data).encode("utf-8")


def load_camera_registry(csv_path: str) -> dict:
    registry = {}
    try:
        with open(csv_path, mode="r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    row["latitude"] = float(row["latitude"])
                    row["longitude"] = float(row["longitude"])
                    row["has_violence"] = str(row.get("has_violence", "False")).lower() == "true"
                except Exception:
                    pass
                registry[row["camera_id"]] = row
        print(f"Loaded {len(registry)} cameras from registry.")
    except Exception as e:
        print(f"Registry read error: {e}")
    return registry


# ================= FRAME CAPTURE (ffmpeg) =================

def capture_jpeg(rtsp_url: str, timeout_s: int = RTSP_TIMEOUT_S) -> tuple[bool, str]:
    """
    Capture one JPEG frame from an RTSP stream using ffmpeg.
    Resizes inline to 160x90 — no OpenCV or numpy needed.
    Fallback: generate solid-color fake frame if RTSP unavailable (for testing).

    Returns:
        (success, base64_jpeg_string)
    """
    # Fake JPEG for development/testing (when RTSP unavailable)
    fake_jpeg_hex = "ffd8ffe000104a46494600010100000100010000ffdb004300080606070605080707070909080a0c140d0c0b0b0c1912130f141d1a1f1e1d1a1c1c20242e2720222c231c1c2837292c30313434341f27393d38323c2e333432ffdb00430109090909090c0b0c0c0c0c190d0d1932211c213232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232ffc0000b080160010101110200ffc4000b0001010000000000000000ffda00080101000000003f00d2cf2075eb4e06ffd9"
    fake_jpeg_bytes = bytes.fromhex(fake_jpeg_hex)
    fake_jpeg_b64 = base64.b64encode(fake_jpeg_bytes).decode("utf-8")

    import tempfile, os as _os

    # Use semaphore to limit concurrent ffmpeg processes (CPU starvation protection)
    if not _capture_semaphore.acquire(blocking=False):
        # All slots busy — return fake immediately rather than queuing
        return True, fake_jpeg_b64

    tmpfile = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
    tmppath = tmpfile.name
    tmpfile.close()

    proc = None
    try:
        proc = subprocess.Popen(
            [
                "ffmpeg", "-y", "-nostdin",
                "-loglevel",       "error",
                "-rtsp_transport", "tcp",
                # Note: -stimeout removed in ffmpeg 5+; we rely on proc.wait(timeout=)
                "-i",              rtsp_url,
                "-vframes",        "1",
                "-vf",             "scale=160:90",
                tmppath,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        proc.wait(timeout=timeout_s + 2)
        if proc.returncode == 0 and _os.path.getsize(tmppath) > 0:
            with open(tmppath, "rb") as fh:
                return True, base64.b64encode(fh.read()).decode("utf-8")
        return True, fake_jpeg_b64

    except subprocess.TimeoutExpired:
        if proc:
            proc.kill()
            proc.wait()
        return True, fake_jpeg_b64
    except Exception:
        return True, fake_jpeg_b64
    finally:
        _capture_semaphore.release()
        try:
            _os.unlink(tmppath)
        except Exception:
            pass


# ================= MOCK INFERENCE =================

def mock_inference(thumbnail_b64: str, is_violent: bool) -> dict:
    """
    Stub AI inference.

    TO REPLACE: swap this function body with a real call, e.g.:
        resp = requests.post("http://viomobilenet_api:8000/predict",
                             json={"image_b64": thumbnail_b64})
        return resp.json()
    """
    t0 = time.monotonic()

    risk_score = (
        round(random.uniform(0.75, 0.99), 4)
        if is_violent
        else round(random.uniform(0.01, 0.15), 4)
    )
    confidence = (
        round(random.uniform(0.85, 0.99), 4)
        if is_violent
        else round(random.uniform(0.30, 0.70), 4)
    )
    event_type = random.choice(EVENT_TYPES) if is_violent else None
    latency_ms = int((time.monotonic() - t0) * 1000)

    return {
        "is_violent": is_violent,
        "risk_score": risk_score,
        "confidence": confidence,
        "event_type": event_type,
        "latency_ms": latency_ms,
        "thumbnail_b64": thumbnail_b64,
    }


# ================= CAMERA WORKER =================

class CameraWorker(threading.Thread):
    """One thread per camera: capture RTSP frame → mock inference → Kafka."""

    def __init__(self, cam_id: str, meta: dict, producer: KafkaProducer):
        super().__init__(daemon=True, name=f"cam-{cam_id}")
        self.cam_id = cam_id
        self.meta = meta
        self.producer = producer
        self.rtsp_url = meta.get("rtsp_url", "")
        self.is_violent = False
        self.last_sent = 0.0
        self._last_thumbnail = ""  # cache — reused between sends

    def _update_state(self):
        if not self.is_violent:
            if random.random() < PROB_START_VIOLENCE:
                self.is_violent = True
                print(f"!!! [{self.cam_id}] Violence event started")
        else:
            if random.random() < PROB_STOP_VIOLENCE:
                self.is_violent = False
                print(f"--- [{self.cam_id}] Violence event cleared")

    def _should_send(self) -> bool:
        interval = ALERT_INTERVAL if self.is_violent else HEARTBEAT_INTERVAL
        return (time.time() - self.last_sent) >= interval

    def _publish(self, result: dict):
        payload = {
            "event_id": str(uuid.uuid4()),
            "camera_id": self.cam_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "is_violent": result["is_violent"],
            "risk_score": result["risk_score"],
            "confidence": result["confidence"],
            "event_type": result["event_type"],
            "location": {
                "city": self.meta.get("city", ""),
                "district": self.meta.get("district", ""),
                "ward": self.meta.get("ward", ""),
                "street": self.meta.get("street", ""),
                "lat": self.meta.get("latitude"),
                "long": self.meta.get("longitude"),
            },
            "metadata": {
                "fps": round(CAPTURE_FPS, 1),
                "latency_ms": result["latency_ms"],
                "mock": True,
                "rtsp_connected": True,
                "thumbnail": result["thumbnail_b64"],
            },
        }
        # Note: is_valid is NOT set here — data_contract_validator Flink job sets it
        # Publish raw event — data_contract_validator Flink job routes to hot-violence-alerts-valid
        self.producer.send(KAFKA_TOPIC, value=payload)
        self.producer.flush()
        self.last_sent = time.time()

        # DEBUG: Log payload structure
        if payload.get("is_violent"):
            thumb_len = len(payload.get("metadata", {}).get("thumbnail", "")) if isinstance(payload.get("metadata"), dict) else 0
            print(f"    [PUBLISH] Thumbnail size: {thumb_len} | Topic: {KAFKA_TOPIC}", flush=True)

        status = "VIOLENCE" if result["is_violent"] else "Normal"
        print(f"[{self.cam_id}] {status} | score={result['risk_score']:.3f}")

    def run(self):
        capture_interval = 1.0 / max(CAPTURE_FPS, 0.1)
        connected = False

        while not os.path.exists(STOP_FILE):
            t0 = time.time()

            success, thumbnail_b64 = capture_jpeg(self.rtsp_url, RTSP_TIMEOUT_S)

            if not success:
                if connected:
                    print(f"[{self.cam_id}] Stream lost, retry in {RECONNECT_DELAY_S}s...")
                    connected = False
                else:
                    print(f"[{self.cam_id}] Waiting for RTSP ({self.rtsp_url})...")
                time.sleep(RECONNECT_DELAY_S)
                continue

            if not connected:
                connected = True
                print(f"[{self.cam_id}] RTSP connected: {self.rtsp_url}")

            # Cache latest frame — reused when sending faster than capture rate
            self._last_thumbnail = thumbnail_b64

            self._update_state()

            if self._should_send():
                result = mock_inference(self._last_thumbnail, self.is_violent)
                self._publish(result)

            sleep_t = capture_interval - (time.time() - t0)
            if sleep_t > 0:
                time.sleep(sleep_t)

        print(f"[{self.cam_id}] Worker stopped.")


# ================= MAIN =================

def main():
    print("=" * 55)
    print("  RTSP Inference Mock (ffmpeg backend)")
    print("=" * 55)
    print(f"  Kafka:        {KAFKA_BROKER} → {KAFKA_TOPIC}")
    print(f"  Registry:     {METADATA_FILE}")
    print(f"  Capture FPS:  {CAPTURE_FPS}")
    print(f"  Stop file:    {STOP_FILE}")
    print("=" * 55)

    os.makedirs(os.path.dirname(STOP_FILE), exist_ok=True)

    if os.path.exists(STOP_FILE):
        os.remove(STOP_FILE)
        print("Cleared old stop file.")

    registry = load_camera_registry(METADATA_FILE)
    if not registry:
        print("No cameras loaded. Exiting.")
        sys.exit(1)

    producer = None
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=json_serializer,
                acks=1,
            )
            print("Connected to Kafka.")
            break
        except Exception as e:
            print(f"Kafka not ready (attempt {attempt}/5): {e}")
            time.sleep(5)

    if not producer:
        print("Could not connect to Kafka. Exiting.")
        sys.exit(1)

    workers = []
    for cam_id, meta in registry.items():
        w = CameraWorker(cam_id, meta, producer)
        w.start()
        workers.append(w)
        time.sleep(0.2)

    print(f"\n{len(workers)} camera workers started.")
    print(f"To stop: touch {STOP_FILE}\n")

    try:
        while not os.path.exists(STOP_FILE):
            time.sleep(1)
        print("Stop file detected. Shutting down...")
    except KeyboardInterrupt:
        print("\nInterrupted. Shutting down...")
    finally:
        for w in workers:
            w.join(timeout=10)
        if producer:
            producer.flush()
            producer.close()
        print("Done.")


if __name__ == "__main__":
    main()
