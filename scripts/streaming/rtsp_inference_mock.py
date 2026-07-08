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
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "34.124.131.144:9093")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "urban-safety-alerts")
METADATA_FILE = os.getenv("METADATA_FILE", "/root/streamhouse/data/metadata/camera_registry.csv")
STOP_FILE = os.getenv("STOP_FILE", "/tmp/STOP")
CAPTURE_FPS = float(os.getenv("RTSP_FPS", "1"))       # frames/sec per camera
RTSP_TIMEOUT_S = int(os.getenv("RTSP_TIMEOUT_S", "10"))
RECONNECT_DELAY_S = int(os.getenv("RECONNECT_DELAY_S", "5"))
# Comma-separated list of active camera IDs. If unset, all cameras are used.
_active_env = os.getenv("ACTIVE_CAMERAS", "").strip()
ACTIVE_CAMERAS = set(_active_env.split(",")) if _active_env else None

HEARTBEAT_INTERVAL = 5.0
ALERT_INTERVAL = 0.5

# Sessionization: một VỤ (incident) = chuỗi event violent liên tục của 1 camera.
# incident_uid sinh khi violence bắt đầu, giữ nguyên đến khi hết violent liên tục
# INCIDENT_GAP_SECONDS (chống flapping score quanh threshold). Downstream (Fluss/
# Paimon/Iceberg) GROUP BY incident_uid để đếm đúng số vụ thay vì đếm raw event.
INCIDENT_GAP_SECONDS = float(os.getenv("INCIDENT_GAP_SECONDS", "30"))

# Kích thước ảnh evidence: to khi đang violent (thấy rõ bbox), nhỏ cho heartbeat.
VIOLENT_FRAME_SCALE = os.getenv("VIOLENT_FRAME_SCALE", "640:360")
NORMAL_FRAME_SCALE = os.getenv("NORMAL_FRAME_SCALE", "160:90")

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

def capture_jpeg(rtsp_url: str, timeout_s: int = RTSP_TIMEOUT_S,
                 scale: str = NORMAL_FRAME_SCALE) -> tuple[bool, str]:
    """
    Capture one JPEG frame from an RTSP stream using ffmpeg.
    Resizes inline theo `scale` (violent: 640x360 để thấy rõ bbox; heartbeat: 160x90).

    Returns:
        (success, base64_jpeg_string)
        success=False khi ffmpeg thất bại → caller fallback từ stream _bbox về raw.
        Khi semaphore đầy trả (True, fake) để pipeline không nghẽn.
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
                "-vf",             f"scale={scale}",
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
        return False, fake_jpeg_b64

    except subprocess.TimeoutExpired:
        if proc:
            proc.kill()
            proc.wait()
        return False, fake_jpeg_b64
    except Exception:
        return False, fake_jpeg_b64
    finally:
        _capture_semaphore.release()
        try:
            _os.unlink(tmppath)
        except Exception:
            pass


# ================= REAL INFERENCE =================

def real_inference(camera_id: str, thumbnail_b64: str) -> dict:
    """
    Read real-time inference status written by visualize_stream.py.
    Returns dict with inference_ms for E2E latency tracking.
    """
    import json
    path = f"/tmp/status_{camera_id}.json"
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                data = json.load(f)
                score = float(data.get("score", 0.0))
                is_violent = bool(data.get("is_violent", False))
                fps = float(data.get("fps", 12.0))
                confidence = float(max(score, 1.0 - score))
                inf_ms = float(data.get("inference_ms", 0.0))
                return {
                    "is_violent": is_violent,
                    "risk_score": round(score, 4),
                    "confidence": round(confidence, 4),
                    "event_type": "FIGHTING" if is_violent else None,
                    "latency_ms": round(inf_ms, 2),
                    "thumbnail_b64": thumbnail_b64,
                    "fps": fps,
                    "mock": False
                }
        except Exception as e:
            print(f"[{camera_id}] Error reading status file: {e}")

    # Fallback to inactive normal state if status file doesn't exist
    return {
        "is_violent": False,
        "risk_score": 0.0,
        "confidence": 1.0,
        "event_type": None,
        "latency_ms": 0,
        "thumbnail_b64": thumbnail_b64,
        "fps": 12.0,
        "mock": False
    }


# ================= CAMERA WORKER =================

class CameraWorker(threading.Thread):
    """One thread per camera: capture RTSP frame → mock inference → Kafka."""

    def __init__(self, cam_id: str, meta: dict, producer: KafkaProducer):
        super().__init__(daemon=True, name=f"cam-{cam_id}")
        self.cam_id = cam_id
        self.meta = meta
        self.producer = producer
        self.rtsp_url = meta.get("rtsp_url", "").replace("rtsp://mediamtx:", "rtsp://localhost:")
        self.is_violent = False
        self.last_sent = 0.0
        self._last_thumbnail = ""  # cache — reused between sends
        # Episode (vụ) đang mở của camera này — None khi không có bạo lực
        self.incident_uid = None
        self._last_violent_ts = 0.0

    # (removed _update_state as it's now queried from the real visualizer status)

    def _should_send(self) -> bool:
        interval = ALERT_INTERVAL if self.is_violent else HEARTBEAT_INTERVAL
        return (time.time() - self.last_sent) >= interval

    def _publish(self, result: dict):
        # Record Kafka send timestamp for E2E latency measurement
        kafka_sent_at = time.time()
        payload = {
            "event_id": str(uuid.uuid4()),
            "incident_uid": self.incident_uid,
            "camera_id": self.cam_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "is_violent": result["is_violent"],
            "risk_score": result["risk_score"],
            "confidence": result["confidence"],
            "event_type": result["event_type"],
            "people_count": len(result.get("people", [])),
            "location": {
                "city": self.meta.get("city", ""),
                "district": self.meta.get("district", ""),
                "ward": self.meta.get("ward", ""),
                "street": self.meta.get("street", ""),
                "lat": self.meta.get("latitude"),
                "long": self.meta.get("longitude"),
            },
            "metadata": {
                "fps": round(result.get("fps", CAPTURE_FPS), 1),
                "latency_ms": result["latency_ms"],
                "inference_ms": result.get("latency_ms", 0),
                "mock": result.get("mock", False),
                "rtsp_connected": True,
                "thumbnail": result["thumbnail_b64"],
                # E2E latency tracking: downstream subtracts this from their write_ts
                "kafka_sent_at": round(kafka_sent_at, 3),
                # Bbox enrichment
                "bbox_status": result.get("bbox_status", "unavailable"),
                "people": result.get("people", []),
            },
        }
        # Note: is_valid is NOT set here — data_contract_validator Flink job sets it
        # Publish raw event — data_contract_validator Flink job routes to hot-violence-alerts-valid
        print(f"[{self.cam_id}] Publishing event to Kafka (mock={result.get('mock')}, is_violent={result['is_violent']}, bbox_status={result.get('bbox_status')})...", flush=True)
        try:
            self.producer.send(KAFKA_TOPIC, value=payload)
        except Exception as e:
            print(f"[{self.cam_id}] Kafka Send Error: {e}", flush=True)
        self.last_sent = time.time()

        # DEBUG: Log payload structure
        if payload.get("is_violent"):
            thumb_len = len(payload.get("metadata", {}).get("thumbnail", "")) if isinstance(payload.get("metadata"), dict) else 0
            print(f"    [PUBLISH] Thumbnail size: {thumb_len} | Topic: {KAFKA_TOPIC} | People count: {len(payload['metadata']['people'])}", flush=True)

        status = "VIOLENCE" if result["is_violent"] else "Normal"
        print(f"[{self.cam_id}] {status} | score={result['risk_score']:.3f}")

    def run(self):
        capture_interval = 1.0 / max(CAPTURE_FPS, 0.1)
        connected = False

        while not os.path.exists(STOP_FILE):
            t0 = time.time()

            # 1. Query bboxAPI to check if active and fetch people list
            bbox_status = "unavailable"
            people_list = []
            try:
                import urllib.request, json
                url = f"http://localhost:8081/streams/{self.cam_id}/latest"
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=1.0) as resp:
                    bbox_data = json.loads(resp.read().decode("utf-8"))
                    bbox_status = "ok"
                    people_list = bbox_data.get("people", [])
            except Exception:
                bbox_status = "unavailable"

            # 2. Determine capture URL: try bbox stream if bboxAPI is active
            capture_url = self.rtsp_url
            if bbox_status == "ok":
                # Convert e.g., rtsp://localhost:8554/cam_01 to rtsp://localhost:8554/cam_01_bbox
                capture_url = self.rtsp_url + "_bbox"

            # Violent (theo kết quả vòng trước) → ảnh 640x360 để bbox nhìn rõ trong evidence
            frame_scale = VIOLENT_FRAME_SCALE if self.is_violent else NORMAL_FRAME_SCALE
            success, thumbnail_b64 = capture_jpeg(capture_url, RTSP_TIMEOUT_S, scale=frame_scale)
            if not success and capture_url != self.rtsp_url:
                # Fallback to raw stream if bbox capture failed
                print(f"[{self.cam_id}] Bbox stream capture failed, falling back to raw stream...", flush=True)
                capture_url = self.rtsp_url
                success, thumbnail_b64 = capture_jpeg(capture_url, RTSP_TIMEOUT_S, scale=frame_scale)

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
                print(f"[{self.cam_id}] RTSP connected: {capture_url}")

            # Cache latest frame — reused when sending faster than capture rate.
            # Chỉ cache frame THẬT (fake placeholder ~292 bytes khi semaphore bận
            # không được đè lên frame thật đã có — evidence phải là ảnh thật).
            if success and len(thumbnail_b64) > 1000:
                self._last_thumbnail = thumbnail_b64
            elif not self._last_thumbnail:
                self._last_thumbnail = thumbnail_b64

            # Get real inference result from status file
            result = real_inference(self.cam_id, self._last_thumbnail)
            result["bbox_status"] = bbox_status
            result["people"] = people_list
            self.is_violent = result["is_violent"]

            # Sessionization: mở/đóng vụ (incident) theo trạng thái violent.
            # uid giữ nguyên qua các dip ngắn (< INCIDENT_GAP_SECONDS) để 1 vụ
            # không bị tách thành nhiều vụ khi score dao động quanh threshold.
            now_t = time.time()
            if self.is_violent:
                if self.incident_uid is None:
                    self.incident_uid = str(uuid.uuid4())
                    print(f"[{self.cam_id}] Incident OPEN: {self.incident_uid}", flush=True)
                self._last_violent_ts = now_t
            elif self.incident_uid and (now_t - self._last_violent_ts) > INCIDENT_GAP_SECONDS:
                print(f"[{self.cam_id}] Incident CLOSE: {self.incident_uid}", flush=True)
                self.incident_uid = None

            if self._should_send():
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

    if ACTIVE_CAMERAS:
        registry = {k: v for k, v in registry.items() if k in ACTIVE_CAMERAS}
        print(f"Filtered to {len(registry)} active cameras: {sorted(registry.keys())}")

    producer = None
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=json_serializer,
                acks=1,
                api_version=(3, 7, 0),
                enable_idempotence=False,
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
