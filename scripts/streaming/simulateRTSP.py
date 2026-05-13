"""
simulateRTSP.py — Simulate RTSP streams using RWF-2000 video clips.
Reads playlist files and pushes each camera as an RTSP stream to MediaMTX.

Usage:
    python simulateRTSP.py start
"""

import glob
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

# ── CONFIG ─────────────────────────────────────────────────────────────────────
MEDIAMTX_HOST = os.getenv("MEDIAMTX_HOST", "mediamtx")
MEDIAMTX_PORT = int(os.getenv("MEDIAMTX_PORT", "8554"))
PLAYLIST_DIR  = os.getenv("PLAYLIST_DIR",  "/app/data/playlist")
FPS           = os.getenv("STREAM_FPS",    "15")
STOP_FILE     = os.getenv("STOP_FILE",     "/app/tmp/STOP")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("simulateRTSP")

# ── BANNER ─────────────────────────────────────────────────────────────────────
print("=" * 55)
print("  simulateRTSP — RWF-2000 RTSP Stream Simulator")
print("=" * 55)
print(f"  MediaMTX: {MEDIAMTX_HOST}:{MEDIAMTX_PORT}")
print(f"  Playlists: {PLAYLIST_DIR}")
print(f"  FPS: {FPS}")
print("=" * 55)


def get_playlists():
    """Return sorted list of (cam_id, playlist_path) tuples."""
    pattern = os.path.join(PLAYLIST_DIR, "playlist_*.txt")
    files = sorted(glob.glob(pattern))
    result = []
    for f in files:
        name = Path(f).stem  # e.g. playlist_cam_01
        cam_id = name.replace("playlist_", "")  # e.g. cam_01
        result.append((cam_id, f))
    return result


def stream_camera(cam_id: str, playlist_path: str, stop_event: threading.Event):
    """Stream a single camera via ffmpeg → RTSP to MediaMTX (loops forever)."""
    rtsp_url = f"rtsp://{MEDIAMTX_HOST}:{MEDIAMTX_PORT}/{cam_id}"
    logger = logging.getLogger(f"cam.{cam_id}")
    logger.info("Starting stream → %s", rtsp_url)

    while not stop_event.is_set():
        # Wait for MediaMTX to be ready
        time.sleep(1)

        cmd = [
            "ffmpeg",
            "-re",                          # Read at native frame rate
            "-stream_loop", "-1",           # Loop indefinitely
            "-f", "concat",                 # Concat demuxer
            "-safe", "0",                   # Allow absolute paths
            "-i", playlist_path,            # Input playlist
            "-vf", f"fps={FPS}",            # Set output FPS
            "-c:v", "libx264",             # H.264 encoder
            "-preset", "ultrafast",         # Low latency
            "-tune", "zerolatency",         # Zero latency tuning
            "-pix_fmt", "yuv420p",          # Compatible pixel format
            "-g", "30",                     # Keyframe every 30 frames
            "-b:v", "800k",                 # Video bitrate
            "-an",                          # No audio
            "-f", "rtsp",                   # RTSP output format
            "-rtsp_transport", "tcp",       # Use TCP for reliability
            rtsp_url,
        ]

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )

            # Monitor process
            while not stop_event.is_set():
                if proc.poll() is not None:
                    stderr_tail = ""
                    try:
                        stderr_tail = proc.stderr.read()[-200:]
                    except Exception:
                        pass
                    logger.warning("ffmpeg exited (code %d). Restarting in 3s... %s",
                                   proc.returncode, stderr_tail[-100:] if stderr_tail else "")
                    break
                time.sleep(2)

            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()

        except FileNotFoundError:
            logger.error("ffmpeg not found! Is it installed in the container?")
            stop_event.wait(10)
        except Exception as e:
            logger.error("Stream error: %s. Retrying in 3s...", e)

        if not stop_event.is_set():
            time.sleep(3)

    logger.info("Stopped.")


def main():
    playlists = get_playlists()
    if not playlists:
        log.error("No playlist files found in %s", PLAYLIST_DIR)
        sys.exit(1)

    log.info("Found %d camera playlists: %s", len(playlists),
             [c for c, _ in playlists])

    # Wait a few seconds for MediaMTX to be fully ready
    log.info("Waiting 5s for MediaMTX to be ready...")
    time.sleep(5)

    stop_event = threading.Event()

    # Handle SIGTERM/SIGINT
    def _shutdown(sig, frame):
        log.info("Shutdown signal received.")
        stop_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    # Start per-camera threads
    threads = []
    for cam_id, playlist_path in playlists:
        t = threading.Thread(
            target=stream_camera,
            args=(cam_id, playlist_path, stop_event),
            name=f"cam-{cam_id}",
            daemon=True,
        )
        t.start()
        threads.append(t)
        time.sleep(0.5)  # Stagger starts slightly

    log.info("All %d camera streams started. Monitoring stop file: %s",
             len(threads), STOP_FILE)

    try:
        while not stop_event.is_set():
            if Path(STOP_FILE).exists():
                log.info("STOP file detected. Shutting down.")
                stop_event.set()
                break
            time.sleep(2)
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt — shutting down.")
        stop_event.set()

    for t in threads:
        t.join(timeout=10)

    log.info("All streams stopped.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "start":
        main()
    else:
        main()
