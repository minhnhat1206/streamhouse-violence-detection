"""
RTSP Pusher Service
===================
Reads .avi clips from the RWF-2000 dataset and pushes them as live RTSP streams
into MediaMTX (rtsp://mediamtx:8554/cam_XX).

One ffmpeg subprocess per camera, loops its playlist continuously.
Does NOT call any AI API — purely an RTSP source for rtsp-inference-mock.

Camera assignment:
  has_violence=True  → Fight clips  (ffmpeg pushes fight footage)
  has_violence=False → NonFight clips

Env vars:
  METADATA_FILE   Path to camera_registry.csv  (/app/data/metadata/camera_registry.csv)
  FIGHT_DIR       Fight clips directory          (/app/data/raw/RWF-2000/train/Fight)
  NON_FIGHT_DIR   NonFight clips directory       (/app/data/raw/RWF-2000/train/NonFight)
  MEDIAMTX_HOST   MediaMTX hostname              (mediamtx)
  MAX_CAMERAS     Max cameras to push            (4) — limit CPU usage
  CLIPS_PER_CAM   Clips per camera playlist      (6)
  STOP_FILE       Graceful stop trigger          (/app/tmp/STOP)

Graceful stop:
  docker exec rtsp_pusher touch /app/tmp/STOP
"""

import csv
import os
import random
import subprocess
import sys
import tempfile
import threading
import time

# ─── Configuration ────────────────────────────────────────────────────────────
METADATA_FILE    = os.getenv("METADATA_FILE",    "/app/data/metadata/camera_registry.csv")
FIGHT_DIR        = os.getenv("FIGHT_DIR",        "/app/data/raw/RWF-2000/train/Fight")
NON_FIGHT_DIR    = os.getenv("NON_FIGHT_DIR",    "/app/data/raw/RWF-2000/train/NonFight")
FIGHT_MANIFEST   = os.getenv("FIGHT_MANIFEST",   "/app/data/metadata/fight_clips_manifest.txt")
NONFIGHT_MANIFEST= os.getenv("NONFIGHT_MANIFEST","/app/data/metadata/nonfight_clips_manifest.txt")
MEDIAMTX_HOST    = os.getenv("MEDIAMTX_HOST",    "mediamtx")
MAX_CAMERAS      = int(os.getenv("MAX_CAMERAS",  "4"))   # keep CPU manageable
CLIPS_PER_CAM    = int(os.getenv("CLIPS_PER_CAM","6"))
STOP_FILE        = os.getenv("STOP_FILE",        "/app/tmp/STOP")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def load_clips(directory: str, manifest: str = "") -> list[str]:
    """
    Return sorted list of .avi paths.
    Tries directory listing first; falls back to manifest file (one filename per line)
    when directory listing fails (e.g. Docker-on-Windows I/O error with large dirs).
    """
    if not os.path.exists(directory):
        print(f"[WARN] Clip directory not found: {directory}")
        return []
    # Try direct listing
    try:
        clips = [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if f.lower().endswith(".avi") and not f.startswith(".")
        ]
        clips.sort()
        if clips:
            return clips
    except OSError as e:
        print(f"[WARN] Directory listing failed ({e}), trying manifest fallback...")

    # Manifest fallback: file contains one filename (not full path) per line
    if manifest and os.path.exists(manifest):
        print(f"[INFO] Loading clips from manifest: {manifest}")
        clips = []
        with open(manifest) as mf:
            for line in mf:
                fname = line.strip()
                if fname and fname.lower().endswith(".avi"):
                    clips.append(os.path.join(directory, fname))
        clips.sort()
        return clips

    print(f"[ERROR] Cannot load clips from {directory} — no manifest provided or found.")
    return []


def write_playlist(clips: list[str], repeat: int = 200) -> str:
    """
    Write an ffmpeg concat-demuxer playlist file.
    repeat=200 means ~200 loops before ffmpeg exits (pusher restarts it).
    Returns the temp file path.
    """
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", prefix="rtsp_playlist_", delete=False
    )
    for _ in range(repeat):
        for clip in clips:
            f.write(f"file '{clip}'\n")
    f.flush()
    f.close()
    return f.name


# ─── Camera pusher thread ─────────────────────────────────────────────────────

class CameraPusher(threading.Thread):
    """
    One thread per camera.
    Starts ffmpeg to push its clip playlist to MediaMTX via RTSP.
    Restarts ffmpeg automatically if it exits.
    """

    def __init__(self, cam_id: str, rtsp_url: str, clips: list[str]):
        super().__init__(daemon=True, name=f"push-{cam_id}")
        self.cam_id   = cam_id
        self.rtsp_url = rtsp_url
        self.clips    = clips
        self._proc: subprocess.Popen | None = None
        self._playlist: str | None = None

    # ── internal ──────────────────────────────────────────────────────────────

    def _cleanup_playlist(self):
        if self._playlist and os.path.exists(self._playlist):
            try:
                os.unlink(self._playlist)
            except OSError:
                pass
        self._playlist = None

    def _start_ffmpeg(self):
        """Kill old process, write fresh shuffled playlist, spawn ffmpeg."""
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()

        self._cleanup_playlist()

        shuffled = self.clips.copy()
        random.shuffle(shuffled)
        self._playlist = write_playlist(shuffled)

        cmd = [
            "ffmpeg",
            "-re",                       # real-time playback rate
            "-f", "concat",
            "-safe", "0",
            "-i", self._playlist,
            # Encode to H.264 baseline — wide compatibility, low latency
            "-c:v", "libx264",
            "-profile:v", "baseline",
            "-level", "3.0",
            "-bf", "0",                  # no B-frames → lower latency
            "-g", "15",                  # keyframe every 15 frames (~0.5s) — clients connect faster
            "-preset", "ultrafast",
            "-tune", "zerolatency",
            "-pix_fmt", "yuv420p",
            "-an",                       # strip audio
            "-f", "rtsp",
            "-rtsp_transport", "tcp",
            self.rtsp_url,
        ]

        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print(f"[{self.cam_id}] ffmpeg started → {self.rtsp_url}", flush=True)

    # ── thread entry ──────────────────────────────────────────────────────────

    def run(self):
        while not os.path.exists(STOP_FILE):
            # (Re)start ffmpeg if not running
            if self._proc is None or self._proc.poll() is not None:
                self._start_ffmpeg()
            time.sleep(5)

        # Graceful shutdown
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        self._cleanup_playlist()
        print(f"[{self.cam_id}] Pusher stopped.", flush=True)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 56, flush=True)
    print("  RTSP Pusher  (ffmpeg → MediaMTX)", flush=True)
    print("=" * 56, flush=True)
    print(f"  MediaMTX : {MEDIAMTX_HOST}:8554", flush=True)
    print(f"  Fight dir: {FIGHT_DIR}", flush=True)
    print(f"  NonFight : {NON_FIGHT_DIR}", flush=True)
    print(f"  Max cams : {MAX_CAMERAS}", flush=True)
    print(f"  Stop file: {STOP_FILE}", flush=True)
    print("=" * 56, flush=True)

    # Prepare stop-file dir and clear stale stop
    os.makedirs(os.path.dirname(STOP_FILE), exist_ok=True)
    if os.path.exists(STOP_FILE):
        os.remove(STOP_FILE)
        print("Cleared stale stop file.")

    # Load clip pools
    fight_clips     = load_clips(FIGHT_DIR,     FIGHT_MANIFEST)
    non_fight_clips = load_clips(NON_FIGHT_DIR, NONFIGHT_MANIFEST)

    if not fight_clips and not non_fight_clips:
        print("[ERROR] No .avi clips found in Fight or NonFight directories. Exiting.")
        sys.exit(1)

    print(
        f"[INFO] Found {len(fight_clips)} fight + "
        f"{len(non_fight_clips)} non-fight clips.",
        flush=True,
    )

    # Load camera registry
    if not os.path.exists(METADATA_FILE):
        print(f"[ERROR] Camera registry not found: {METADATA_FILE}")
        sys.exit(1)

    with open(METADATA_FILE, newline="", encoding="utf-8") as f:
        cameras = list(csv.DictReader(f))

    # Limit number of cameras pushed (CPU budget)
    selected = cameras[:MAX_CAMERAS]
    print(
        f"[INFO] Pushing {len(selected)} of {len(cameras)} cameras "
        f"(MAX_CAMERAS={MAX_CAMERAS}).",
        flush=True,
    )

    workers: list[CameraPusher] = []

    for cam in selected:
        cam_id      = cam["camera_id"]
        has_violence = str(cam.get("has_violence", "False")).lower() == "true"
        rtsp_url    = f"rtsp://{MEDIAMTX_HOST}:8554/{cam_id}"

        # Select clip pool: violent cameras get fight clips, others get non-fight
        if has_violence and fight_clips:
            pool = fight_clips
        elif non_fight_clips:
            pool = non_fight_clips
        else:
            pool = fight_clips  # fallback: all fight

        k     = min(CLIPS_PER_CAM, len(pool))
        clips = random.sample(pool, k)

        label = "FIGHT" if has_violence and fight_clips else "NORMAL"
        print(
            f"  {cam_id}: {label} ({k} clips) → {rtsp_url}",
            flush=True,
        )

        w = CameraPusher(cam_id, rtsp_url, clips)
        w.start()
        workers.append(w)
        time.sleep(0.5)  # stagger ffmpeg starts to spread CPU spike

    print(f"\n{len(workers)} RTSP streams running. Touch {STOP_FILE} to stop.\n",
          flush=True)

    try:
        while not os.path.exists(STOP_FILE):
            time.sleep(2)
        print("Stop file detected — shutting down pushers...")
    except KeyboardInterrupt:
        print("\nKeyboard interrupt — shutting down...")

    for w in workers:
        w.join(timeout=15)

    print("All RTSP pushers stopped.")


if __name__ == "__main__":
    main()
