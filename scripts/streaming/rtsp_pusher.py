"""
RTSP Pusher Service
===================
Reads video clips (.avi/.mp4) from the SCVD dataset (SmartCity CCTV Violence
Detection) and pushes them as live RTSP streams into MediaMTX.
SCVD is the stream/eval dataset — kept separate from RWF-2000 (used to train the
model) to avoid testing on training data.

One ffmpeg subprocess per camera, loops its playlist continuously.
Does NOT call any AI API — purely an RTSP source for rtsp-inference-mock.

Camera assignment:
  has_violence=True  → Fight clips  (ffmpeg pushes fight footage)
  has_violence=False → NonFight clips

Env vars:
  METADATA_FILE   Path to camera_registry.csv   (/app/data/metadata/camera_registry.csv)
  FIGHT_DIR       Violence clips dir (or auto-discovered under SCVD_DATA_ROOT)
  NON_FIGHT_DIR   Non-violence clips dir (or auto-discovered under SCVD_DATA_ROOT)
  SCVD_DATA_ROOT  SCVD dataset root             (/app/data/raw/SCVD)
  MEDIAMTX_HOST   MediaMTX hostname              (mediamtx)
  MAX_CAMERAS     Max cameras to push            (4) — limit CPU usage
  CLIPS_PER_CAM   Clips per camera playlist      (6)
  STOP_FILE       Graceful stop trigger          (/app/tmp/STOP)
  CAMERA_PLAYLISTS_FILE  Ordered per-camera clip playlists (JSON) produced by
                  prepare_cameras_context.py (/app/data/metadata/camera_playlists.json).
                  When present, clips stream in this exact order (no shuffle) so each
                  camera stays in one consistent scene. Absent → random sampling.

Graceful stop:
  docker exec rtsp_pusher touch /app/tmp/STOP
"""

import csv
import json
import os
import random
import subprocess
import sys
import tempfile
import threading
import time

# ─── Configuration ────────────────────────────────────────────────────────────
METADATA_FILE    = os.getenv("METADATA_FILE",    "/app/data/metadata/camera_registry.csv")
# SCVD dataset (stream/eval). Layout varies by download ({Train,Test}/{Class A,Class B},
# or violence/non_violence). If FIGHT_DIR/NON_FIGHT_DIR are missing, clips are
# auto-discovered under SCVD_DATA_ROOT by folder-name aliases (see discover_scvd_dirs).
SCVD_DATA_ROOT   = os.getenv("SCVD_DATA_ROOT",   "/app/data/raw/SCVD")
FIGHT_DIR        = os.getenv("FIGHT_DIR",        "/app/data/raw/SCVD/Violence")
NON_FIGHT_DIR    = os.getenv("NON_FIGHT_DIR",    "/app/data/raw/SCVD/NonViolence")
VIDEO_EXTENSIONS = (".avi", ".mp4")
FIGHT_MANIFEST   = os.getenv("FIGHT_MANIFEST",   "/app/data/metadata/fight_clips_manifest.txt")
NONFIGHT_MANIFEST= os.getenv("NONFIGHT_MANIFEST","/app/data/metadata/nonfight_clips_manifest.txt")
MEDIAMTX_HOST    = os.getenv("MEDIAMTX_HOST",    "mediamtx")
MAX_CAMERAS      = int(os.getenv("MAX_CAMERAS",  "4"))   # keep CPU manageable
CLIPS_PER_CAM    = int(os.getenv("CLIPS_PER_CAM","6"))
STOP_FILE        = os.getenv("STOP_FILE",        "/app/tmp/STOP")
# Ordered per-camera clip playlists (from prepare_cameras_context.py).
# When present, the pusher streams clips in this exact order (no shuffle) so each
# camera stays in one consistent scene. Absent → legacy random sampling.
CAMERA_PLAYLISTS_FILE = os.getenv(
    "CAMERA_PLAYLISTS_FILE", "/app/data/metadata/camera_playlists.json")
# Comma-separated list of active camera IDs (e.g. "cam_01,cam_03,cam_07").
# If unset, all cameras from registry are used (up to MAX_CAMERAS).
_active_env      = os.getenv("ACTIVE_CAMERAS", "").strip()
ACTIVE_CAMERAS   = set(_active_env.split(",")) if _active_env else None


# ─── SCVD auto-discovery ─────────────────────────────────────────────────────
# Folder-name aliases (normalized: lowercase, alnum only) for detecting SCVD
# violence / non-violence class folders across whatever layout the download
# produced ({Train,Test}/{Class A,Class B}, violence/non_violence, 3-class, ...).
_VIOLENCE_ALIASES     = {"classa", "violence", "fight", "violent", "weaponized", "v"}
_NON_VIOLENCE_ALIASES = {"classb", "nonviolence", "nonviolent", "normal", "safe", "nv", "nonfight"}
_SCVD_SPLITS          = ("train", "test", "val")


def _norm_name(name: str) -> str:
    """Normalize a folder name for alias matching (lowercase, alnum only)."""
    return "".join(c for c in name.lower() if c.isalnum())


def discover_scvd_dirs(root: str = SCVD_DATA_ROOT) -> tuple[list[str], list[str]]:
    """Walk `root` and return (violence_dirs, nonviolence_dirs), pooling all splits.

    Handles {Train,Test}/{Class A,Class B} and flat violence/non_violence layouts.
    Returns empty lists if `root` is missing (caller decides how to fail).
    """
    v_dirs, nv_dirs = [], []
    if not os.path.isdir(root):
        return v_dirs, nv_dirs
    # Detect split folders case-insensitively (Train/train, Test/test, Val/val).
    entries = [e for e in os.listdir(root) if os.path.isdir(os.path.join(root, e))]
    splits = [e for e in entries if _norm_name(e) in _SCVD_SPLITS]
    parents = [os.path.join(root, s) for s in splits] or [root]
    for parent in parents:
        for entry in os.listdir(parent):
            full = os.path.join(parent, entry)
            if not os.path.isdir(full):
                continue
            n = _norm_name(entry)
            if n in _VIOLENCE_ALIASES or "weapon" in n or "violent" in n:
                v_dirs.append(full)
            elif n in _NON_VIOLENCE_ALIASES or "normal" in n:
                nv_dirs.append(full)
    return v_dirs, nv_dirs


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
    # Try recursive walk (SCVD may nest clips under {Train,Test}/{Class A,B})
    try:
        clips = []
        for dirpath, _dirs, files in os.walk(directory):
            for f in files:
                if f.lower().endswith(VIDEO_EXTENSIONS) and not f.startswith("."):
                    clips.append(os.path.join(dirpath, f))
        clips.sort()
        if clips:
            return clips
    except OSError as e:
        print(f"[WARN] Directory walk failed ({e}), trying manifest fallback...")

    # Manifest fallback: file contains one filename (not full path) per line
    if manifest and os.path.exists(manifest):
        print(f"[INFO] Loading clips from manifest: {manifest}")
        clips = []
        with open(manifest) as mf:
            for line in mf:
                fname = line.strip()
                if fname and fname.lower().endswith(VIDEO_EXTENSIONS):
                    clips.append(os.path.join(directory, fname))
        clips.sort()
        return clips

    print(f"[ERROR] Cannot load clips from {directory} — no manifest provided or found.")
    return []


def load_clips_multi(dirs: list[str], manifest: str = "") -> list[str]:
    """Load + concatenate clips from multiple directories (pool SCVD splits)."""
    out: list[str] = []
    for d in (dirs or []):
        out.extend(load_clips(d, manifest))
    return out


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


def load_context_playlists(path: str) -> dict[str, list[str]]:
    """Load ordered per-camera clip playlists from JSON.

    Returns {cam_id: [clip_path, ...]} filtered to clips that exist on disk.
    Returns {} if the file is absent or unreadable, so the caller falls back to
    the legacy random-sampling path.
    """
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError) as exc:
        print(f"[WARN] Could not read context playlists ({exc}); "
              f"using random sampling.", flush=True)
        return {}
    out: dict[str, list[str]] = {}
    for cam_id, clips in data.items():
        clips = [c for c in clips if os.path.exists(c)]
        if clips:
            out[cam_id] = clips
    print(f"[INFO] Loaded context playlists for {len(out)} cameras from {path}",
          flush=True)
    return out


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

        # Clips are pre-ordered for scene-continuous playback — do NOT shuffle
        # (shuffling would jump between unrelated scenes mid-stream).
        self._playlist = write_playlist(self.clips)

        cmd = [
            "ffmpeg",
            "-re",                       # real-time playback rate
            "-f", "concat",
            "-safe", "0",
            "-i", self._playlist,
            # Downscale to 360p @ 15fps: source is 720p mpeg4 @ 30fps; encoding
            # 15 concurrent 720p30 streams saturates the CPU (load ~50 on 12
            # cores) and streams drop. MoViNet infers at 256px and the webapp
            # grid is small, so 360p15 is plenty and ~5x cheaper.
            "-vf", "scale=-2:360",
            "-r", "15",
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
    print(f"  Active   : {','.join(sorted(ACTIVE_CAMERAS)) if ACTIVE_CAMERAS else 'all (from registry)'}", flush=True)
    print(f"  Stop file: {STOP_FILE}", flush=True)
    print("=" * 56, flush=True)

    # Prepare stop-file dir and clear stale stop
    os.makedirs(os.path.dirname(STOP_FILE), exist_ok=True)
    if os.path.exists(STOP_FILE):
        os.remove(STOP_FILE)
        print("Cleared stale stop file.")

    # Load clip pools. If configured dirs are missing, auto-discover SCVD layout.
    if not os.path.isdir(FIGHT_DIR) and not os.path.isdir(NON_FIGHT_DIR):
        v_dirs, nv_dirs = discover_scvd_dirs(SCVD_DATA_ROOT)
        if v_dirs or nv_dirs:
            print(f"[INFO] SCVD auto-discovered: {len(v_dirs)} violence / "
                  f"{len(nv_dirs)} non-violence dirs under {SCVD_DATA_ROOT}", flush=True)
        fight_clips     = load_clips_multi(v_dirs,  FIGHT_MANIFEST)
        non_fight_clips = load_clips_multi(nv_dirs, NONFIGHT_MANIFEST)
    else:
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

    # Context-continuous playlists (scene-clustering). {} when absent.
    context_playlists = load_context_playlists(CAMERA_PLAYLISTS_FILE)

    # Filter by ACTIVE_CAMERAS list if provided, then apply MAX_CAMERAS CPU budget
    if ACTIVE_CAMERAS:
        cameras = [c for c in cameras if c["camera_id"] in ACTIVE_CAMERAS]
        print(f"[INFO] Filtered to {len(cameras)} cameras by ACTIVE_CAMERAS.", flush=True)
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

        # Context-continuous mode: use the pre-ordered scene playlist if present.
        if cam_id in context_playlists:
            clips = context_playlists[cam_id]
            label = "CONTEXT"
        else:
            # Legacy random fallback (kept for when no playlists.json exists).
            if has_violence and fight_clips:
                pool = fight_clips
            elif non_fight_clips:
                pool = non_fight_clips
            else:
                pool = fight_clips  # fallback: all fight
            k = min(CLIPS_PER_CAM, len(pool))
            clips = random.sample(pool, k)
            label = "FIGHT" if has_violence and fight_clips else "NORMAL"

        print(
            f"  {cam_id}: {label} ({len(clips)} clips) → {rtsp_url}",
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
