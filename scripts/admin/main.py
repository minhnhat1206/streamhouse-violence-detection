"""
Admin API — RTSP Pipeline Management
Port 5003, chỉ chạy local (profile admin).
"""
import csv
import json
import os
from pathlib import Path
from typing import Dict

import docker
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

app = FastAPI(title="Admin API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

MEDIAMTX_API = os.getenv("MEDIAMTX_API_URL", "http://mediamtx:9997")
STREAMING_CONTAINERS = ["rtsp_pusher", "rtsp-inference-mock"]
HTML_PATH = Path(__file__).parent / "index.html"

CAMERA_REGISTRY_PATH = os.getenv("CAMERA_REGISTRY_PATH", "/app/data/metadata/camera_registry.csv")
CAMERA_STATE_PATH    = os.getenv("CAMERA_STATE_PATH",    "/app/data/metadata/camera_state.json")


# ─── Camera State Helpers ─────────────────────────────────────────────────────

def _load_registry() -> list[dict]:
    path = Path(CAMERA_REGISTRY_PATH)
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _load_state() -> Dict[str, bool]:
    """Return {camera_id: active} mapping, initializing from registry if needed."""
    state_path = Path(CAMERA_STATE_PATH)
    if state_path.exists():
        try:
            data = json.loads(state_path.read_text(encoding="utf-8"))
            return {k: bool(v.get("active", True)) for k, v in data.get("cameras", {}).items()}
        except Exception:
            pass
    # Initialize from registry — all cameras active by default
    registry = _load_registry()
    state = {row["camera_id"]: True for row in registry}
    _save_state(state)
    return state


def _save_state(state: Dict[str, bool]) -> None:
    state_path = Path(CAMERA_STATE_PATH)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"cameras": {k: {"active": v} for k, v in state.items()}}
    state_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


@app.get("/", response_class=HTMLResponse)
def admin_ui():
    return HTML_PATH.read_text(encoding="utf-8")


@app.get("/health")
def health():
    return {"ok": True}


# ─── Camera Management ────────────────────────────────────────────────────────

@app.get("/api/cameras")
def list_cameras():
    registry = _load_registry()
    state    = _load_state()
    cameras  = []
    for row in registry:
        cid = row["camera_id"]
        cameras.append({
            "camera_id":       cid,
            "location":        row.get("street", ""),
            "district":        row.get("district", ""),
            "ward":            row.get("ward", ""),
            "has_violence":    str(row.get("has_violence", "False")).lower() == "true",
            "active":          state.get(cid, True),
        })
    active_count = sum(1 for c in cameras if c["active"])
    return {"cameras": cameras, "total": len(cameras), "active_count": active_count}


@app.get("/api/cameras/active-count")
def active_count():
    state = _load_state()
    total  = len(_load_registry())
    active = sum(1 for v in state.values() if v)
    return {"active": active, "total": total}


@app.post("/api/cameras/{camera_id}/toggle")
def toggle_camera(camera_id: str):
    state = _load_state()
    if camera_id not in state:
        raise HTTPException(status_code=404, detail=f"Camera '{camera_id}' not found")
    state[camera_id] = not state[camera_id]
    _save_state(state)
    return {"camera_id": camera_id, "active": state[camera_id]}


@app.get("/api/pipeline-status")
def pipeline_status():
    result = {
        "containers": {},
        "streams": {"ok": False, "active": [], "count": 0},
    }
    try:
        client = docker.from_env()
        for name in ["mediamtx"] + STREAMING_CONTAINERS:
            try:
                result["containers"][name] = client.containers.get(name).status
            except docker.errors.NotFound:
                result["containers"][name] = "not_found"
    except Exception:
        pass

    try:
        r = requests.get(f"{MEDIAMTX_API}/v3/paths/list", timeout=2)
        if r.status_code == 200:
            active = [i["name"] for i in r.json().get("items", []) if i.get("ready")]
            result["streams"] = {"ok": True, "active": sorted(active), "count": len(active)}
    except Exception:
        pass

    return result


@app.post("/api/start")
def start_pipeline():
    # Build ACTIVE_CAMERAS value from current state
    state = _load_state()
    active_cams = ",".join(k for k, v in sorted(state.items()) if v)

    results = {}
    try:
        client = docker.from_env()
        for name in STREAMING_CONTAINERS:
            try:
                c = client.containers.get(name)
                if c.status == "running":
                    # Clear stop file and inject active cameras env into the process env file
                    c.exec_run("rm -f /app/tmp/STOP")
                    c.exec_run(f"sh -c 'echo ACTIVE_CAMERAS={active_cams} > /app/tmp/active_cameras.env'")
                    results[name] = "already_running"
                else:
                    c.start()
                    results[name] = "started"
            except docker.errors.NotFound:
                results[name] = "not_found"
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "results": results, "active_cameras": active_cams}


@app.post("/api/stop")
def stop_pipeline():
    results = {}
    try:
        client = docker.from_env()
        for name in STREAMING_CONTAINERS:
            try:
                c = client.containers.get(name)
                if c.status == "running":
                    c.exec_run("touch /app/tmp/STOP")
                    results[name] = "stop_sent"
                else:
                    results[name] = f"already_{c.status}"
            except docker.errors.NotFound:
                results[name] = "not_found"
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "results": results}
