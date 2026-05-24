"""
Admin API — RTSP Pipeline Management
Port 5003, chỉ chạy local (profile admin).
"""
import os
from pathlib import Path

import docker
import requests
from fastapi import FastAPI
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


@app.get("/", response_class=HTMLResponse)
def admin_ui():
    return HTML_PATH.read_text(encoding="utf-8")


@app.get("/health")
def health():
    return {"ok": True}


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
    results = {}
    try:
        client = docker.from_env()
        for name in STREAMING_CONTAINERS:
            try:
                c = client.containers.get(name)
                if c.status == "running":
                    c.exec_run("rm -f /app/tmp/STOP")
                    results[name] = "already_running"
                else:
                    c.start()
                    results[name] = "started"
            except docker.errors.NotFound:
                results[name] = "not_found"
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "results": results}


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
