# Plan B — Phase 1: RTSP Streaming Admin Page

> **Mục tiêu**: Trang `/admin/streaming` hiển thị và điều khiển pipeline RTSP
> — container status, start/stop, active streams, camera grid.
>
> **Nguyên tắc**: Đơn giản, đúng trách nhiệm. Chatbot không quản lý infrastructure.  
> **Thời gian ước tính**: ~1h  
> **Trạng thái**: Chưa triển khai (2026-05-23)

---

## Kiến trúc

```
Frontend (localhost:5174)
├── /api/chat, /api/camera-status, ...   →  chatbot   :5002  (AI/RAG — không đổi)
└── /api/pipeline-status                 →  admin-api :5003  (infrastructure mới)
    /api/start
    /api/stop
```

**Chatbot giữ nguyên** — không thêm / không bớt endpoint nào.  
**admin-api** là service mới, nhỏ gọn, chỉ chạy local (profile `admin`).

---

## Rollback việc đã làm sai

Trước khi bắt đầu, xóa endpoint `/api/streaming-status` khỏi `scripts/chatbot/main.py`
(đã thêm nhầm ở session trước). Endpoint này sẽ nằm trong `admin-api`.

---

## Tổng hợp files cần tạo / sửa

| File | Action | Ghi chú |
|------|--------|---------|
| `scripts/chatbot/main.py` | **Edit** | Xóa `/api/streaming-status` (rollback) |
| `scripts/admin/main.py` | **Create** | Admin API service (~80 dòng) |
| `docker/Dockerfile.admin` | **Create** | Python slim, chỉ fastapi + docker + requests |
| `docker/docker-compose.yml` | **Edit** | Thêm service `admin-api` với profile `admin` |
| `frontend/src/pages/admin/StreamingAdmin.jsx` | **Create** | Trang admin |
| `frontend/src/routers/router.jsx` | **Edit** | Thêm route `/admin/streaming` |
| `frontend/src/components/layout/SideBar.jsx` | **Edit** | Thêm link "Streaming Admin" |
| `frontend/.env.admin` | **Create** | `VITE_ADMIN_API_URL=http://localhost:5003` |

**Tổng: 6 files mới, 3 files sửa.**

---

## Step 1 — Rollback chatbot

**File**: `scripts/chatbot/main.py`

Xóa toàn bộ function `get_streaming_status()` và decorator `@app.get("/api/streaming-status")`.
Khoảng 35 dòng, từ dòng ~747.

---

## Step 2 — Tạo `scripts/admin/main.py`

Service FastAPI đơn giản, ~80 dòng:

```python
"""
Admin API — Pipeline RTSP Management
Chạy port 5003, chỉ dùng local (profile admin).
"""
import os
import docker
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Admin API")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"],
    allow_methods=["*"], allow_headers=["*"],
)

MEDIAMTX_API  = os.getenv("MEDIAMTX_API_URL", "http://mediamtx:9997")
STREAMING_CONTAINERS = ["rtsp_pusher", "rtsp-inference-mock"]


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/api/pipeline-status")
def pipeline_status():
    """Trạng thái containers + MediaMTX active streams."""
    result = {"containers": {}, "streams": {"ok": False, "active": [], "count": 0}}

    # Container statuses
    try:
        client = docker.from_env()
        for name in ["mediamtx"] + STREAMING_CONTAINERS:
            try:
                c = client.containers.get(name)
                result["containers"][name] = c.status          # running | exited | ...
            except docker.errors.NotFound:
                result["containers"][name] = "not_found"
    except Exception:
        pass

    # MediaMTX active paths
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
    """Start rtsp_pusher và rtsp-inference-mock."""
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
    """Stop graceful: touch /app/tmp/STOP."""
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
```

---

## Step 3 — Tạo `docker/Dockerfile.admin`

```dockerfile
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn docker requests
COPY scripts/admin/main.py .
EXPOSE 5003
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5003"]
```

---

## Step 4 — Thêm service vào `docker/docker-compose.yml`

Thêm vào phần services (gần cuối, trước networks):

```yaml
  # --- Admin API (local only — pipeline management) ---
  admin-api:
    build:
      context: ..
      dockerfile: docker/Dockerfile.admin
    container_name: admin-api
    profiles: [admin]
    ports:
      - "5003:5003"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      MEDIAMTX_API_URL: http://mediamtx:9997
    deploy:
      resources:
        limits:
          memory: 128m
          cpus: "0.25"
    networks:
      - violence-detection-net
    restart: unless-stopped
```

**Chạy cùng streaming profile:**
```bash
docker compose -f docker/docker-compose.yml --profile streaming --profile admin up -d
```

---

## Step 5 — Tạo `frontend/.env.admin`

```env
VITE_ADMIN_API_URL=http://localhost:5003
```

> File này không commit (thêm vào `.gitignore`).
> Frontend dev server đọc file này khi `npm run dev`.

---

## Step 6 — Tạo `frontend/src/pages/admin/StreamingAdmin.jsx`

Gồm 3 phần: **Pipeline Control** | **Active Streams** | **Camera Grid 15 cams**

```jsx
import React, { useState, useEffect, useCallback } from 'react';
import { Play, Square, RefreshCw, Radio, VideoOff } from 'lucide-react';

const ADMIN_API = import.meta.env.VITE_ADMIN_API_URL || 'http://localhost:5003';

// ─── usePoll hook ─────────────────────────────────────────────────────────
const usePoll = (url, interval = 5000) => {
  const [data, setData]     = useState(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(() => {
    fetch(url)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, [url]);

  useEffect(() => { refresh(); }, [refresh]);
  useEffect(() => {
    const id = setInterval(refresh, interval);
    return () => clearInterval(id);
  }, [refresh, interval]);

  return { data, loading, refresh };
};

// ─── Status badge ────────────────────────────────────────────────────────
const StatusBadge = ({ status }) => {
  const map = {
    running:   'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
    exited:    'bg-red-500/15 text-red-400 border-red-500/30',
    not_found: 'bg-slate-800 text-slate-600 border-slate-700',
  };
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs font-mono border
      ${map[status] ?? 'bg-slate-800 text-slate-500 border-slate-700'}`}>
      {status ?? '...'}
    </span>
  );
};

// ─── Main Page ────────────────────────────────────────────────────────────
export default function StreamingAdmin() {
  const { data, loading, refresh } = usePoll(`${ADMIN_API}/api/pipeline-status`, 5000);
  const [actionLoading, setActionLoading] = useState(null);

  const handleAction = async (action) => {
    setActionLoading(action);
    await fetch(`${ADMIN_API}/api/${action}`, { method: 'POST' }).catch(() => {});
    setTimeout(() => { refresh(); setActionLoading(null); }, 1500);
  };

  const containers = data?.containers ?? {};
  const streams    = data?.streams    ?? {};
  const CAMS = Array.from({ length: 15 }, (_, i) => `cam_${String(i + 1).padStart(2, '0')}`);

  return (
    <div className="space-y-5">

      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-white flex items-center gap-2">
            <Radio size={18} className="text-emerald-400" /> Streaming Admin
          </h1>
          <p className="text-slate-500 text-sm">Local pipeline control · port 5003</p>
        </div>
        <button onClick={refresh}
          className="p-2 rounded-lg hover:bg-slate-700 text-slate-400 hover:text-white transition-colors">
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
        </button>
      </div>

      {/* Top row: Pipeline Control + Active Streams */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">

        {/* Pipeline Control */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
          <h2 className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
            Pipeline Control
          </h2>

          <div className="space-y-2">
            {[
              { key: 'mediamtx',            label: 'MediaMTX' },
              { key: 'rtsp_pusher',         label: 'RTSP Pusher' },
              { key: 'rtsp-inference-mock', label: 'Inference Mock' },
            ].map(({ key, label }) => (
              <div key={key} className="flex items-center justify-between text-sm">
                <span className="text-slate-300">{label}</span>
                <StatusBadge status={containers[key]} />
              </div>
            ))}
          </div>

          <div className="flex gap-2 pt-1">
            <button onClick={() => handleAction('start')}
              disabled={actionLoading !== null}
              className="flex-1 flex items-center justify-center gap-1.5 py-2 rounded-lg
                bg-emerald-700 hover:bg-emerald-600 disabled:opacity-40 text-white text-sm
                font-medium transition-colors">
              {actionLoading === 'start'
                ? <RefreshCw size={13} className="animate-spin" />
                : <Play size={13} />}
              Start
            </button>
            <button onClick={() => handleAction('stop')}
              disabled={actionLoading !== null}
              className="flex-1 flex items-center justify-center gap-1.5 py-2 rounded-lg
                bg-red-800 hover:bg-red-700 disabled:opacity-40 text-white text-sm
                font-medium transition-colors">
              {actionLoading === 'stop'
                ? <RefreshCw size={13} className="animate-spin" />
                : <Square size={13} />}
              Stop
            </button>
          </div>
        </div>

        {/* Active Streams */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-3">
          <h2 className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
            Active Streams
          </h2>

          <div className="flex items-baseline gap-1.5">
            <span className="text-4xl font-bold text-white">{streams.count ?? 0}</span>
            <span className="text-slate-500 text-sm">/ 15 cameras</span>
          </div>

          <p className={`text-xs flex items-center gap-1
            ${streams.ok ? 'text-emerald-400' : 'text-red-400'}`}>
            <Radio size={10} className={streams.ok ? 'animate-pulse' : ''} />
            {streams.ok ? 'MediaMTX reachable' : 'MediaMTX offline'}
          </p>

          {(streams.active ?? []).length > 0 ? (
            <div className="flex flex-wrap gap-1">
              {streams.active.map(s => (
                <span key={s} className="px-1.5 py-0.5 rounded text-xs font-mono
                  bg-emerald-900/40 text-emerald-300 border border-emerald-700/30">
                  {s}
                </span>
              ))}
            </div>
          ) : (
            <p className="text-xs text-slate-600 italic flex items-center gap-1">
              <VideoOff size={11} /> Không có stream nào đang publish
            </p>
          )}
        </div>
      </div>

      {/* Camera Grid */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <h2 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-4">
          Camera Grid — 15 cameras
        </h2>
        <div className="grid grid-cols-5 sm:grid-cols-8 md:grid-cols-10 lg:grid-cols-15 gap-2">
          {CAMS.map(cam => {
            const isLive = (streams.active ?? []).includes(cam);
            return (
              <div key={cam} className={`rounded-lg border p-2 text-center text-xs transition-colors
                ${isLive
                  ? 'bg-emerald-500/10 border-emerald-500/25 text-emerald-400'
                  : 'bg-slate-800 border-slate-700 text-slate-600'}`}>
                <div className="font-mono font-semibold">{cam.replace('cam_', '')}</div>
                <div className="text-[9px] mt-0.5">{isLive ? '● live' : '○'}</div>
              </div>
            );
          })}
        </div>
      </div>

    </div>
  );
}
```

---

## Step 7 — Update router + sidebar

### `frontend/src/routers/router.jsx`

Thêm import và route:
```jsx
import StreamingAdmin from '../pages/admin/StreamingAdmin';

// Thêm vào children:
{ path: '/admin/streaming', element: <StreamingAdmin /> },
```

### `frontend/src/components/layout/SideBar.jsx`

Thêm link (luôn hiện — đây là local admin app):
```jsx
import { Radio } from 'lucide-react';

// Thêm vào nav list, sau Settings:
{ name: 'Streaming Admin', path: '/admin/streaming', icon: Radio },
```

---

## Cách chạy

```bash
# 1. Build + start admin-api (lần đầu)
docker compose -f docker/docker-compose.yml --profile streaming --profile admin up -d

# 2. Start frontend
cd Violence-Urban-Safety-UI/frontend
npm run dev

# 3. Mở http://localhost:5174/admin/streaming
```

---

## Test nhanh (sau khi xong)

```bash
# Backend
curl http://localhost:5003/health
curl http://localhost:5003/api/pipeline-status
curl -X POST http://localhost:5003/api/stop
curl -X POST http://localhost:5003/api/start

# Frontend: mở localhost:5174/admin/streaming
# ✅ 3 container badges hiển thị đúng status
# ✅ Active streams count đúng với MediaMTX
# ✅ Start/Stop buttons hoạt động (container status thay đổi sau 10-15s)
# ✅ Camera grid 15 ô, ô nào live thì sáng xanh
```

---

## Thứ tự thực hiện

```
[1]  2 phút  — Rollback chatbot (xóa /api/streaming-status)
[2]  5 phút  — scripts/admin/main.py
[3]  2 phút  — docker/Dockerfile.admin
[4]  3 phút  — docker-compose.yml (thêm admin-api service)
[5]  2 phút  — frontend/.env.admin
[6] 15 phút  — StreamingAdmin.jsx
[7]  3 phút  — router.jsx + SideBar.jsx
[8] 10 phút  — docker build + test
               ─────────────────
Total: ~42 phút
```

---

*Updated: 2026-05-23 — chatbot removed, dedicated admin-api service*
