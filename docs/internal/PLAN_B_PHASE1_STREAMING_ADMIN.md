# Plan B — Phase 1: RTSP Streaming Admin Page

> **Mục tiêu**: Trang `/admin/streaming` trên Local Admin hiển thị và điều khiển
> toàn bộ pipeline RTSP — pipeline health, start/stop, live stream counts, Kafka lag.
>
> **Trạng thái**: Chưa triển khai (2026-05-23)  
> **Ưu tiên**: 🔴 Cao — cần trước khi demo thesis

---

## Tổng quan những gì cần làm

```
Backend (chatbot main.py)          Frontend (React)
──────────────────────────         ──────────────────────────────────────
+ Docker SDK mount                 + src/config/mode.js
+ GET /api/admin/pipeline-health   + src/pages/admin/StreamingAdmin.jsx
+ POST /api/admin/start-streaming  + src/routers/router.jsx  (update)
+ POST /api/admin/stop-streaming   + src/components/layout/SideBar.jsx (update)
                                   + frontend/.env.admin  (new)
                                   + frontend/.env.production  (new)
                                   + vite.config.js  (update)
docker/docker-compose.yml
+ Mount /var/run/docker.sock → chatbot
docker/requirements.txt
+ docker>=7.0.0
```

---

## Step 1 — Backend: Docker SDK trong chatbot container

### 1.1 Thêm `docker` SDK vào requirements

**File**: `docker/requirements.txt`

```
# Thêm vào cuối file:
docker>=7.0.0
```

### 1.2 Mount Docker socket vào chatbot service

**File**: `docker/docker-compose.yml` — tìm service `chatbot`, thêm vào `volumes`:

```yaml
chatbot:
  # ... existing config ...
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock   # ← THÊM DÒNG NÀY
    # (các volumes hiện có giữ nguyên)
```

> ⚠️ Security: chỉ dùng trên máy local. Không mount Docker socket trên production VM.

### 1.3 Rebuild chatbot image sau khi thêm package

```bash
docker compose -f docker/docker-compose.yml build chatbot
docker compose -f docker/docker-compose.yml up -d chatbot
```

---

## Step 2 — Backend: 3 Endpoints admin mới

**File**: `scripts/chatbot/main.py`

### 2.1 `GET /api/admin/pipeline-health`

Trả về trạng thái đầy đủ của pipeline RTSP:

```python
@app.get("/api/admin/pipeline-health")
async def get_pipeline_health():
    """
    Kiểm tra toàn bộ trạng thái pipeline RTSP:
    - Container status: mediamtx, rtsp_pusher, rtsp-inference-mock
    - MediaMTX active streams (via port 9997 API)
    - Kafka consumer lag (topic hot-violence-alerts-valid)
    """
    import asyncio as _asyncio

    def _check_sync() -> dict:
        import docker as _docker
        import requests as _req

        result = {
            "containers": {},
            "streams": {"mediamtx_ok": False, "active": [], "count": 0},
            "kafka": {"lag": -1, "messages_per_sec": -1, "ok": False},
        }

        # ── Container status ──────────────────────────────────────────────
        WATCHED = ["mediamtx", "rtsp_pusher", "rtsp-inference-mock"]
        try:
            client = _docker.from_env()
            for name in WATCHED:
                try:
                    c = client.containers.get(name)
                    result["containers"][name] = {
                        "status": c.status,          # "running" | "exited" | "paused"
                        "health": (
                            c.attrs.get("State", {})
                            .get("Health", {})
                            .get("Status", "none")
                        ),
                    }
                except _docker.errors.NotFound:
                    result["containers"][name] = {"status": "not_found", "health": "none"}
        except Exception as e:
            logger.warning(f"Docker SDK error: {e}")

        # ── MediaMTX active paths ──────────────────────────────────────────
        try:
            r = _req.get("http://mediamtx:9997/v3/paths/list", timeout=2)
            if r.status_code == 200:
                data = r.json()
                active = [i["name"] for i in data.get("items", []) if i.get("ready")]
                result["streams"] = {
                    "mediamtx_ok": True,
                    "active": sorted(active),
                    "count": len(active),
                }
        except Exception as e:
            logger.debug(f"MediaMTX API error: {e}")

        # ── Kafka lag cho topic hot-violence-alerts-valid ─────────────────
        try:
            from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
            kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
            consumer = KafkaConsumer(
                bootstrap_servers=kafka_servers,
                group_id=None,
                consumer_timeout_ms=1000,
            )
            topic = "hot-violence-alerts-valid"
            parts = consumer.partitions_for_topic(topic) or set()
            tps = [TopicPartition(topic, p) for p in parts]
            if tps:
                consumer.assign(tps)
                end_offsets = consumer.end_offsets(tps)
                begin_offsets = consumer.beginning_offsets(tps)
                total_messages = sum(
                    end_offsets[tp] - begin_offsets[tp] for tp in tps
                )
                result["kafka"] = {
                    "ok": True,
                    "total_messages": total_messages,
                    "partitions": len(tps),
                    "lag": -1,   # cần consumer group để tính lag thật
                }
            consumer.close()
        except Exception as e:
            logger.warning(f"Kafka health check error: {e}")

        return result

    loop = _asyncio.get_event_loop()
    return await loop.run_in_executor(None, _check_sync)
```

### 2.2 `POST /api/admin/start-streaming`

```python
@app.post("/api/admin/start-streaming")
async def start_streaming():
    """
    Khởi động rtsp_pusher và rtsp-inference-mock.
    Xóa STOP file nếu còn tồn tại trước khi start.
    """
    import asyncio as _asyncio

    def _start_sync() -> dict:
        import docker as _docker
        client = _docker.from_env()
        results = {}

        for name in ["rtsp_pusher", "rtsp-inference-mock"]:
            try:
                c = client.containers.get(name)
                # Xóa STOP file trước khi start (nếu container đang exited)
                if c.status == "running":
                    # Container đang chạy → xóa STOP file để resume
                    try:
                        c.exec_run("rm -f /app/tmp/STOP")
                    except Exception:
                        pass
                    results[name] = "already_running"
                else:
                    c.start()
                    results[name] = "started"
            except _docker.errors.NotFound:
                results[name] = "container_not_found"
            except Exception as e:
                results[name] = f"error: {e}"

        return {"action": "start", "results": results}

    loop = _asyncio.get_event_loop()
    return await loop.run_in_executor(None, _start_sync)
```

### 2.3 `POST /api/admin/stop-streaming`

```python
@app.post("/api/admin/stop-streaming")
async def stop_streaming():
    """
    Dừng graceful rtsp_pusher và rtsp-inference-mock
    bằng cách touch /app/tmp/STOP (không kill cứng).
    """
    import asyncio as _asyncio

    def _stop_sync() -> dict:
        import docker as _docker
        client = _docker.from_env()
        results = {}

        for name in ["rtsp_pusher", "rtsp-inference-mock"]:
            try:
                c = client.containers.get(name)
                if c.status == "running":
                    # Graceful stop: touch STOP file
                    exit_code, _ = c.exec_run("touch /app/tmp/STOP")
                    results[name] = "stop_signal_sent" if exit_code == 0 else "exec_failed"
                else:
                    results[name] = f"already_{c.status}"
            except _docker.errors.NotFound:
                results[name] = "container_not_found"
            except Exception as e:
                results[name] = f"error: {e}"

        return {"action": "stop", "results": results}

    loop = _asyncio.get_event_loop()
    return await loop.run_in_executor(None, _stop_sync)
```

**Vị trí chèn trong `main.py`**: ngay sau endpoint `/api/streaming-status` (dòng ~780).

---

## Step 3 — Frontend: Mode Configuration

### 3.1 Tạo `frontend/src/config/mode.js`

```js
// Đọc VITE_APP_MODE từ env khi build
// admin  → full features (WebRTC, pipeline control)
// public → user-facing features chỉ (alerts, analytics, chatbot)
export const IS_ADMIN  = import.meta.env.VITE_APP_MODE === 'admin';
export const API_BASE  = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:5002';
export const APP_MODE  = import.meta.env.VITE_APP_MODE ?? 'admin';
```

### 3.2 Tạo `frontend/.env.admin`

```env
VITE_APP_MODE=admin
VITE_API_BASE_URL=http://localhost:5002
```

### 3.3 Tạo `frontend/.env.production`

```env
VITE_APP_MODE=public
VITE_API_BASE_URL=https://api.ORACLE_VM_IP.nip.io
```

> Thay `ORACLE_VM_IP.nip.io` bằng IP thật sau khi VM tạo xong.

### 3.4 Tạo `frontend/.env.example`

```env
# Copy sang .env.admin (local) hoặc set trên Vercel dashboard (production)
VITE_APP_MODE=admin           # admin | public
VITE_API_BASE_URL=http://localhost:5002
```

### 3.5 Update `frontend/vite.config.js`

Thêm `envDir` để đọc `.env.admin` thay vì `.env` mặc định:

```js
// vite.config.js
import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  return {
    plugins: [react()],
    server: {
      port: 5174,
      proxy: {
        '/api': {
          target: loadEnv(mode, process.cwd(), '').VITE_API_BASE_URL
            || 'http://localhost:5002',
          changeOrigin: true,
        },
      },
    },
  }
})
```

### 3.6 Update `frontend/package.json` — thêm scripts

```json
{
  "scripts": {
    "dev":         "vite --mode admin",
    "dev:public":  "vite --mode production",
    "build":       "vite build",
    "build:admin": "vite build --mode admin",
    "preview":     "vite preview"
  }
}
```

---

## Step 4 — Frontend: StreamingAdmin Page

**File mới**: `frontend/src/pages/admin/StreamingAdmin.jsx`

Trang gồm 4 panel chính:

```
StreamingAdmin
├── [1] PipelineControlPanel   — start/stop, container statuses
├── [2] ActiveStreamsPanel     — MediaMTX live count, per-stream badges
├── [3] KafkaHealthPanel       — message count, topic status
└── [4] CameraStatusSummary   — grid 15 cams, NORMAL/ALERT/OFFLINE
```

### Skeleton đầy đủ:

```jsx
import React, { useState, useEffect, useCallback } from 'react';
import { 
  Play, Square, RefreshCw, Radio, Wifi, WifiOff,
  AlertTriangle, CheckCircle2, Clock, Activity,
  Server, Database, Video
} from 'lucide-react';
import { API_BASE } from '../config/mode';

// ── polling hook (reuse từ StreamhouseStatus) ─────────────────────────────
const usePoll = (url, fallback, ms = 10000) => {
  const [data, setData]   = useState(fallback);
  const [state, setState] = useState('loading'); // loading | ok | error

  const fetch_ = useCallback(() => {
    fetch(url)
      .then(r => { if (!r.ok) throw Error(r.status); return r.json(); })
      .then(d  => { setData(d); setState('ok'); })
      .catch(() => setState('error'));
  }, [url]);

  useEffect(() => { fetch_(); }, [fetch_]);
  useEffect(() => {
    const id = setInterval(fetch_, ms);
    return () => clearInterval(id);
  }, [fetch_, ms]);

  return { data, state, refresh: fetch_ };
};

// ── Status dot ────────────────────────────────────────────────────────────
const Dot = ({ status }) => {
  const color = {
    running:    'bg-emerald-500',
    exited:     'bg-red-500',
    paused:     'bg-yellow-500',
    not_found:  'bg-slate-600',
  }[status] ?? 'bg-slate-600';
  return <span className={`inline-block w-2 h-2 rounded-full ${color} mr-1.5`} />;
};

// ── [1] Pipeline Control Panel ────────────────────────────────────────────
const PipelineControlPanel = ({ health, onRefresh }) => {
  const [loading, setLoading] = useState(null); // 'start' | 'stop' | null

  const handleAction = async (action) => {
    setLoading(action);
    try {
      await fetch(`${API_BASE}/api/admin/${action}-streaming`, { method: 'POST' });
      setTimeout(() => { onRefresh(); setLoading(null); }, 1500);
    } catch {
      setLoading(null);
    }
  };

  const containers  = health?.containers ?? {};
  const allRunning  = ['mediamtx', 'rtsp_pusher', 'rtsp-inference-mock']
    .every(n => containers[n]?.status === 'running');
  const allStopped  = ['rtsp_pusher', 'rtsp-inference-mock']
    .every(n => containers[n]?.status !== 'running');

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-4 flex items-center gap-2">
        <Server size={14} /> Pipeline Control
      </h2>

      {/* Container status badges */}
      <div className="space-y-2 mb-5">
        {[
          { key: 'mediamtx',             label: 'MediaMTX (RTSP server)' },
          { key: 'rtsp_pusher',          label: 'RTSP Pusher (video source)' },
          { key: 'rtsp-inference-mock',  label: 'Inference Mock (AI detection)' },
        ].map(({ key, label }) => {
          const c = containers[key] ?? {};
          return (
            <div key={key} className="flex items-center justify-between text-sm">
              <span className="text-slate-300 flex items-center">
                <Dot status={c.status} />
                {label}
              </span>
              <span className={`text-xs font-mono px-2 py-0.5 rounded
                ${c.status === 'running'   ? 'text-emerald-400 bg-emerald-500/10' :
                  c.status === 'exited'    ? 'text-red-400 bg-red-500/10' :
                  c.status === 'not_found' ? 'text-slate-600 bg-slate-800' :
                                             'text-yellow-400 bg-yellow-500/10'}`}>
                {c.status ?? '...'}
              </span>
            </div>
          );
        })}
      </div>

      {/* Action buttons */}
      <div className="flex gap-3">
        <button
          onClick={() => handleAction('start')}
          disabled={loading !== null || allRunning}
          className="flex-1 flex items-center justify-center gap-2 py-2.5 rounded-lg
            bg-emerald-600 hover:bg-emerald-500 disabled:opacity-40 disabled:cursor-not-allowed
            text-white text-sm font-medium transition-colors"
        >
          {loading === 'start'
            ? <RefreshCw size={14} className="animate-spin" />
            : <Play size={14} />}
          Start Streaming
        </button>

        <button
          onClick={() => handleAction('stop')}
          disabled={loading !== null || allStopped}
          className="flex-1 flex items-center justify-center gap-2 py-2.5 rounded-lg
            bg-red-700 hover:bg-red-600 disabled:opacity-40 disabled:cursor-not-allowed
            text-white text-sm font-medium transition-colors"
        >
          {loading === 'stop'
            ? <RefreshCw size={14} className="animate-spin" />
            : <Square size={14} />}
          Stop Streaming
        </button>
      </div>
    </div>
  );
};

// ── [2] Active Streams Panel ──────────────────────────────────────────────
const ActiveStreamsPanel = ({ health }) => {
  const streams = health?.streams ?? {};
  const active  = streams.active ?? [];
  const total   = 15;

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-4 flex items-center gap-2">
        <Video size={14} /> Live Streams
      </h2>

      <div className="flex items-baseline gap-1 mb-3">
        <span className="text-4xl font-bold text-white">{streams.count ?? 0}</span>
        <span className="text-slate-500 text-sm">/ {total} cameras</span>
      </div>

      {streams.mediamtx_ok
        ? <p className="text-xs text-emerald-400 flex items-center gap-1 mb-3">
            <Radio size={11} className="animate-pulse" /> MediaMTX API reachable
          </p>
        : <p className="text-xs text-red-400 flex items-center gap-1 mb-3">
            <WifiOff size={11} /> MediaMTX không accessible
          </p>}

      {/* Active stream badges */}
      {active.length > 0 ? (
        <div className="flex flex-wrap gap-1.5">
          {active.map(s => (
            <span key={s}
              className="px-2 py-0.5 rounded text-xs bg-emerald-900/40 text-emerald-300
                border border-emerald-700/30 font-mono">
              {s}
            </span>
          ))}
        </div>
      ) : (
        <p className="text-xs text-slate-600 italic">
          {streams.mediamtx_ok ? 'Không có stream nào đang publish' : 'Không thể kiểm tra (MediaMTX offline)'}
        </p>
      )}
    </div>
  );
};

// ── [3] Kafka Health Panel ────────────────────────────────────────────────
const KafkaHealthPanel = ({ health }) => {
  const kafka = health?.kafka ?? {};

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-4 flex items-center gap-2">
        <Database size={14} /> Kafka Pipeline
      </h2>

      <div className="space-y-3 text-sm">
        <div className="flex justify-between">
          <span className="text-slate-400">Topic</span>
          <span className="text-slate-300 font-mono text-xs">hot-violence-alerts-valid</span>
        </div>
        <div className="flex justify-between">
          <span className="text-slate-400">Total messages</span>
          <span className="text-white font-bold">
            {kafka.ok ? (kafka.total_messages ?? 0).toLocaleString() : '—'}
          </span>
        </div>
        <div className="flex justify-between">
          <span className="text-slate-400">Partitions</span>
          <span className="text-slate-300">{kafka.ok ? kafka.partitions : '—'}</span>
        </div>
        <div className="flex justify-between items-center">
          <span className="text-slate-400">Status</span>
          {kafka.ok
            ? <span className="flex items-center gap-1 text-emerald-400 text-xs">
                <CheckCircle2 size={12} /> Connected
              </span>
            : <span className="flex items-center gap-1 text-red-400 text-xs">
                <AlertTriangle size={12} /> Unreachable
              </span>}
        </div>
      </div>
    </div>
  );
};

// ── [4] Camera Status Summary ─────────────────────────────────────────────
const CameraStatusSummary = ({ cameraStatus, liveStreams }) => {
  const CAMERAS = Array.from({ length: 15 }, (_, i) => `cam_${String(i + 1).padStart(2, '0')}`);

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 col-span-full">
      <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-4 flex items-center gap-2">
        <Activity size={14} /> Camera Status (15 cameras)
      </h2>

      <div className="grid grid-cols-5 sm:grid-cols-8 lg:grid-cols-15 gap-2">
        {CAMERAS.map(cam => {
          const isLive   = liveStreams.has(cam);
          const status   = cameraStatus[cam] ?? (isLive ? 'NORMAL' : 'OFFLINE');
          const bgColor  =
            status === 'VIOLENCE_DETECTED' ? 'bg-red-500/20 border-red-500/40 text-red-300' :
            status === 'NORMAL'            ? 'bg-emerald-500/10 border-emerald-500/25 text-emerald-400' :
                                             'bg-slate-800 border-slate-700 text-slate-600';

          return (
            <div key={cam}
              className={`rounded-lg border p-2 text-center text-xs font-mono ${bgColor}`}>
              <div className="font-semibold">{cam.replace('cam_', '')}</div>
              <div className="text-[10px] mt-0.5 opacity-70">
                {isLive ? '● live' : '○ off'}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

// ── Main Page ─────────────────────────────────────────────────────────────
const StreamingAdmin = () => {
  const {
    data: health,
    state: healthState,
    refresh: refreshHealth,
  } = usePoll(`${API_BASE}/api/admin/pipeline-health`, null, 8000);

  const {
    data: cameraStatusData,
  } = usePoll(`${API_BASE}/api/camera-status`, { cameras: {} }, 5000);

  const [liveStreams, setLiveStreams] = useState(new Set());
  useEffect(() => {
    if (health?.streams?.active) {
      setLiveStreams(new Set(health.streams.active));
    }
  }, [health]);

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-xl font-bold text-white flex items-center gap-2">
            <Radio size={20} className="text-emerald-400" />
            Streaming Admin
          </h1>
          <p className="text-slate-500 text-sm mt-0.5">Pipeline RTSP · Local Admin Only</p>
        </div>
        <button
          onClick={refreshHealth}
          className="flex items-center gap-2 px-3 py-2 rounded-lg bg-slate-800 hover:bg-slate-700
            text-slate-300 text-sm transition-colors"
        >
          <RefreshCw size={13} className={healthState === 'loading' ? 'animate-spin' : ''} />
          Refresh
        </button>
      </div>

      {/* 3-column top grid */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
        <PipelineControlPanel health={health} onRefresh={refreshHealth} />
        <ActiveStreamsPanel   health={health} />
        <KafkaHealthPanel    health={health} />
      </div>

      {/* Full-width camera grid */}
      <CameraStatusSummary
        cameraStatus={cameraStatusData?.cameras ?? {}}
        liveStreams={liveStreams}
      />
    </div>
  );
};

export default StreamingAdmin;
```

---

## Step 5 — Frontend: Router + Navigation

### 5.1 Update `frontend/src/routers/router.jsx`

Thêm import và route cho `/admin/streaming`:

```jsx
// Thêm import:
import { lazy } from 'react';
import { IS_ADMIN } from '../config/mode';
import StreamingAdmin from '../pages/admin/StreamingAdmin';

// Thêm vào children array:
...(IS_ADMIN ? [
  {
    path: '/admin/streaming',
    element: <StreamingAdmin />,
  },
] : []),
```

### 5.2 Update `frontend/src/components/layout/SideBar.jsx`

Thêm section Admin trong navigation:

```jsx
// Thêm import:
import { IS_ADMIN } from '../../config/mode';
import { Radio } from 'lucide-react';

// Trong component SideBar, thêm admin nav items:
{IS_ADMIN && (
  <div className="mt-4 pt-4 border-t border-slate-800">
    <p className="text-xs text-slate-600 uppercase tracking-wider px-3 mb-2">Admin</p>
    <NavLink
      to="/admin/streaming"
      className={({ isActive }) =>
        `flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors
         ${isActive
           ? 'bg-emerald-500/15 text-emerald-400'
           : 'text-slate-400 hover:bg-slate-800 hover:text-white'}`
      }
    >
      <Radio size={16} />
      Streaming Admin
    </NavLink>
  </div>
)}
```

---

## Step 6 — Testing Checklist

Sau khi hoàn thành tất cả steps trên, chạy theo thứ tự:

### Pre-flight
```bash
# 1. Build lại chatbot (sau khi thêm docker SDK)
docker compose -f docker/docker-compose.yml build chatbot

# 2. Restart chatbot
docker compose -f docker/docker-compose.yml up -d chatbot

# 3. Kiểm tra chatbot healthy
docker compose -f docker/docker-compose.yml ps chatbot
```

### Backend tests
```bash
# Test pipeline-health endpoint
curl -s http://localhost:5002/api/admin/pipeline-health | python3 -m json.tool

# Kết quả expected:
# {
#   "containers": {
#     "mediamtx": {"status": "running", "health": "..."},
#     "rtsp_pusher": {"status": "running" | "exited", ...},
#     "rtsp-inference-mock": {...}
#   },
#   "streams": {"mediamtx_ok": true, "active": ["cam_01", ...], "count": 4},
#   "kafka": {"ok": true, "total_messages": 12345, ...}
# }

# Test stop-streaming
curl -s -X POST http://localhost:5002/api/admin/stop-streaming | python3 -m json.tool
# Expected: {"action": "stop", "results": {"rtsp_pusher": "stop_signal_sent", ...}}

# Chờ 5 giây rồi test start
curl -s -X POST http://localhost:5002/api/admin/start-streaming | python3 -m json.tool
# Expected: {"action": "start", "results": {"rtsp_pusher": "started", ...}}
```

### Frontend tests
```bash
# 1. Start dev server ở admin mode
cd Violence-Urban-Safety-UI/frontend
npm run dev    # loads .env.admin → VITE_APP_MODE=admin

# 2. Mở trình duyệt: http://localhost:5174/admin/streaming
# Kiểm tra:
# ✅ Sidebar có mục "Streaming Admin"
# ✅ 3 containers hiển thị status (running/exited)
# ✅ MediaMTX active streams count đúng
# ✅ Kafka total messages hiển thị số thật
# ✅ 15 camera badges với màu đúng (NORMAL/ALERT/OFFLINE)
# ✅ Nút "Start Streaming" → containers chuyển sang running
# ✅ Nút "Stop Streaming" → containers chuyển sang exited sau ~10s
# ✅ Refresh button cập nhật data
```

### Integration test
```bash
# Mở 2 cửa sổ:
# Window 1: localhost:5174/admin/streaming  (StreamingAdmin)
# Window 2: localhost:5174/               (LiveStreams)

# Test flow:
# 1. Click "Stop Streaming" → StreamingAdmin shows exited
# 2. Chuyển sang LiveStreams → cameras show "Pipeline offline"
# 3. Quay lại StreamingAdmin → click "Start Streaming"
# 4. Chờ 10-15s → streams count tăng
# 5. Chuyển sang LiveStreams → WebRTC players load
```

---

## Thứ tự thực hiện (gợi ý)

```
[Step 1]  5 phút    → requirements.txt + docker-compose.yml (Docker socket)
[Step 2] 20 phút    → main.py (3 endpoints: pipeline-health, start, stop)
[Step 3]  5 phút    → mode.js + .env.admin + .env.production + vite.config.js
[Step 4] 30 phút    → StreamingAdmin.jsx (toàn bộ component)
[Step 5]  5 phút    → router.jsx + SideBar.jsx
[Step 6] 15 phút    → Testing & fix bugs
                     ──────────────────
Total:  ~1h 20 phút
```

---

## Files cần tạo mới / sửa — tổng hợp

| File | Action | Ghi chú |
|------|--------|---------|
| `docker/requirements.txt` | Edit | Thêm `docker>=7.0.0` |
| `docker/docker-compose.yml` | Edit | Mount `/var/run/docker.sock` vào chatbot |
| `scripts/chatbot/main.py` | Edit | Thêm 3 endpoint admin (sau dòng ~780) |
| `frontend/src/config/mode.js` | **Create** | IS_ADMIN + API_BASE |
| `frontend/.env.admin` | **Create** | VITE_APP_MODE=admin |
| `frontend/.env.production` | **Create** | VITE_APP_MODE=public |
| `frontend/.env.example` | **Create** | Template |
| `frontend/vite.config.js` | Edit | Dev server proxy |
| `frontend/package.json` | Edit | Thêm `dev:public`, `build:admin` scripts |
| `frontend/src/pages/admin/StreamingAdmin.jsx` | **Create** | Trang admin chính |
| `frontend/src/routers/router.jsx` | Edit | Thêm route `/admin/streaming` |
| `frontend/src/components/layout/SideBar.jsx` | Edit | Thêm Admin nav section |

---

*Created: 2026-05-23*  
*Author: Claude Code — Plan B Phase 1 Implementation Guide*
