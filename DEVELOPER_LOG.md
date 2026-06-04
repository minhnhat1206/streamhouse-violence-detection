# 🤖 Joint Agent Mission Control

## 🎯 Current Global Objective
Xây dựng hệ thống phát hiện bạo lực thời gian thực (**Streamhouse Trio** — Fluss/Paimon/Iceberg).
Đây là **Khóa luận tốt nghiệp** của Nguyễn Ngọc Minh Nhật & Nguyễn Quốc Huy.

## 📋 Project Context
> **BẮT BUỘC ĐỌC** trước khi bắt đầu: `docs/PROJECT_CONTEXT.md`
> Chứa toàn bộ trạng thái services, ports, tiến độ, phân công, và Docker commands.

## 🤝 Handover Protocol
*Mỗi khi kết thúc phiên làm việc, Agent PHẢI cập nhật phần "Last State" bên dưới.*

---

### 🗺️ Last State — Session 2026-06-04 (Bug Fixes + Full Test) ✅ COMPLETED

**Branch:** `deploy/hybrid-cloud`
**GCP VM:** `34.124.131.144` (RUNNING), static IP

#### Services Status (GCP)
| Service | Status |
|---------|--------|
| kafka, minio, flink, fluss, chatbot | ✅ UP |
| grafana (port 3001), prometheus | ✅ UP |
| rtsp pipeline (mediamtx + rtsp_pusher + rtsp-inference-mock) | ✅ UP |
| frame-extractor | ✅ UP (MinIO evidence-frames ~7k+ files) |
| pipeline-manager | ✅ UP (2 Flink jobs RUNNING) |

#### Data State
- **HOT (Fluss)**: ~25k rows, 106ms latency
- **WARM (Paimon)**: 366,135 rows (2026-05-25 → 2026-06-04)
- **COLD (Iceberg)**: 10,312 rows (historical)
- **MinIO evidence-frames**: ~7,000+ JPEG thumbnails (cam_01–cam_15, 2026-06-04)

#### Bugs Fixed This Session
| Bug | Fix |
|-----|-----|
| Grafana "Error loading: stat" (Trino plugin unavailable) | Migrate tất cả panels sang Prometheus custom gauges |
| Grafana violence-security-monitor + analytics "No data" | Rebuild với Prometheus datasource |
| Evidence chatbot trả 20 ảnh random | Fix: query MinIO theo camera_id+date từ Paimon, respect count limit |
| Evidence: UUID mismatch (Paimon ≠ MinIO) | Dùng DISTINCT camera_id+DATE, list actual MinIO files |
| Evidence: Deadlock asyncio (HTTP self-call) | Dùng `_trino_client.query_paimon()` trực tiếp |
| GCP firewall block MinIO port 9000 | Tạo rule `streamhouse-minio` allow tcp:9000,9001 |
| MinIO credentials mismatch (minio/mypassword vs minioadmin/minioadmin) | Update `.env.gcp` |
| HOT count null in /api/layer-counts | Fix metric param `0.numRecordsIn` + return 0 thay vì None |
| Chatbot hallucination khi row_count=0 | Guard + anti-hallucination prompt |

#### Grafana Dashboards (All Working)
| Dashboard | UID | Datasource |
|-----------|-----|-----------|
| Violence Incidents Analytics | violence-incidents-v2 | Prometheus + Infinity |
| Security Monitor | violence-security-monitor | Prometheus |
| Violence Analytics | violence_analytics | Prometheus |
| Chatbot Performance | chatbot-metrics | Prometheus |
| Streamhouse Architecture | streamhouse-arch-001 | Prometheus |

**Prometheus metrics refreshed every 5 min:**
- `violence_incidents_24h_total = 160,404`
- `violence_incidents_7d_total = 182,982`
- `violence_cameras_active = 15`
- `violence_incidents_by_type{event_type=...}` (4 types)
- `violence_incidents_by_camera{camera_id=...}` (15 cameras)
- `streamhouse_hot/warm/cold_rows_total`

#### Test Results (5/5 PASS)
| Query | Layer | Result |
|-------|-------|--------|
| "1 ảnh đường Nguyễn Huệ" | WARM | 1 ảnh thật ✅ |
| "5 ảnh Hàm Nghi" | WARM | "Không tìm thấy" (đúng) ✅ |
| "3 ảnh gần đây" | WARM | 3 ảnh thật ✅ |
| "15 phút qua bao nhiêu alert?" | HOT · Fluss | 100 alerts ✅ |
| "Camera nguy hiểm nhất 7 ngày?" | WARM · Paimon | cam_15 ✅ |

#### Important Notes for Next Session
- **MinIO credentials on GCP**: `minioadmin/minioadmin` (NOT `minio/mypassword` như trong `.env.gcp` cũ)
- **Prometheus refresh**: auto mỗi 5 phút. Manual trigger: `POST /api/grafana/refresh-metrics`
- **Evidence images**: MinIO port 9000 đã mở public. Frame URL pattern: `34.124.131.144:9000/evidence-frames/{cam}/{YYYY-MM-DD}/{uuid}.jpg`
- **RTSP pipeline data files** trên GCP: chỉ có ~4 Fight clips (local upload) — đủ để test
- **Grafana dashboard URL**: `http://34.124.131.144:3001/d/violence-incidents-v2`
- **Local UI**: `Violence-Urban-Safety-UI/frontend/` → `npm run dev` (port 5173)

---

### 🗺️ Plan Session 46 — Local RTSP → GCP Kafka ✅ COMPLETED

> **Kết quả:** Toàn bộ P0–P3 hoàn thành. RTSP pipeline local verified E2E.
> HLS player trên Vercel ready (chỉ cần ngrok để dùng).

---

### 🗺️ Plan Session 47 — Vercel HLS Live Demo + Thesis

> **Mục tiêu:** Demo đầy đủ với live video trên Vercel, viết báo cáo thesis.

#### Bước 1 — Bật ngrok expose HLS
```bash
# Cài ngrok nếu chưa có: https://ngrok.com/download
# Expose local MediaMTX HLS port:
ngrok http 8888

# Copy HTTPS URL (e.g., https://xxxx.ngrok-free.app)
# Vào Vercel app → Settings page → dán URL → Save
```

#### Bước 2 — Start local RTSP + ngrok
```bash
# Start local RTSP stack
docker compose -f docker/docker-compose.local-stream.yml up -d

# Start ngrok (cửa sổ riêng)
ngrok http 8888

# Mở Vercel app, vào Settings, paste ngrok URL, Save
# Live Streams page sẽ tự load HLS streams
```

#### Bước 3 — Verify E2E demo
```bash
# Query GCP chatbot
curl -X POST http://34.124.131.144:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"camera nao co canh bao trong 30 phut qua?"}'

# Stop sau demo:
docker exec rtsp-inference-mock touch /app/tmp/STOP
docker exec rtsp_pusher touch /app/tmp/STOP
```

#### Bước 4 — Thesis work (P4)
- Benchmark: GCP vs local latency (HOT <100ms target)
- Architecture diagrams (Streamhouse Trio flow)
- Performance metrics section

#### Lưu ý QUAN TRỌNG
- ngrok URL thay đổi mỗi lần restart → cần update lại trong Settings page
- GCP VM IP: `34.124.131.144` (có thể thay đổi sau VM restart)
- Local RTSP stack: `docker compose -f docker/docker-compose.local-stream.yml up -d`
- KHÔNG dùng `send_test_events.py` để demo (fake data)

---

### 🗺️ Plan Session 45 — Local RTSP Stream → GCP Kafka ✅ COMPLETED

> **Mục tiêu:** Chạy RTSP pipeline trên máy local, gửi inference events lên GCP Kafka.
> Vercel HLS display để session sau.

#### Bối cảnh & phát hiện

| Hạng mục | Trạng thái |
|----------|-----------|
| `config/mediamtx/mediamtx.yml` — HLS port 8888 | ✅ Đã bật (`hlsAlwaysRemux: yes`, segment 1s) |
| `config/mediamtx/mediamtx.yml` — WebRTC port 8889 | ✅ Đã bật |
| `rtsp_inference_mock.py` — timestamp `.isoformat()` | ✅ Đã fix (không cần sửa) |
| `rtsp_inference_mock.py` — `KAFKA_BROKER` env var | ✅ Đọc từ env, default `kafka:9092` |
| GCP Kafka port 9093 accessible từ local | ✅ Đã verify (send_test_events.py) |
| `docker-compose.yml` streaming profile | ⚠️ `rtsp-inference-mock` depends_on `kafka` local |

**Vấn đề chính:** Service `rtsp-inference-mock` trong `docker/docker-compose.yml` có `depends_on: kafka` → không start được nếu không kéo cả local Kafka lên.

#### Kế hoạch implement (Session 45)

**Bước 1 — Tạo `docker/docker-compose.local-stream.yml`**

File compose riêng, chỉ 3 services, không cần local Kafka:
```yaml
services:
  mediamtx:           # RTSP server + HLS (image sẵn có)
  rtsp_pusher:        # ffmpeg đẩy RWF-2000 clips → MediaMTX
  rtsp-inference-mock:
    environment:
      KAFKA_BROKER: 136.110.16.108:9093   # → GCP Kafka trực tiếp
    # KHÔNG có depends_on kafka
```

Lệnh chạy:
```bash
docker compose -f docker/docker-compose.local-stream.yml up -d
# Stop:
docker exec rtsp-inference-mock touch /app/tmp/STOP
docker exec rtsp_pusher touch /app/tmp/STOP
```

**Bước 2 — Start GCP VM + verify pipeline**
```bash
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"
"$GCLOUD" compute instances start instance-20260524-104630 --zone=asia-southeast1-b
# Chờ ~2 phút → start services
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b --command='
  cd ~/streamhouse
  docker compose -f deploy/docker-compose.gcp.yml --env-file deploy/.env.gcp up -d
'
# Fix S3 plugin nếu containers bị recreate (xem bước trong Session 44 state bên dưới)
```

**Bước 3 — Test E2E**
```bash
# Verify events vào GCP Kafka (local terminal)
docker logs rtsp-inference-mock | tail -20
# Expected: [cam_01] VIOLENCE | score=0.92x hoặc Normal | score=0.0xx

# Verify HOT layer trên GCP (sau ~2 phút)
curl -X POST http://136.110.16.108:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"camera nao co canh bao trong 30 phut qua?"}'
# Expected: "layer": "Fluss", data thật từ RTSP stream local
```

**Bước 4 — Verify chatbot fix (session 44 pending)**
- Nếu chatbot vẫn lỗi `HTTPConnectionPool(host='jobmanager')`:
  ```bash
  "$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b --command='
    docker exec chatbot sed -i "s/default=\"jobmanager\"/default=\"flink-sql-gateway\"/" \
      /app/scripts/chatbot/config.py
    docker restart chatbot
  '
  ```

#### Deferred (Session 46+) — Vercel HLS Display
- ngrok expose port 8888 → public HTTPS URL
- React Camera Grid: thêm `hls.js` player, đọc `https://<ngrok>/cam_XX/index.m3u8`
- Config: env var `VITE_HLS_BASE_URL` cho ngrok URL
- Không cần deploy lại Vercel mỗi lần (ngrok URL thay đổi) → có thể dùng settings page

---

### 📍 Last State (Updated: 2026-05-26 — Session 51) ✅ GCP Tiering VERIFIED + Commit DONE

- **Agent vừa làm:** Claude (Session 51 — verify GCP tiering fix, Paimon data confirmed, taskmanager rebuilt)
- **Trạng thái:** GCP pipeline STABLE. Tất cả 3 Flink jobs RUNNING. Paimon có 10,226 rows. Commit `c37fa4b` pushed.
- **Nhánh git:** `deploy/hybrid-cloud` (clean — đã commit `deploy/docker-compose.gcp.yml`)
- **GCP VM:** `instance-20260524-104630` — **ĐANG CHẠY** (IP: `34.124.131.144`)

---

#### ✅ Session 51 — Đã hoàn thành

| Hạng mục | Chi tiết |
|----------|---------|
| Tiering verified ✅ | `✓ Tiering job completed successfully.` @ 03:41:42 UTC (4 phút sau trigger 03:37:55) |
| Paimon data verified ✅ | `SELECT COUNT(*) FROM violence_incidents` = **10,226 rows** |
| Taskmanager image rebuild ✅ | `deploy-taskmanager:latest` built successfully — S3 plugin baked in |
| Taskmanager recreated ✅ | `docker compose up -d --force-recreate taskmanager` — new image active |
| 3 Flink jobs RUNNING ✅ | Contract Validator + hot_violence_alerts (Fluss) + daily_incident_stats (Paimon) |
| Git commit ✅ | `c37fa4b` — `fix(gcp): add shared fluss-remote-data volume for tiering coordinator-tablet-taskmanager` |

#### ✅ Session 50 — Đã hoàn thành

| Hạng mục | Chi tiết |
|----------|---------|
| Root cause GCP tiering fail | `_METADATA` files ở coordinator `/tmp/fluss-remote`; taskmanager đọc từ TMP riêng (không shared) ✅ Đã xác định |
| Fix: Shared Docker volume | Thêm `fluss-remote-data` vào `deploy/docker-compose.gcp.yml` — coordinator + tablet + taskmanager mount cùng `/var/fluss/remote-data` ✅ |
| `_METADATA` files verified | `/var/fluss/remote-data/kv/security/hot_violence_alerts-14/*/snap-0/_METADATA` — tất cả 3 buckets ✅ |
| Secondary: S3 plugin missing | `deploy-taskmanager` image (built 2026-05-24) thiếu S3 plugin trong `/opt/flink/plugins/`. Quick-fix: copy jar + restart taskmanager ✅ |
| Taskmanager rebuild | Completed in Session 51 ✅ |
| 3 Flink jobs RUNNING | Contract Validator + sink_to_fluss_enriched + aggregate_paimon ✅ |
| Tiering triggered | Completed in Session 51 ✅ — 10,226 rows in Paimon |

#### 🔍 Session kế — VIỆC CẦN LÀM TIẾP THEO

> **Tất cả checklist Session 50 đã hoàn thành trong Session 51.** GCP pipeline stable.

**[P1] Thesis writeup — Performance Evaluation chapter:**
- Dùng benchmark table từ Session 49 (bên dưới)
- HOT 100ms native ✅, WARM 5.9s ✅, COLD 9.5s ✅
- Ghi rõ: Chatbot E2E = Gemini intent (~8s) + ChromaDB (~1s) + query + Gemini answer (~8s)

**[P2] Demo script cho buổi bảo vệ:**
```
Q1: "Camera nào có cảnh báo bạo lực trong 30 phút qua?" → HOT (Fluss)
Q2: "Thống kê bạo lực trong 3 giờ qua?" → WARM (Paimon)
Q3: "Dữ liệu tháng trước?" → COLD (Iceberg)
```

**[P3] HLS Live Streams — chạy local, chiếu màn hình (CHỐT):**
```bash
# Terminal 1: Start RTSP → GCP Kafka
docker compose \
  -f docker/docker-compose.local-stream.yml \
  -f docker/docker-compose.gcp-stream.yml \
  up -d

# Terminal 2: Chạy frontend local
cd Violence-Urban-Safety-UI/frontend && npm run dev
# Mở http://localhost:5173 → Settings → HLS URL = http://localhost:8888 → Save
```
> Không dùng Vercel/ngrok. Chiếu màn hình laptop lên projector. Chi tiết: `docs/DEMO_SCRIPT.md`

**[GCP restart] Quy trình nếu VM bị stop:**
```bash
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"
"$GCLOUD" compute instances start instance-20260524-104630 --zone=asia-southeast1-b
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b --strict-host-key-checking=no --command='
  cd ~/streamhouse/deploy
  docker compose -f docker-compose.gcp.yml --env-file .env.gcp up -d
'
# Chờ ~5 phút cho dim_camera seed + Flink jobs start
```

#### ⚠️ Root Cause Analysis (quan trọng — đọc nếu troubleshoot tiếp)

**GCP Fluss KvSnapshotNotExistException (đã fix):**
- `_METADATA` file path: `file:///tmp/fluss-remote/kv/security/hot_violence_alerts-12/0/snap-4/_METADATA`
- Lý do fail: `coordinator` write `_METADATA` vào `/tmp/fluss-remote` của CONTAINER MÌNH; `taskmanager` (Flink) đọc từ `/tmp/fluss-remote` của CONTAINER MÌNH — hai container có `/tmp` riêng biệt, không shared!
- Tại sao local OK: Local dùng named Docker volume `fluss-tablet-remote` mounted tại `/var/fluss/remote-data` cho CẢ coordinator + tablet + taskmanager → share cùng 1 filesystem.
- Fix đã apply: Thêm `fluss-remote-data` named volume vào `deploy/docker-compose.gcp.yml` với mount tại `/var/fluss/remote-data` cho cả 3 containers; đổi `remote.data.dir` từ `file:///tmp/fluss-remote` → `/var/fluss/remote-data`.

**GCP S3 plugin thiếu (ĐÃ FIX PERMANENT — Session 51):**
- `deploy-taskmanager` image rebuilt + recreated trong Session 51 — S3 plugin baked in ✅
- Không cần manual patch nữa

#### 📊 GCP State khi kết thúc Session 51 (UTC ~04:10)

| Component | State |
|-----------|-------|
| GCP VM IP | `34.124.131.144` |
| Kafka topics | `hot-violence-alerts-valid`: growing (continuous) |
| Contract Validator | RUNNING ✅ |
| sink_to_fluss_enriched | RUNNING ✅ |
| aggregate_paimon | RUNNING ✅ |
| Fluss HOT | `_METADATA` files on shared volume ✅ |
| Paimon WARM | **10,226 rows** — verified Session 51 ✅ |
| Taskmanager image rebuild | **deploy-taskmanager:latest** built + recreated — Session 51 ✅ |

#### ✅ Session 49 — Đã hoàn thành

| Hạng mục | Chi tiết |
|----------|---------|
| HOT real data | Fluss `hot_violence_alerts` = **4,995 rows** (sink_to_fluss_enriched đang chạy) ✅ |
| HOT benchmark (chatbot) | warm session: **32–44s** E2E; cold/no-data: ~60s timeout ✅ |
| HOT pipeline verified | rtsp-inference-mock → kafka:9092 → ContractValidator → Fluss ✅ |
| WARM benchmark (Trino direct) | warm: **11–13s**; first call: ~16s ✅ |
| COLD benchmark (Trino direct) | warm: **8–11s**; first call: ~10.5s ✅ |
| `/api/latency` truth | HOT native=**100ms**, WARM=**5.9s**, COLD=**9.5s** ✅ |
| `/api/layer-counts` | HOT=4,995 / WARM=15,834 / COLD=15,834 (duration=7.4s) ✅ |
| Tiering test | Pipeline-manager auto-tiered @ 15:24 (6 min), completed ✅. WARM count = 15,834 (unchanged — same incident_ids, deduplicate upsert — ĐÚNG) |
| GCP pipeline | Kafka rebuilt (KRaft cleared), all 7 topics OK, 3 Flink jobs RUNNING ✅ |
| GCP topics | urban-safety-alerts, hot-violence-alerts-valid, và 5 topic khác ✅ |

#### 📊 Thesis Benchmark Table (Session 49 — final numbers)

| Layer | Công nghệ | Storage Latency (API) | Direct Query (Trino/Gateway) | Chatbot E2E (warm) |
|-------|-----------|----------------------|-----------------------------|--------------------|
| HOT | Fluss | **100ms** | ~8s (SQL Gateway LIMIT scan) | **32–44s** |
| WARM | Paimon + Trino | **5.9s** | 11–16s (cold→warm) | **35–41s** |
| COLD | Iceberg + Trino | **9.5s** | 8–11s | **31–35s** |

> **Ghi chú cho thesis:**
> - "Storage Latency" = thời gian query thuần (không có LLM), đo từ `/api/latency`
> - HOT native 100ms là target thiết kế của Fluss được đạt ✅
> - Chatbot E2E gồm: Gemini intent (~8s) + ChromaDB retrieval (~1s) + query + Gemini answer (~8s)
> - WARM 14–23× faster hơn Flink Gateway cũ (3–5 phút → 6s)

#### ⚠️ Lưu ý quan trọng (Session 48–49)

- **GCP IP mới**: `34.124.131.144` (thay cho `34.124.131.144`). Cập nhật mọi lần VM restart.
- **HOT data issue**: rtsp-inference-mock trỏ về `kafka:9092` (local). Old data trong Kafka có timestamp cũ → chatbot filter "30 phút" sẽ thấy data chỉ khi rtsp mới chạy đủ lâu.
- **WARM latency**: 6s (API level) = ~5s Trino + ~1s overhead. Chatbot E2E 35s = thêm 2× Gemini (8+8s) + ChromaDB.
- **GCP Kafka KRaft issue**: Sau `TERMINATED`, `listTopics` timeout → fix bằng cách clear `/tmp/kafka-logs/` và restart kafka container.
- **dim_camera local**: Seeded 15 cameras qua SQL Gateway REST. GCP seeded tự động bởi pipeline-manager.
- **Paimon data (local)**: 15,834 rows từ Session 38–39. GCP Paimon bắt đầu accumulate sau khi pipeline chạy đủ lâu.

#### 🚀 Bước tiếp theo (Session 50)

1. **Thesis writeup**: Dùng benchmark table bên trên cho chapter Performance Evaluation.
2. **Demo script cho bảo vệ**:
   ```
   Q1: "Camera nào có cảnh báo bạo lực trong 30 phút qua?" → HOT (Fluss)
   Q2: "Thống kê bạo lực trong 3 giờ qua?" → WARM (Paimon)
   Q3: "Dữ liệu tháng trước?" → COLD (Iceberg)
   ```
3. **GCP WARM data**: Để pipeline GCP chạy 1–2 ngày để accumulate Paimon data → verify tiering GCP.
4. **Architecture diagram**: Cập nhật diagram với Trino+Paimon native connector (thay Flink Gateway cho WARM).

---

#### ✅ Đã hoàn thành (session 43–46)

| Hạng mục | Chi tiết |
|----------|---------|
| Kafka external port 9093 | Local → GCP Kafka hoạt động ✅ |
| Contract Validator | Valid events → `hot-violence-alerts-valid` ✅ |
| HOT job Fluss | Enriched location, 15 cameras ✅ |
| dim_camera seeding | 15 cameras, Quận 1 HCM, temporal join ✅ |
| flink-sql-gateway | rest.address + FLINK_GATEWAY_HOST fixed ✅ |
| S3 plugin — PERMANENT | Baked into `docker/Dockerfile.flink` (commit `431c60b`), GCP rebuilt ✅ |
| Chatbot | "30 phut qua" → Fluss → 15 camera locations, layer=Fluss ✅ |
| RTSP E2E test | `docker compose -f docker/docker-compose.local-stream.yml up -d` → 15 cameras → GCP Kafka → Fluss → chatbot 15 rows ✅ |
| GCP VM git sync | `git pull` + `docker compose build jobmanager chatbot` + containers restarted ✅ |
| HLS player (Vercel) | `HLSPlayer.jsx` (hls.js), Settings page ngrok URL input, localStorage persist ✅ |
| Admin API | Standalone `admin-api` service (port 5003, profile admin) for RTSP start/stop ✅ |

**RTSP E2E test PASS (session 46):**
- `docker compose -f docker/docker-compose.local-stream.yml up -d` → 15 cameras live
- Violence events generated by rtsp-inference-mock → GCP Kafka
- Chatbot: "camera nao co canh bao trong 30 phut qua?" → **15 rows, layer=Fluss** ✅

---

#### ⚠️ Trạng thái local

- **Local RTSP stack**: `mediamtx + rtsp_pusher + rtsp-inference-mock` đang chạy (session 46 test)
- **Stop khi xong**: `docker exec rtsp-inference-mock touch /app/tmp/STOP && docker exec rtsp_pusher touch /app/tmp/STOP`

---

#### 🔄 Quy trình khởi động lại GCP VM

```bash
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"

# 1. Start VM
"$GCLOUD" compute instances start instance-20260524-104630 --zone=asia-southeast1-b

# 2. Chờ ~30s rồi SSH
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b --strict-host-key-checking=no --command='
  cd ~/streamhouse/deploy
  docker compose -f docker-compose.gcp.yml --env-file .env.gcp up -d
'
# NOTE: S3 plugin đã baked trong image — KHÔNG cần exec-fix nữa
# Chờ ~5 phút để pipeline-manager seed dim_camera và submit Flink jobs

# 3. Verify chatbot (sau ~5 phút)
curl -X POST http://34.124.131.144:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"camera nao co canh bao trong 30 phut qua?"}'
```

---

#### 📂 Files đã thay đổi (session 46, đã commit)

| File | Thay đổi |
|------|---------|
| `scripts/admin/main.py` + `index.html` | MỚI — admin-api service |
| `docker/Dockerfile.admin` | MỚI — admin-api Docker image |
| `docker/docker-compose.yml` | Thêm admin-api service (profile admin) |
| `config/mediamtx/mediamtx.yml` | Auth config cho internal API access |
| `scripts/chatbot/main.py` | Xóa `/api/streaming-status` (moved to admin-api) |
| `Violence-Urban-Safety-UI/frontend/src/common/HLSPlayer.jsx` | MỚI — hls.js player |
| `Violence-Urban-Safety-UI/frontend/src/pages/LiveStreams.jsx` | Replace WebRTC → HLS, fix streaming-status |
| `Violence-Urban-Safety-UI/frontend/src/pages/Settings.jsx` | Add HLS URL config section |

---

#### 🎯 Next Steps (Session 46+)

1. **[P0]** Commit tất cả local changes lên git
2. **[P0]** Sync `deploy/docker-compose.gcp.yml` changes với GCP VM (để sau VM restart lại không cần manual patch)
3. **[P1]** Test local RTSP → GCP pipeline: `docker compose -f docker/docker-compose.local-stream.yml up -d`
4. **[P2]** Vercel HLS Display: ngrok expose port 8888, React Camera Grid với hls.js
5. **[P3]** Rebuild Flink images trên GCP để bake S3 plugin permanently
6. **[P4]** Thesis: benchmark GCP vs local latency, architecture diagrams, performance section

---

#### 📊 Stack state (cuối session 45 — VM ĐANG CHẠY)

```
GCP VM:           RUNNING (instance-20260524-104630, asia-southeast1-b, IP: 34.124.131.144)
HOT layer (Fluss): 60+ rows verified (session 45 test events)
WARM layer (Paimon): chưa có data (cần ~2h data để tier từ Fluss)
COLD layer (Iceberg): chưa có data (archive chỉ chạy 2:00 UTC)
Flink jobs:       3 RUNNING: Contract Validator, HOT sink, daily_incident_stats
dim_camera:       15 cameras, Quận 1 HCM, seeded ✅
Chatbot:          WORKING — Fluss routing verified (15 camera locations returned)
```

---

> **Lịch sử sessions cũ (Session 1–43):** Xem trong git history hoặc file `.claude/projects/.../memory/`.  
> Tóm tắt: Toàn bộ local stack hoàn thiện qua Sessions 18–43.  
> Session 43: Grafana/Prometheus setup, React UI (Analytics + StreamhouseStatus pages), 22/23 E2E tests PASS.  
> Session 40: Hard reset, RTSP pipeline sole data source, 9624+ HOT events, chatbot routing 100% correct.  
> Session 46: RTSP E2E verified (local → GCP → Fluss → chatbot). HLS player deployed to Vercel (hls.js + ngrok URL). S3 plugin baked permanently in Dockerfile.flink.

