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
curl -X POST http://34.21.199.109:5002/chat \
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
- GCP VM IP: `34.21.199.109` (có thể thay đổi sau VM restart)
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

### 📍 Last State (Updated: 2026-05-25 — Session 48) ✅ Trino+Paimon WARM Connector VERIFIED

- **Agent vừa làm:** Claude (Session 48 — Trino+Paimon connector, TIERING_HOURS fix, GCP deploy)
- **Trạng thái:** Trino+Paimon WARM queries working (local + GCP) ✅
- **Nhánh git:** `deploy/hybrid-cloud` (commit `5c6d8f3`)
- **GCP VM:** `instance-20260524-104630` — **ĐANG CHẠY** (IP: `34.87.122.219` ← **IP MỚI**)

#### ✅ Session 48 — Đã hoàn thành

| Hạng mục | Chi tiết |
|----------|---------|
| `trino-with-paimon` image (local) | `trino-with-s3a:latest` đã có paimon plugin (3.49GB) ✅ |
| `trino-with-paimon` image (GCP) | Built từ `Dockerfile.trino` (Maven 21, paimon 0.8, HDFS patch) ✅ |
| `SHOW CATALOGS` local+GCP | `paimon` xuất hiện ✅ — kết nối MinIO S3 native ✅ |
| Paimon data local | `paimon.security.violence_incidents` = 15,834 rows ✅ |
| WARM latency benchmark | **~13s avg** (vs 3–5 phút qua Flink Gateway = **14–23× speedup**) ✅ |
| COLD latency benchmark | ~12–20s (Iceberg via Trino, cold start 20s) |
| HOT routing | `layer=Fluss` ✅ (empty data → timeout 57s; với real data ~14s) |
| `TIERING_HOURS` fix | 2 → 1 (commit `c432e77`) — đóng data gap 1h–2h ✅ |
| GCP IP updated | `34.21.199.109` → `34.87.122.219` (VM restarted) ✅ |
| `trino_client.py` WARM routing | Đã dùng Trino native (implemented trước session 48) ✅ |
| GCP pipeline | All 3 streaming jobs RUNNING: ContractValidator, hot_violence_alerts, daily_incident_stats ✅ |
| RTSP → GCP Kafka | `rtsp-inference-mock` gửi events đến `34.87.122.219:9093` ✅ |

#### ⚠️ Lưu ý quan trọng

- **GCP IP mới**: `34.87.122.219` (thay cho `34.21.199.109`). Cập nhật mọi lần VM restart.
- **HOT latency thực tế**: Với real data, HOT query ~14–25s E2E (Flink Gateway session + LLM). Con số 35–130ms trong memory cũ là Flink execution time thôi, không phải E2E.
- **WARM latency**: 13s E2E = ~5s Trino query + ~8s Gemini LLM (Text-to-SQL + answer).
- **trino-with-s3a vs trino-with-paimon**: Local image tên `trino-with-s3a`, GCP tên `trino-with-paimon`. Cùng một Dockerfile.trino — đều có paimon plugin.
- **dim_camera**: Seeded thành công local (15 cameras) qua SQL Gateway REST. GCP seeded tự động bởi pipeline-manager.
- **Paimon data GCP = 0**: Fresh start (VM bị TERMINATED trước đó). Data sẽ accumulate khi pipeline chạy.

#### 🚀 Bước tiếp theo (Session 49)

1. **HOT benchmark với real data**: Chạy local RTSP pipeline hướng GCP Kafka ~30 phút, rồi test HOT query → đo latency thực.
2. **Tiering test**: Sau khi có HOT data, trigger `tier_fluss_to_paimon.py` thủ công, verify WARM tăng.
3. **Thesis benchmark table** (3 layers × 3 runs):
   - HOT (Fluss via Gateway): target <5s với warm session + real data
   - WARM (Paimon via Trino): confirmed ~13s E2E ✅
   - COLD (Iceberg via Trino): confirmed ~12–20s E2E ✅
4. **Demo script**: Prepare for thesis defense — sequence of chatbot queries showcasing 3 layers.

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
curl -X POST http://34.21.199.109:5002/chat \
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
GCP VM:           RUNNING (instance-20260524-104630, asia-southeast1-b, IP: 34.21.199.109)
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

