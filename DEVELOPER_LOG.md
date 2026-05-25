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

### 🗺️ Plan Session 46 — Local RTSP → GCP Kafka (rtsp_inference_mock thay AI model)

> **Mục tiêu chính:** Chạy RTSP pipeline local (`rtsp_pusher` + `rtsp-inference-mock`) để gửi
> events lên GCP Kafka — thay cho `send_test_events.py` (fake hoàn toàn).
> `rtsp_inference_mock` capture frame thật từ RWF-2000 clips, tạo mock AI scores, gửi lên Kafka.
> AI model thật sẽ thay thế mock sau khi model training hoàn tất.

#### Bước 1 — Start local RTSP stack
```bash
# Tất cả images đã build sẵn (kiểm tra: docker-rtsp_pusher:latest, docker-rtsp-inference-mock:latest)
docker compose -f docker/docker-compose.local-stream.yml up -d

# Theo dõi:
docker logs rtsp-inference-mock -f
# Expected: [cam_01] VIOLENCE | score=0.92x hoặc Normal | score=0.0xx

# Stop sau khi test:
docker exec rtsp-inference-mock touch /app/tmp/STOP
docker exec rtsp_pusher touch /app/tmp/STOP
```

#### Bước 2 — Verify events lên GCP Kafka và HOT layer
```bash
# Sau ~2 phút, query chatbot:
curl -X POST http://34.21.199.109:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"camera nao co canh bao trong 30 phut qua?"}'
# Expected: layer=Fluss, danh sách cameras với violence alerts thật từ RTSP
```

#### Bước 3 (tùy chọn) — Vercel HLS Display
- ngrok expose port 8888 → public HTTPS URL
- React Camera Grid: thêm `hls.js` player, đọc `https://<ngrok>/cam_XX/index.m3u8`
- Config: env var `VITE_HLS_BASE_URL` cho ngrok URL

#### Lưu ý QUAN TRỌNG
- `send_test_events.py` là **fake hoàn toàn** (random, không có RTSP frame) — KHÔNG dùng để demo
- `rtsp_inference_mock.py` là **mock AI** nhưng có RTSP frame thật từ RWF-2000 dataset
- GCP VM IP có thể thay đổi sau mỗi lần restart — kiểm tra `gcloud compute instances list`
- GCP VM cần được start trước: `gcloud compute instances start instance-20260524-104630 --zone=asia-southeast1-b`

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

### 📍 Last State (Updated: 2026-05-25 — Session 45) ✅ HOT Pipeline + Chatbot VERIFIED

- **Agent vừa làm:** Claude (Session 45 — Local stream compose, GCP VM restart, chatbot E2E verify)
- **Trạng thái:** HOT pipeline WORKING ✅, chatbot Fluss routing + data VERIFIED ✅
- **Nhánh git:** `deploy/hybrid-cloud`
- **GCP VM:** `instance-20260524-104630` — **ĐANG CHẠY** (IP: `34.21.199.109`)

---

#### ✅ Đã hoàn thành (session 43–45, verified end-to-end)

| Hạng mục | Chi tiết |
|----------|---------|
| Kafka external port 9093 | Local → GCP Kafka hoạt động ✅ |
| Contract Validator | Valid events → `hot-violence-alerts-valid` ✅ |
| HOT job Fluss | `insert-into_fluss.security.hot_violence_alerts` — 60 rows với enriched location ✅ |
| dim_camera seeding | 15 cameras, Quận 1 HCM, temporal join hoạt động ✅ |
| flink-sql-gateway | `rest.address: jobmanager` fixed, FLINK_GATEWAY_HOST fixed ✅ |
| S3 plugin (exec-fix) | JAR copied vào tất cả Flink containers ✅ |
| Chatbot Fluss routing | "30 phut qua" → Fluss → **15 rows real data** ✅ |
| DISTINCT bug fix | `_adapt_sql_for_flink_hot` now strips SELECT DISTINCT → SELECT ✅ |
| `docker-compose.local-stream.yml` | Created for local RTSP → GCP Kafka (no local Kafka needed) |

**Pipeline test PASS:**
- 60 events gửi từ local `send_test_events.py` → GCP Kafka → Contract Validator → Fluss HOT (60 rows)
- Chatbot query: "camera nao co canh bao trong 30 phut qua?" → Layer=Fluss, 15 camera locations ✅

---

#### ❌ Bugs còn tồn đọng

**1. S3 plugin sẽ mất khi container RECREATE:**
- Docker images build TRƯỚC khi có S3 plugin fix trong Dockerfile
- Nếu container bị recreate → `UnsupportedFileSystemSchemeException: scheme 's3'`
- **Temporary fix** (chạy sau khi start containers):
  ```bash
  for C in jobmanager taskmanager flink-sql-gateway pipeline-manager; do
    docker exec $C bash -c 'mkdir -p /opt/flink/plugins/s3-fs-hadoop && cp /opt/flink/opt/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/' 2>/dev/null
  done
  docker restart jobmanager taskmanager flink-sql-gateway pipeline-manager
  ```
- **Permanent fix:** rebuild images từ `docker/Dockerfile.flink` (đã có RUN cp S3 jar)

**2. RTSP pipeline local chưa test end-to-end với GCP:**
- `docker/docker-compose.local-stream.yml` đã tạo nhưng chưa chạy end-to-end test
- `docker-rtsp_pusher:latest` image cần build trước (local image)
- Stop: `docker exec rtsp-inference-mock touch /app/tmp/STOP && docker exec rtsp_pusher touch /app/tmp/STOP`

---

#### 🔄 Quy trình khởi động lại GCP VM

```bash
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"

# 1. Start VM
"$GCLOUD" compute instances start instance-20260524-104630 --zone=asia-southeast1-b

# 2. Chờ ~30s rồi SSH
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b --strict-host-key-checking=no --command='
  cd ~/streamhouse
  docker compose -f deploy/docker-compose.gcp.yml --env-file deploy/.env.gcp up -d
'

# 3. Fix S3 plugin (quan trọng nếu containers bị recreate)
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b --strict-host-key-checking=no --command='
  for C in jobmanager taskmanager flink-sql-gateway pipeline-manager; do
    docker exec $C bash -c "mkdir -p /opt/flink/plugins/s3-fs-hadoop && cp /opt/flink/opt/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/" 2>/dev/null
  done
  docker restart jobmanager taskmanager flink-sql-gateway pipeline-manager
'

# 4. Chờ ~5 phút để pipeline-manager seed dim_camera và submit Flink jobs

# 5. Gửi test events từ local
python3 send_test_events.py   # GCP IP đã được cập nhật (34.21.199.109)

# 6. Verify chatbot (sau ~60s)
curl -X POST http://34.21.199.109:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"camera nao co canh bao trong 30 phut qua?"}'
# Expected: "layer": "Fluss", 15+ cameras/locations
```

---

#### 📂 Files đã thay đổi (session 45, cần commit)

| File | Thay đổi |
|------|---------|
| `docker/docker-compose.local-stream.yml` | MỚI — lightweight local RTSP → GCP Kafka compose |
| `send_test_events.py` | Update GCP IP: `136.110.16.108` → `34.21.199.109` |
| `scripts/chatbot/components/trino_client.py` | Fix SELECT DISTINCT bug (streaming vs KV scan) |
| `deploy/docker-compose.gcp.yml` | Fix sql-gateway FLINK_PROPERTIES + chatbot FLINK_GATEWAY_HOST |
| `scripts/chatbot/config.py` | `FLINK_GATEWAY_HOST` default `"jobmanager"` → `"flink-sql-gateway"` |

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

