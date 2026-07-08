# 🚀 Hướng dẫn Setup Streamhouse trên GCP VM (mới hoặc dựng lại)

> **Mục tiêu:** Có GCP VM mới (hoặc dựng lại từ đầu) → làm theo hướng dẫn này để có toàn bộ
> Streamhouse stack (Kafka, Flink, Fluss, Paimon, Iceberg, Trino, MinIO, Chatbot, Grafana)
> chạy đúng như hiện tại, với **star schema v2** tự khởi tạo.
>
> Bản GPU/Vast.ai xem: `vastVidStream/VASTAI_SETUP_GUIDE.md`. Checklist refactor v2:
> `docs/TASK_PHASES_V2.md`. Cập nhật lần cuối: 2026-07-08 (sau khi deploy Phase 1–3).

---

## 📋 Mục lục
1. [Thông tin hệ thống hiện tại](#1-thông-tin-hệ-thống-hiện-tại)
2. [Tạo VM + SSH](#2-tạo-vm--ssh)
3. [Setup VM lần đầu](#3-setup-vm-lần-đầu)
4. [Đưa code lên VM](#4-đưa-code-lên-vm)
5. [Tạo file .env.gcp (QUAN TRỌNG NHẤT)](#5-tạo-file-envgcp)
6. [Mở firewall](#6-mở-firewall)
7. [Cập nhật IP mới](#7-cập-nhật-ip-mới)
8. [Build images + khởi động stack](#8-build-images--khởi-động-stack)
9. [Schema v2 tự khởi tạo + migrate dữ liệu cũ](#9-schema-v2-tự-khởi-tạo--migrate-dữ-liệu-cũ)
10. [Kiểm tra toàn bộ (verify)](#10-kiểm-tra-toàn-bộ-verify)
11. [Vận hành thường ngày](#11-vận-hành-thường-ngày)
12. [Troubleshooting (sự cố ĐÃ GẶP THẬT)](#12-troubleshooting)

---

## 1. Thông tin hệ thống hiện tại

| Thông tin | Giá trị |
|---|---|
| Instance | `instance-20260524-104630`, zone `asia-southeast1-b` |
| Machine type | `e2-standard-4` (4 vCPU, 16GB RAM), Ubuntu 22.04, disk ≥ 100GB |
| External IP | `34.124.131.144` (VM mới sẽ KHÁC — xem mục 7) |
| Repo trên VM | `/home/user/streamhouse` (branch `refactor/star-schema-v2`) |
| Compose chính | `deploy/docker-compose.gcp.yml` + env file `deploy/.env.gcp` |
| Docker network | `violence-detection-net` (external — phải tạo trước) |

**Port public (firewall):**

| Port | Service | Ai dùng |
|---|---|---|
| 9093 | Kafka external listener | Producer trên Vast.ai |
| 5002 | Chatbot API | Web UI (`/api/chat`, dashboard endpoints) |
| 8082 | Trino (map 8080 trong container) | UI backend, debug |
| 3001 | Grafana (map 3000, subpath `/grafana-proxy/`) | Web UI iframe |
| 8081 | Flink JobManager UI | debug |
| 9000/9001 | MinIO API/Console | ảnh evidence (frame_url), debug |

---

## 2. Tạo VM + SSH

```bash
# Tạo VM (đổi tên/zone nếu cần)
gcloud compute instances create streamhouse-vm \
  --zone=asia-southeast1-b \
  --machine-type=e2-standard-4 \
  --image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud \
  --boot-disk-size=100GB

# SSH vào
gcloud compute ssh streamhouse-vm --zone=asia-southeast1-b
```

---

## 3. Setup VM lần đầu

Script có sẵn trong repo: cài Docker + compose plugin + tạo network.

```bash
# (sau khi code đã lên VM — hoặc copy riêng file này lên trước)
bash ~/streamhouse/deploy/gcp/setup-vm.sh
# Sau khi cài Docker: logout/login lại hoặc `newgrp docker`
```

Script làm: apt update → cài Docker → compose plugin → `docker network create violence-detection-net`.

---

## 4. Đưa code lên VM

Repo GitHub là **private** và VM **không có credentials** → 3 cách (đã dùng thật cách C):

**Cách A — git clone (nếu gắn deploy key / repo public):**
```bash
git clone https://github.com/minhnhat1206/streamhouse-violence-detection.git ~/streamhouse
cd ~/streamhouse && git checkout refactor/star-schema-v2
```

**Cách B — scp cả thư mục từ laptop:**
```bash
gcloud compute scp --recurse \
  '/home/dataguy/Documents/01 - Projects/KLTN/streamhouse-violence-detection' \
  streamhouse-vm:~/streamhouse --zone=asia-southeast1-b
```

**Cách C — git bundle (cách đang dùng để UPDATE code, nhẹ + giữ lịch sử git):**
```bash
# Trên laptop: đóng gói phần VM chưa có (VM_HEAD = commit hiện tại trên VM)
cd streamhouse-violence-detection
git bundle create /tmp/update.bundle refactor/star-schema-v2 --not <VM_HEAD>
gcloud compute scp /tmp/update.bundle streamhouse-vm:/tmp/ --zone=asia-southeast1-b

# Trên VM:
cd ~/streamhouse
git fetch /tmp/update.bundle refactor/star-schema-v2:refs/tmp/u
git merge --ff-only refs/tmp/u
```

> Nếu repo thuộc user khác (vd `/home/user/...` mà SSH bằng user khác):
> `sudo git config --global --add safe.directory /home/user/streamhouse` rồi thao tác bằng `sudo`.

---

## 5. Tạo file .env.gcp

**File này KHÔNG nằm trong git** (chứa secrets) — copy từ VM cũ hoặc tạo mới tại
`~/streamhouse/deploy/.env.gcp`:

```bash
# ── Bắt buộc ──────────────────────────────────────────────
GCP_VM_EXTERNAL_IP=<IP_MỚI>            # IP external của VM
MINIO_ROOT_USER=<user>                 # creds MinIO (hiện tại: minioadmin)
MINIO_ROOT_PASSWORD=<password>
GEMINI_API_KEY=<key>                   # chatbot Text-to-SQL
# ── Endpoint nội bộ (giữ nguyên) ─────────────────────────
MINIO_ENDPOINT=minio:9000
MINIO_SECURE=false
MINIO_EXTERNAL_URL=http://<IP_MỚI>:9000   # frame_url public cho browser
```

Copy từ VM cũ:
```bash
gcloud compute scp instance-20260524-104630:/home/user/streamhouse/deploy/.env.gcp \
  /tmp/.env.gcp --zone=asia-southeast1-b   # rồi scp sang VM mới
```

> ⚠️ **RULE QUAN TRỌNG NHẤT KHI VẬN HÀNH:** mọi lệnh `docker compose` PHẢI kèm
> `--env-file .env.gcp`. Sự cố 06–08/07/2026: recreate trino-coordinator không có
> env-file → biến `${MINIO_ROOT_USER:-minio}` trong compose rơi về default sai
> → S3 403 → toàn bộ WARM/COLD federation gãy 2 ngày. (Block override đã bị xoá
> khỏi service trino nhưng nhiều service khác vẫn còn pattern default này.)

---

## 6. Mở firewall

```bash
# Sửa GCP_PROJECT_ID trong file rồi chạy TỪ LAPTOP:
bash deploy/gcp/firewall.sh
# Hoặc tối thiểu:
for p in 9093 5002 8082 3001 8081 9000; do
  gcloud compute firewall-rules create streamhouse-$p \
    --allow=tcp:$p --source-ranges=0.0.0.0/0 --network=default
done
```

---

## 7. Cập nhật IP mới

IP cũ `34.124.131.144` còn hardcode ở các chỗ sau — VM mới phải đổi:

| File | Ghi chú |
|---|---|
| `deploy/.env.gcp` | `GCP_VM_EXTERNAL_IP`, `MINIO_EXTERNAL_URL` |
| `Violence-Urban-Safety-UI/frontend/vite.config.js` | ~14 chỗ (proxy grafana, trino…) |
| `scripts/streaming/rtsp_inference_mock.py` | default `KAFKA_BROKER` (đặt env khi chạy trên Vast là đủ) |
| `deploy/docker-compose.gcp.yml` | default fallback `${GCP_VM_EXTERNAL_IP:-...}` |
| `vastVidStream/VASTAI_SETUP_GUIDE.md` + docs | tài liệu tham chiếu |

Bên **Vast.ai** chỉ cần export env khi start producer: `KAFKA_BROKER=<IP_MỚI>:9093`.

---

## 8. Build images + khởi động stack

```bash
cd ~/streamhouse/deploy

# 1. Build (lần đầu). LƯU Ý: trino-with-paimon build paimon-trino TỪ SOURCE bằng
#    Maven → 15–30 phút. Các image khác nhanh.
docker compose --env-file .env.gcp -f docker-compose.gcp.yml build

# 2. Khởi động toàn bộ (kèm monitoring = prometheus + grafana)
docker compose --env-file .env.gcp -f docker-compose.gcp.yml --profile monitoring up -d

# 3. Theo dõi tới khi healthy (mysql → hive-metastore → kafka/minio → trino → chatbot)
watch -n 5 'docker ps --format "table {{.Names}}\t{{.Status}}" | sort'
```

**Bảng container (17) + memory limit:**

| Container | Vai trò | Mem |
|---|---|---|
| kafka, fluss-zookeeper | message bus / coordination | — |
| fluss-coordinator, fluss-tablet | HOT layer (Fluss) | — |
| minio | S3 (warehouse + evidence-frames) | — |
| mysql, hive-metastore | metastore cho Iceberg | — |
| jobmanager, taskmanager | Flink (taskmanager 2500m, network buffers 320MB) | 2500m |
| flink-sql-gateway | chatbot query Fluss HOT | — |
| pipeline-manager | orchestrator (init schema, watchdog, tiering, fact build, archival) | 1g |
| trino-coordinator | federation WARM/COLD (image custom `trino-with-paimon`) | 2g |
| chatbot | Agentic RAG API :5002 | 2g |
| frame-extractor | Kafka → MinIO evidence upload | 256m |
| prometheus, grafana | monitoring (grafana 1g, subpath `/grafana-proxy/`) | — |

---

## 9. Schema v2 tự khởi tạo + migrate dữ liệu cũ

**VM sạch (không mang data cũ): KHÔNG cần làm gì.** `pipeline-manager` khi khởi động tự:
1. Chạy `init_star_schema_v2.py` — tạo toàn bộ bảng Fluss/Paimon v2 + seed
   `dim_camera` (từ `data/metadata/camera_registry.csv` — nguồn duy nhất),
   `dim_date` (730 ngày), `dim_time` (24 giờ), `dim_event_type` (5 loại).
2. Seed bản copy dim_camera bên Fluss qua SQL Gateway.
3. Submit 4 streaming jobs: Contract Validator, hot sink (events + incidents),
   gold aggregate (đọc fact), update_frame_url.
4. Mỗi 30′: tiering Fluss→Paimon rồi `build_incident_facts.py` (events → fact grain=1 vụ).
5. 02:00 hằng ngày: archive Paimon→Iceberg (fact + events, giữ frame_url).

**Nếu mang dữ liệu v1 cũ theo** (copy volume MinIO/Kafka từ VM cũ) — chạy MỘT lần:
```bash
# Backfill incident_uid cho event cũ + rename backup + dọn bảng trùng
sudo docker exec pipeline-manager bash -c '/opt/flink/bin/flink run \
  -Drest.address=jobmanager -Drest.port=8081 \
  -Djobmanager.rpc.address=jobmanager -Djobmanager.rpc.port=6123 \
  --python /opt/flink/scripts/migrate_v2.py'

# Build fact cho toàn bộ lịch sử
sudo docker exec pipeline-manager bash -c 'BUILD_LOOKBACK_HOURS=8760 /opt/flink/bin/flink run \
  -Drest.address=jobmanager -Drest.port=8081 \
  -Djobmanager.rpc.address=jobmanager -Djobmanager.rpc.port=6123 \
  --python /opt/flink/scripts/build_incident_facts.py'
```
Copy volume MinIO giữa 2 VM (nếu cần giữ evidence + warehouse):
```bash
# VM cũ: nén volume
sudo docker run --rm -v deploy_minio-data:/data -v /tmp:/backup alpine \
  tar czf /backup/minio-data.tgz -C /data .
# scp sang VM mới rồi giải nén vào volume cùng tên TRƯỚC khi start minio
```

---

## 10. Kiểm tra toàn bộ (verify)

```bash
# 1. Trino federation (số quan trọng: fact = SỐ VỤ, không phải raw events)
sudo docker exec trino-coordinator trino --execute \
  "SELECT 'fact(SO VU)', COUNT(*) FROM paimon.security.fact_violence_incident
   UNION ALL SELECT 'raw events', COUNT(*) FROM paimon.security.violence_incidents"

# 2. Flink: 4 jobs RUNNING
curl -s http://localhost:8081/jobs/overview | python3 -m json.tool | grep -E 'name|state'

# 3. Chatbot
curl -s http://localhost:5002/health
curl -s http://localhost:5002/api/layer-counts
curl -s 'http://localhost:5002/api/recent-incidents?limit=3'
curl -s -X POST http://localhost:5002/chat -H 'Content-Type: application/json' \
  -d '{"query": "7 ngày qua có bao nhiêu vụ bạo lực?"}'

# 4. Grafana (qua subpath)
curl -s http://localhost:3001/grafana-proxy/api/health

# 5. Kafka nhận data từ Vast (sau khi producer chạy)
sudo docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic urban-safety-alerts
```

Chuẩn nghiệm thu chi tiết (số vụ khớp kịch bản RTSP, ảnh bbox...): `docs/TASK_PHASES_V2.md` Phase 5.

---

## 11. Vận hành thường ngày

**Code mới → container nào cần gì:**

| Service | Code | Cách áp dụng code mới |
|---|---|---|
| pipeline-manager, taskmanager, frame-extractor | **mount** `scripts/transform` | `up -d --force-recreate --no-deps <svc>` |
| chatbot | **bake** vào image | `build chatbot` rồi `up -d --no-deps chatbot` |
| trino-coordinator | image custom + mount `config/trino` | đổi config → recreate; đổi Dockerfile → build lại (lâu) |

```bash
# LUÔN LUÔN kèm --env-file:
cd ~/streamhouse/deploy
docker compose --env-file .env.gcp -f docker-compose.gcp.yml up -d --force-recreate --no-deps <service>

# Xem log
docker logs <container> --since 10m | tail -50
# Flink jobs
curl -s http://localhost:8081/jobs/overview
# Watchdog tự resubmit job chết mỗi 5 phút — muốn ngay: restart pipeline-manager
```

---

## 12. Troubleshooting

> Tất cả lỗi dưới đây ĐÃ GẶP THẬT trong quá trình deploy 07–08/07/2026.

### ❌ Trino query Paimon/Iceberg lỗi `S3 403 Access Key Id does not exist` / `UnsupportedSchemeException scheme 's3'`
Creds MinIO trong container Trino sai — container được recreate không kèm `--env-file .env.gcp`.
So sánh nhanh: `docker exec trino-coordinator sh -c 'echo $MINIO_ROOT_USER | md5sum'` vs
container minio/chatbot. Fix: recreate với `--env-file .env.gcp`.

### ❌ Flink batch job: `Insufficient number of network buffers: required 512...`
4 streaming jobs chiếm gần hết buffer pool. Đã fix trong compose:
`taskmanager.memory.network.min/max: 320mb` + `taskmanager.memory.process.size: 2304m`.
Nếu thêm streaming job mới mà lỗi tái diễn → tăng tiếp network.min/max.

### ❌ `flink run` trong pipeline-manager bị Killed (exit 137)
OOM — PyFlink driver chạy client-side trong container này. Limit đã nâng 512m→1g;
job batch mới nặng hơn thì nâng tiếp.

### ❌ `Paimon Sink does not support Flink's Adaptive Parallelism mode`
Batch job thiếu `execution.batch.adaptive.auto-parallelism.enabled=false` —
các script v2 đã set sẵn; script batch MỚI phải nhớ thêm dòng này + `parallelism.default=1`.

### ❌ `Interval field value X exceeds precision of HOUR(2) field`
Flink không nhận `INTERVAL '8760' HOUR` — dùng TIMESTAMP literal tính sẵn ở Python.

### ❌ Schema mismatch khi INSERT (`Column types do not match`)
`CREATE TABLE IF NOT EXISTS` giữ nguyên bảng CŨ nếu đã tồn tại → migrate schema phải
rename/drop trước (xem `migrate_v2.py`, và guard rename `dim_time_v1_backup` trong init).
`EXTRACT(...)` trả BIGINT — CAST về INT khi sink cột INT.

### ❌ `git: dubious ownership` / `Permission denied` trên /home/user/streamhouse
```bash
sudo git config --global --add safe.directory /home/user/streamhouse
# và thao tác git/compose bằng: sudo bash -c 'cd /home/user/streamhouse && ...'
```

### ❌ `git fetch` từ GitHub: `could not read Username`
VM không có creds (repo private) → dùng git bundle (mục 4, cách C).

### ❌ Số vụ hiển thị hàng nghìn (như v1)
Đang đếm bảng event grain. Số VỤ phải lấy từ `paimon.security.fact_violence_incident`
(WARM), `fluss.security.hot_violence_incidents` (HOT realtime),
`iceberg.security.historical_incident_facts` (COLD). Bảng `violence_incidents` là
event thô (0.5s/event khi violent) — chỉ dùng cho evidence/drill-down.

### ❌ frame_url toàn NULL
Kiểm tra job Flink `insert-into_paimon.security.violence_incidents` (update_frame_url)
có RUNNING không — watchdog pipeline-manager quản lý nó từ v2. Bảng v2 dùng
merge-engine `partial-update` nên tiering không còn ghi đè NULL lên URL thật.

### ❌ Grafana trắng / mixed content qua HTTPS tunnel
Grafana chạy subpath: `GF_SERVER_ROOT_URL=...:3001/grafana-proxy/` +
`GF_SERVER_SERVE_FROM_SUB_PATH=true` (đã trong compose). UI dùng Vite proxy
`/grafana-proxy` — xem VASTAI_SETUP_GUIDE.md mục 10.

---

*Tài liệu tạo sau khi deploy Phase 1–3 refactor v2 thành công — 2026-07-08.*
