# Smart Security Monitoring — Streamhouse Architecture

Hệ thống giám sát an ninh thông minh phát hiện bạo lực real-time, sử dụng kiến trúc **Streamhouse** với Apache Flink, Fluss, Paimon và Iceberg.

## Kiến trúc tổng quan

```
Camera (RTSP) → VioMobileNet → Kafka → Flink
                                         ├─ Data Contract Validator
                                         │   ├─ Valid   → hot-violence-alerts-valid
                                         │   └─ Invalid → urban-safety-quarantine
                                         │
                                         ├─ Fluss  (HOT)  — <100ms,  1-2 giờ retention
                                         ├─ Paimon (WARM) — phút,    7-30 ngày retention
                                         └─ Iceberg (COLD) — phút+,  năm retention
                                                ↓
                                         Trino (Federated SQL)
                                                ↓
                                         Agentic RAG (Gemini + ChromaDB)
                                                ↓
                                         React Dashboard
```

## Yêu cầu hệ thống

| Yêu cầu | Tối thiểu |
|----------|-----------|
| RAM | 16 GB |
| CPU | 4 cores (khuyến nghị 8) |
| Disk | 20 GB trống |
| Docker | v24+ với Docker Compose v2 |
| OS | Windows 10/11, macOS, hoặc Linux |

## Cấu trúc thư mục

```
├── scripts/
│   ├── streaming/       # Kafka producers, mock inference
│   ├── transform/       # Flink jobs (validator, sinks, init tables)
│   ├── chatbot/         # Agentic RAG (Gemini + ChromaDB)
│   └── setup/           # Kafka topic creation
├── docker/              # docker-compose.yml, Dockerfiles, .env
├── config/              # Kafka, Trino, Hive, Grafana, Prometheus
├── frontend/            # React dashboard
├── data/                # Datasets & metadata
└── docs/                # Architecture docs & agent guides
```

## Ports

| Service | Port | URL |
|---------|------|-----|
| Kafka | 19092 | — |
| Kafka UI | 18085 | http://localhost:18085 |
| Flink Web UI | 8081 | http://localhost:8081 |
| MinIO Console | 9001 | http://localhost:9001 |
| MinIO API | 9000 | — |
| Trino | 8082 | — |
| Hive Metastore | 9083 | — |
| Fluss Coordinator | 9123 | — |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3001 | http://localhost:3001 |
| Chatbot API | 5002 | http://localhost:5002 |

---

## Hướng dẫn cài đặt & chạy từng bước

### Bước 0 — Clone repo & chuẩn bị environment

```bash
git clone https://github.com/minhnhat1206/violence-detection-system.git
cd violence-detection-system
```

Tạo file `.env` từ template:

```bash
cp docker/.env.example docker/.env
```

Mở `docker/.env` và điền `GEMINI_API_KEY` (lấy từ [Google AI Studio](https://aistudio.google.com/apikey)):

```env
GEMINI_API_KEY=your_actual_api_key_here
```

Các giá trị còn lại có default sẵn, không cần thay đổi.

### Bước 1 — Tạo Docker network

```bash
docker network create violence-detection-net
```

> Network này dùng chung cho tất cả containers. Chỉ cần tạo **một lần duy nhất**.

### Bước 2 — Khởi động infrastructure cơ bản

```bash
docker compose -f docker/docker-compose.yml up -d kafka minio minio_client mysql
```

Đợi Kafka và MySQL healthy (~30 giây):

```bash
# Kiểm tra status
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Kết quả mong đợi:
```
NAMES          STATUS
kafka          Up ... (healthy)
minio          Up ... (healthy)
minio_client   Up ...
mysql          Up ... (healthy)
```

> **Mẹo:** Nếu muốn xem chi tiết health:
> ```bash
> docker inspect --format='{{.State.Health.Status}}' kafka mysql minio
> ```

### Bước 3 — Khởi động Hive Metastore

Hive Metastore dùng MySQL làm backend, phục vụ catalog cho Paimon và Iceberg.

```bash
docker compose -f docker/docker-compose.yml up -d hive-metastore
```

Đợi ~15 giây, kiểm tra:

```bash
docker logs hive-metastore --tail 5
```

Kết quả mong đợi:
```
schemaTool completed
[entrypoint] Starting Hive Metastore service...
```

> Nếu thấy `schemaTool failed` hoặc container restart liên tục, kiểm tra MySQL đã healthy chưa.

### Bước 4 — Khởi động Apache Flink

```bash
docker compose -f docker/docker-compose.yml up -d jobmanager taskmanager
```

Đợi ~30 giây. Kiểm tra Flink Web UI: **http://localhost:8081**

Verify TaskManager đã đăng ký:

```bash
curl -s http://localhost:8081/taskmanagers | python3 -c "
import sys, json
d = json.load(sys.stdin)
tms = d.get('taskmanagers', [])
print(f'TaskManagers: {len(tms)}')
for t in tms:
    print(f'  slots={t[\"slotsNumber\"]}, free={t[\"freeSlots\"]}')
"
```

Kết quả mong đợi:
```
TaskManagers: 1
  slots=4, free=4
```

### Bước 5 — Khởi động Apache Fluss (Hot Storage)

```bash
docker compose -f docker/docker-compose.yml up -d fluss-zookeeper fluss-coordinator fluss-tablet
```

Đợi ~20 giây, kiểm tra:

```bash
docker ps --filter "name=fluss" --format "table {{.Names}}\t{{.Status}}"
```

Kết quả mong đợi — cả 3 service đều `Up` và `(healthy)`:
```
NAMES              STATUS
fluss-tablet       Up ... (healthy)
fluss-coordinator  Up ... (healthy)
fluss-zookeeper    Up ... (healthy)
```

### Bước 6 — Tạo Kafka Topics

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic urban-safety-alerts --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic hot-violence-alerts-valid --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic urban-safety-quarantine --partitions 3 --replication-factor 1
```

Verify:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Kết quả mong đợi:
```
hot-violence-alerts-valid
urban-safety-alerts
urban-safety-quarantine
```

### Bước 7 — Khởi tạo Fluss & Paimon tables

Chạy trong Flink JobManager container:

```bash
# Init Fluss tables (Hot Storage)
docker exec jobmanager python /opt/flink/scripts/init_fluss_tables.py

# Init Paimon tables (Warm Storage)
docker exec jobmanager python /opt/flink/scripts/init_paimon_tables.py
```

Kết quả mong đợi:
```
[SUCCESS] Fluss Catalog and Tables initialized successfully.
[SUCCESS] Paimon Catalog and violence_incidents table initialized successfully.
```

### Bước 8 — Submit Flink Streaming Jobs

Submit 3 jobs theo thứ tự. Mỗi job chiếm 1 slot trong TaskManager.

**Job 1 — Data Contract Validator** (Kafka raw → validated + quarantine):

```bash
docker exec jobmanager flink run -py /opt/flink/scripts/data_contract_validator.py
```

**Job 2 — Sink to Fluss** (validated → Fluss hot storage):

```bash
docker exec jobmanager flink run -py /opt/flink/scripts/sink_to_fluss.py
```

**Job 3 — Sink to Paimon** (validated → Paimon warm storage):

```bash
docker exec jobmanager flink run -py /opt/flink/scripts/sink_to_paimon.py
```

Verify trên Flink Web UI (http://localhost:8081) hoặc:

```bash
curl -s http://localhost:8081/jobs/overview | python3 -c "
import sys, json
for j in json.load(sys.stdin).get('jobs', []):
    print(f'{j[\"state\"]:10s} | {j[\"name\"]}')
"
```

Kết quả mong đợi — **3 jobs RUNNING**:
```
RUNNING    | Data Contract Validator Job
RUNNING    | insert-into_fluss.security.hot_violence_alerts
RUNNING    | insert-into_paimon.security.violence_incidents
```

### Bước 9 — Test pipeline với Mock Inference

Khởi động inference-mock để sinh dữ liệu test:

```bash
docker compose -f docker/docker-compose.yml up -d inference-mock
```

Xem dữ liệu đang được tạo:

```bash
docker logs -f inference-mock
```

Kết quả mong đợi (mỗi giây 1 message):
```
!!! [ALERT] Violence detected on cam_06
--- [NORMAL] Situation cleared on cam_12
!!! [ALERT] Violence detected on cam_09
```

### Bước 10 — Verify dữ liệu trong Paimon (MinIO)

Đợi ~40 giây (để checkpoint Paimon chạy ít nhất 1 lần), rồi kiểm tra:

```bash
docker exec minio mc ls local/warehouse/paimon/security.db/violence_incidents/ --recursive | head -10
```

Kết quả mong đợi — thấy file `.orc` (data) và `snapshot`:
```
[...] 20KiB STANDARD bucket-0/data-xxx.orc
[...] 20KiB STANDARD bucket-0/changelog-xxx.orc
[...] 781B  STANDARD manifest/...
[...] 585B  STANDARD snapshot/snapshot-1
```

### Bước 11 — Dừng Mock Inference

**Quan trọng**: inference-mock chạy vô tận. Sau khi test xong, **bắt buộc** dừng:

```bash
docker exec inference-mock touch /app/tmp/STOP
```

> Container sẽ tự dừng sau vài giây. Khi restart lần sau, stop file tự xóa.

---

## Services tùy chọn (Profiles)

Các service không thiết yếu được gom vào profiles để tiết kiệm RAM:

```bash
# Kafka UI — quản lý topics, xem messages
docker compose -f docker/docker-compose.yml --profile ui up -d

# Monitoring — Prometheus + Grafana dashboards
docker compose -f docker/docker-compose.yml --profile monitoring up -d

# Streaming — RTSP camera simulation
docker compose -f docker/docker-compose.yml --profile streaming up -d

# Trino workers — tăng query performance
docker compose -f docker/docker-compose.yml --profile scaling up -d
```

## Trino & Chatbot (chưa tích hợp đầy đủ)

```bash
# Khởi động Trino (query engine) và Chatbot (Agentic RAG)
docker compose -f docker/docker-compose.yml up -d trino-coordinator chatbot
```

> Trino hiện chỉ có Iceberg catalog. Paimon và Fluss catalogs sẽ được thêm trong các bước tiếp theo.

---

## Lệnh hữu ích

```bash
# Xem tất cả containers
docker compose -f docker/docker-compose.yml ps

# Xem logs của service
docker compose -f docker/docker-compose.yml logs -f <service-name>

# Dừng tất cả
docker compose -f docker/docker-compose.yml down

# Dừng và xóa volumes (reset dữ liệu)
docker compose -f docker/docker-compose.yml down -v

# Cancel 1 Flink job (thay JOB_ID)
curl -X PATCH "http://localhost:8081/jobs/<JOB_ID>?mode=cancel"
```

## Resource Budget (16 GB RAM)

| Service | RAM | CPU | Profile |
|---------|-----|-----|---------|
| kafka | 512m | 0.50 | core |
| minio | 512m | 0.50 | core |
| minio_client | 64m | 0.10 | core |
| inference-mock | 256m | 0.25 | core |
| jobmanager | 1g | 0.50 | core |
| taskmanager | 2g | 2.00 | core |
| fluss-zookeeper | 256m | 0.25 | core |
| fluss-coordinator | 512m | 0.50 | core |
| fluss-tablet | 512m | 0.50 | core |
| mysql | 512m | 0.50 | core |
| hive-metastore | 512m | 0.25 | core |
| trino-coordinator | 1536m | 1.00 | core |
| chatbot | 1536m | 1.00 | core |
| **Tổng core** | **~9.6 GB** | **7.85** | |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Message Broker | Apache Kafka (KRaft) |
| Stream Processing | Apache Flink 1.18 (PyFlink) |
| Hot Storage | Apache Fluss 0.9 |
| Warm Storage | Apache Paimon 0.8 |
| Cold Storage | Apache Iceberg (planned) |
| Object Store | MinIO (S3-compatible) |
| Query Engine | Trino |
| AI/LLM | Google Gemini 2.0 Flash |
| Vector DB | ChromaDB |
| Frontend | React.js + Tailwind CSS |

## Tác giả

- Nguyen Ngoc Minh Nhat
- Nguyen Quoc Huy

Khoa luan tot nghiep — Dai hoc Ton Duc Thang.
