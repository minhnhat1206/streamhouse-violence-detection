# Project Context — Realtime Violence Detection (Streamhouse)
> File này dùng chung cho **tất cả agents (Claude, Gemini)** và **người dùng**.
> Cập nhật lần cuối: **2026-04-12**

---

## 1. Dự án là gì?
Hệ thống giám sát an ninh thông minh phát hiện bạo lực real-time (<100ms latency).
- **Khóa luận tốt nghiệp** — Nguyễn Ngọc Minh Nhật & Nguyễn Quốc Huy
- Kiến trúc: **Streamhouse Trio** (Fluss / Paimon / Iceberg) thay thế Lakehouse + Spark cũ

---

## 2. Kiến trúc tổng quan

```
Camera (RTSP) → VioMobileNet → Kafka → Flink
  │
  ├─ Data Contract Validation
  │   ├─ Valid   → Fluss  (HOT,  <100ms, 1-2hr retention)
  │   └─ Invalid → Quarantine Topic
  │
  ├─ Paimon (WARM, 1-10min, 7-30 day, CDC + ACID)
  └─ Iceberg (COLD, 10+min, years, time-travel)
      │
      ▼
  Trino (Unified Query Federation)
      │
      ▼
  Agentic RAG (LangGraph → Gemini → Text-to-SQL → Self-correct)
      │
      ▼
  React Dashboard (Real-time command center)
```

---

## 3. Services trong Docker Compose

### Đã triển khai & test thành công
| Service | Image | Port | Trạng thái |
|---------|-------|------|------------|
| `kafka` | apache/kafka:4.0.1-rc2 | 19092 | Healthy, KRaft mode |
| `kafka-ui` | provectuslabs/kafka-ui | 18085 | OK |
| `minio` | dataguy16/minio | 9000, 9001 | Healthy, 3 buckets tạo sẵn |
| `minio_client` | dataguy16/minio-client | — | Init buckets rồi idle |
| `producer` | docker/Dockerfile.producer | — | Đẩy data liên tục (có stop file) |
| `inference-mock` | docker/Dockerfile.producer | — | Mock AI output liên tục (có stop file) |
| `jobmanager` | flink:1.18.1 (Custom) | 8081 | Healthy, PyFlink + Kafka/Fluss support |
| `taskmanager` | flink:1.18.1 (Custom) | — | 2 task slots, Python environment OK |
| `fluss-zookeeper` | zookeeper:3.9.2 | 2181 | Healthy |
| `fluss-coordinator` | apache/fluss:0.9.0 | 9123 | Healthy, Gateway cho Flink |
| `fluss-tablet` | apache/fluss:0.9.0 | 9094 | Healthy, Hot Storage thực thụ |

### Đã chuẩn bị code (chưa test)
| Service/Script | Mục đích | Trạng thái |
|----------------|----------|------------|
| `init_paimon_tables.py` | Tạo Paimon catalog + Warm table (MinIO S3) | Code ready, chưa chạy |
| `sink_to_paimon.py` | Flink sink Kafka → Paimon Warm | Code ready, chưa chạy |
| `Dockerfile.flink` | Đã thêm Paimon JARs (`paimon-flink-1.18`, `paimon-s3`) | Cần rebuild |

### Chưa khởi chạy
| Service | Mục đích | Phụ thuộc |
|---------|----------|-----------|
| `mysql` + `hive-metastore` | Metadata cho Iceberg | — (Paimon dùng filesystem catalog) |
| `trino-coordinator` + workers | Federated query | hive-metastore |
| `prometheus` + `grafana` | Monitoring | jobmanager, taskmanager |
| `chatbot` | Agentic RAG API | minio |
| `mediamtx` + `rtsp_pusher` | RTSP simulation thật | kafka |


---

## 4. Kafka Topics

| Topic | Mô tả | Producer |
|-------|--------|----------|
| `hot-violence-alerts-valid` | Validated alerts (after contract) | Flink validator |
| `urban-safety-quarantine` | Data vi phạm contract | Flink validator |

---

## 5. Cơ chế Graceful Stop (Streaming Services)

`producer` và `inference-mock` chạy **vòng lặp vô tận**. Dừng bằng **stop file**:

```bash
# Dừng
docker exec inference-mock touch /app/tmp/STOP
docker exec producer touch /app/tmp/STOP

# Restart (stop file tự xóa)
docker compose restart inference-mock producer
```

Chi tiết: `docs/agent-guides/stop-mechanism.md`

---

## 6. Docker trên máy này

Lệnh `docker` không có trong PATH. **Bắt buộc dùng đường dẫn tuyệt đối:**

```bash
# Bash shell (cho Claude)
DOCKER="/c/Program Files/Docker/Docker/resources/bin/docker.exe"
"$DOCKER" ps

# PowerShell (cho người dùng hoặc Gemini)
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" ps
```

Compose shortcut:
```bash
COMPOSE="$DOCKER compose -f docker/docker-compose.yml --env-file docker/.env"
$COMPOSE up -d kafka minio
```

---

## 7. Phân công Agent

| Agent | Phụ trách | Files chính |
|-------|-----------|-------------|
| **Claude** | Infrastructure, Docker, Flink pipelines, data engineering | `docker-compose.yml`, `scripts/transform/`, `config/` |
| **Gemini** | Agentic RAG, Text-to-SQL, chatbot, AI intelligence | `scripts/chatbot/`, `frontend/` |
| **Cả hai** | Đọc `DEVELOPER_LOG.md` trước khi làm, cập nhật "Last State" khi xong | `DEVELOPER_LOG.md` |

---

## 8. Tiến độ hiện tại

### Week 1-2: Core Infrastructure — ✅ DONE
- [x] Docker environment (Kafka, MinIO, Flink)
- [x] Docker-compose optimization (healthchecks, resource limits, env vars)
- [x] Refactor scripts structure
- [x] Mock inference service + `.env.example`
- [x] Graceful stop mechanism cho streaming services
- [x] E2E Pipeline: `inference-mock → Kafka → Flink Validator → Kafka (valid) → Flink Sink → Fluss`
- [x] Data Contract Validation (valid → hot topic, invalid → quarantine)
- [x] Fluss Hot Storage initialized + Sink job RUNNING

### Week 3-4: Warm & Cold Layers — 🔄 IN PROGRESS
- [x] Paimon connector JARs added to `Dockerfile.flink`
- [x] `init_paimon_tables.py` — Paimon filesystem catalog + Warm table (MinIO S3)
- [x] `sink_to_paimon.py` — Flink streaming job Kafka → Paimon Warm
- [ ] **Test Paimon pipeline** (rebuild Flink, init tables, submit sink job)
- [ ] Paimon Gold aggregation tables + jobs
- [ ] Iceberg historical table + archival job
- [ ] Setup Prometheus/Grafana monitoring

---

## 9. Ports tổng hợp

| Port | Service | Ghi chú |
|------|---------|---------|
| 8081 | Flink Web UI | JobManager dashboard |
| 9000 | MinIO API | S3-compatible |
| 9001 | MinIO Console | Web UI (minio/mypassword) |
| 18085 | Kafka UI | Cluster monitoring |
| 19092 | Kafka | Broker (external) |
| 8082 | Trino | *(chưa chạy)* |
| 9083 | Hive Metastore | *(chưa chạy)* |
| 9090 | Prometheus | *(chưa chạy)* |
| 3001 | Grafana | *(chưa chạy, mapped 3001→3000)* |
| 9123 | Fluss Coordinator | CLIENT listener (Flink catalog) |
| 9094 | Fluss TabletServer | CLIENT listener (Flink read/write) |
| 2181 | Fluss ZooKeeper | Cluster coordination |

---

## 10. Tài liệu chi tiết

| File | Nội dung |
|------|----------|
| `CLAUDE.md` | Instructions cho Claude agent |
| `GEMINI.md` | Instructions cho Gemini agent |
| `DEVELOPER_LOG.md` | Handover log giữa các agent |
| `docker/instruction.md` | Hướng dẫn khởi chạy từng bước |
| `docs/FLINK_ARCHITECTURE.md` | Kiến trúc Flink & Data Contract |
| `docs/FLUSS_GUIDE.md` | Chi tiết về Hot Storage (Apache Fluss) |
| `docs/agent-guides/architecture.md` | Kiến trúc cũ vs mới |
| `docs/agent-guides/storage-layers.md` | Hot/Warm/Cold chi tiết |
| `docs/agent-guides/data-contracts.md` | Validation rules |
| `docs/agent-guides/agentic-rag.md` | LangGraph + Text-to-SQL |
| `docs/agent-guides/stop-mechanism.md` | Graceful stop streaming |
| `docs/agent-guides/roadmap.md` | 8-week plan + demo script |

