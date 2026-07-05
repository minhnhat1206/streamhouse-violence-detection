# Smart Security Monitoring — Streamhouse Architecture

## Project Overview
Hệ thống giám sát an ninh thông minh phát hiện bạo lực real-time (<100ms latency).
Chuyển đổi từ **Lakehouse + Spark** sang **Streamhouse Trio** (Fluss/Paimon/Iceberg).
**Khóa luận tốt nghiệp** — Nguyễn Ngọc Minh Nhật & Nguyễn Quốc Huy.

## ⚡ Deployment Topology — RULE CỨNG (AI/agent PHẢI tuân)

Hệ thống chạy trên **3 môi trường**, KHÔNG trộn lẫn. AI/khi deploy/run phải tôn trọng để không chạy nhầm sang máy dev:

| Môi trường | Chạy gì | Không chạy gì |
|---|---|---|
| **AI server (Vast.ai)** | VioMoViNet inference + **RTSP source sim** (mediamtx + `scripts/streaming/rtsp_pusher.py` + SCVD) + MinIO + Kafka producer. **Source CO-LOCATED với inference** (cùng box, `localhost:8554`). | — |
| **Data platform (GCP `34.124.131.144`)** | Kafka broker, Flink Streamhouse, Trino/Fluss/Paimon/Iceberg, Grafana, chatbot service. | inference / RTSP source |
| **Máy dev/local** | Repo (dev) + **web app frontend** (Vite `Violence-Urban-Safety-UI`) + SSH tunnels để xem. | **KHÔNG chạy inference / RTSP sim / Kafka producer** |

**RULE CHO AI (Claude):**
- **KHÔNG BAO GIỜ tự ý `docker compose -f local-stream.yml up`** (mediamtx + rtsp_pusher) hay start VioMoViNet inference trên máy dev local. Source sim + inference → **AI server (Vast.ai)**.
- `local-stream.yml` **CHỈ** chạy khi (a) dev độc lập không có producer thật, HOẶC (b) cần HLS cho web app xem (lúc đó nó là nguồn hiển thị, KHÔNG phải nguồn cho inference).
- **Đã verify**: source ở máy dev rồi tunnel tới AI server = bottleneck uplink → chỉ ~4/15 cam. Source + inference phải cùng box (localhost).
- Chi tiết runbook: `../VioMoViNet/SETUP_VASTAI.html` + `CONNECT_VASTAI.html`.

## Tech Stack
| Layer | Technology | Purpose |
|-------|-----------|---------|
| Message Broker | Apache Kafka (KRaft) | Event streaming |
| Compute | Apache Flink | True streaming, exactly-once |
| Hot Storage | Apache Fluss | <100ms real-time queries |
| Warm Storage | Apache Paimon | 1-10 min, ACID, CDC, LSM-tree |
| Cold Storage | Apache Iceberg | Historical, Parquet, time-travel |
| Object Store | MinIO | S3-compatible |
| Query Engine | Trino | Federated SQL across all layers |
| AI/LLM | Google Gemini 2.0 | Text-to-SQL, Agentic RAG |
| Vector DB | ChromaDB | RAG context retrieval |
| ML Model | VioMobileNet | Violence detection inference |
| Frontend | React.js + Tailwind | Command center dashboard |
| Monitoring | Prometheus + Grafana | Metrics & dashboards |

## Project Structure
```
├── scripts/
│   ├── streaming/       # RTSP producers, simulators, mock inference
│   ├── transform/       # Bronze & Gold layer ETL (Flink/Spark)
│   ├── chatbot/         # Agentic RAG (LangGraph + Gemini + ChromaDB)
│   └── setup/           # Infrastructure init (Kafka topics, etc.)
├── docker/              # docker-compose.yml, Dockerfiles, .env
├── config/              # Kafka, Spark, Trino, Hive, Grafana, Prometheus
├── frontend/            # React dashboard
├── data/                # Datasets & metadata
├── docs/agent-guides/   # Detailed architecture & implementation docs
└── assets/              # Screenshots for thesis
```

## Essential Commands
```bash
# Start full stack
docker compose -f docker/docker-compose.yml up -d

# Start specific service
docker compose -f docker/docker-compose.yml up -d kafka minio

# View logs
docker compose -f docker/docker-compose.yml logs -f <service>

# Run RTSP pipeline (real video frames via MediaMTX)
docker compose -f docker/docker-compose.yml --profile streaming up -d

# Kafka topics setup
docker exec -it kafka bash /scripts/setup/create-topics.sh

# Trino CLI
docker exec -it trino-coordinator trino
```

## Architecture (High-Level)
```
Camera (RTSP) → VioMobileNet → Kafka → Flink
  ├─ Data Contract Validation
  │   ├─ Valid → Fluss (HOT, <100ms, 1-2hr retention)
  │   └─ Invalid → Quarantine Topic
  ├─ Paimon (WARM, 1-10min, 7-30 day retention, CDC+ACID)
  └─ Iceberg (COLD, 10+min, years retention, time-travel)
      ↓
  Trino (Unified Query Federation)
      ↓
  Agentic RAG (LangGraph → Text-to-SQL → Self-correct)
      ↓
  React Dashboard (Real-time command center)
```

## Data Flow Rules
- **Hot queries** (< 1 hour): Route to Fluss
- **Warm queries** (1 hour – 7 days): Route to Paimon
- **Cold queries** (> 7 days): Route to Iceberg
- **Data Contracts**: Validate at source (schema-on-write), reject invalid → quarantine

## Code Conventions
- **Python**: snake_case, type hints, docstrings for public functions
- **Docker**: Always use env vars (no hardcoded credentials), healthchecks, resource limits
- **Kafka topics**: kebab-case (e.g., `urban-safety-alerts`)
- **SQL tables**: snake_case with layer prefix (e.g., `bronze_violence_incidents`)
- **Commits**: Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`)
- **Language**: Code & comments in English, user-facing docs in Vietnamese

## Agent Collaboration Protocol
- **Handover file**: `DEVELOPER_LOG.md` — cập nhật "Last State" mỗi khi kết thúc phiên
- **Claude**: Infrastructure, Docker, Flink pipelines, data engineering
- **Gemini**: Agentic RAG, Text-to-SQL, chatbot, AI intelligence
- Cả hai agent đều tham chiếu `docs/agent-guides/` cho tài liệu chi tiết

## Streaming Services — Graceful Stop
`rtsp-inference-mock` và `rtsp_pusher` chạy **vô tận** (while True). Sau khi test, **BẮT BUỘC** dừng:
```bash
docker exec rtsp-inference-mock touch /app/tmp/STOP
docker exec rtsp_pusher touch /app/tmp/STOP
```
Khi restart, stop file tự xóa. Chi tiết: `docs/agent-guides/stop-mechanism.md`

## RTSP Simulation (Context-Continuous)
RTSP source = `scripts/streaming/rtsp_pusher.py` (clip → ffmpeg → MediaMTX → `rtsp://mediamtx:8554/cam_NN`). Mỗi camera = **1 bối cảnh cố định** (scene-cluster), KHÔNG random shuffle.
- **Sinh/cluster playlist:** `scripts/prepare_cameras_context.py` (chạy trên host, zero-install numpy/sklearn/cv2/ffmpeg) → cluster 481 clip SCVD thành 15 camera, dùng **100% dataset**, density ≈ `--target-density` (0.12). Output: `data/metadata/camera_registry.csv` + `camera_playlists.json`.
- **Dataset SCVD** = symlink `data/raw/SCVD → ../MSA-MoViNet/data/SCVD/SCVD_converted` (sibling repo, KHÔNG copy 358MB).
- **Reload sau khi rerun prep:** `docker compose -f docker/docker-compose.local-stream.yml up -d --force-recreate rtsp_pusher` (BẮT BUỘC `--force-recreate`, `up -d` thường không đọc lại playlist).
- **Build image (lần đầu):** `docker build -f docker/Dockerfile.rtsp-pusher -t docker-rtsp_pusher:latest .`
- **Xem luồng:** `ffplay rtsp://localhost:8554/cam_01`. HLS remap `:18888` (VS Code chiếm host :8888).
- Chi tiết: `docs/RTSP_SIMULATION.md`, memory `rtsp-context-clustering`.

## Project Context (Shared)
Đọc `docs/PROJECT_CONTEXT.md` để nắm toàn bộ trạng thái dự án (services, ports, tiến độ, phân công).

## Detailed Documentation
- [Architecture](docs/agent-guides/architecture.md) — Kiến trúc cũ vs mới, flow diagrams
- [Storage Layers](docs/agent-guides/storage-layers.md) — Hot/Warm/Cold chi tiết + SQL
- [Data Contracts](docs/agent-guides/data-contracts.md) — Validation rules, quarantine flow
- [Agentic RAG](docs/agent-guides/agentic-rag.md) — LangGraph agent, Text-to-SQL
- [Stop Mechanism](docs/agent-guides/stop-mechanism.md) — Graceful stop cho streaming services
- [Roadmap](docs/agent-guides/roadmap.md) — 8-week plan, checklist, demo script

## GCP Cloud VM — Access & Management

### VM Info
```
Instance : instance-20260524-104630
Zone     : asia-southeast1-b
IP (ext) : 34.124.131.144
Username : user   ← KHÔNG phải ubuntu
Project  : project-65c40e4a-6eda-4c02-87a
```

### gcloud CLI Location (Windows)
```
PowerShell / cmd : C:\Users\user\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd
Bash (Git Bash)  : /c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud
```
Không dùng `oracle-streamhouse.key` cho GCP — key đó dành cho Oracle Cloud (project cũ).

### SSH vào VM (Bash)
```bash
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b
```

### Chạy lệnh từ xa (không cần SSH shell)
```bash
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b \
  --command='docker ps'
```

### Copy file lên VM
```bash
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"
"$GCLOUD" compute scp deploy/docker-compose.gcp.yml \
  instance-20260524-104630:~/streamhouse/deploy/ --zone=asia-southeast1-b
```

### Start / Stop VM (tiết kiệm chi phí)
```bash
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"

# Dừng VM (giữ nguyên disk, không tính phí CPU)
"$GCLOUD" compute instances stop instance-20260524-104630 --zone=asia-southeast1-b

# Khởi động lại VM
"$GCLOUD" compute instances start instance-20260524-104630 --zone=asia-southeast1-b

# Kiểm tra trạng thái
"$GCLOUD" compute instances list
```

### Sau khi start VM lại — Khởi động services
```bash
# SSH vào VM
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b --command='
  cd ~/streamhouse
  docker compose -f deploy/docker-compose.gcp.yml --env-file deploy/.env.gcp up -d
'
```
**LƯU Ý:** Sau khi start lại VM, pipeline-manager tự khởi động lại tất cả Flink jobs.
Chờ ~5 phút để dim_camera được seed và các jobs ổn định.

### Gửi test events từ local lên GCP Kafka
```bash
# GCP Kafka external port: 9093
# Xem send_test_events.py trong project root
python3 send_test_events.py
```

## Key Ports
| Service | Port |
|---------|------|
| Kafka | 19092 |
| Kafka UI | 18085 |
| Flink Web UI | 8081 |
| MinIO Console | 9001 |
| MinIO API | 9000 |
| Trino | 8082 |
| Hive Metastore | 9083 |
| Fluss Coordinator | 9123 |
| Fluss TabletServer | 9094 |
| MediaMTX (RTSP) | 8554 |
| Prometheus | 9090 |
| Grafana | 3001 |
| Chatbot API | 5002 |
