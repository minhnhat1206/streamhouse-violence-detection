# Realtime Violence Detection — Streamhouse Architecture

**Khóa luận tốt nghiệp** — Nguyễn Ngọc Minh Nhật & Nguyễn Quốc Huy  
**Thesis project** — Smart security monitoring system for realtime violence detection from RTSP cameras.

[![Stack](https://img.shields.io/badge/Stack-Flink%20%7C%20Fluss%20%7C%20Paimon%20%7C%20Iceberg-blue)]()
[![Latency](https://img.shields.io/badge/HOT%20latency-%3C100ms-green)]()
[![Tests](https://img.shields.io/badge/E2E%20tests-22%2F23%20PASS-brightgreen)]()

---

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Tech Stack](#tech-stack)
3. [Prerequisites](#prerequisites)
4. [Quick Start](#quick-start)
5. [Running the Full Pipeline](#running-the-full-pipeline)
6. [Service Profiles](#service-profiles)
7. [Verifying the Setup](#verifying-the-setup)
8. [Using the Chatbot](#using-the-chatbot)
9. [Project Structure](#project-structure)
10. [Key Ports](#key-ports)
11. [Stopping Services](#stopping-services)
12. [Troubleshooting](#troubleshooting)
13. [Documentation](#documentation)

---

## System Architecture

The system implements a **Streamhouse Trio** — three storage layers with automatic tiering:

```
Camera (RTSP / RWF-2000 dataset)
        │
        ▼  rtsp_pusher → MediaMTX → rtsp-inference-mock
Kafka: urban-safety-alerts  (raw inference events from VioMobileNet)
        │
        ▼  [Flink: data_contract_validator]
        ├── valid   → hot-violence-alerts-valid
        └── invalid → urban-safety-quarantine

[Flink: sink_to_fluss_enriched]          [Flink: aggregate_paimon]
        │                                         │
        ▼                                         ▼
   Fluss HOT                               Paimon WARM
   (<100ms, 1-2h)    ──tier every 30min──  (minutes latency, 7-30d)
                                                  │
                                         archive daily at 02:00
                                                  ▼
                                          Iceberg COLD
                                          (Trino, years)
                                                  │
                                                  ▼
                              Agentic RAG Chatbot (LangGraph + Gemini 2.0)
                                                  │
                                                  ▼
                              React Dashboard (Violence-Urban-Safety-UI)
```

### Data Tiering (True Streamhouse — no dual-write)

| Layer | Technology | Write | Retention | Query latency | Use when |
|-------|-----------|-------|-----------|---------------|---------|
| HOT   | Fluss     | Flink streaming | ~1-2h | <100ms | Last hour real-time |
| WARM  | Paimon    | Periodic tiering from Fluss | 7-30d | 1-5min | 1h → 7d |
| COLD  | Iceberg   | Daily batch archive | Forever | <5s (Trino) | >7d history |

---

## Tech Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| Message Broker | Apache Kafka (KRaft) | 4.0.1 | Event streaming, no ZooKeeper |
| Compute | Apache Flink | 1.18.1 | Streaming, exactly-once semantics |
| HOT Storage | Apache Fluss | 0.9.0 | Real-time columnar store, <100ms |
| WARM Storage | Apache Paimon | 0.8.2 | ACID, CDC, LSM-tree |
| COLD Storage | Apache Iceberg | 1.5.2 | Historical, Parquet, time-travel |
| Object Store | MinIO | Latest | S3-compatible, all warehouse data |
| Query Engine | Trino | 476 | Federated SQL across all layers |
| AI/LLM | Google Gemini 2.0 Flash | Latest | Text-to-SQL, Agentic RAG |
| Vector DB | ChromaDB | Latest | RAG schema context retrieval |
| ML Model | VioMobileNet (mock) | — | Violence detection (mock for demo) |
| Frontend | React + Tailwind CSS | — | Command center dashboard |
| Monitoring | Prometheus + Grafana | — | Metrics & dashboards |
| Orchestration | Apache Airflow | 2.9.1 | DAG scheduling (optional) |

---

## Prerequisites

### Hardware

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 12GB free | 16GB total machine |
| CPU | 4 cores | 8 cores |
| Disk | 30GB free | 50GB free |
| OS | Windows 10/11 (WSL2) or Linux | — |

> **Note:** The ML model (VioMobileNet) runs on a **separate machine**. This repo uses a mock inference service by default.

### Software

- **Docker Desktop** 24+ with WSL2 backend (Windows) or Docker Engine 24+ (Linux)
- **Git** with submodule support
- **Google Gemini API key** (free tier works) — [Get one here](https://aistudio.google.com/app/apikey)
- Optional: Node.js 18+ (for running the frontend locally)

### Dataset (optional — only for RTSP streaming profile)

Download [RWF-2000 dataset](https://github.com/mchengny/RWF2000-Video-Database-for-Violence-Detection) and place at:

```
data/raw/RWF-2000/
├── norTrain/
│   ├── Fight/      ← used by rtsp_pusher for live RTSP simulation
│   └── NonFight/
└── train/
```

Without the dataset, the system uses `inference-mock` (generates synthetic data — fully functional for testing).

---

## Quick Start

```bash
# 1. Clone with submodules
git clone --recurse-submodules https://github.com/minhnhat1206/realtime-violence-detection.git
cd realtime-violence-detection

# 2. Configure environment
cp docker/.env.example docker/.env
# Edit docker/.env — at minimum set GEMINI_API_KEY

# 3. Create Docker network
docker network create violence-detection-net

# 4. Start core stack
cd docker && docker compose up -d

# 5. Wait ~3 minutes for pipeline-manager to init tables and submit Flink jobs
docker logs -f pipeline-manager

# 6. Open dashboard
# Frontend: http://localhost:5173 (after npm install in Violence-Urban-Safety-UI/)
# Chatbot API: http://localhost:5002/docs
# Flink UI: http://localhost:8081
```

### Frontend setup

```bash
git submodule update --init --recursive
cd Violence-Urban-Safety-UI
npm install
npm run dev   # http://localhost:5173
```

---

## Running the Full Pipeline

### Core services (always on)

```bash
cd docker
docker compose up -d
```

Starts: kafka, minio, mysql, hive-metastore, fluss-zookeeper, fluss-coordinator, fluss-tablet, jobmanager, taskmanager, trino-coordinator, chatbot, pipeline-manager, inference-mock, frame-extractor.

The `pipeline-manager` container automatically:
1. Waits for Flink JobManager to be ready
2. Creates Kafka topics via `create-topics.sh`
3. Initializes Fluss, Paimon, Iceberg table schemas
4. Seeds `dim_camera` (15 HCMC cameras) via Flink SQL Gateway
5. Submits 3 streaming Flink jobs:
   - **Contract Validator** — validates events, routes to valid/quarantine
   - **Fluss HOT Sink** — Kafka → temporal join → Fluss (real-time, enriched with location)
   - **Paimon Aggregation** — CDC → `daily_incident_stats` + `camera_stats`
6. Runs periodic tiering (every 30min): Fluss HOT → Paimon WARM
7. Runs daily archival (02:00 UTC): Paimon WARM → Iceberg COLD

### With RTSP streaming (real video frames)

```bash
docker compose --profile streaming up -d
```

Adds: mediamtx (RTSP relay), rtsp_pusher (pushes RWF-2000 video), rtsp-inference-mock (reads frames, mocks AI inference).

> **Important:** When streaming profile is active, stop the default mock to avoid duplicate data:
> ```bash
> docker exec inference-mock touch /app/tmp/STOP
> ```

### After machine restart

Flink jobs are lost on restart. The `pipeline-manager` container automatically resubmits them on startup. Just run:

```bash
cd docker && docker compose up -d
```

---

## Service Profiles

Profiles control optional services to manage memory usage (16GB machine):

| Profile | Services added | RAM added | Use case |
|---------|---------------|-----------|---------|
| `streaming` | mediamtx, rtsp_pusher, rtsp-inference-mock | ~640MB | Real RTSP video frames |
| `ui` | kafka-ui, flink-sql-gateway | ~768MB | Browse Kafka topics, query Paimon via SQL |
| `monitoring` | prometheus, grafana, node-exporter | ~576MB | Metrics dashboards |
| `orchestration` | airflow | ~768MB | DAG scheduling (Airflow on port 8089) |
| `scaling` | trino-worker-1, trino-worker-2 | ~2GB | Faster Trino queries |

```bash
# Multiple profiles
docker compose --profile streaming --profile monitoring up -d

# All optional services (except scaling)
docker compose --profile streaming --profile monitoring --profile ui --profile orchestration up -d
```

---

## Verifying the Setup

### 1. All containers healthy

```bash
docker compose ps
# All STATUS should be: Up (healthy) or Up
```

### 2. Flink has 3 running jobs

Open **http://localhost:8081** → Running Jobs should show:
- `Contract Validator Job`
- `Flink job: Kafka to Fluss HOT Sink`
- `Flink job: Paimon Aggregation`

### 3. Data flowing through Kafka

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hot-violence-alerts-valid \
  --max-messages 3
```

### 4. HOT layer has data (Fluss)

```bash
# Via Flink SQL Gateway (requires --profile ui)
curl -X POST http://localhost:8083/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{"sessionName":"test"}'
# Use returned sessionHandle to query:
# SELECT COUNT(*) FROM fluss.`security`.`hot_violence_alerts` LIMIT 1
```

### 5. WARM layer has data (Paimon)

```bash
# Wait ~30 minutes for first tiering cycle, then:
docker exec minio_client mc ls \
  minio/warehouse/paimon/security.db/violence_incidents/snapshot/
```

### 6. API endpoints

```bash
curl http://localhost:5002/api/layer-counts
# Expected: {"hot": N, "warm": M, "cold": 0}  (cold=0 until 7 days of data)

curl http://localhost:5002/api/latency
# Expected: {"hot_latency_ms": ~35, "warm_latency_ms": ~180000}
```

### 7. Data contract validation

```bash
# Inject an invalid event
docker exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic urban-safety-alerts << 'EOF'
{"event_id":"test-bad","camera_id":"INVALID","timestamp":"2099-01-01T00:00:00Z","is_violent":true,"risk_score":1.5,"confidence":0.9}
EOF

# Should appear in quarantine with violations listed
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic urban-safety-quarantine \
  --max-messages 3 --from-beginning
```

---

## Using the Chatbot

The chatbot automatically routes queries to the correct storage layer based on time period:

| Query time range | Layer | Table |
|-----------------|-------|-------|
| < 1 hour | Fluss HOT | `hot_violence_alerts` |
| 1 hour – 7 days | Paimon WARM | `violence_incidents` |
| > 7 days | Iceberg COLD | `historical_violence_incidents` |

### Example queries (Vietnamese)

```bash
# HOT layer — last 30 minutes
curl -X POST http://localhost:5002/api/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Trong 30 phút qua có bao nhiêu vụ bạo lực?"}'

# WARM layer — today  
curl -X POST http://localhost:5002/api/chat \
  -d '{"query": "Hôm nay camera nào ghi nhận nhiều vụ nhất?"}'

# HOT layer — recent alerts
curl -X POST http://localhost:5002/api/chat \
  -d '{"query": "Cảnh báo nào được phát ra trong 30 phút qua?"}'

# Evidence retrieval
curl -X POST http://localhost:5002/api/chat \
  -d '{"query": "Cho tôi xem ảnh bằng chứng của sự cố gần nhất"}'
```

### API endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/chat` | POST | Main chatbot — `{"query": "..."}` |
| `/api/layer-counts` | GET | Row counts: HOT / WARM / COLD |
| `/api/latency` | GET | Query latency per layer (ms) |
| `/api/recent-incidents` | GET | Last 20 incidents with frame URLs |
| `/api/evidence` | GET | `?camera_id=cam_01&date=YYYY-MM-DD` |
| `/docs` | GET | FastAPI Swagger UI |

---

## Project Structure

```
realtime-violence-detection/
├── scripts/
│   ├── streaming/                  # Data producers
│   │   ├── inference_mock.py       # Default: synthetic events (no video needed)
│   │   ├── rtsp_inference_mock.py  # Profile streaming: reads MediaMTX frames
│   │   └── rtsp_pusher.py          # Profile streaming: pushes RWF-2000 to MediaMTX
│   ├── transform/                  # Flink streaming jobs
│   │   ├── pipeline_manager.py     # Orchestrates all jobs + tiering + archival
│   │   ├── data_contract_validator.py  # Validates events, routes valid/quarantine
│   │   ├── sink_to_fluss_enriched.py   # Kafka → Fluss HOT (with location enrichment)
│   │   ├── tier_fluss_to_paimon.py     # HOT → WARM tiering (runs every 30min)
│   │   ├── archive_to_iceberg.py       # WARM → COLD archival (runs daily at 02:00)
│   │   ├── aggregate_paimon.py         # CDC → daily_incident_stats + camera_stats
│   │   ├── setup_star_schema.py        # Creates dim_camera (Fluss) + fact tables (Paimon)
│   │   ├── frame_extractor_sink.py     # Saves evidence frames to MinIO
│   │   └── init_*_tables.py            # Table initialization scripts
│   ├── chatbot/                    # Agentic RAG backend
│   │   ├── app.py                  # FastAPI + LangGraph agent graph
│   │   ├── ingest.py               # Schema metadata → ChromaDB
│   │   ├── rag_store.py            # ChromaDB wrapper
│   │   └── trino_client.py         # Trino + Flink SQL Gateway query routing
│   └── setup/
│       ├── create-topics.sh        # Creates Kafka topics
│       └── start-pipeline.sh       # Manual pipeline start script
├── docker/
│   ├── docker-compose.yml          # Full stack definition
│   ├── Dockerfile.flink            # Flink + PyFlink + Paimon/Fluss connectors
│   ├── Dockerfile.chatbot          # Chatbot + ChromaDB + LangGraph
│   ├── Dockerfile.producer         # Base image for streaming services
│   ├── Dockerfile.hive             # Hive Metastore
│   ├── Dockerfile.trino            # Trino + S3A + Iceberg connector
│   ├── Dockerfile.rtsp-pusher      # RTSP video pusher
│   ├── .env.example                # Template — copy to .env and fill in secrets
│   └── airflow/dags/               # Airflow DAGs (optional orchestration)
├── config/
│   ├── kafka/                      # Producer config
│   ├── mediamtx/                   # RTSP relay config
│   ├── hive_metastore/             # Hive schema SQL
│   ├── trino/                      # Trino catalog (Iceberg, Fluss disabled)
│   └── prometheus/                 # Prometheus scrape config
├── data/
│   ├── metadata/
│   │   └── camera_registry.csv     # 15 cameras: cam_01–cam_15, Quận 1 TP.HCM
│   └── raw/RWF-2000/               # Video dataset (gitignored, optional)
├── docs/
│   ├── agent-guides/               # Detailed architecture documentation
│   └── PROJECT_CONTEXT.md          # Current project status + decisions
├── Violence-Urban-Safety-UI/       # React dashboard (git submodule)
├── DEVELOPER_LOG.md                # Session-by-session development history
└── CLAUDE.md                       # AI agent collaboration guide
```

---

## Key Ports

| Service | Port | Notes |
|---------|------|-------|
| Flink Web UI | 8081 | Monitor streaming jobs, logs |
| MinIO Console | 9001 | Browse warehouse data visually |
| MinIO API (S3) | 9000 | Trino catalog endpoint |
| Trino | 8082 | SQL query engine (Iceberg COLD) |
| Chatbot API | 5002 | FastAPI + Swagger at `/docs` |
| Kafka | 19092 | External bootstrap server |
| Fluss Coordinator | 9123 | Real-time HOT storage |
| Fluss TabletServer | 9094 | Data plane |
| Kafka UI | 18085 | Profile `ui` — browse topics |
| Flink SQL Gateway | 8083 | Profile `ui` — query Paimon/Fluss via SQL |
| MediaMTX (RTSP) | 8554 | Profile `streaming` |
| Prometheus | 9090 | Profile `monitoring` |
| Grafana | 3001 | Profile `monitoring` |
| Airflow | 8089 | Profile `orchestration` (admin/admin) |

---

## Stopping Services

```bash
# Graceful stop for infinite streaming loops first
docker exec inference-mock touch /app/tmp/STOP
docker exec rtsp-inference-mock touch /app/tmp/STOP  # if --profile streaming

# Stop all services (keeps volumes / data)
docker compose -f docker/docker-compose.yml down

# Stop and delete all data (hard reset)
docker compose -f docker/docker-compose.yml down -v
```

---

## Troubleshooting

### "violence-detection-net not found"

```bash
docker network create violence-detection-net
```

### Flink jobs not starting

```bash
# Check pipeline-manager logs
docker logs pipeline-manager

# Check Flink JobManager
docker logs jobmanager | tail -30

# Flink UI shows no running jobs → pipeline-manager may need more time
# Fluss + Paimon catalog init takes ~3-5 min on first start
```

### Paimon queries return 0 rows

Paimon WARM layer is populated by tiering from Fluss every 30 minutes. Wait for:
1. Fluss HOT has data: check `api/layer-counts` → `hot > 0`
2. First tiering cycle: ~30 minutes after startup
3. Then `api/layer-counts` → `warm > 0`

### Chatbot returns wrong layer / old data

```bash
# Check layer routing
curl http://localhost:5002/api/layer-counts

# Restart chatbot to reload ChromaDB schema
docker compose restart chatbot
```

### Flink SQL Gateway (Paimon/Fluss SQL queries) is slow

Expected behavior: Flink SQL Gateway queries run as mini streaming jobs, taking 3-5 minutes. This is normal. For faster queries, use:
- Trino on Iceberg COLD layer (< 5 seconds)
- Direct Fluss HOT queries (< 100ms, bounded scan via LIMIT N)

### MinIO data not persisting after restart

```bash
# Check volume exists
docker volume ls | grep minio

# If missing, volumes were deleted — normal after `docker compose down -v`
# Data will re-populate as inference-mock sends events
```

### Chatbot cannot query Paimon (paimon.properties disabled)

The Paimon-Trino connector JAR does not exist on Maven Central. Paimon queries route through Flink SQL Gateway instead:

```bash
# Start Flink SQL Gateway
docker compose --profile ui up -d flink-sql-gateway

# Chatbot auto-detects and routes warm queries there
```

---

## Documentation

| Doc | Description |
|-----|-------------|
| [Architecture](docs/agent-guides/architecture.md) | Streamhouse vs Lambda, flow diagrams, star schema |
| [Storage Layers](docs/agent-guides/storage-layers.md) | HOT/WARM/COLD detailed specs + SQL examples |
| [Data Contracts](docs/agent-guides/data-contracts.md) | Validation rules, quarantine flow |
| [Agentic RAG](docs/agent-guides/agentic-rag.md) | LangGraph agent, Text-to-SQL, self-correction |
| [Stop Mechanism](docs/agent-guides/stop-mechanism.md) | Graceful stop for streaming services |
| [Roadmap](docs/agent-guides/roadmap.md) | 8-week plan, checklist, demo script |
| [Project Context](docs/PROJECT_CONTEXT.md) | Current system state, decisions, known issues |
| [Developer Log](DEVELOPER_LOG.md) | Session-by-session history (40 sessions) |

---

## E2E Test Results

Latest run: **Session 39 — 22/23 PASS** (2026-05-22)

```
✅ S1: Data ingestion (Kafka → Fluss)
✅ S2: HOT layer queries (<100ms latency)
✅ S3: WARM layer tiering (Fluss → Paimon every 30min)
✅ S4: COLD layer archival (Paimon → Iceberg daily)
✅ S5: Trino federated queries (Iceberg)
✅ S6: Chatbot routing (HOT/WARM/COLD based on time period)
✅ S7: Data contract validation + quarantine
✅ T6.1: "cảnh báo" not misrouted to evidence endpoint
✅ T6.4: "45 phút"→Fluss, "2 giờ"→Paimon boundary routing
```

Full report: [E2E_TEST_REPORT_2026-05-22_SESSION39.md](docs/E2E_TEST_REPORT_2026-05-22_SESSION39.md)

---

*Khóa luận tốt nghiệp, Khoa Công nghệ Thông tin — 2026*
