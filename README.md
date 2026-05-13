# Hệ Thống Giám Sát An Ninh Thông Minh — Kiến Trúc Streamhouse

**Khóa luận tốt nghiệp** — Nguyễn Ngọc Minh Nhật & Nguyễn Quốc Huy

Hệ thống phát hiện bạo lực thời gian thực từ camera RTSP, xử lý qua **Streamhouse Trio** (Fluss + Paimon + Iceberg) với Flink, truy vấn liên hợp qua Trino và trả lời tự nhiên qua Agentic RAG chatbot.

---

## Kiến Trúc Tổng Quan

```
Camera RTSP (RWF-2000 dataset)
        │
        ▼  [rtsp_pusher → MediaMTX → rtsp-inference-mock]
Kafka: urban-safety-alerts  (raw inference events)
        │
        ▼  [Flink: data_contract_validator]
        ├── valid   → hot-violence-alerts-valid
        └── invalid → urban-safety-quarantine
                          │
              ┌───────────┼──────────────┐
              ▼           ▼              ▼
         [Flink]      [Flink]        [Flink]
        Fluss HOT    Paimon WARM   aggregate_paimon
        (<100ms)     (30s commit)   (daily/camera stats)
              │           │
              └─────┬─────┘
                    ▼
              Trino (Iceberg COLD — batch archive)
                    │
                    ▼
         Agentic RAG Chatbot (LangGraph + Gemini)
                    │
                    ▼
         React Dashboard (Violence-Urban-Safety-UI)
```

---

## Tech Stack

| Lớp | Công nghệ | Mục đích |
|-----|-----------|----------|
| Message Broker | Apache Kafka (KRaft) | Event streaming |
| Compute | Apache Flink 1.18.1 | Streaming, exactly-once |
| HOT Storage | Apache Fluss 0.9.0 | <100ms real-time queries |
| WARM Storage | Apache Paimon 0.8.2 | ACID, CDC, LSM-tree, 7-30 ngày |
| COLD Storage | Apache Iceberg 1.5.2 | Historical, Parquet, time-travel |
| Object Store | MinIO | S3-compatible |
| Query Engine | Trino | Federated SQL across layers |
| AI/LLM | Google Gemini 2.0 Flash | Text-to-SQL, Agentic RAG |
| Vector DB | ChromaDB | RAG context retrieval |
| ML Model | VioMobileNet (mock) | Violence detection inference |
| Frontend | React + Tailwind | Command center dashboard |
| Monitoring | Prometheus + Grafana | Metrics & dashboards |
| Orchestration | Apache Airflow 2.9.1 | DAG scheduling |

---

## Cổng Dịch Vụ

| Dịch vụ | Cổng | Ghi chú |
|---------|------|---------|
| Flink Web UI | 8081 | Giám sát streaming jobs |
| MinIO Console | 9001 | Object storage browser |
| MinIO API | 9000 | S3 endpoint |
| Trino | 8082 | SQL query engine |
| Chatbot API | 5002 | FastAPI + LangGraph |
| Kafka | 19092 | External access |
| Kafka UI | 18085 | Profile `ui` |
| Flink SQL Gateway | 8083 | Profile `ui` |
| Fluss Coordinator | 9123 | |
| MediaMTX (RTSP) | 8554 | Profile `streaming` |
| Prometheus | 9090 | Profile `monitoring` |
| Grafana | 3001 | Profile `monitoring` |
| Airflow | 8089 | Profile `orchestration` |

---

## Chuẩn Bị

### 1. Dataset RWF-2000

Tải dataset và đặt vào:
```
data/raw/RWF-2000/
├── norTrain/
│   ├── Fight/        ← dùng bởi rtsp_pusher (RTSP simulation)
│   └── NonFight/
├── train/            ← dùng cho training ML model
└── val/
```

### 2. Cấu hình môi trường

```bash
cp docker/.env.example docker/.env
# Chỉnh sửa docker/.env nếu cần (mặc định dùng được ngay)
```

### 3. Frontend submodule

```bash
git submodule update --init --recursive
cd Violence-Urban-Safety-UI && npm install
```

---

## Khởi Động Pipeline

### Lần đầu tiên

```bash
# Core services (không RTSP)
bash scripts/setup/start-pipeline.sh

# Có RTSP streaming (video RWF-2000 thật + mock AI inference)
bash scripts/setup/start-pipeline.sh --profile streaming
```

Script tự động thực hiện:
1. Tạo Docker network `violence-detection-net`
2. Khởi động tất cả core services
3. Tạo Kafka topics
4. Init bảng Fluss, Paimon, Iceberg
5. Submit 4 Flink streaming jobs

### Sau khi restart máy

Flink jobs bị mất sau restart, cần submit lại:

```bash
docker compose -f docker/docker-compose.yml up -d

for script in data_contract_validator sink_to_fluss sink_to_paimon aggregate_paimon; do
  docker exec jobmanager flink run -d -py /opt/flink/scripts/${script}.py
done
```

### Profiles tuỳ chọn

```bash
--profile streaming      # MediaMTX + rtsp_pusher + rtsp-inference-mock
--profile monitoring     # Prometheus + Grafana + node-exporter
--profile ui             # Flink SQL Gateway + Kafka UI
--profile orchestration  # Airflow (port 8089, admin/admin)
--profile scaling        # Trino worker nodes
```

---

## Dòng Chảy Dữ Liệu

### Nguồn dữ liệu

| Service | Khi nào | Mô tả |
|---------|---------|-------|
| `inference-mock` | Core (mặc định) | Payload ngẫu nhiên, không có video |
| `rtsp-inference-mock` | `--profile streaming` | Frame thật từ MediaMTX, AI inference giả |

> Khi chạy `--profile streaming`, dừng `inference-mock` để tránh duplicate data:
> ```bash
> docker exec inference-mock touch /app/tmp/STOP
> ```
> Khi muốn tích hợp AI model thật: thay phần thân hàm `mock_inference()` trong
> `scripts/streaming/rtsp_inference_mock.py` bằng HTTP call đến model API.

### Data Contract Validation

Mọi event qua `urban-safety-alerts` đều được validator kiểm tra:

| Rule | Điều kiện hợp lệ |
|------|-----------------|
| Timestamp | Không ở tương lai (> 1 phút) |
| Camera ID | Format `cam_XX` (2 chữ số) |
| Risk score | [0.0, 1.0] |
| Confidence | [0.0, 1.0] |
| Event type | Bắt buộc khi `is_violent = true` |

- Hợp lệ → `hot-violence-alerts-valid` → Fluss + Paimon
- Vi phạm → `urban-safety-quarantine`

### Lớp lưu trữ Streamhouse

| Lớp | Công nghệ | Retention | Latency | Dùng khi |
|-----|-----------|-----------|---------|---------|
| HOT | Fluss | ~1-2 giờ | <100ms | Dữ liệu real-time |
| WARM | Paimon | 7-30 ngày | 3-5 phút | Truy vấn 1 giờ → 7 ngày |
| COLD | Iceberg | Vĩnh viễn | <5 giây | Lịch sử > 7 ngày (qua Trino) |

---

## Kiểm Tra Pipeline

### Flink jobs đang chạy

Truy cập **http://localhost:8081** — Running Jobs phải đủ 4:
- `Data Contract Validator Job`
- `Flink job: Kafka to Fluss Sink`
- `Flink job: Kafka to Paimon Warm Sink`
- `Flink job: Paimon Aggregation`

### Kafka nhận data

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic urban-safety-alerts \
  --max-messages 3 --from-beginning
```

### Paimon có snapshot

```bash
docker exec minio_client mc ls \
  minio/warehouse/paimon/security.db/violence_incidents/snapshot/
```

### Test data contract (inject record lỗi)

```bash
docker exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic urban-safety-alerts << 'EOF'
{"event_id":"test-bad","camera_id":"INVALID","timestamp":"2099-01-01T00:00:00Z","is_violent":true,"risk_score":1.5,"confidence":0.9}
EOF

# Phải xuất hiện trong quarantine với violations
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic urban-safety-quarantine \
  --max-messages 3 --from-beginning
```

### Query chatbot end-to-end

```bash
curl -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Tổng cộng có bao nhiêu sự cố trong 24 giờ qua?"}'
```

---

## Dừng Dịch Vụ

```bash
# Dừng streaming producers gracefully trước
docker exec inference-mock touch /app/tmp/STOP
docker exec rtsp-inference-mock touch /app/tmp/STOP

# Tắt toàn bộ stack
docker compose -f docker/docker-compose.yml down
```

---

## Cấu Trúc Thư Mục

```
├── scripts/
│   ├── streaming/          # RTSP producers, simulators, mock inference
│   ├── transform/          # Flink jobs: validator, Fluss/Paimon/Iceberg sinks
│   ├── chatbot/            # Agentic RAG (LangGraph + Gemini + ChromaDB)
│   └── setup/              # create-topics.sh, start-pipeline.sh
├── docker/
│   ├── docker-compose.yml
│   ├── Dockerfile.flink
│   ├── Dockerfile.producer
│   ├── .env.example
│   └── airflow/dags/       # Airflow DAGs (flink monitor, archive, quality check)
├── config/
│   ├── mediamtx/           # RTSP relay config
│   ├── hive_metastore/     # Hive schema config
│   ├── trino/              # Trino catalog (Iceberg)
│   └── prometheus/         # Prometheus scrape config
├── data/
│   ├── metadata/           # camera_registry.csv (15 cameras, TP.HCM Quận 1)
│   ├── raw/RWF-2000/       # Video dataset (không commit lên git)
│   └── playlist/           # ffmpeg playlists (auto-generated)
├── Violence-Urban-Safety-UI/   # React frontend (git submodule)
└── docs/agent-guides/          # Tài liệu kiến trúc chi tiết
```

---

## Tài Liệu

- [Kiến trúc Streamhouse](docs/agent-guides/architecture.md)
- [Storage Layers](docs/agent-guides/storage-layers.md)
- [Data Contracts](docs/agent-guides/data-contracts.md)
- [Agentic RAG Chatbot](docs/agent-guides/agentic-rag.md)
- [Roadmap](docs/agent-guides/roadmap.md)
