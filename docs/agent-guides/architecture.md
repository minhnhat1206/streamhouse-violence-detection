# Kiến Trúc Hệ Thống — Chi Tiết

## Kiến Trúc Hiện Tại (Legacy — Lakehouse)

```
Camera Feeds (RTSP)
    ↓
VioMobileNet (Violence Detection)
    ↓
Kafka (Message Broker)
    ↓
Spark Structured Streaming (Micro-batch: 1-30 giây)  ← BOTTLENECK
    ↓
Iceberg (Data Lake)
    ↓
Trino (Query)
    ↓
RAG Assistant (Static - lookup only)  ← LIMITED
    ↓
React Dashboard + Grafana
```

### Vấn Đề Của Kiến Trúc Cũ
- **Latency quá cao** (1-30 giây): Spark micro-batch không đáp ứng yêu cầu cảnh báo tức thời
- **Dữ liệu bẩn**: Không filter ở nguồn → contaminate toàn bộ hệ thống
- **RAG tĩnh**: Chỉ lookup, không thể trả lời câu hỏi thống kê
- **Time Travel giới hạn**: Chỉ có Iceberg, khó forensic analysis

---

## Kiến Trúc Mới (Streamhouse Trio)

```
Camera Feeds (RTSP)
    ↓
VioMobileNet (Violence Detection)
    ↓
Kafka (Message Broker)
    ↓
APACHE FLINK (True Streaming, Exactly-once)
    ├─ Data Contract Validation
    │  ├─ Check camera_id format (^cam_\d{2}$)
    │  ├─ Check risk_score [0-1]
    │  ├─ Check timestamp validity
    │  └─ Valid? → Hot Layer | Invalid? → Quarantine
    │
    ├─ Transformation & Enrichment
    └─ Multi-sink Output

    ↓
┌─────────────┬──────────────┬──────────────┐
│   FLUSS     │   PAIMON     │   ICEBERG    │
│   (HOT)     │   (WARM)     │   (COLD)     │
├─────────────┼──────────────┼──────────────┤
│ <100ms      │ 1-10 min     │ 10+ min      │
│ Queryable   │ ACID+CDC     │ Batch+SQL    │
│ Streaming   │ LSM-tree     │ File-based   │
│ 1-2 hours   │ 7-30 days    │ Years        │
└─────────────┴──────────────┴──────────────┘
    ↓
TRINO (Unified Query Federation)
    ├─ Hot path queries (Fluss)
    ├─ Warm path queries (Paimon)
    └─ Cold path queries (Iceberg)

    ↓
AGENTIC RAG (LangGraph Framework)
    ├─ Parse natural language
    ├─ Generate SQL automatically
    ├─ Execute on appropriate layer
    ├─ Self-correct if needed
    └─ Provide grounded answers

    ↓
React UI (Command Center)
    ├─ Real-time dashboards
    ├─ Analytics
    ├─ Agentic chatbot
    └─ Forensic queries
```

## Streamhouse — Khái Niệm

**Streamhouse** là kiến trúc dữ liệu thế hệ 3, kết hợp streaming real-time với lakehouse.
- **Coined by**: Jing Ge (CTO Ververica, Flink PMC) tại Flink Forward Seattle 10/2023
- **Evolution**: Data Warehouse → Data Lakehouse → **Streamhouse**
- **Core idea**: Ghi 1 lần vào Fluss → tự động tiering xuống Paimon/Iceberg

### 4 Gaps Của Lakehouse Thuần Mà Streamhouse Giải Quyết
1. **Metadata overhead**: Mỗi Iceberg commit rewrite metadata.json → bloat tại tần suất cao
2. **Polling-based reads**: Không push notification → +5-15s latency
3. **No enforced PK**: Iceberg V2 PK chỉ là DDL hint, không enforce uniqueness
4. **Write amplification**: Merge-on-read + delete files → small-file problem

### So Sánh: Lakehouse vs Streamhouse

| Tiêu chí | Lakehouse (Cũ) | Streamhouse (Mới) |
|-----------|----------------|-------------------|
| Compute | Spark (micro-batch) | Flink (true streaming) |
| Latency | 1-30 giây | <100 milliseconds |
| Quality | Schema-on-read | Schema-on-write (Data Contracts) |
| Storage | 1 tier (Iceberg) | 3 tiers (Fluss/Paimon/Iceberg) |
| RAG | Static lookup | Agentic (Text-to-SQL + self-correct) |
| Cost | 1 storage tier | Tiered → 30-50% cheaper |
| Write path | Batch commits | Single write → auto tiering |
| Read path | Polling-based | Union read (hot + lake merged) |

### Key Insights
1. **Flink > Spark** cho security: True streaming, event-at-a-time, exactly-once semantics
2. **3-tier storage** tối ưu cost: Hot data expensive nhưng ít, cold data cheap nhưng nhiều
3. **Data Contracts** shift-left: Phát hiện lỗi ngay tại nguồn, không chờ downstream
4. **Agentic RAG** thông minh hơn: Tự chọn layer, tự sinh SQL, tự sửa lỗi
5. **Auto tiering**: Ghi 1 lần vào Fluss, Tiering Service (Flink job) tự chuyển xuống lake

Chi tiết triển khai Streamhouse: xem `.claude/skills/streamhouse/SKILL.md`

## Docker Services Map

```
┌──────────────────────────────────────────────────────────┐
│                      DOCKER STACK                         │
├─────────────┬────────────────┬───────────────────────────┤
│ Ingestion   │ Compute        │ Storage                   │
│ ─────────── │ ────────────── │ ─────────────────────────│
│ Kafka:19092 │ Flink JM:8081  │ MinIO:9000-9001           │
│ KafkaUI:    │ Flink TM       │ Fluss Coord:9123          │
│   18085     │                │ Fluss Tablet:9094          │
│ MediaMTX:   │ Flink Jobs:    │ Fluss ZK:2181             │
│   8554      │  - Validator   │                           │
│             │  - Fluss Sink  │ Paimon (filesystem catalog)│
│             │  - Paimon Sink │  → s3://warehouse/paimon   │
├─────────────┼────────────────┼───────────────────────────┤
│ Query       │ AI             │ Monitoring                │
│ ─────────── │ ────────────── │ ─────────────────────────│
│ Trino:8082  │ Chatbot:5002   │ Prometheus:9090            │
│             │ (Agentic RAG)  │ Grafana:3001               │
└─────────────┴────────────────┴───────────────────────────┘
```

### Current Flink Jobs (3 total)
| Job | Source | Sink | Consumer Group |
|-----|--------|------|----------------|
| Data Contract Validator | `urban-safety-alerts` | `hot-violence-alerts-valid` + `urban-safety-quarantine` | — |
| Fluss Sink | `hot-violence-alerts-valid` | `fluss.security.hot_violence_alerts` (HOT) | `fluss-sink-group` |
| Paimon Sink | `hot-violence-alerts-valid` | `paimon.security.violence_incidents` (WARM) | `paimon-sink-group` |
