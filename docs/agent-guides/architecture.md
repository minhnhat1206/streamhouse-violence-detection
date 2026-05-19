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

## Kiến Trúc Mới (Streamhouse Trio — Final 2026-05-19)

```
Camera Feeds (RTSP)
    ↓
VioMobileNet (Violence Detection)
    ↓
Kafka (Message Broker)  [urban-safety-alerts]
    ↓
APACHE FLINK — Job 1: Data Contract Validator
    ├─ Check camera_id format (^cam_\d{2}$)
    ├─ Check risk_score ∈ [0.0, 1.0]
    ├─ Check timestamp validity (not future, not too old)
    ├─ Valid   → hot-violence-alerts-valid
    └─ Invalid → urban-safety-quarantine

    ↓ hot-violence-alerts-valid
    ├── FLINK Job 2: Fluss Sink ───────────────────────────────►  FLUSS (HOT)
    │     hot_violence_alerts table                               ├─ <100ms query latency
    │     schema: incident_id, camera_id, timestamp,             ├─ Streaming writes
    │     risk_score, confidence, is_violent, event_type         └─ 1-2 hour retention
    │
    └── FLINK Job 3: Paimon Star Schema Sink ───────────────────► PAIMON (WARM)
          ├─ Temporal Join: FOR SYSTEM_TIME AS OF proc_time       ├─ fact_violence_incidents
          │   with fluss.security.dim_camera (camera metadata)    │   (enriched star schema)
          ├─ Enriched with: location, ward_id, district           ├─ violence_incidents
          └─ Writes to 2 tables in parallel (StatementSet)        │   (backward compat)
                                                                  └─ 7-30 day retention

         FLINK Job 4: Paimon Aggregation ──────────────────────► PAIMON (WARM GOLD)
           Reads Paimon CDC changelog                             ├─ daily_incident_stats
           Computes daily counts, camera stats                    └─ camera_stats

         Daily Archival (02:00 batch) ──────────────────────────► ICEBERG (COLD)
           archive_to_iceberg.py                                  historical_violence_incidents
           Paimon WARM → Iceberg COLD                             Years retention, time-travel

┌─────────────────────────────────────────────────────────────────────────┐
│ STAR SCHEMA (Paimon WARM)                                               │
│                                                                         │
│  dim_camera (Fluss HOT)          dim_time (Paimon)                      │
│  camera_id PK                    date_id PK                             │
│  location, ward_id, district     date, year, month, day, day_of_week    │
│  latitude, longitude                                                     │
│       ↓ temporal join                    ↓                              │
│  fact_violence_incidents (Paimon WARM)                                  │
│  incident_id PK | camera_id FK | date_id FK                             │
│  timestamp | risk_score | confidence | is_violent | event_type          │
│  location | ward_id | district | frame_url                              │
└─────────────────────────────────────────────────────────────────────────┘

    ↓
┌─────────────┬──────────────────────────────────────┬──────────────┐
│   FLUSS     │   PAIMON                              │   ICEBERG    │
│   (HOT)     │   (WARM)                              │   (COLD)     │
├─────────────┼──────────────────────────────────────┼──────────────┤
│ <100ms      │ 1-10 min                              │ 10+ min      │
│ Queryable   │ ACID+CDC+LSM                          │ Parquet+SQL  │
│ Streaming   │ fact_violence_incidents               │ Time-travel  │
│ 1-2 hours   │ violence_incidents                    │ Years        │
│             │ daily_incident_stats                  │              │
│             │ camera_stats                          │              │
│             │ dim_time                              │              │
│             │ 7-30 days                             │              │
└─────────────┴──────────────────────────────────────┴──────────────┘

    ↓
TRINO (Unified Query Federation)
    ├─ Hot path queries  → Flink SQL Gateway → Fluss
    ├─ Warm path queries → paimon catalog    → Paimon / MinIO
    └─ Cold path queries → iceberg catalog   → Iceberg / MinIO

    ↓
AGENTIC RAG (LangGraph + Gemini 2.0 Flash)
    ├─ Time-period classification (HOT / WARM / COLD)
    ├─ Text-to-SQL per layer (Flink SQL or Trino SQL)
    ├─ Self-correction (max 3 retries)
    ├─ Anti-hallucination (ChromaDB schema context)
    └─ Grounded answers with citations

    ↓
React UI (Command Center)
    ├─ LayerBadge: real-time row counts + latency_ms per layer
    ├─ RTSP camera grid + live risk scores
    ├─ Analytics dashboard (multi-layer charts)
    └─ Vigilance Terminal (Agentic RAG chatbot)
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
┌──────────────────────────────────────────────────────────────┐
│                  DOCKER STACK (core + streaming)              │
├───────────────┬──────────────────┬───────────────────────────┤
│ Ingestion     │ Compute           │ Storage                   │
│ ───────────── │ ────────────────  │ ─────────────────────────│
│ Kafka:19092   │ Flink JM:8081     │ MinIO:9000-9001           │
│ MediaMTX:8554 │ Flink TM (8slots) │ Fluss Coord:9123          │
│ rtsp_pusher   │                   │ Fluss Tablet:9094          │
│ rtsp-infer-   │ Pipeline-manager  │ Fluss ZK:2181             │
│   mock        │ (4 jobs watchdog) │                           │
│               │                   │ Paimon → s3://warehouse/  │
│               │  Job1: Validator  │   paimon/                 │
│               │  Job2: Fluss Sink │ Iceberg → s3a://warehouse/│
│               │  Job3: Paimon Star│   iceberg_warehouse/      │
│               │    (temporal join)│                           │
│               │  Job4: Aggregation│                           │
├───────────────┼──────────────────┼───────────────────────────┤
│ Query         │ AI                │ Metadata                  │
│ ─────────────│ ────────────────  │ ─────────────────────────│
│ Trino:8082    │ Chatbot:5002      │ Hive Metastore:9083        │
│ Flink SQL     │ (Agentic RAG)     │ MySQL:3306                 │
│  Gateway:8083 │ ChromaDB (local)  │                           │
│ (ui profile)  │ Gemini 2.0 Flash  │ Monitoring (--profile):   │
│               │                   │  Prometheus:9090           │
│               │                   │  Grafana:3001              │
└───────────────┴──────────────────┴───────────────────────────┘
```

### Current Flink Jobs (4 streaming + 1 batch daily)
| Job | Script | Source | Sink | Notes |
|-----|--------|--------|------|-------|
| Data Contract Validator | `data_contract_validator.py` | `urban-safety-alerts` | `hot-violence-alerts-valid` + `urban-safety-quarantine` | Schema-on-write enforcement |
| Fluss Sink (HOT) | `sink_to_fluss.py` | `hot-violence-alerts-valid` | `fluss.security.hot_violence_alerts` | <100ms queryable |
| Paimon Star Schema Sink (WARM) | `sink_to_paimon_star.py` | `hot-violence-alerts-valid` | `paimon.security.fact_violence_incidents` + `paimon.security.violence_incidents` | Temporal join with `dim_camera FOR SYSTEM_TIME AS OF proc_time` |
| Paimon Aggregation (WARM Gold) | `aggregate_paimon.py` | Paimon CDC | `paimon.security.daily_incident_stats` + `paimon.security.camera_stats` | Reads CDC changelog, dedup |
| Daily Archive (COLD, batch) | `archive_to_iceberg.py` | Paimon WARM | `iceberg.security.historical_violence_incidents` | Runs 02:00 daily via pipeline-manager |

### Star Schema Tables
| Table | Storage | Type | Purpose |
|-------|---------|------|---------|
| `dim_camera` | Fluss HOT | Dimension | Camera registry (15 cameras), temporal join source |
| `dim_time` | Paimon WARM | Dimension | Date lookup 2025-2026 (730 rows) |
| `fact_violence_incidents` | Paimon WARM | Fact | Star schema: incident + enriched camera + date |
| `violence_incidents` | Paimon WARM | Fact (legacy) | Backward compat for aggregate_paimon.py |
| `daily_incident_stats` | Paimon WARM | Aggregate | Daily counts per camera |
| `camera_stats` | Paimon WARM | Aggregate | Camera-level statistics |
