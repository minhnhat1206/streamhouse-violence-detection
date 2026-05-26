# Chương 3: Kiến trúc và Triển khai Hệ thống

> **Khóa luận tốt nghiệp** — Nguyễn Ngọc Minh Nhật & Nguyễn Quốc Huy
> *(Markdown — copy vào Word/LaTeX để định dạng theo template)*

---

## 3.1 Đặt vấn đề kiến trúc

### 3.1.1 Yêu cầu của bài toán giám sát an ninh thời gian thực

Hệ thống giám sát an ninh đô thị đặt ra những yêu cầu đặc thù mà các kiến trúc dữ liệu truyền thống khó đáp ứng đồng thời:

**Yêu cầu về thời gian:**

| Loại truy vấn | Mục đích | Yêu cầu latency |
|--------------|---------|----------------|
| Cảnh báo tức thời (<1 giờ) | Ứng phó sự cố đang diễn ra | < 100ms |
| Phân tích vận hành (1h–7 ngày) | Theo dõi xu hướng, báo cáo ca trực | Vài giây |
| Phân tích lịch sử (>7 ngày) | Báo cáo định kỳ, nghiên cứu hình sự | Dưới 1 phút |

**Yêu cầu về chất lượng dữ liệu:** Dữ liệu từ camera thực tế có thể chứa lỗi timestamp, camera_id không hợp lệ, hoặc risk_score ngoài phạm vi. Hệ thống phải phát hiện và cách ly dữ liệu bất hợp lệ ngay tại nguồn để tránh contaminate toàn bộ pipeline.

**Yêu cầu về chi phí lưu trữ:** Dữ liệu an ninh cần lưu trữ dài hạn (nhiều năm) cho mục đích pháp lý. Tuy nhiên, dữ liệu cũ ít được truy vấn — lưu trữ trên cùng một medium đắt tiền với dữ liệu real-time là lãng phí.

### 3.1.2 Tension cốt lõi

Không có công nghệ lưu trữ đơn lẻ nào tối ưu cho cả ba trục:

```
Nhanh (HOT, <100ms)  ←→  Rẻ (COLD, Parquet nén)  ←→  Đầy đủ tính năng (upsert, ACID, time-travel)
```

Các kiến trúc truyền thống giải quyết tension này theo những cách có giới hạn. Phần tiếp theo phân tích hai kiến trúc phổ biến nhất và lý do chúng không phù hợp với bài toán này.

---

## 3.2 Phân tích kiến trúc hiện có

### 3.2.1 Lambda Architecture (2011)

Lambda Architecture giải quyết vấn đề bằng cách duy trì hai pipeline song song: batch layer (Hadoop/Spark) xử lý toàn bộ dữ liệu lịch sử, và speed layer (Storm/Flink) xử lý dữ liệu mới trong thời gian thực. Kết quả từ hai layer được merge tại serving layer.

```
Nguồn dữ liệu
  ├── Batch Layer  (Spark)  ────→ Batch View  ──┐
  └── Speed Layer (Flink)  ────→ Speed View  ──┘
                                                 └→ Serving Layer (merge thủ công)
```

**Giới hạn trong bài toán giám sát an ninh:**

| Vấn đề | Hệ quả |
|--------|--------|
| Dual codebase | Logic phát hiện bạo lực phải viết hai lần cho batch và streaming — dễ phân kỳ |
| Reconciliation | Batch view và speed view có thể cho kết quả khác nhau về cùng một sự kiện |
| Operational overhead | Vận hành hai stack song song đòi hỏi đội ngũ chuyên biệt |
| Không có temporal join native | Không thể truy vấn "vị trí của camera tại thời điểm sự cố xảy ra" mà không tự implement SCD Type 2 |

### 3.2.2 Medallion Architecture / Lakehouse (2021)

Medallion Architecture (popularized by Databricks) tổ chức dữ liệu thành ba lớp chất lượng: Bronze (raw copy), Silver (cleaned), Gold (aggregated). Mỗi lớp là một bản sao vật lý của dữ liệu.

```
Nguồn → [job] → Bronze → [job] → Silver → [job] → Gold
          (raw copy)      (cleaned)         (aggregated)
```

**Giới hạn:**

| Vấn đề | Hệ quả |
|--------|--------|
| Lưu trữ trùng lặp 3× | Mỗi sự kiện tồn tại ở Bronze + Silver + Gold |
| Polling-based | Consumer phải hỏi "có dữ liệu mới chưa?" mỗi 30s–5 phút → latency floor 30+ giây |
| Không có enforced PK | Iceberg V2 primary key là DDL hint, không đảm bảo uniqueness → duplicate events |
| Manual routing | Developer phải tự biết query bảng nào cho khoảng thời gian nào |

**Thực nghiệm đối chiếu:** Trong phiên bản đầu của hệ thống (dùng Spark + Iceberg), latency của một query về sự kiện vừa xảy ra là 1–30 giây (phụ thuộc vào micro-batch interval của Spark). Điều này không đáp ứng yêu cầu cảnh báo tức thời.

---

## 3.3 Đề xuất kiến trúc: Streamhouse Trio

### 3.3.1 Khái niệm Streamhouse

Streamhouse là kiến trúc dữ liệu thế hệ thứ ba, được đề xuất bởi Jing Ge (CTO Ververica, Apache Flink PMC) tại Flink Forward Seattle tháng 10/2023. Nguyên lý cốt lõi:

> **Ghi một lần. Phục vụ mọi nơi. (Write once. Serve everywhere.)**

Thay vì tổ chức dữ liệu theo chất lượng (Bronze/Silver/Gold), Streamhouse tổ chức theo **độ tuổi thời gian** (hot/warm/cold), với cơ chế tự động tiering khi dữ liệu già đi:

```
Ghi một lần vào Fluss (HOT)
         │
         ├── HOT  (Fluss, <1 giờ)   → Truy vấn <100ms
         │         ↓ Tiering (tự động)
         ├── WARM (Paimon, 1h–7 ngày) → Truy vấn vài giây, ACID, upsert
         │         ↓ Archival (hàng ngày)
         └── COLD (Iceberg, >7 ngày)  → Phân tích lịch sử, time-travel
```

### 3.3.2 So sánh tổng hợp ba kiến trúc

| Tiêu chí | Lambda | Medallion/Lakehouse | **Streamhouse Trio** |
|----------|:------:|:-------------------:|:--------------------:|
| Latency tối thiểu | ~30 giây | 30s–5 phút | **<100ms** |
| Số codebase | 2 (batch + streaming) | 1 | **1** |
| Lưu trữ trùng lặp | 2× | 3× | **1× (mỗi record chỉ ở 1 layer)** |
| Upsert / dedup | Phức tạp | MERGE INTO (tốn kém) | **Native Primary Key** |
| Temporal join | Không native | Không native | **Native (FOR SYSTEM_TIME AS OF)** |
| Push notification | Không | Không | **Có (Change Feed)** |
| Auto-tiering | Không | Không | **Có (Tiering Service)** |
| Query thống nhất | Không | Một phần | **Một catalog — Trino federation** |

### 3.3.3 Lý do chọn Streamhouse cho bài toán giám sát an ninh

Ba yêu cầu đặc thù của bài toán ánh xạ trực tiếp vào ba tầng của Streamhouse:

1. **Cảnh báo tức thời → HOT (Fluss):** Khi camera phát hiện bạo lực, hệ thống cần phản hồi trong vòng mili-giây. Fluss đạt latency <100ms nhờ WAL (Write-Ahead Log) in-memory.

2. **Phân tích vận hành → WARM (Paimon):** Trực ban cần xem thống kê ca làm việc, so sánh camera theo giờ. Paimon với Trino native connector trả kết quả trong 6–16 giây — phù hợp với dashboard vận hành.

3. **Lưu trữ pháp lý → COLD (Iceberg):** Dữ liệu hình sự cần lưu giữ nhiều năm. Iceberg với Parquet Snappy compression giảm dung lượng ~10× và hỗ trợ time-travel query cho forensic analysis.

---

## 3.4 Kiến trúc tổng thể hệ thống

### 3.4.1 Sơ đồ luồng dữ liệu

```
┌─────────────────────────────────────────────────────────────────────┐
│                         TẦNG THU THẬP                               │
│  Camera RTSP × 15  →  VioMobileNet (*)  →  Kafka [urban-safety-alerts] │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                    TẦNG XỬ LÝ (Apache Flink)                        │
│                                                                     │
│  Job 1: Data Contract Validator                                     │
│    ├─ Hợp lệ   → [hot-violence-alerts-valid]                        │
│    └─ Bất hợp lệ → [urban-safety-quarantine]                        │
│                                                                     │
│  Job 2: HOT Sink (sink_to_fluss_enriched.py)                        │
│    Kafka → Temporal Join dim_camera → Fluss hot_violence_alerts      │
│                                                                     │
│  Job 3: Paimon Aggregation (aggregate_paimon.py)                    │
│    Paimon CDC → daily_incident_stats + camera_stats                 │
│                                                                     │
│  Job 4 (periodic): Tiering (tier_fluss_to_paimon.py)               │
│    Fluss (aged >1h) → Paimon violence_incidents                     │
│                                                                     │
│  Job 5 (daily): Archival (archive_to_iceberg.py)                    │
│    Paimon (aged >7 ngày) → Iceberg historical_violence_incidents    │
└──────────┬──────────────────┬──────────────────┬───────────────────┘
           │                  │                  │
┌──────────▼──────┐  ┌────────▼────────┐  ┌──────▼──────────────────┐
│  FLUSS (HOT)    │  │  PAIMON (WARM)  │  │     ICEBERG (COLD)      │
│  <1 giờ        │  │  1h – 7 ngày    │  │     >7 ngày             │
│  <100ms        │  │  6–16s (Trino)  │  │     8–11s (Trino)       │
│  hot_violence_ │  │  violence_       │  │  historical_violence_   │
│    alerts       │  │    incidents    │  │    incidents            │
│  dim_camera     │  │  daily_stats    │  │  (Parquet, partitioned) │
└──────────┬──────┘  │  camera_stats   │  └──────┬──────────────────┘
           │         └────────┬────────┘         │
           └────────────────┬─┘                  │
                            │                    │
┌───────────────────────────▼────────────────────▼───────────────────┐
│                    TẦNG TRUY VẤN THỐNG NHẤT                         │
│              Trino (Federated SQL Query Engine)                     │
│     HOT path: Flink SQL Gateway → Fluss                             │
│     WARM path: paimon catalog → Paimon/MinIO                        │
│     COLD path: iceberg catalog → Iceberg/MinIO                      │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────┐
│                      TẦNG AI & GIAO DIỆN                            │
│   Agentic RAG Chatbot (LangGraph + Gemini 2.0 + ChromaDB)           │
│   React Dashboard (Command Center, Analytics, Vigilance Terminal)    │
└─────────────────────────────────────────────────────────────────────┘
```

> (*) VioMobileNet: trong triển khai thực nghiệm, module này được thay bằng `rtsp-inference-mock` sinh dữ liệu mô phỏng để kiểm tra toàn bộ pipeline. Tích hợp model thực là hướng mở rộng trong phần 5.3.

### 3.4.2 Các thành phần công nghệ

| Tầng | Công nghệ | Phiên bản | Vai trò |
|------|-----------|-----------|---------|
| Message Broker | Apache Kafka (KRaft) | 4.0.1 | Event streaming, decoupling |
| Stream Processing | Apache Flink + PyFlink | 1.18.1 | Exactly-once, stateful computation |
| HOT Storage | Apache Fluss | 0.9.0 | Real-time queryable streaming storage |
| WARM Storage | Apache Paimon | 0.8.2 | ACID lakehouse, LSM-tree, CDC |
| COLD Storage | Apache Iceberg | 1.5.2 | Parquet archival, time-travel |
| Object Store | MinIO | RELEASE.2024 | S3-compatible, Paimon/Iceberg warehouse |
| Metadata | Hive Metastore + MySQL | 3.1.3 | Iceberg catalog management |
| Query Engine | Trino | 440 | Federated SQL across all layers |
| AI/LLM | Google Gemini 2.0 Flash | API | Intent parsing, Text-to-SQL, answer generation |
| Vector DB | ChromaDB | 0.4.x | Schema context retrieval cho RAG |
| Backend | FastAPI | 0.104 | REST API cho chatbot và dashboard |
| Frontend | React + Tailwind CSS | 18 | Command Center UI |
| Monitoring | Prometheus + Grafana | — | Metrics và alerting |
| Orchestration | Pipeline Manager (custom) | — | Watchdog, tiering scheduler, archival |

---

## 3.5 Tầng HOT — Apache Fluss

### 3.5.1 Đặc điểm kỹ thuật của Fluss

Apache Fluss (Fast Lakehouse Unified Storage Service) là một streaming storage engine được thiết kế để lấp đầy khoảng cách giữa Kafka (fast, limited queryability) và Lakehouse (queryable, slow). Các đặc điểm chính:

- **WAL-based write path:** Ghi vào Write-Ahead Log trước khi flush xuống columnar storage → latency ghi <10ms.
- **Push-based change feed:** Consumer nhận notification khi có dữ liệu mới, không cần polling như Iceberg/Delta.
- **Primary Key enforcement:** Fluss enforce uniqueness trên PK tại write time — không thể tồn tại hai record cùng `incident_id`.
- **Retention-based TTL:** Dữ liệu tự động xóa sau khoảng thời gian cấu hình (1–2 giờ cho HOT layer).

### 3.5.2 Cấu trúc bảng HOT

```sql
CREATE TABLE hot_violence_alerts (
    incident_id  STRING,
    camera_id    STRING,
    `timestamp`  TIMESTAMP(3),
    risk_score   DOUBLE,
    confidence   DOUBLE,
    is_violent   BOOLEAN,
    event_type   STRING,
    location     STRING,   -- enriched từ dim_camera
    ward_id      STRING,
    district     STRING,
    PRIMARY KEY (incident_id) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'table.dml.request.timeout' = '120s'
)
```

**Bảng dimension `dim_camera`** (cũng lưu trong Fluss):

```sql
CREATE TABLE dim_camera (
    camera_id   STRING,
    location    STRING,
    ward_id     STRING,
    district    STRING,
    latitude    DOUBLE,
    longitude   DOUBLE,
    PRIMARY KEY (camera_id) NOT ENFORCED
)
```

### 3.5.3 Temporal Join — điểm khác biệt quan trọng

Fluss cho phép join với **trạng thái của dimension table tại thời điểm event xảy ra**, nhờ cơ chế `FOR SYSTEM_TIME AS OF`:

```sql
INSERT INTO fluss.security.hot_violence_alerts
SELECT
    a.incident_id,
    a.camera_id,
    a.`timestamp`,
    a.risk_score,
    a.confidence,
    a.is_violent,
    a.event_type,
    c.location,    -- location của camera TẠI THỜI ĐIỂM event
    c.ward_id,
    c.district
FROM kafka_source_table AS a
LEFT JOIN fluss.security.dim_camera
    FOR SYSTEM_TIME AS OF a.proc_time AS c  -- temporal join
ON a.camera_id = c.camera_id
```

Với Medallion/Iceberg, để đạt được kết quả tương tự cần tự implement SCD Type 2 (Slowly Changing Dimension) — một kỹ thuật phức tạp đòi hỏi thêm nhiều bảng và logic xử lý.

---

## 3.6 Tầng WARM — Apache Paimon

### 3.6.1 Đặc điểm kỹ thuật của Paimon

Apache Paimon là một stream-native lakehouse format với LSM-tree (Log-Structured Merge-tree) làm nền tảng — tương tự cách RocksDB hoạt động nhưng trên object storage. Các đặc điểm:

- **LSM-tree write path:** Ghi vào in-memory buffer → flush xuống L0 → compaction dần lên Ln → hiệu quả với workload write-heavy liên tục.
- **Changelog producer:** Paimon tự động sinh changelog khi có upsert/delete → downstream consumer (aggregation job) nhận trực tiếp stream of changes.
- **Native Primary Key:** Merge engine `deduplicate` đảm bảo chỉ tồn tại một record cho mỗi `incident_id`.
- **Trino native connector:** Trino đọc Paimon manifest file trực tiếp, không qua Flink — đây là điểm mấu chốt giúp giảm latency WARM từ 3–5 phút (Flink Gateway) xuống còn 6–16 giây.

### 3.6.2 Star Schema trong WARM layer

Hệ thống tổ chức dữ liệu WARM theo mô hình Star Schema:

```
dim_camera (Fluss)              dim_time (Paimon)
  camera_id PK                    date_id PK
  location, ward_id               date, year, month, day
  district, lat, lon
          ↘                      ↙
        fact_violence_incidents (Paimon)
          incident_id PK
          camera_id FK | date_id FK
          timestamp, risk_score, confidence
          is_violent, event_type, location
```

**Bảng `violence_incidents`** (bảng chính, phục vụ chatbot WARM):

```sql
CREATE TABLE violence_incidents (
    incident_id  STRING,
    camera_id    STRING,
    `timestamp`  TIMESTAMP(3),
    risk_score   DOUBLE,
    confidence   DOUBLE,
    is_violent   BOOLEAN,
    event_type   STRING,
    location     STRING,
    is_deleted   BOOLEAN,
    PRIMARY KEY (incident_id) NOT ENFORCED
) WITH (
    'merge-engine'       = 'deduplicate',
    'changelog-producer' = 'input',
    'snapshot.time-retained' = '7d',
    'bucket'             = '4'
)
```

**Bảng aggregate `daily_incident_stats`** (được sinh bởi Job 3 từ CDC changelog):

```sql
CREATE TABLE daily_incident_stats (
    stat_date         DATE,
    location          STRING,
    total_incidents   BIGINT,
    violent_incidents BIGINT,
    avg_risk_score    DOUBLE,
    max_risk_score    DOUBLE,
    PRIMARY KEY (stat_date, location) NOT ENFORCED
) WITH (
    'merge-engine'       = 'deduplicate',
    'changelog-producer' = 'input'
)
```

### 3.6.3 Trino native connector cho Paimon

Kết nối Trino với Paimon đòi hỏi build connector từ source do phiên bản stable chưa hỗ trợ Trino 440. Quá trình gồm hai bước:

1. **Build `paimon-trino-440` từ source** (Maven 21, release-0.8):
   ```dockerfile
   FROM maven:3.9-eclipse-temurin-21 AS builder
   RUN git clone --depth 1 --branch release-0.8 \
       https://github.com/apache/paimon-trino.git
   RUN mvn -pl paimon-trino-440 package -DskipTests
   ```

2. **HDFS patch:** Loại bỏ `HdfsModule` khỏi `TrinoConnectorFactory` để tránh dependency conflict với Hive connector, thay bằng S3FileSystem.

Catalog configuration (`paimon.properties`):
```properties
connector.name=paimon
warehouse=s3://warehouse/paimon
s3.endpoint=http://minio:9000
s3.access-key=<access-key>
s3.secret-key=<secret-key>
s3.path-style-access=true
```

---

## 3.7 Tầng COLD — Apache Iceberg

### 3.7.1 Vai trò trong kiến trúc

COLD layer phục vụ hai mục đích: lưu trữ dài hạn với chi phí thấp, và hỗ trợ forensic analysis với time-travel query. Apache Iceberg format V2 được chọn vì:

- Parquet Snappy compression giảm dung lượng ~10× so với raw JSON.
- Partitioning theo `incident_date` cho phép Trino bỏ qua toàn bộ partition không liên quan (partition pruning).
- Time-travel: `SELECT * FROM iceberg_table FOR TIMESTAMP AS OF '2026-01-01 00:00:00'`.

### 3.7.2 Cấu trúc bảng COLD

```sql
CREATE TABLE historical_violence_incidents (
    incident_id  STRING,
    camera_id    STRING,
    `timestamp`  TIMESTAMP(3),
    risk_score   DOUBLE,
    confidence   DOUBLE,
    is_violent   BOOLEAN,
    event_type   STRING,
    location     STRING,
    incident_date DATE
) PARTITIONED BY (incident_date)
WITH (
    'format-version'                 = '2',
    'write.parquet.compression-codec' = 'snappy'
)
```

### 3.7.3 Chiến lược archival

Job archival (`archive_to_iceberg.py`) chạy mỗi ngày lúc 2:00 UTC, được pipeline-manager trigger:

```python
# Chỉ archive data cũ hơn 7 ngày
INSERT INTO iceberg.security.historical_violence_incidents
SELECT * FROM paimon.security.violence_incidents
WHERE `timestamp` < LOCALTIMESTAMP - INTERVAL '7' DAY
  AND incident_id NOT IN (
      SELECT incident_id FROM iceberg.security.historical_violence_incidents
  )
```

Điều kiện `NOT IN` đảm bảo idempotency — re-run không tạo duplicate.

---

## 3.8 Data Contract — Kiểm soát chất lượng tại nguồn

### 3.8.1 Nguyên tắc Shift-Left

Thay vì phát hiện lỗi dữ liệu ở downstream (schema-on-read), hệ thống áp dụng **schema-on-write**: kiểm tra ngay khi dữ liệu đến Kafka, trước khi vào bất kỳ storage nào.

### 3.8.2 Data Contract định nghĩa

| Rule ID | Điều kiện | Hành động |
|---------|-----------|----------|
| `no_future_timestamps` | `timestamp ≤ now + 1 phút` | REJECT |
| `valid_camera_id` | `camera_id khớp ^cam_\d{2}$` | REJECT |
| `risk_score_range` | `0.0 ≤ risk_score ≤ 1.0` | REJECT |
| `confidence_range` | `0.0 ≤ confidence ≤ 1.0` | REJECT |
| `event_type_required` | Nếu `is_violent=true` thì `event_type ≠ null` | REJECT |
| `high_confidence_critical` | Nếu `event_type ∈ {STABBING, SHOOTING}` thì `confidence ≥ 0.85` | WARN |

### 3.8.3 Luồng xử lý validation

```
[urban-safety-alerts]  →  Flink Validator  ─┬→ [hot-violence-alerts-valid]  → HOT/WARM/COLD
                                             └→ [urban-safety-quarantine]    → Paimon quarantine table
```

Record bất hợp lệ không bị mất — được gửi vào quarantine topic kèm danh sách violation codes để phân tích sau.

---

## 3.9 Cơ chế Tiering tự động

### 3.9.1 Pipeline Manager

`pipeline_manager.py` là một long-running service đảm nhận ba nhiệm vụ:

| Nhiệm vụ | Chu kỳ | Cơ chế |
|---------|--------|--------|
| **Watchdog** | 5 phút/lần | Kiểm tra các Flink job còn sống, restart nếu chết |
| **Tiering HOT→WARM** | 30 phút/lần | Chạy `tier_fluss_to_paimon.py` blocking |
| **Archival WARM→COLD** | 1 lần/ngày lúc 2:00 UTC | Chạy `archive_to_iceberg.py` |

### 3.9.2 Tiering HOT → WARM

```python
# tier_fluss_to_paimon.py (rút gọn)

TIERING_HOURS = 1   # Data cũ hơn 1 giờ được tiering

cutoff = now - timedelta(hours=TIERING_HOURS)

# Phase 1: Flink streaming job đọc từ Fluss → ghi vào Paimon
stmt.add_insert_sql(f"""
    INSERT INTO paimon.security.violence_incidents
    SELECT ... FROM fluss.security.hot_violence_alerts
    WHERE `timestamp` < TO_TIMESTAMP('{cutoff}')
""")
result = stmt.execute()
time.sleep(120)        # Chờ Paimon checkpoint commit (4 × 30s)
job_client.cancel()    # Cancel job sau khi checkpoint xong

# Phase 2: Xóa data cũ khỏi Fluss (best-effort)
t_env.execute_sql(f"""
    DELETE FROM fluss.security.hot_violence_alerts
    WHERE `timestamp` < TO_TIMESTAMP('{cutoff}')
""")
```

**Lưu ý thiết kế:** Cơ chế tiering được triển khai dưới dạng **periodic micro-batch** (30 phút/lần) thay vì continuous streaming, do Apache Fluss 0.9 chưa hỗ trợ native event-time TTL tiering. Phase 2 (xóa khỏi Fluss) có thể không thành công trong một số phiên bản connector — khi đó query routing theo `time_period` tại chatbot vẫn đảm bảo dữ liệu cũ không bị phục vụ nhầm layer. Đây là hạn chế kỹ thuật của Fluss 0.9 và là hướng cải thiện khi nền tảng ra phiên bản mới.

---

## 3.10 Agentic RAG — Truy vấn ngôn ngữ tự nhiên

### 3.10.1 Kiến trúc agent

Chatbot sử dụng LangGraph để tổ chức luồng xử lý theo đồ thị agent:

```
Câu hỏi người dùng
        │
   ┌────▼────┐
   │ Node 1  │ understand_query
   │         │ Gemini trích xuất: time_period, location, metric, intent
   └────┬────┘
        │
   ┌────▼────┐
   │ Node 2  │ select_data_layer
   │         │ time_period < 1h  → Fluss (HOT)
   │         │ 1h ≤ time ≤ 7d   → Paimon (WARM)
   │         │ time > 7d         → Iceberg (COLD)
   └────┬────┘
        │
   ┌────▼────┐
   │ Node 3  │ generate_sql
   │         │ Gemini sinh SQL phù hợp với layer được chọn
   │         │ Schema context từ ChromaDB (anti-hallucination)
   └────┬────┘
        │
   ┌────▼────┐
   │ Node 4  │ execute_query
   │         │ HOT: Flink SQL Gateway REST API
   │         │ WARM/COLD: Trino via PyTrino
   └────┬────┘
        │
   ┌────▼────┐     lỗi, retry < 3
   │ Node 5  │ self_correct ◄──────────────────┐
   │         │ Gemini phân tích lỗi, sửa SQL   │
   └────┬────┘ ──────────────────────────────►─┘
        │
   ┌────▼────┐
   │ Node 6  │ generate_response
   │         │ Câu trả lời ngôn ngữ tự nhiên + citation
   └────┬────┘
        │
   Trả về người dùng (kèm: layer, source_table, latency_ms)
```

### 3.10.2 Anti-hallucination với ChromaDB

ChromaDB lưu metadata schema của tất cả tables. Khi Gemini sinh SQL, nó nhận schema context từ ChromaDB thay vì dựa vào training knowledge — tránh hallucinate tên column/table không tồn tại:

```python
# Ví dụ document trong ChromaDB
{
    "id": "violence_incidents",
    "document": "Table violence_incidents in Paimon WARM layer. "
                "Columns: incident_id (STRING PK), camera_id (STRING), "
                "timestamp (TIMESTAMP), risk_score (DOUBLE 0-1), "
                "confidence (DOUBLE 0-1), is_violent (BOOLEAN), "
                "event_type (STRING: FIGHTING/ASSAULT/STABBING/SHOOTING), "
                "location (STRING), is_deleted (BOOLEAN). "
                "Use for queries about 1h–7d timeframe.",
    "metadata": {"layer": "warm", "catalog": "paimon"}
}
```

### 3.10.3 Routing logic theo time_period

```python
def detect_layer(time_period_hours: float) -> str:
    if time_period_hours < 1:
        return "fluss"    # HOT
    elif time_period_hours <= 168:  # 7 ngày
        return "paimon"   # WARM
    else:
        return "iceberg"  # COLD
```

Ví dụ routing từ câu hỏi tự nhiên:

| Câu hỏi | time_period | Layer |
|---------|------------|-------|
| "30 phút qua" | 0.5h | Fluss (HOT) |
| "3 giờ qua" | 3h | Paimon (WARM) |
| "tuần trước" | 168h | Paimon (WARM) |
| "tháng trước" | ~720h | Iceberg (COLD) |

---

## 3.11 Triển khai hệ thống

### 3.11.1 Môi trường triển khai

Hệ thống được containerized hoàn toàn bằng Docker Compose và triển khai trên Google Cloud Platform:

| Môi trường | Mục đích | Cấu hình |
|-----------|---------|---------|
| **Local** | Development, testing | Windows 11, 16GB RAM, Docker Desktop |
| **GCP** | Production demo | `e2-standard-4` (4 vCPU, 16GB), Singapore |

### 3.11.2 Tổ chức service theo profile

```
Profile mặc định (core):
  kafka, minio, flink-jobmanager, flink-taskmanager,
  fluss-zookeeper, fluss-coordinator, fluss-tablet,
  mysql, hive-metastore, trino-coordinator,
  chatbot, pipeline-manager

Profile streaming:   mediamtx, rtsp_pusher, rtsp-inference-mock
Profile monitoring:  prometheus, grafana, node-exporter
Profile ui:          kafka-ui, flink-sql-gateway
Profile scaling:     trino-worker-1, trino-worker-2
```

Thiết kế profile cho phép khởi động đúng services cần thiết tùy ngữ cảnh, tiết kiệm RAM trên máy 16GB.

### 3.11.3 Pipeline Manager — vòng lặp chính

```
Khởi động:
  1. Chờ Flink JobManager sẵn sàng (tối đa 3 phút)
  2. Seed dim_camera (15 cameras) qua Flink SQL Gateway
  3. Submit các Flink streaming job còn thiếu

Vòng lặp chính (mỗi 5 phút):
  ┌─ Watchdog: kiểm tra 3 jobs bắt buộc đang chạy
  │    → Restart nếu FAILED/CANCELED
  ├─ Tiering check: nếu đủ 30 phút từ lần trước
  │    → Chạy tier_fluss_to_paimon.py (blocking ~4 phút)
  └─ Archival check: nếu đúng 2:00 AM UTC
       → Chạy archive_to_iceberg.py
```

### 3.11.4 Quy trình khởi động production (GCP)

```bash
# 1. Start VM
gcloud compute instances start instance-20260524-104630 --zone=asia-southeast1-b

# 2. Start toàn bộ services
gcloud compute ssh instance-20260524-104630 --zone=asia-southeast1-b --command='
  cd ~/streamhouse/deploy
  docker compose -f docker-compose.gcp.yml --env-file .env.gcp up -d
'

# 3. Chờ ~5 phút cho pipeline-manager seed dim_camera và submit Flink jobs

# 4. Verify
curl http://34.87.122.219:5002/health
```

---

## 3.12 Dashboard và giao diện người dùng

React dashboard gồm bốn trang chức năng:

| Trang | Chức năng | Data source |
|-------|----------|-------------|
| **Command Center** | Real-time camera grid, live risk scores, layer status badge | `/api/layer-counts`, `/api/latency` |
| **Live Streams** | HLS video stream từ camera (qua MediaMTX + ngrok) | MediaMTX HLS |
| **Analytics** | Biểu đồ thống kê theo camera, ngày, giờ | Paimon (WARM) qua chatbot |
| **Vigilance Terminal** | Chatbot Agentic RAG, lịch sử hội thoại | `/api/chat` |

`LayerBadge` component trên Command Center hiển thị số liệu thực tế từ API:

```
HOT  [Fluss]   10,312 rows  │  100ms
WARM [Paimon]  10,312 rows  │  5.9s
COLD [Iceberg]     0  rows  │  9.5s
```

---

## 3.13 Tổng kết chương

Chương này trình bày kiến trúc Streamhouse Trio được đề xuất như một giải pháp thay thế cho Lambda và Medallion Architecture trong bài toán giám sát an ninh thời gian thực. Ba đóng góp kỹ thuật chính:

1. **Ba tầng lưu trữ chuyên biệt:** Fluss (HOT, <100ms), Paimon (WARM, 6–16s), Iceberg (COLD, 8–11s) — mỗi tầng được tối ưu cho một use case cụ thể, không có tầng nào làm compromise cho tầng khác.

2. **Trino native connector cho Paimon:** Giải pháp kỹ thuật build từ source và HDFS patch giúp giảm latency WARM từ 3–5 phút (Flink SQL Gateway) xuống còn 6–16 giây — cải thiện 14–23 lần.

3. **Agentic RAG với multi-store routing:** Chatbot tự động phân loại câu hỏi theo time_period và routing đến đúng storage layer, với self-correction tối đa 3 lần và anti-hallucination qua ChromaDB.

Chương tiếp theo trình bày kết quả đánh giá hiệu năng thực nghiệm trên môi trường GCP.
