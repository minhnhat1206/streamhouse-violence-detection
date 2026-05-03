# Apache Flink — Compute Layer

> **Vai trò**: Stream processing engine cho kiến trúc Streamhouse
> **Version**: Flink 1.18.1 + PyFlink
> **Cập nhật**: 2026-04-13

---

## 1. Tại sao chọn Flink thay vì Spark?

| Tiêu chí | Apache Spark | Apache Flink |
|----------|-------------|-------------|
| Mô hình xử lý | Micro-batching (gom nhóm mỗi 0.5-2s) | Event-at-a-time (từng sự kiện) |
| Latency | Hàng trăm ms đến giây | **<100ms** |
| Semantics | At-least-once (mặc định) | **Exactly-once** (native) |
| State management | Stateless (phải dùng external store) | **Stateful** (managed state + RocksDB) |
| Fluss/Paimon support | Không có | **Native connector** |
| Checkpoint | Qua RDD lineage (tốn I/O) | **Barrier-based** (nhẹ, nhanh) |

Flink là lựa chọn duy nhất có thể kết nối đồng thời Kafka, Fluss, Paimon và Iceberg trong cùng một job.

---

## 2. Cluster Architecture

```
┌──────────────────────────────────────────────────┐
│                  JobManager                       │
│  Container: jobmanager | Port: 8081 (Web UI)     │
│                                                   │
│  • Nhận job từ flink run -py ...                  │
│  • Tạo execution graph (DAG)                      │
│  • Lập lịch task → TaskManager                    │
│  • Điều phối checkpoint barriers                  │
│  • RAM: 1 GB | CPU: 0.50                         │
├──────────────────────────────────────────────────┤
│                 TaskManager                       │
│  Container: taskmanager                           │
│                                                   │
│  • Thực thi task (operator instances)             │
│  • 4 task slots (mỗi slot ~384 MB)               │
│  • Process memory: 1536m                          │
│  • RAM: 2 GB | CPU: 2.00                         │
│                                                   │
│  ┌─────────┬─────────┬─────────┬─────────┐      │
│  │ Slot 0  │ Slot 1  │ Slot 2  │ Slot 3  │      │
│  │Validator│Validator│ Fluss   │ Paimon  │      │
│  │ (p=0)   │ (p=1)   │  Sink   │  Sink   │      │
│  └─────────┴─────────┴─────────┴─────────┘      │
└──────────────────────────────────────────────────┘
```

### Task Slots

Mỗi slot là một **đơn vị tài nguyên cố định** (memory slice) trong TaskManager.
Một job chiếm N slots = parallelism của job đó.

| Cấu hình | Giá trị | Giải thích |
|-----------|---------|------------|
| `taskmanager.numberOfTaskSlots` | 4 | Tối đa 4 task chạy song song |
| `taskmanager.memory.process.size` | 1536m | Tổng memory cho JVM process |
| Memory per slot | ~384m | 1536m ÷ 4 slots |

**Hiện tại 4/4 slots đã dùng hết.** Để thêm job mới (ví dụ aggregation), cần giảm parallelism của Validator xuống 1, hoặc tăng `numberOfTaskSlots` (kèm tăng memory).

---

## 3. Custom Docker Image

```dockerfile
# docker/Dockerfile.flink
FROM flink:1.18.1-scala_2.12

# Python 3.10 + PyFlink
RUN apt-get install python3.10 && pip install apache-flink==1.18.1

# Connectors → /opt/flink/lib/ (system classpath)
├── flink-sql-connector-kafka-3.1.0-1.18.jar      # Kafka source/sink
├── fluss-flink-1.18-0.9.0-incubating.jar          # Fluss connector
├── paimon-flink-1.18-0.8.2.jar                    # Paimon connector
├── paimon-s3-0.8.2.jar                            # Paimon ↔ MinIO S3
└── flink-shaded-hadoop-2-uber-2.8.3-10.0.jar      # Hadoop S3A config
```

**Tại sao JARs nằm trong `/opt/flink/lib/` thay vì dùng `-j` flag?**

Khi dùng `-j` hoặc `pipeline.jars`, mỗi job có classloader riêng → gây conflict giữa Paimon và Hadoop. Đặt JARs vào system classpath (`/opt/flink/lib/`) giải quyết vấn đề này.

---

## 4. Flink Jobs — Chi tiết

### 4.1. Job 1: Data Contract Validator

**File**: `scripts/transform/data_contract_validator.py`
**API**: PyFlink DataStream (không phải SQL)
**Parallelism**: 2
**Slots**: 2

```
Kafka: urban-safety-alerts
         │
         ▼
  FlinkKafkaConsumer
         │
         ▼
  map(validate_record)          ← Python function
         │
    ┌────┴────┐
    ▼         ▼
  Valid     Invalid
    │         │
    ▼         ▼
  FlinkKafkaProducer          FlinkKafkaProducer
  → hot-violence-alerts-valid → urban-safety-quarantine
```

**Tại sao dùng DataStream API thay vì SQL?**

Validation logic phức tạp (regex, multiple conditions, warning vs rejection) — SQL không linh hoạt đủ. DataStream cho phép viết Python thuần trong `map()`.

**Validation rules:**

| Rule | Check | Action |
|------|-------|--------|
| `FUTURE_TIMESTAMP` | `timestamp > now + 1 phút` | REJECT |
| `INVALID_CAMERA_ID` | Không khớp `^cam_\d{2}$` | REJECT |
| `RISK_SCORE_OUT_OF_RANGE` | `< 0` hoặc `> 1` | REJECT |
| `CONFIDENCE_OUT_OF_RANGE` | `< 0` hoặc `> 1` | REJECT |
| `MISSING_EVENT_TYPE` | `is_violent=true` nhưng thiếu `event_type` | REJECT |
| `LOW_CONFIDENCE_CRITICAL` | STABBING/SHOOTING với `confidence < 0.85` | WARNING (vẫn pass) |

**Output message format** (thêm 2 fields vào original message):

```json
{
  "event_id": "evt_abc123",
  "camera_id": "cam_06",
  "timestamp": "2026-04-13T10:30:00+07:00",
  "risk_score": 0.85,
  "confidence": 0.92,
  "is_violent": true,
  "event_type": "FIGHTING",
  "location": "...",
  "metadata": "...",
  "is_valid": true,
  "violations": []
}
```

---

### 4.2. Job 2: Sink to Fluss (Hot Storage)

**File**: `scripts/transform/sink_to_fluss.py`
**API**: Flink Table/SQL
**Parallelism**: 1
**Slots**: 1
**Checkpoint**: Không cần (Fluss tự quản lý durability)

```
Kafka: hot-violence-alerts-valid
  (consumer group: fluss-sink-group)
         │
         ▼
  CREATE TEMPORARY TABLE kafka_valid_alerts (...)
  WITH ('connector' = 'kafka', ...)
         │
         ▼
  INSERT INTO fluss.security.hot_violence_alerts
  SELECT
      event_id AS incident_id,
      camera_id,
      row_time AS `timestamp`,    ← parsed từ ISO string
      risk_score, confidence,
      is_violent, event_type
  FROM kafka_valid_alerts
  WHERE is_valid = true
```

**Xử lý timestamp:**

Inference-mock gửi timestamp dạng ISO8601 (`2026-04-13T10:30:00+07:00`).
Flink SQL `TO_TIMESTAMP()` không hiểu timezone offset, nên cần:

```sql
row_time AS TO_TIMESTAMP(
  SUBSTR(
    REPLACE(REPLACE(`timestamp`, 'T', ' '), 'Z', ''),
    1, 23
  )
)
```

Logic: `2026-04-13T10:30:00+07:00` → `2026-04-13 10:30:00` → `TIMESTAMP(3)`

**Fluss table schema** (tạo bởi `init_fluss_tables.py`):

```sql
CREATE TABLE hot_violence_alerts (
    incident_id STRING PRIMARY KEY NOT ENFORCED,
    camera_id   STRING,
    `timestamp` TIMESTAMP(3),
    risk_score  DOUBLE,
    confidence  DOUBLE,
    is_violent  BOOLEAN,
    event_type  STRING
) WITH ('connector' = 'fluss')
```

- PK table → hỗ trợ upsert (dedup tự động theo `incident_id`)
- Data lưu trong Fluss TabletServer (RAM + disk)
- Retention: 1-2 giờ → query latency <100ms

---

### 4.3. Job 3: Sink to Paimon (Warm Storage)

**File**: `scripts/transform/sink_to_paimon.py`
**API**: Flink Table/SQL
**Parallelism**: 1
**Slots**: 1
**Checkpoint**: 30 giây (bắt buộc)

```
Kafka: hot-violence-alerts-valid
  (consumer group: paimon-sink-group)
         │
         ▼
  CREATE CATALOG paimon WITH (
      'type' = 'paimon',
      'warehouse' = 's3://warehouse/paimon',
      's3.endpoint' = 'http://minio:9000',
      ...
  )
         │
         ▼
  INSERT INTO paimon.security.violence_incidents
  SELECT
      event_id AS incident_id,
      camera_id,
      row_time AS `timestamp`,
      risk_score, confidence,
      is_violent, event_type,
      location,
      CAST(false AS BOOLEAN) AS is_deleted    ← soft-delete support
  FROM kafka_valid_alerts
  WHERE is_valid = true
```

**Tại sao Paimon bắt buộc checkpoint?**

Paimon dùng **LSM-tree** storage. Data được buffer trong memory, chỉ flush thành file ORC trên MinIO khi Flink checkpoint hoàn thành. Không có checkpoint = không có data trên disk.

```python
env.enable_checkpointing(30000)  # 30 giây
```

Mỗi checkpoint → Paimon tạo:
- **Data file** (`.orc`): Dữ liệu thực tế
- **Changelog file** (`.orc`): CDC changes cho downstream consumers
- **Snapshot**: Metadata trỏ đến data files
- **Manifest**: Danh sách data files trong snapshot

**Paimon table schema** (tạo bởi `init_paimon_tables.py`):

```sql
CREATE TABLE violence_incidents (
    incident_id STRING PRIMARY KEY NOT ENFORCED,
    camera_id   STRING,
    `timestamp` TIMESTAMP(3),
    risk_score  DOUBLE,
    confidence  DOUBLE,
    is_violent  BOOLEAN,
    event_type  STRING,
    location    STRING,
    is_deleted  BOOLEAN
) WITH (
    'merge-engine' = 'deduplicate',
    'changelog-producer' = 'input',
    'snapshot.time-retained' = '7d',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '50'
)
```

| Config | Giá trị | Giải thích |
|--------|---------|------------|
| `merge-engine` | `deduplicate` | Giữ row mới nhất theo PK (incident_id) |
| `changelog-producer` | `input` | CDC changelog từ input stream |
| `snapshot.time-retained` | `7d` | Giữ snapshots 7 ngày cho time-travel |
| `snapshot.num-retained.min` | `5` | Giữ tối thiểu 5 snapshots |
| `snapshot.num-retained.max` | `50` | Giữ tối đa 50 snapshots |

---

## 5. Hai job Fluss & Paimon đọc cùng topic như thế nào?

```
           hot-violence-alerts-valid
           (Kafka topic, 3 partitions)
                     │
        ┌────────────┼────────────┐
        ▼                         ▼
  Consumer Group:           Consumer Group:
  fluss-sink-group          paimon-sink-group
        │                         │
        ▼                         ▼
   Job 2 (Fluss)            Job 3 (Paimon)
```

Kafka deliver **cùng message** cho mỗi consumer group **độc lập**.
Hai job dùng 2 group khác nhau → cả hai nhận đầy đủ data → ghi vào 2 storage layer song song.

Mỗi group theo dõi offset riêng. Nếu Job 2 lag hoặc restart, không ảnh hưởng Job 3.

---

## 6. Data Flow tổng thể

```
inference-mock (1 msg/giây)
       │
       ▼
  Kafka: urban-safety-alerts
       │
       ▼
  ┌──────────────────────────────────────┐
  │ Job 1: Data Contract Validator       │  DataStream API
  │ Parallelism: 2 | Slots: 2           │  Python validate_record()
  │                                      │
  │ Input:  urban-safety-alerts          │
  │ Output: hot-violence-alerts-valid    │  (valid)
  │         urban-safety-quarantine      │  (invalid)
  └──────────────┬───────────────────────┘
                 │
    ┌────────────┴────────────┐
    ▼                         ▼
  ┌────────────────┐   ┌─────────────────┐
  │ Job 2: Fluss   │   │ Job 3: Paimon   │  Flink SQL API
  │ Sink           │   │ Sink            │
  │ P=1 | Slot: 1  │   │ P=1 | Slot: 1  │
  │ No checkpoint  │   │ CP: 30s         │
  │                │   │                 │
  │ Group:         │   │ Group:          │
  │ fluss-sink     │   │ paimon-sink     │
  └───────┬────────┘   └───────┬─────────┘
          │                     │
          ▼                     ▼
   Fluss TabletServer    MinIO S3 (ORC files)
   (HOT, <100ms)         (WARM, 7-30 ngày)
```

**Slot summary:**

| Job | Parallelism | Slots |
|-----|-------------|-------|
| Data Contract Validator | 2 | 2 |
| Sink to Fluss | 1 | 1 |
| Sink to Paimon | 1 | 1 |
| **Tổng** | | **4 / 4** |

---

## 7. Checkpointing

Checkpoint = Flink lưu trạng thái (Kafka offsets, internal state) vào durable storage để recovery khi failure.

| Config | Job 1 (Validator) | Job 2 (Fluss) | Job 3 (Paimon) |
|--------|-------------------|---------------|----------------|
| Checkpoint enabled | Không | Không | **Có (30s)** |
| Lý do | Stateless (chỉ map/filter) | Fluss tự quản lý | Paimon cần CP để commit |

Khi Paimon job checkpoint:
1. Flink gửi **checkpoint barrier** qua data stream
2. Tất cả operators lưu state
3. Paimon **flush buffer → ORC files** trên MinIO
4. Paimon tạo **snapshot** trỏ đến data files
5. JobManager ghi checkpoint metadata

---

## 8. Lệnh quản lý

### Submit job

```bash
# DataStream job
docker exec jobmanager flink run -py /opt/flink/scripts/data_contract_validator.py

# SQL job
docker exec jobmanager flink run -py /opt/flink/scripts/sink_to_paimon.py
```

### Xem danh sách jobs

```bash
curl -s http://localhost:8081/jobs/overview | python3 -c "
import sys, json
for j in json.load(sys.stdin).get('jobs', []):
    print(f'{j[\"state\"]:10s} | {j[\"name\"]}')
"
```

### Cancel job (REST API)

```bash
# Lấy JOB_ID từ lệnh trên
curl -X PATCH "http://localhost:8081/jobs/<JOB_ID>?mode=cancel"
```

> **Lưu ý:** Lệnh `flink cancel <JOB_ID>` bị lỗi do conflict giữa `hadoop-2-uber.jar` và `commons-cli`. Dùng REST API thay thế.

### Xem checkpoint status

```bash
curl -s http://localhost:8081/jobs/<JOB_ID>/checkpoints | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'Completed: {d[\"counts\"][\"completed\"]}')
print(f'Failed: {d[\"counts\"][\"failed\"]}')
"
```

### Xem TaskManager slots

```bash
curl -s http://localhost:8081/taskmanagers | python3 -c "
import sys, json
for t in json.load(sys.stdin).get('taskmanagers', []):
    print(f'slots={t[\"slotsNumber\"]}, free={t[\"freeSlots\"]}')
"
```

---

## 9. Monitoring — Flink Web UI

Truy cập: **http://localhost:8081**

| Tab | Thông tin |
|-----|-----------|
| **Overview** | Số jobs running/completed/failed, TaskManagers, slots |
| **Running Jobs** | Danh sách jobs đang chạy |
| **Job Detail** | DAG visualization, records in/out per operator |
| **Checkpoints** | Số checkpoint completed/failed, duration, size |
| **Exceptions** | Stacktrace khi job fail |
| **TaskManagers** | Memory usage, CPU, logs per TaskManager |

---

## 10. Troubleshooting

### Job FAILED ngay sau submit

**Nguyên nhân phổ biến**: `NoResourceAvailableException` — hết slots.

```bash
# Kiểm tra slots khả dụng
curl -s http://localhost:8081/taskmanagers | python3 -c "
import sys, json
for t in json.load(sys.stdin).get('taskmanagers', []):
    print(f'free={t[\"freeSlots\"]}/{t[\"slotsNumber\"]}')
"
```

**Fix**: Cancel job không cần thiết hoặc tăng `taskmanager.numberOfTaskSlots`.

### NullPointerException khi parse timestamp

**Nguyên nhân**: Timestamp có timezone offset (`+07:00`) mà `TO_TIMESTAMP()` không hiểu.

**Fix**: Dùng `SUBSTR(REPLACE(...))` cắt timezone trước khi parse (đã áp dụng trong sink scripts).

### Paimon không có data trên MinIO

**Nguyên nhân**: Thiếu `env.enable_checkpointing()`.

**Fix**: Thêm `env.enable_checkpointing(30000)` trước `StreamTableEnvironment.create(env)`.

### `flink cancel` crash với NoSuchMethodError

**Nguyên nhân**: `hadoop-2-uber.jar` gây conflict `commons-cli`.

**Fix**: Dùng REST API: `curl -X PATCH "http://localhost:8081/jobs/<ID>?mode=cancel"`

### Hive Metastore crash — "no such file or directory"

**Nguyên nhân**: `entrypoint.sh` có CRLF line endings (Windows).

**Fix**: `sed -i 's/\r$//' config/hive_metastore/entrypoint.sh` rồi rebuild.
