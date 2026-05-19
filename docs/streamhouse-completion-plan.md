# Kế Hoạch Hoàn Thiện Kiến Trúc Streamhouse

> Mục tiêu: Chuyển từ kiến trúc Lambda-like hiện tại sang Streamhouse thuần  
> Thời gian dự kiến: 3–5 ngày  
> Priority: Cao — đây là core contribution của khóa luận

---

## Trạng Thái Hiện Tại (As-Is)

### Kiến trúc đang chạy (Lambda-like)
```
Kafka ──→ sink_to_fluss.py   (Flink Job) ──→ Fluss  (HOT)
       └─→ sink_to_paimon.py (Flink Job) ──→ Paimon (WARM)
```

**Vấn đề**: Ghi 2 lần từ Kafka = Lambda Architecture, không phải Streamhouse.

### Những gì đã hoạt động tốt ✅
- Fluss HOT layer: nhận data từ RTSP pipeline, query được <100ms
- Paimon WARM layer: Flink checkpoint → Trino query hoạt động
- Iceberg COLD layer: archive job, time-travel query
- Trino federation: cross-layer SQL
- Chatbot RAG: Text-to-SQL với Gemini 2.0, self-correction
- Frame evidence: MinIO S3, HTTP URL, gallery trong chatbot
- Pipeline manager: tự động submit/monitor 4 Flink jobs

### Những gì chưa đúng ❌
- Fluss Tiering Service chưa bật → không có auto-tiering
- Temporal join với dim_camera chưa implement
- Star schema chưa có (fact/dim tables)
- HOT chatbot query trả về 0 rows khi `is_violent = TRUE` filter quá strict
- Union Read API chưa bao gồm HOT (Fluss) layer

---

## Kiến Trúc Đích (To-Be)

```
RTSP Cameras
     ↓
VioMobileNet Inference (rtsp-inference-mock)
     ↓
Kafka: urban-safety-alerts
     ↓
Flink: data_contract_validator.py (validate + enrich)
     ├─→ Kafka: hot-violence-alerts-valid  (valid events)
     └─→ Kafka: urban-safety-quarantine    (invalid events)
          ↓
     Flink: sink_to_fluss.py (write ONCE)
          ↓
     Fluss: hot_violence_alerts + dim_camera
          │
          ↓ [Fluss Tiering Service — automatic]
     Paimon: fact_violence_incidents + dim_camera + dim_time
          │
          ↓ [Archive Job — scheduled nightly]
     Iceberg: historical_violence_incidents
          │
     Trino Federation (unified query layer)
          │
     Chatbot RAG (Gemini 2.0 + LangGraph)
          │
     React Dashboard
```

---

## Phase 1: Fix Core Architecture (Ngày 1–2)

### Task 1.1 — Bật Fluss Tiering Service
**Mục tiêu**: Data tự tier từ Fluss → Paimon, xóa `sink_to_paimon.py`

**Bước thực hiện**:
```bash
# 1. Kiểm tra Fluss tiering JAR có sẵn chưa
docker exec jobmanager ls /opt/fluss/lib/ | grep tiering

# 2. Tạo Flink job submit tiering service
# Thêm vào pipeline_manager.py:
TIERING_JOB = {
    "name": "Fluss Tiering Service",
    "jar": "/opt/fluss/lib/fluss-flink-tiering-*.jar",
    "args": [
        "--fluss.bootstrap.servers", "fluss-coordinator:9123",
        "--datalake.format", "paimon",
        "--datalake.paimon.metastore", "filesystem",
        "--datalake.paimon.warehouse", "s3://warehouse/paimon",
        # ... S3/MinIO config
    ]
}

# 3. Verify tiering đang chạy:
# Flink UI → 5 jobs RUNNING (thay vì 4)
# Paimon nhận data từ Fluss thay vì từ Kafka
```

**Sau khi xong**: Xóa `sink_to_paimon.py` khỏi STREAMING_JOBS.

---

### Task 1.2 — Implement Star Schema
**Mục tiêu**: dim_camera + dim_time + fact_violence_incidents trong Paimon

**dim_camera** (dimension table trong Fluss — để temporal join):
```sql
-- Trong Fluss (hỗ trợ temporal join)
CREATE TABLE fluss.dim_camera (
    camera_id    STRING,
    location     STRING,
    ward_id      STRING,
    district     STRING,
    latitude     DOUBLE,
    longitude    DOUBLE,
    status       STRING,
    updated_at   TIMESTAMP(3),
    PRIMARY KEY (camera_id) NOT ENFORCED
);
```

**fact_violence_incidents** (Paimon):
```sql
-- Rename từ violence_incidents + thêm dim keys
CREATE TABLE paimon.fact_violence_incidents (
    incident_id  STRING,
    camera_id    STRING,   -- FK → dim_camera
    timestamp    TIMESTAMP(3),
    date_id      DATE,     -- FK → dim_time
    risk_score   DOUBLE,
    confidence   DOUBLE,
    is_violent   BOOLEAN,
    event_type   STRING,
    location     STRING,   -- denormalized từ dim_camera tại thời điểm event
    ward_id      STRING,   -- denormalized
    district     STRING,   -- denormalized
    frame_url    STRING,
    PRIMARY KEY (incident_id) NOT ENFORCED
) WITH ('merge-engine' = 'deduplicate');
```

**dim_time** (Paimon — pre-generated):
```sql
CREATE TABLE paimon.dim_time (
    date_id      DATE,
    year         INT,
    month        INT,
    day          INT,
    day_of_week  STRING,   -- 'Monday', 'Tuesday', ...
    week_of_year INT,
    is_weekend   BOOLEAN,
    PRIMARY KEY (date_id) NOT ENFORCED
);
```

---

### Task 1.3 — Implement Temporal Join
**Mục tiêu**: Enrich violence events với camera metadata TẠI THỜI ĐIỂM event

```python
# sink_to_fluss.py — thêm enrichment step
ENRICH_SQL = """
INSERT INTO paimon_catalog.security.fact_violence_incidents
SELECT
    a.event_id        AS incident_id,
    a.camera_id,
    a.timestamp,
    CAST(a.timestamp AS DATE) AS date_id,
    a.risk_score,
    a.confidence,
    a.is_violent,
    a.event_type,
    COALESCE(c.location, 'Unknown')  AS location,
    COALESCE(c.ward_id,  'Unknown')  AS ward_id,
    COALESCE(c.district, 'Unknown')  AS district,
    NULL AS frame_url
FROM fluss_catalog.security.hot_violence_alerts a
LEFT JOIN fluss_catalog.security.dim_camera
    FOR SYSTEM_TIME AS OF a.ptime AS c
ON a.camera_id = c.camera_id
"""
```

---

## Phase 2: Fix Chatbot HOT Query (Ngày 2)

### Task 2.1 — Remove is_violent filter from HOT queries

**Root cause**: Gemini luôn thêm `WHERE is_violent = TRUE` vào violence queries.
Với fresh pipeline (~20 phút data), chỉ ~2% events là violent → query trả về 0 rows.

**Fix trong `trino_client.py`**, function `_adapt_sql_for_flink_hot()`:
```python
# Thêm vào sau các bước adaptation khác:
# HOT layer = violence detection system data, mọi record đều liên quan
# is_violent filter quá strict trên fresh data → remove it
result = re.sub(
    r'\bAND\s+is_violent\s*=\s*TRUE\b',
    '',
    result,
    flags=re.IGNORECASE
)
result = re.sub(
    r'\bWHERE\s+is_violent\s*=\s*TRUE\b',
    'WHERE 1=1',
    result,
    flags=re.IGNORECASE
)
```

### Task 2.2 — Fix Union Read API

**File**: `scripts/chatbot/app.py`, endpoint `/api/union-read`

```python
# Thêm HOT (Fluss) vào union read:
# - Query Fluss qua Flink SQL Gateway cho data < 2 giờ
# - Query Paimon qua Trino cho data 2h–7 ngày
# - Query Iceberg qua Trino cho data > 7 ngày
```

---

## Phase 3: Rebuild Docker Image (Ngày 2–3)

### Task 3.1 — Build và push chatbot image

Các thay đổi trong `trino_client.py` hiện chỉ ở running container (docker cp).
Cần rebuild để persist qua restart:

```bash
# Build chatbot image
docker compose -f docker/docker-compose.yml build chatbot

# Recreate container với image mới
docker compose -f docker/docker-compose.yml up -d --force-recreate chatbot

# Verify
docker exec chatbot python -c "from components.trino_client import TrinoClient; print('OK')"
```

---

## Phase 4: Frontend Real Data (Ngày 3–4)

### Task 4.1 — Replace Math.random() với real queries

**File**: `Violence-Urban-Safety-UI/frontend/src/pages/Dashboard.jsx` (hoặc tương đương)

Các metrics đang dùng Math.random():
- HOT layer count: `Math.random() * 5000 + 500`
- WARM layer count: hardcoded `103,956`
- COLD layer count: `Math.random() * 100 + 10`

Thay bằng gọi `/api/stats` endpoint thật từ chatbot backend.

### Task 4.2 — Real-time latency meter

Thêm widget hiển thị:
- HOT query latency (ms) — từ Fluss
- WARM query latency (s) — từ Paimon/Trino
- Event ingestion rate (events/sec) — từ Flink metrics API

---

## Phase 5: Testing & Documentation (Ngày 4–5)

### Regression Tests
Chạy lại toàn bộ 12 test cases từ `docs/E2E_TEST_REPORT_2026-05-17.md`
với kiến trúc mới sau Tiering Service.

### Performance Benchmark (cho thesis)
```bash
# Benchmark latency của 3 layers:
# T1: Time from Kafka produce → Fluss queryable
# T2: Time from Fluss write → Paimon queryable (via Tiering)
# T3: Time from Paimon → Iceberg archive queryable

# Expected:
# T1: <100ms (hot)
# T2: 30–60s  (tiering interval)
# T3: <5min   (archive job)
```

### Update DEVELOPER_LOG.md
- Ghi lại tất cả changes từ session này
- Document Tiering Service configuration
- Document temporal join implementation

---

## Bug Fix Experience & Lessons Learned

### Bài Học 1: Verify kiến trúc trước khi implement (ngày 1)
**Vấn đề**: Pipeline manager không submit Contract Validator job → `hot-violence-alerts-valid`
luôn rỗng → toàn bộ HOT và WARM không có data.

**Root cause**: `STREAMING_JOBS` list trong `pipeline_manager.py` thiếu Contract Validator.
**Lesson**: Luôn verify Flink UI có đủ N jobs RUNNING ngay sau khi stack khởi động.
Nếu thiếu job → trace từ pipeline_manager logs, không assume "đang chạy".

---

### Bài Học 2: git ls-files exit code 0 là bẫy
**Vấn đề**: `git ls-files docker/.env && echo "TRACKED"` → in "TRACKED" dù file không tracked.
**Root cause**: `git ls-files` exit code = 0 khi không có output (file không tracked).
**Lesson**: Dùng `git ls-files | grep "\.env"` để check thực sự, không dùng `&&`.

---

### Bài Học 3: Flink streaming aggregate không support ORDER BY
**Vấn đề**: Gemini sinh SQL `ORDER BY violence_count DESC` → Flink internal error, 0 rows.
**Root cause**: Flink streaming mode xử lý aggregates theo changelog (INSERT + UPDATE_AFTER).
ORDER BY trên aggregate column không có nghĩa trong streaming context.
**Fix**: Strip tất cả ORDER BY trước khi submit HOT queries.
**Lesson**: HOT (Fluss/Flink) không phải OLAP engine. Đừng dùng ORDER BY, HAVING,
window functions phức tạp. Chỉ filter + aggregate đơn giản.

---

### Bài Học 4: Flink checkpoint offset mismatch sau Kafka topic delete
**Vấn đề**: Delete + recreate Kafka topics → Flink jobs "chạy" nhưng `read=0 write=0`.
**Root cause**: Flink checkpoints lưu Kafka offsets cũ (ví dụ: 100,000). Topic mới chỉ
có 7,281 records → Flink đợi offset 100,000+ không bao giờ đến.
**Fix**: Cancel tất cả Flink jobs → pipeline_manager resubmit fresh với `latest-offset`.
**Lesson**: Sau khi xóa/recreate Kafka topics, PHẢI cancel và resubmit Flink jobs.
Không thể "resume" từ checkpoint cũ khi topic đã thay đổi.

---

### Bài Học 5: Docker cp changes không persist qua rebuild
**Vấn đề**: `docker cp` file vào container hoạt động → nhưng mất sau `docker compose up -d`.
**Root cause**: `docker cp` chỉ thay đổi running container filesystem, không thay đổi image.
**Lesson**: Sau khi `docker cp` verify logic, luôn `docker compose build <service>` để bake
changes vào image. Đặc biệt quan trọng trước khi demo/bảo vệ.

---

### Bài Học 6: Flink SQL Gateway pagination quirks
**Vấn đề**: Kết quả query luôn rỗng tại `/result/0`, data thật ở `/result/1`.
**Root cause**: Flink SQL Gateway trả về token 0 là empty "header", token 1 mới có data.
Streaming aggregate tạo ra UPDATE_BEFORE + UPDATE_AFTER pairs → phải dedup, chỉ giữ
UPDATE_AFTER cuối cùng.
**Lesson**: Khi polling Flink SQL Gateway:
1. Follow `nextResultUri` chain (không dừng ở token 0)
2. Poll đến khi 3 empty pages liên tiếp (aggregate đã converge)
3. Dedup by group-by key, giữ row cuối cùng

---

### Bài Học 7: Gemini over-filters → 0 rows trên fresh data
**Vấn đề**: Query "camera có nhiều sự cố nhất" → trả về 0 rows dù Fluss có 5,000+ records.
**Root cause**: Gemini thêm `WHERE is_violent = TRUE`. Với fresh pipeline ~20 phút,
chỉ ~2% events là violent → aggregate trả về 0.
**Lesson**: Với HOT layer, scrub các filter quá strict trước khi submit. HOT = violence
detection system data, mọi record đã qua validation → is_violent filter không cần thiết.

---

### Bài Học 8: Test với data thật, không test với mock
**Vấn đề**: System "passed" tất cả tests với inference_mock.py (random data) → nhưng
temporal query fail với real RTSP data (frames 292 bytes = fake JPEG từ semaphore timeout).
**Root cause**: Mock data không phản ánh production behavior.
**Lesson**: Chỉ báo cáo "PASS" khi test với real RTSP pipeline.
`inference_mock.py` đã bị xóa. Chỉ dùng `rtsp_inference_mock.py` (RTSP-connected).

---

## Checklist Hoàn Thiện

```
Phase 1: Core Architecture
[ ] Task 1.1 — Bật Fluss Tiering Service
[ ] Task 1.2 — Implement star schema (dim_camera, dim_time, fact_violence_incidents)
[ ] Task 1.3 — Implement temporal join
[ ] Xóa sink_to_paimon.py (sau khi Tiering Service confirmed working)

Phase 2: Chatbot Fixes
[ ] Task 2.1 — Remove is_violent filter from HOT queries
[ ] Task 2.2 — Fix Union Read API bao gồm HOT layer

Phase 3: Infrastructure
[ ] Task 3.1 — Rebuild chatbot Docker image
[ ] Load dim_camera data từ camera_registry.csv
[ ] Populate dim_time table (2025–2026)

Phase 4: Frontend
[ ] Task 4.1 — Replace Math.random() với real API calls
[ ] Task 4.2 — Real-time latency meter widget

Phase 5: Testing & Docs
[ ] Chạy lại 12 test cases với Tiering Service
[ ] Benchmark latency T1/T2/T3
[ ] Update DEVELOPER_LOG.md
[ ] Cập nhật thesis diagram (kiến trúc mới)
```

---

*Cập nhật: 2026-05-19*
