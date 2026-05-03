# Tính Năng Lưu Ảnh Bằng Chứng (Frame Evidence Storage)

## 📋 Tổng Quan

**Frame Evidence Storage** là tính năng cốt lõi của hệ thống giám sát an ninh thông minh, giúp lưu trữ, quản lý và phục hồi các ảnh chứng cứ từ các sự cố bạo lực được phát hiện. Các ảnh được lưu trữ trong **MinIO S3** (`evidence-frames` bucket) với tổ chức có cấu trúc, bảo mật cao, và tuân thủ các quy chuẩn pháp lý.

### 🎯 Mục Đích Chính

1. **Lưu trữ bằng chứng kỹ thuật số** - Bảo toàn ảnh từ các sự cố phát hiện được
2. **Hỗ trợ phân tích pháp lý** - Cung cấp chứng cứ cho các cuộc điều tra
3. **Quản lý vòng đời dữ liệu** - Tự động xóa ảnh cũ hơn 30 ngày
4. **Truy xuất nhanh** - API REST cấp phép, lưu ảnh thu nhỏ trong Paimon, lưu ảnh gốc trong S3
5. **Kiểm toán lưu trữ** - Ghi lại dấu thời gian, metadata, và sự kiện cleanup

---

## 🏗️ Kiến Trúc Chi Tiết

### Luồng Dữ Liệu Đầy Đủ

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. CAPTURE (Chụp ảnh)                                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Camera RTSP → MediaMTX → ffmpeg (1 FPS)                            │
│                           │                                         │
│                           └─→ Capture 160x90 JPEG                   │
│                               Base64 encode                         │
│                               Timestamp: HH:MM:SS.mmm               │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ 2. PUBLISH (Công bố)                                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  rtsp_inference_mock.py                                             │
│  │                                                                  │
│  ├─→ Mock AI inference (risk_score, confidence, event_type)        │
│  │                                                                  │
│  ├─→ Kafka Topic: urban-safety-alerts (raw)                        │
│  │   {                                                             │
│  │     "event_id": "uuid",                                         │
│  │     "camera_id": "cam_01",                                      │
│  │     "timestamp": "2026-04-28T14:30:45Z",                        │
│  │     "is_violent": true,                                         │
│  │     "risk_score": 0.95,                                         │
│  │     "metadata": {                                               │
│  │       "thumbnail": "base64_encoded_jpeg...",  ← KEY!           │
│  │       "fps": 1.0,                                               │
│  │       "latency_ms": 45                                          │
│  │     }                                                            │
│  │   }                                                              │
│  │                                                                  │
│  └─→ Kafka Topic: hot-violence-alerts-valid (after validation)     │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ 3. EXTRACT (Trích xuất ảnh)                                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  frame_extractor_sink.py (Dịch vụ sidecar)                         │
│  │                                                                  │
│  ├─→ Subscribe: Kafka topic `hot-violence-alerts-valid`            │
│  │   Receive per-message: { event_id, camera_id, timestamp,        │
│  │                          metadata: { thumbnail: "..." } }       │
│  │                                                                  │
│  ├─→ Extract base64 thumbnail                                      │
│  │   Base64 decode → JPEG bytes                                    │
│  │                                                                  │
│  ├─→ Upload to MinIO S3:                                           │
│  │   s3://evidence-frames/cam_01/2026-04-28/event_id.jpg           │
│  │                                                                  │
│  │   Metadata tags:                                                │
│  │   ├─ incident_id: "event_id"                                    │
│  │   ├─ camera_id: "cam_01"                                        │
│  │   ├─ risk_score: "0.95"                                         │
│  │   └─ capture_date: "2026-04-28"                                 │
│  │                                                                  │
│  ├─→ If SUCCESS:                                                   │
│  │   Publish enriched → Kafka topic: hot-violence-frames-uploaded  │
│  │   {                                                             │
│  │     "event_id": "uuid",                                         │
│  │     "frame_url": "s3://evidence-frames/cam_01/2026-04-28/uuid", │
│  │     "frame_capture_ts": 1714332645123,  (milliseconds)          │
│  │     ... rest of original data                                   │
│  │   }                                                              │
│  │                                                                  │
│  └─→ If FAILURE (retry 3x, then fail):                             │
│      Publish to DLQ → Kafka topic: frame-extraction-dlq            │
│      {                                                              │
│        "original": { ... original message ... },                   │
│        "error": "S3 upload failed after 3 retries",                │
│        "timestamp": "2026-04-28T14:30:50Z"                         │
│      }                                                              │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ 4. INDEX (Lập chỉ mục trong Paimon)                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  sink_to_paimon.py (Flink streaming job)                           │
│  │                                                                  │
│  ├─→ Subscribe: hot-violence-alerts-valid, hot-violence-frames-uploaded
│  │                                                                  │
│  ├─→ Insert into Paimon `violence_incidents` table:               │
│  │   ├─ incident_id: from event_id                                 │
│  │   ├─ camera_id: from metadata                                   │
│  │   ├─ timestamp: incident time                                   │
│  │   ├─ risk_score: AI confidence                                  │
│  │   ├─ is_violent: detection result                               │
│  │   ├─ event_type: FIGHTING, ASSAULT, STABBING, SHOOTING          │
│  │   ├─ frame_url: from enriched message (s3://...)                │
│  │   ├─ thumbnail_b64: from original Kafka message                 │
│  │   └─ frame_capture_ts: from enriched message (milli)            │
│  │                                                                  │
│  └─→ Paimon Merge Engine: `deduplicate` on incident_id             │
│      (Tránh trùng lặp nếu cùng 1 event được xử lý nhiều lần)       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ 5. RETRIEVE (Lấy ảnh)                                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Frontend / Chatbot API                                             │
│  │                                                                  │
│  ├─→ Method 1: Quick thumbnail (instant)                           │
│  │   SELECT thumbnail_b64 FROM violence_incidents WHERE incident_id = "xxx"
│  │   → display data:image/jpeg;base64,...                          │
│  │                                                                  │
│  ├─→ Method 2: Full resolution evidence                            │
│  │   GET /api/evidence/{incident_id}/frame?format=image            │
│  │   → return JPEG file from S3                                    │
│  │                                                                  │
│  └─→ Method 3: S3 URL only (for external integration)               │
│      GET /api/evidence/{incident_id}/frame?format=url              │
│      → return { frame_url: "s3://...", s3_endpoint: "..." }        │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ 6. CLEANUP (Dọn dẹp ảnh cũ)                                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  frame_cleaner.py (Batch job, chạy weekly)                         │
│  │                                                                  │
│  ├─→ List all objects in s3://evidence-frames/                     │
│  │                                                                  │
│  ├─→ Filter: LastModified < (now - 30 days)                        │
│  │   Config: FRAME_RETENTION_DAYS = 30                             │
│  │                                                                  │
│  ├─→ Batch delete (100 objects/request)                            │
│  │   Retry: 3x with exponential backoff                            │
│  │                                                                  │
│  ├─→ Publish cleanup event:                                        │
│  │   Kafka topic: frame-cleanup-events                             │
│  │   {                                                             │
│  │     "timestamp": "2026-04-28T02:05:42Z",                        │
│  │     "deleted_count": 156,                                       │
│  │     "deleted_size_mb": 10.92,                                   │
│  │     "scanned_count": 1234,                                      │
│  │     "errors": 0                                                 │
│  │   }                                                              │
│  │                                                                  │
│  └─→ Log kết quả vào Prometheus metrics                             │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 💾 Tầng Lưu Trữ & Thời Gian Giữ Lại

| Tầng | Công Nghệ | Dung Lượng | Thời Gian Giữ | Độ Trễ | Trường Hợp Sử Dụng |
|------|----------|-----------|--------------|--------|-------------------|
| **HOT** | Fluss (In-Memory) | ~ 1-2 GB | 1-2 giờ | < 100ms | Xem trực tiếp trên command center |
| **WARM** | Paimon (MinIO) | ~ 5-100 MB | 7-30 ngày | 1-10 phút | Phân tích hoạt động, điều tra 24-48h |
| **COLD** | S3 (evidence-frames bucket) | ~ 100 MB | 30 ngày | 10+ phút | Bảo lưu chứng cứ dài hạn |

### Giải Thích Chi Tiết

**HOT (Fluss)**
- Dữ liệu tất cả incidents đang xảy ra (trong 1-2 giờ gần nhất)
- Lưu trữ trong bộ nhớ của Fluss → tốc độ siêu nhanh
- Mục đích: Cảnh báo real-time, command center
- **Không lưu frame ảnh** (chỉ metadata)

**WARM (Paimon + S3)**
- Dữ liệu incidents 7-30 ngày gần nhất
- Lưu trong Paimon (MinIO S3) dạng ORC/Parquet
- **Lưu frame URLs** và thumbnail base64
- Mục đích: Phân tích xu hướng, điều tra nhanh
- Thời gian query: 1-10 phút (chấp nhận được)

**COLD (S3 evidence-frames bucket)**
- Ảnh gốc (640x480, 70KB mỗi cái)
- Lưu trữ dài hạn (30 ngày, có thể mở rộng)
- Mục đích: Chứng cứ pháp lý, audit trail
- Sẽ được xóa tự động sau 30 ngày (configurable)

---

## 📁 Quy Ước Đặt Đường Dẫn S3

### Mẫu Tiêu Chuẩn

```
s3://evidence-frames/{camera_id}/{YYYY-MM-DD}/{incident_id}.jpg
```

### Ví Dụ Thực Tế

```
s3://evidence-frames/cam_01/2026-04-28/evt_abc123def456-a1b2c3d4e5f6.jpg
s3://evidence-frames/cam_02/2026-04-28/evt_xyz789uvw012-z9y8x7w6v5u4.jpg
s3://evidence-frames/cam_01/2026-04-27/evt_old_incident_789-old_uuid.jpg
```

### Lợi Ích Của Quy Ước Này

✅ **Phân vùng theo camera** - Truy vấn nhanh ảnh từ camera cụ thể
✅ **Phân vùng theo ngày** - Lọc thời gian hiệu quả (VD: "hôm nay", "tuần trước")
✅ **Tên cụ thể theo incident** - Tìm kiếm trực tiếp không cần join bảng
✅ **Compatible với S3 Inventory** - Có thể scan toàn bộ partition nếu cần
✅ **Dễ cleanup** - Xóa toàn bộ ngày cũ: `s3://evidence-frames/cam_01/2026-03-28/`

---

## 🖼️ Chất Lượng & Kích Thước Ảnh

### Thumbnail (Lưu Trực Tiếp Trong Paimon)

```
Độ phân giải: 160 x 90 pixels
Format:       JPEG, base64-encoded
Vị trí:       Cột 'thumbnail_b64' trong violation_incidents table
Dùng cho:     Preview nhanh trên dashboard
Kích thước:   ~2-3 KB per frame
Giữ lại:      7-30 ngày (với Paimon retention)
```

**Ưu điểm:**
- Hiển thị ngay lập tức (không cần fetch S3)
- Nhỏ gọn, không tốn dung lượng S3
- Đủ để nhân viên xác nhận sự cố

### Ảnh Chứng Cứ Gốc (Lưu Trong S3)

```
Độ phân giải: 640 x 480 pixels
Format:       JPEG, quality=85 (cân bằng chất lượng ↔ dung lượng)
Vị trí:       s3://evidence-frames/{camera_id}/{date}/{incident_id}.jpg
Dùng cho:     Phân tích pháp lý, điều tra chi tiết
Kích thước:   ~60-80 KB per frame
Giữ lại:      30 ngày (configurable)
Số lượng:     1 keyframe per incident (không continuous)
```

**Ưu điểm:**
- Độ phân giải đủ cao để nhìn chi tiết
- Không quá lớn (tiết kiệm S3 storage)
- Bảo toàn bằng chứng pháp lý

### Ước Tính Chi Phí Lưu Trữ

**Giả định:**
- 50-100 sự cố bạo lực thực tế per tháng
- 1 keyframe per incident (chụp on-demand)

**Tính toán:**
```
75 incidents/month × 70 KB/incident = 5.25 MB/month
Giữ lại 7-30 ngày = Peak storage ~5-20 MB
Năm thứ nhất: ~63 MB (rất nhỏ)
```

**So sánh với liên tục 1 FPS:**
- Liên tục: 7 MB/day/camera = 210 MB/month (gấp 100x)
- Keyframe on-demand: 5.25 MB/month (hiệu quả hơn nhiều)

---

## 📊 Lược Đồ Paimon Chi Tiết

### Bảng `violence_incidents`

```sql
CREATE TABLE violence_incidents (
    -- Khóa chính & thông tin cơ bản
    incident_id STRING,             -- PK: UUID từ Kafka
    camera_id STRING,               -- Camera nào phát hiện (cam_01, cam_02...)
    timestamp TIMESTAMP(3),         -- Thời điểm sự cố (exact: HH:MM:SS.mmm)
    
    -- Kết quả AI inference
    risk_score DOUBLE,              -- Điểm rủi ro (0.0-1.0) từ model
    confidence DOUBLE,              -- Độ tin cậy của model (0.0-1.0)
    is_violent BOOLEAN,             -- true = phát hiện bạo lực, false = bình thường
    event_type STRING,              -- Loại bạo lực: FIGHTING, ASSAULT, STABBING, SHOOTING
    
    -- Metadata địa điểm
    location STRING,                -- JSON hoặc string: { city, district, ward, street, lat, long }
    
    -- Quản lý vòng đời
    is_deleted BOOLEAN,             -- Soft-delete flag (nếu cần ẩn sự cố)
    
    -- ✨ FRAME EVIDENCE COLUMNS (NEW)
    frame_url STRING,               -- S3 path: s3://evidence-frames/cam_01/2026-04-28/evt_xyz.jpg
    thumbnail_b64 STRING,           -- Base64-encoded 160x90 JPEG (inline, quick preview)
    frame_capture_ts BIGINT,        -- Timestamp when frame was uploaded to S3 (milliseconds)
    
    PRIMARY KEY (incident_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'deduplicate',        -- Merge engine: loại bỏ duplicate
    'changelog-producer' = 'input',        -- Xuất changelog cho CDC
    'snapshot.time-retained' = '7d',       -- Giữ snapshots 7 ngày
    'snapshot.num-retained.min' = '5',     -- Giữ tối thiểu 5 snapshots
    'snapshot.num-retained.max' = '50'     -- Tối đa 50 snapshots
)
```

### Giải Thích Cột Frame

| Cột | Mục Đích | Kích Thước | Cập Nhật Bởi |
|-----|----------|-----------|-------------|
| `frame_url` | Đường dẫn S3 của ảnh gốc | ~100 bytes | frame_extractor_sink |
| `thumbnail_b64` | Ảnh preview nhỏ (inline) | ~2-3 KB | rtsp_inference_mock |
| `frame_capture_ts` | Dấu thời gian upload ảnh (ms) | 8 bytes (BIGINT) | frame_extractor_sink |

**Timeline Cập Nhật:**
1. `rtsp_inference_mock` publish → `thumbnail_b64` có giá trị
2. `frame_extractor_sink` upload S3 → `frame_url` có giá trị
3. `sink_to_paimon` ghi → cả 2 cột được lưu

---

## 🔍 Các Truy Vấn Phân Tích Pháp Lý

### Truy Vấn 1: Lấy Frame URL Cho 1 Sự Cố

```sql
-- Lấy đường dẫn frame gốc của 1 incident
SELECT 
  incident_id, 
  camera_id, 
  timestamp, 
  risk_score,
  event_type,
  frame_url,
  frame_capture_ts
FROM paimon.security.violence_incidents
WHERE incident_id = 'evt_abc123def456'
LIMIT 1;
```

**Kết quả:**
```
incident_id      | camera_id | timestamp           | risk_score | event_type | frame_url                                                | frame_capture_ts
evt_abc123def456 | cam_01    | 2026-04-28 14:30:45 | 0.95      | FIGHTING   | s3://evidence-frames/cam_01/2026-04-28/evt_abc123def456 | 1714332645123
```

**Dùng cho:** Điều tra viên muốn lấy ảnh gốc từ 1 sự cố cụ thể

---

### Truy Vấn 2: Tất Cả Sự Cố Bạo Lực Từ Camera X Hôm Nay

```sql
-- Lấy tất cả incidents bạo lực từ camera cam_01 hôm nay
SELECT 
  incident_id,
  timestamp,
  risk_score,
  confidence,
  event_type,
  frame_url,
  CASE 
    WHEN frame_url IS NOT NULL THEN 'Có ảnh'
    ELSE 'Chưa có ảnh'
  END AS frame_status
FROM paimon.security.violence_incidents
WHERE camera_id = 'cam_01'
  AND CAST(timestamp AS DATE) = CAST(CURRENT_DATE AS DATE)
  AND is_violent = true
ORDER BY timestamp DESC;
```

**Dùng cho:** Xem toàn bộ sự cố từ 1 camera trong ngày để quản lý an ninh

---

### Truy Vấn 3: Sự Cố Có Ảnh Chứng Cứ, 7 Ngày Gần Nhất

```sql
-- Lấy sự cố bạo lực có ảnh, trong 7 ngày gần nhất
SELECT 
  incident_id,
  camera_id,
  timestamp,
  risk_score,
  frame_url,
  frame_capture_ts,
  ROUND(
    (CURRENT_TIMESTAMP - CAST(FROM_UNIXTIME(frame_capture_ts/1000) AS TIMESTAMP)) 
    / INTERVAL '1' DAY, 1
  ) AS age_days
FROM paimon.security.violence_incidents
WHERE is_violent = true
  AND frame_url IS NOT NULL
  AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
ORDER BY timestamp DESC
LIMIT 100;
```

**Dùng cho:** Bộ phận điều tra muốn xem tất cả chứng cứ còn tươi mới

---

### Truy Vấn 4: Thống Kê Coverage Ảnh

```sql
-- Thống kê: bao nhiêu sự cố có ảnh?
SELECT 
  DATE(timestamp) AS incident_date,
  camera_id,
  COUNT(*) AS total_incidents,
  SUM(CASE WHEN frame_url IS NOT NULL THEN 1 ELSE 0 END) AS with_frame,
  ROUND(
    100.0 * SUM(CASE WHEN frame_url IS NOT NULL THEN 1 ELSE 0 END) 
    / COUNT(*), 1
  ) AS coverage_percent
FROM paimon.security.violence_incidents
WHERE is_violent = true
  AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
GROUP BY DATE(timestamp), camera_id
ORDER BY incident_date DESC, camera_id;
```

**Kết quả mẫu:**
```
incident_date | camera_id | total_incidents | with_frame | coverage_percent
2026-04-28    | cam_01    | 12              | 11         | 91.7%
2026-04-28    | cam_02    | 8               | 7          | 87.5%
2026-04-27    | cam_01    | 15              | 15         | 100.0%
```

**Dùng cho:** Kiểm tra tỷ lệ lưu trữ ảnh từng camera

---

### Truy Vấn 5: Tìm Sự Cố Có Ảnh Theo Loại Sự Cố

```sql
-- Tìm tất cả STABBING (đâm) với ảnh chứng cứ
SELECT 
  incident_id,
  camera_id,
  timestamp,
  risk_score,
  frame_url
FROM paimon.security.violence_incidents
WHERE event_type = 'STABBING'
  AND frame_url IS NOT NULL
  AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
ORDER BY risk_score DESC;
```

**Dùng cho:** Phân tích các loại bạo lực cụ thể

---

## 🌐 REST API Để Truy Xuất Ảnh

### Endpoint: `GET /api/evidence/{incident_id}/frame`

**Base URL:** `http://localhost:5002`

#### Ví Dụ 1: Tải Ảnh Gốc (JPEG)

```bash
curl -X GET "http://localhost:5002/api/evidence/evt_abc123def456/frame" \
  --output frame.jpg \
  -H "Accept: image/jpeg"
```

**Response:** 
- Status: 200 OK
- Content-Type: image/jpeg
- Body: JPEG binary (640x480, ~70 KB)

**Dùng cho:** Download ảnh để lưu lại, phân tích chi tiết, làm chứng cứ pháp lý

---

#### Ví Dụ 2: Lấy URL S3 Thôi (JSON)

```bash
curl -X GET "http://localhost:5002/api/evidence/evt_abc123def456/frame?format=url"
```

**Response:**
```json
{
  "incident_id": "evt_abc123def456",
  "camera_id": "cam_01",
  "incident_date": "2026-04-28",
  "frame_url": "s3://evidence-frames/cam_01/2026-04-28/evt_abc123def456.jpg",
  "s3_endpoint": "http://minio:9000",
  "frame_capture_ts": 1714332645123,
  "thumbnail_b64": "base64_string_here..."
}
```

**Dùng cho:** Integration với external systems, manual access

---

#### Ví Dụ 3: Lỗi - Incident Không Tìm Thấy

```bash
curl -X GET "http://localhost:5002/api/evidence/evt_notfound/frame"
```

**Response:**
```json
{
  "error": "No incident found with ID: evt_notfound",
  "hint": "Ensure incident_id is correct and has been processed",
  "checked_at": "2026-04-28T14:35:00Z"
}
```

---

#### Ví Dụ 4: Lỗi - Frame Chưa Upload

```bash
curl -X GET "http://localhost:5002/api/evidence/evt_noframe/frame?format=url"
```

**Response:**
```json
{
  "error": "Frame not found in S3",
  "incident_id": "evt_noframe",
  "frame_url_expected": "s3://evidence-frames/cam_01/2026-04-28/evt_noframe.jpg",
  "reason": "Frame extraction may still be in progress or failed. Please try again in a few seconds.",
  "recommendation": "Check frame-extraction-dlq topic for failures"
}
```

---

## 🗑️ Công Việc Dọn Dẹp Ảnh Cũ (Frame Cleanup Job)

### Mục Đích

Tự động xóa ảnh chứng cứ cũ hơn 30 ngày để:
- Giải phóng dung lượng S3
- Tuân thủ chính sách giữ lại dữ liệu
- Giảm chi phí lưu trữ cloud

### Cấu Hình

```bash
# Trong docker/.env hoặc environment
FRAME_RETENTION_DAYS=30         # Mặc định: 30 ngày
S3_ENDPOINT=http://minio:9000
S3_BUCKET=evidence-frames
KAFKA_BROKER=kafka:9092
```

### Chạy Thủ Công

```bash
# SSH vào container jobmanager
docker exec -it jobmanager bash

# Chạy script cleanup
python /opt/flink/scripts/transform/frame_cleaner.py
```

**Output:**
```
[2026-04-28 02:00:01] [INFO] Frame Evidence Cleanup Job Started
  Bucket: evidence-frames
  Endpoint: http://minio:9000
  Retention Window: 30 days
  Cutoff Date (xóa trước): 2026-03-29

[2026-04-28 02:02:15] [PROGRESS]
  Scanned: 487 frames
  Matched (age > 30d): 156 frames
  Total size to delete: 10.92 MB

[2026-04-28 02:05:42] [RESULTS]
  ✓ Deleted: 156 frames
  ✓ Size freed: 10.92 MB
  ✓ Errors: 0
  ✓ Duration: 5m 41s

[2026-04-28 02:05:43] [KAFKA] 
  Published cleanup event → frame-cleanup-events topic
```

### Lập Lịch Tự Động (Cron)

```bash
# Chạy mỗi Chủ Nhật 2 giờ sáng
0 2 * * 0  cd /app/scripts/transform && python frame_cleaner.py >> /var/log/frame_cleaner.log 2>&1
```

### Sự Kiện Cleanup Được Xuất Bản

Mỗi khi cleanup chạy, một event được ghi lại:

```json
{
  "event_type": "frame_cleanup",
  "timestamp": "2026-04-28T02:05:42.123Z",
  "retention_days": 30,
  "deleted_count": 156,
  "deleted_size_mb": 10.92,
  "scanned_count": 487,
  "errors": 0,
  "duration_seconds": 341
}
```

**Lưu trong:** Kafka topic `frame-cleanup-events`

---

## 🔗 Tích Hợp Với Frontend

### Command Center Dashboard

**Hiển thị:**
- Grid các camera (15 camera)
- Mỗi camera hiển thị incident gần nhất
- Khi click vào incident → hiển thị:
  - Thumbnail preview (từ `thumbnail_b64`)
  - Risk score, timestamp, event type
  - Nút "Download Full Evidence" → gọi API `/api/evidence/{id}/frame`

**Code JavaScript (React):**
```javascript
// Lấy ảnh preview nhanh
const getThumbnailUrl = (incident) => {
  if (incident.thumbnail_b64) {
    return `data:image/jpeg;base64,${incident.thumbnail_b64}`;
  }
  return null;
};

// Download ảnh gốc từ S3
const downloadFullFrame = async (incidentId) => {
  try {
    const response = await fetch(`/api/evidence/${incidentId}/frame`);
    if (!response.ok) throw new Error('Frame not found');
    
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${incidentId}.jpg`;
    link.click();
  } catch (error) {
    console.error('Failed to download frame:', error);
    alert('Không thể tải ảnh. Vui lòng thử lại.');
  }
};
```

### Vigilance Terminal (Chatbot)

**Ví Dụ Tương Tác:**

**User:** "Hôm qua quận 1 có bao nhiêu vụ bạo lực? Cho tôi xem ảnh."

**Chatbot (LangGraph Agent):**
1. Parse intent: query + request evidence frame
2. Select layer: "yesterday" → Paimon (warm layer)
3. Generate SQL:
   ```sql
   SELECT COUNT(*), incident_id, frame_url, thumbnail_b64
   FROM paimon.security.violence_incidents
   WHERE DATE(timestamp) = '2026-04-27'
     AND location LIKE '%District 1%'
     AND is_violent = true
   ```
4. Execute + format response:
   ```
   "Hôm qua quận 1 có 5 vụ bạo lực:
   
   1. evt_abc123 (14:30, FIGHTING)
      [Thumbnail ảnh]
      Link: http://localhost:5002/api/evidence/evt_abc123/frame
   
   2. evt_xyz789 (15:45, ASSAULT)
      [Thumbnail ảnh]
      Link: http://localhost:5002/api/evidence/evt_xyz789/frame
   
   ...
   
   Tổng cộng: 5 vụ. Bấm link để xem ảnh gốc."
   ```

---

## ⚠️ Xử Lý Lỗi & Cơ Chế Retry

### Trường Hợp 1: Thumbnail Bị Thiếu Trong Kafka

**Dấu hiệu:**
- `frame_extractor_sink` log: `[FRAME] No thumbnail for evt_abc123`

**Nguyên nhân:**
- `rtsp_inference_mock` không capture được frame (ffmpeg timeout)
- Thumbnail field trống trong Kafka message

**Xử Lý:**
```python
if not thumbnail_b64:
    logger.warning(f"[FRAME] No thumbnail for {incident_id}")
    producer.send("frame-extraction-dlq", value={
        "original": record,
        "error": "No thumbnail found",
        "timestamp": datetime.utcnow().isoformat(),
    })
    return False  # Skip incident
```

**Kết Quả:**
- Incident vẫn được lưu trong Paimon
- `frame_url` = NULL
- `thumbnail_b64` = ""

---

### Trường Hợp 2: S3 Upload Timeout / Thất Bại

**Dấu hiệu:**
- Log: `[S3] Upload attempt 1/3 failed for evt_abc123`
- Message lặp lại sau 2s, 4s, 8s

**Nguyên nhân:**
- MinIO service down
- Network latency
- S3 storage quota exceeded

**Cơ Chế Retry:**
```python
for attempt in range(1, MAX_RETRIES + 1):  # MAX_RETRIES = 3
    try:
        s3_client.put_object(...)
        return f"s3://{S3_BUCKET}/{s3_key}"
    except Exception as e:
        logger.warning(f"Attempt {attempt}/3 failed: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(2 ** attempt)  # 2s, 4s, 8s exponential backoff
```

**Sau 3 lần thất bại:**
- Ghi lại vào DLQ
- Incident vẫn lưu trong Paimon (nhưng `frame_url` = NULL)

---

### Trường Hợp 3: Base64 Decode Thất Bại

**Dấu hiệu:**
- Log: `[FRAME] Base64 decode error: Incorrect padding`

**Nguyên nhân:**
- Thumbnail base64 bị hỏng
- Encoding lỗi từ producer

**Xử Lý:**
```python
try:
    frame_bytes = base64.b64decode(thumbnail_b64)
except Exception as e:
    logger.error(f"Decode failed: {e}")
    producer.send("frame-extraction-dlq", ...)
    return False
```

---

### Theo Dõi Dead-Letter Topic

```bash
# Xem toàn bộ lỗi
docker exec -it kafka bash

kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic frame-extraction-dlq \
  --from-beginning | jq .
```

**Kết quả mẫu:**
```json
{
  "original": {
    "event_id": "evt_bad_base64",
    "camera_id": "cam_01",
    "timestamp": "2026-04-28T14:30:45Z",
    "metadata": {
      "thumbnail": "invalid@#$base64"
    }
  },
  "error": "Incorrect padding",
  "timestamp": "2026-04-28T14:30:50.123Z"
}
```

---

## 🔐 Bảo Mật & Tuân Thủ Pháp Lý

### Tính Toàn Vẹn Bằng Chứng

✅ **Immutable Storage:** S3 object không thể sửa, chỉ xóa theo lifecycle policy
✅ **Metadata Preserved:** Object tags lưu incident_id, camera_id, risk_score
✅ **Audit Trail:** `frame_capture_ts` ghi dấu upload chính xác
✅ **Versioning:** MinIO có thể bật versioning để keep history

### Kiểm Soát Truy Cập

| Layer | Kiểm Soát |
|-------|-----------|
| REST API | Yêu cầu authentication token (future: OAuth/JWT) |
| S3 bucket | MinIO ACLs (hiện tại: public download cho dev) |
| Paimon table | Trino row-level security (future) |
| Database | User account + password |

### Tuân Thủ Chính Sách Giữ Lại

```
Quy định:      Giữ lại 30 ngày
Cơ chế:        Auto-delete via frame_cleaner.py
Audit log:     Cleanup events → frame-cleanup-events topic
Qui trình:     Có thể pause cleanup nếu cần điều tra đặc biệt
```

---

## 🚨 Troubleshooting

### Vấn Đề 1: Không Có Ảnh Nào Được Upload

**Kiểm tra:**

```bash
# 1. Frame-extractor có chạy không?
docker ps | grep frame-extractor

# 2. Có message trong Kafka?
docker exec -it kafka bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic hot-violence-alerts-valid --max-messages 5

# 3. Xem logs
docker logs frame-extractor -f
```

**Khắc Phục:**
- Đảm bảo `inference-mock` đang chạy
- Kiểm tra Kafka broker sạch khỏi lỗi
- Restart frame-extractor: `docker restart frame-extractor`

---

### Vấn Đề 2: S3 Upload Timeout

**Dấu Hiệu:**
- Logs: `[S3] Upload attempt 1/3 failed`
- Frames không xuất hiện trong MinIO

**Nguyên Nhân Thường Gặp:**
- MinIO service down
- Network bị chặn
- Endpoint URL sai

**Khắc Phục:**
```bash
# Test kết nối MinIO
docker exec -it frame-extractor python -c "
import boto3
s3 = boto3.client('s3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minio',
    aws_secret_access_key='mypassword'
)
s3.head_bucket(Bucket='evidence-frames')
print('✓ S3 OK')
"
```

---

### Vấn Đề 3: DLQ Nhiều Lỗi

**Xem lỗi:**
```bash
docker exec -it kafka bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic frame-extraction-dlq --from-beginning | jq .
```

**Lỗi Phổ Biến:**
- `"No thumbnail found"` → Check `rtsp_inference_mock` produce thumbnail
- `"S3 upload failed"` → Check MinIO storage quota
- `"Incorrect padding"` → Check base64 encoding

---

## 🚀 Tối Ưu Hóa Hiệu Suất

### Frame Extractor Service

```
Parallelism:  1 (sequential, tránh race condition S3)
Batch size:   1 message/lần (streaming, không batch)
Checkpoint:   Mỗi 30 giây
Memory:       256 MB
CPU:          0.50 cores

Độ Trễ Kỳ Vọng: 
- Kafka read:     ~100ms
- Base64 decode:  ~50ms
- S3 upload:      ~200-500ms (tuỳ network)
- Total:          ~500-700ms per frame
```

**Nếu latency > 5s:**
- Tăng memory lên 512 MB
- Tăng CPU lên 1.0 core
- Check MinIO health

### Frame Cleanup Job

```
Frequency:     Weekly (Sunday 2 AM)
Batch size:    100 objects/request (balanced)
Retention:     30 days (configurable)

Duration Kỳ Vọng:
- Scan 1000 objects: ~1-2 phút
- Delete 100 objects: ~30-60 giây
- Total: ~5-10 phút
```

---

## 🔮 Hướng Phát Triển Tương Lai

- [ ] Compression (JPEG → WebP, tiết kiệm 30-50% storage)
- [ ] Video clip extraction (1-5 giây clip, không chỉ keyframe)
- [ ] Frame fingerprinting (detect duplicate frames)
- [ ] ML quality scoring (auto-rate ảnh)
- [ ] Video player với timeline scrubber
- [ ] Chain-of-custody logs (kỹ thi toàn bộ truy cập)
- [ ] S3 encryption at rest
- [ ] Immutable backups (AWS Glacier)

---

## 📚 Tài Liệu Tham Khảo

| File | Mục Đích |
|------|----------|
| `scripts/streaming/rtsp_inference_mock.py` | Capture RTSP frame, publish Kafka |
| `scripts/transform/frame_extractor_sink.py` | Extract frame, upload S3 |
| `scripts/transform/frame_cleaner.py` | Delete frames > 30 days |
| `scripts/check_frames.py` | Verify frames in S3 |
| `docs/agent-guides/frame-evidence-storage.md` | Tài liệu Tiếng Anh đầy đủ |
| `.claude/rules/docker-config.md` | Docker best practices |
| `.claude/rules/streaming-scripts.md` | Kafka conventions |

---

## 📞 Liên Hệ & Hỗ Trợ

**Vấn đề kỹ thuật:** Kiểm tra logs, xem troubleshooting section
**Tối ưu hiệu suất:** Tham khảo performance tuning section
**Yêu cầu tính năng:** Xem future enhancements section

---

**Tài liệu này được cập nhật:** 2026-04-28
**Phiên bản:** 1.0
**Trạng thái:** ✅ Production Ready
