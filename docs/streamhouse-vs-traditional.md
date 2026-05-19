# Streamhouse vs Kiến Trúc Truyền Thống

> Tài liệu giải thích cho hội đồng giám khảo — Khóa luận tốt nghiệp 2026

---

## Vấn Đề Gốc Rễ

Mọi hệ thống data đều đối mặt với tension không thể tránh:

```
Data mới (5 phút trước) → cần truy vấn NHANH (<100ms)
Data cũ (6 tháng trước) → cần lưu trữ RẺ  (Parquet, nén 10x)
```

Không có công nghệ lưu trữ nào vừa nhanh vừa rẻ. Các kiến trúc truyền thống
giải quyết tension này theo những cách phức tạp và đắt tiền.

---

## Lambda Architecture (2011)

### Cách hoạt động
```
Raw Data
  ├── Batch Layer  (Hadoop/Spark) ─────────────────→ Batch Views
  └── Speed Layer (Storm/Flink)  → Realtime Views ─┘
                                                    ↓
                                            Serving Layer (merge thủ công)
```

### Vấn đề
| Vấn đề | Hệ quả |
|--------|--------|
| **Dual codebase** | Cùng 1 business logic phải viết 2 lần (batch + streaming) |
| **Reconciliation** | Batch view và realtime view có thể cho kết quả khác nhau |
| **Operational overhead** | Vận hành 2 stack song song = 2x độ phức tạp |
| **Bug fix** | Fix 1 bug → phải reprocess toàn bộ historical data bằng batch job |

---

## Medallion Architecture (2021 — Databricks)

### Cách hoạt động
```
Bronze (raw copy) ──→ [batch job] ──→ Silver (cleaned copy) ──→ [batch job] ──→ Gold (aggregated copy)
```

### Vấn đề
| Vấn đề | Hệ quả |
|--------|--------|
| **3x data duplication** | Mỗi row tồn tại ở Bronze + Silver + Gold = 3 lần lưu trữ |
| **Polling-based** | Consumer phải hỏi "có data mới chưa?" mỗi 30s–5 phút |
| **Latency floor** | Best case: 30–60 giây (trigger interval + S3 commit time) |
| **Manual stitching** | Muốn "dữ liệu hôm qua + hôm nay"? Phải UNION 2 bảng khác nhau |
| **No native upsert** | MERGE INTO tốn kém, rewrite toàn bộ data files |

---

## Streamhouse (2023 — Apache Fluss)

### Nguyên lý cốt lõi

> **Write once. Serve everywhere.**

```
Ghi 1 lần vào Fluss
       │
       ├── HOT  (Fluss)   → <100ms,  1–2 giờ gần nhất
       │         ↓ Tiering Service (tự động, không cần code)
       ├── WARM (Paimon)  → giây,    7–30 ngày
       │         ↓ Archive Job (scheduled)
       └── COLD (Iceberg) → phút,    năm+, time-travel
```

### Điểm Khác Biệt Lớn Nhất: 1 Bảng Logic

Người dùng chỉ thấy **một bảng duy nhất**:

```sql
-- Query này tự động lấy đúng layer:
SELECT * FROM hot_violence_alerts
WHERE timestamp > NOW() - INTERVAL '2' DAY

-- Fluss tự quyết định:
-- "2 ngày" → lấy từ Paimon (warm layer)
-- Không cần biết data đang nằm ở đâu
```

So sánh với Medallion:

```python
# Medallion — developer phải tự code routing:
if time_range < timedelta(hours=1):
    query("SELECT * FROM fluss_hot_table ...")
elif time_range < timedelta(days=7):
    query("SELECT * FROM paimon_warm_table ...")
else:
    query("SELECT * FROM iceberg_cold_table ...")
```

### 4 Lợi Ích Cụ Thể

#### 1. Không lưu trữ trùng lặp
```
Medallion:   1 event → Bronze + Silver + Gold = 3 bản sao
Streamhouse: 1 event → Fluss → tiers xuống Paimon (xóa khỏi Fluss) → Iceberg
             Mỗi event chỉ tồn tại ở 1 layer tại 1 thời điểm
```

#### 2. Data mới queryable ngay lập tức
```
Medallion:  ghi → đợi batch trigger → promoted → queryable  (30s – 5 phút)
Streamhouse: ghi → queryable ngay   (< 100ms)
```

#### 3. Temporal Join — không thể làm được trong Medallion

Fluss cho phép join với **trạng thái của dimension table tại thời điểm event xảy ra**:

```sql
-- Ví dụ: camera_01 được di chuyển từ Quận 1 sang Quận 3 vào ngày 15/05
-- Query incident ngày 10/05 → vẫn thấy đúng location "Quận 1"

INSERT INTO fact_violence_incidents
SELECT
    a.event_id,
    a.camera_id,
    a.timestamp,
    a.risk_score,
    c.location,    -- location của camera TẠI THỜI ĐIỂM event, không phải hiện tại
    c.ward_id
FROM fluss.hot_violence_alerts a
LEFT JOIN fluss.dim_camera
    FOR SYSTEM_TIME AS OF a.ptime AS c   -- temporal join: chỉ Fluss hỗ trợ natively
ON a.camera_id = c.camera_id;
```

Medallion với Iceberg: không có concept "version của row tại thời điểm T" → phải tự
implement SCD Type 2 + point-in-time join = hàng trăm dòng code bổ sung.

#### 4. Push-based thay vì Poll-based
```
Medallion (Iceberg/Delta):
  Producer ghi → S3 commit → Consumer poll → phát hiện snapshot mới → đọc
  Latency: poll_interval (30s mặc định) + đọc metadata

Streamhouse (Fluss):
  Producer ghi → Fluss WAL → push notification → Consumer nhận ngay
  Latency: network RTT (~1ms trong cùng cluster)
```

---

## Analogy Cho Hội Đồng

> **Medallion** = 3 ngăn kéo riêng biệt.
> Bạn phải biết tờ giấy đang ở ngăn nào mới lấy được.
> Mỗi lần chuyển ngăn, ai đó phải photo copy tờ giấy đó.
>
> **Streamhouse** = 1 ngăn kéo thông minh.
> Bạn chỉ hỏi "tờ giấy X" — hệ thống tự tìm trong hot/warm/cold.
> Tờ giấy di chuyển (không copy) — chỉ 1 bản tồn tại tại mọi thời điểm.

---

## So Sánh Tổng Hợp

| Tiêu chí | Lambda | Medallion | **Streamhouse** |
|----------|:------:|:---------:|:---------------:|
| Latency tối thiểu | ~30s | 30s–5min | **<100ms** |
| Số codebase | 2 | 1 | **1** |
| Lưu trữ trùng lặp | 2x | 3x | **1x** |
| Upsert/dedup | Phức tạp | MERGE INTO | **Native PK** |
| Temporal join | ❌ | ❌ | **✅ Native** |
| Push notification | ❌ | ❌ | **✅ Change feed** |
| Auto-tiering | ❌ | ❌ | **✅ Tiering Service** |
| Cross-layer SQL | ❌ | Partial | **✅ 1 table** |
| Bug fix scope | Full batch rerun | Full rerun | **Incremental** |

---

## Demo Points Cho Hội Đồng

### Demo 1: Latency
```bash
# Gửi 1 event vào Kafka
# Đo thời gian đến khi query được từ Fluss → <100ms
# So sánh: Paimon (~30s), Iceberg (~5min nếu batch trigger)
```

### Demo 2: Temporal Join
```sql
-- Thay đổi location của cam_01 trong dim_camera (Quận 1 → Quận 3)
-- Query incident 1 giờ trước → vẫn thấy "Quận 1" đúng
-- → Không thể làm được trong Medallion mà không có SCD Type 2
```

### Demo 3: 1 Query, 3 Layers
```sql
-- Trino federation: query across all 3 layers
SELECT 'HOT'  as layer, COUNT(*) FROM fluss_table  WHERE timestamp > NOW() - INTERVAL '1' HOUR
UNION ALL
SELECT 'WARM' as layer, COUNT(*) FROM paimon_table WHERE timestamp > NOW() - INTERVAL '7' DAY
UNION ALL
SELECT 'COLD' as layer, COUNT(*) FROM iceberg_table WHERE YEAR(timestamp) = 2025
-- → Kết quả từ 3 storage engines khác nhau, 1 SQL
```

---

## Kết Luận

Streamhouse không chỉ là "thêm một layer nhanh vào Medallion".
Đây là sự thay đổi mô hình (paradigm shift):

- **Medallion**: Data-centric — tổ chức theo chất lượng (raw → cleaned → aggregated)
- **Streamhouse**: Time-centric — tổ chức theo độ tuổi (hot → warm → cold), tự động tiering

Đối với bài toán giám sát an ninh real-time, Streamhouse là kiến trúc phù hợp vì:
1. Cần phản hồi <100ms cho cảnh báo khẩn cấp
2. Cần lưu trữ lịch sử hàng năm với chi phí hợp lý
3. Cần correlation giữa data real-time và historical trong cùng 1 query

---

*Tài liệu này là một phần của khóa luận tốt nghiệp về Streamhouse Architecture cho hệ thống giám sát an ninh đô thị real-time.*
*Tác giả: Nguyễn Ngọc Minh Nhật & Nguyễn Quốc Huy*
