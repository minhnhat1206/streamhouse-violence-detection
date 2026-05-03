# 🌊 Apache Fluss Guide — Hot Storage Layer

Tài liệu này chi tiết về cách triển khai và sử dụng **Apache Fluss** làm tầng lưu trữ **Hot Storage** trong hệ thống phát hiện bạo lực thời gian thực.

---

## 1. Tổng quan
Trong kiến trúc **Streamhouse**, Fluss đóng vai trò là tầng lưu trữ dữ liệu tức thời (Hot Storage):
- **Độ trễ (Latency)**: < 100ms cho cả ghi và đọc.
- **Vai trò**: Lưu trữ các cảnh báo bạo lực vừa xảy ra (retention ~1-2 giờ) để Dashboard truy vấn tức thì hoặc Chatbot AI tra cứu bối cảnh gần nhất.
- **Ưu điểm**: Hỗ trợ SQL query trực tiếp trên luồng dữ liệu (Streaming Storage) với hiệu năng cao hơn so với việc query trực tiếp từ Kafka.

---

## 2. Kiến trúc cụm (Cluster Architecture)

Hệ thống Fluss trong dự án gồm 3 thành phần chính chạy trên Docker:

| Component | Container Name | Port | Vai trò |
|-----------|----------------|------|---------|
| **Zookeeper** | `fluss-zookeeper` | 2181 | Quản lý metadata và bầu chọn leader cho cụm. |
| **Coordinator** | `fluss-coordinator` | 9123 | Quản lý Schema, Catalogs và điều phối các TabletServers. |
| **TabletServer** | `fluss-tablet` | 9094 | Nơi thực sự lưu trữ dữ liệu (KV & Log). |

---

## 3. Cấu hình Mạng (Quan trọng)

Để Flink và các client khác kết nối được vào Fluss trong Docker, chúng ta sử dụng cơ chế **Dual-Listener**:

### A. Coordinator Configuration
- **INTERNAL**: Dùng cho trao đổi nội bộ giữa Coordinator và TabletServer.
- **CLIENT**: Port `9123` dùng cho Flink Catalog Manager.

### B. TabletServer Configuration
- **INTERNAL**: Port `9093` để Coordinator kiểm tra liveness và replication.
- **CLIENT**: Port `9094` để Flink JobManager/TaskManager đẩy data thực tế vào.

> [!CAUTION]
> Nếu thiếu port `9094` trên TabletServer, Flink sẽ báo lỗi `StaleMetadataException: Alive tablet server is empty`.

---

## 4. Định nghĩa Bảng (Schema)

Bảng chính lưu trữ cảnh báo: `fluss.security.hot_violence_alerts`

| Column | Type | Ghi chú |
|--------|------|---------|
| `incident_id` | STRING | Khóa chính (Primary Key) |
| `camera_id` | STRING | ID camera phát hiện |
| `timestamp` | TIMESTAMP(3) | Thời gian xảy ra sự việc |
| `risk_score` | DOUBLE | Độ nguy hiểm (0.0 - 1.0) |
| `confidence` | DOUBLE | Độ tin cậy của AI |
| `is_violent` | BOOLEAN | Gắn nhãn bạo lực |
| `event_type` | STRING | Loại hành vi (FIGHTING, WEAPON, etc.) |

---

## 5. Ví dụ sử dụng

### 5.1 Khởi tạo Catalog và Bảng (PyFlink)
File: `scripts/transform/init_fluss_tables.py`

```python
t_env.execute_sql("""
    CREATE CATALOG fluss WITH (
        'type' = 'fluss',
        'bootstrap.servers' = 'fluss-coordinator:9123'
    )
""")

t_env.execute_sql("USE CATALOG fluss")
t_env.execute_sql("CREATE DATABASE IF NOT EXISTS security")

t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS security.hot_violence_alerts (
        incident_id STRING,
        camera_id STRING,
        `timestamp` TIMESTAMP(3),
        risk_score DOUBLE,
        confidence DOUBLE,
        is_violent BOOLEAN,
        event_type STRING,
        PRIMARY KEY (incident_id) NOT ENFORCED
    ) WITH (
        'connector' = 'fluss'
    )
""")
```

### 5.2 Đẩy dữ liệu từ Kafka sang Fluss
File: `scripts/transform/sink_to_fluss.py`

```sql
INSERT INTO fluss.security.hot_violence_alerts
SELECT 
    event_id as incident_id,
    camera_id,
    TO_TIMESTAMP(REPLACE(`timestamp`, 'Z', '')) as `timestamp`,
    risk_score,
    confidence,
    is_violent,
    event_type
FROM kafka_source_table;
```

---

## 6. Lệnh vận hành thường gặp

### Kiểm tra Metadata trong Zookeeper:
```powershell
docker exec fluss-zookeeper bin/zkCli.sh -server localhost:2181 ls /fluss/tabletservers/ids
```

### Force Recreate cụm Fluss:
```powershell
docker compose -f docker/docker-compose.yml up -d --force-recreate fluss-coordinator fluss-tablet
```

### Kiểm tra Logs kết nối:
```powershell
docker logs fluss-coordinator | grep "New tablet server callback"
```
