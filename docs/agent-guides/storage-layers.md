# Storage Layers — Hot / Warm / Cold

## 1. HOT LAYER — Apache Fluss

**Mục đích**: Real-time alerts & live dashboards
**Latency**: <100 milliseconds
**Retention**: 1-2 hours

### Đặc Điểm
- Queryable streaming storage (giống Kafka nhưng có SQL)
- Columnar format (tối ưu cho analytics)
- In-memory + distributed

### Khi Nào Dùng
- Multi-camera grid real-time update
- Live incident ticker
- Risk score heatmap (millisecond updates)

### Ví Dụ SQL
```sql
-- Real-time alerts trong 5 phút gần nhất
SELECT camera_id, risk_score, event_type
FROM hot_violence_alerts
WHERE timestamp > NOW() - INTERVAL 5 MINUTE
ORDER BY risk_score DESC;

-- Camera đang có risk cao nhất
SELECT camera_id, MAX(risk_score) as max_risk
FROM hot_violence_alerts
WHERE timestamp > NOW() - INTERVAL 1 MINUTE
GROUP BY camera_id
ORDER BY max_risk DESC
LIMIT 5;
```

---

## 2. WARM LAYER — Apache Paimon

**Mục đích**: Operational analytics & CDC sink
**Latency**: 1-10 minutes
**Retention**: 7-30 days

### Đặc Điểm
- Stream-native lakehouse format
- LSM tree structure (write-optimized)
- Support upsert (continuous updates)
- Multiple merge engines (LAST-ROW, AGGREGATION, etc.)

### Tính Năng
- ACID transactions
- Schema evolution
- Time travel (snapshots)
- CDC native support
- Changelog production

### Khi Nào Dùng
- Operational dashboard (last 24 hours)
- Incident tracking (with soft-delete)
- Feature engineering for ML
- Gold layer aggregations

### Warm Layer (Validated Data) SQL
```sql
-- Incidents hôm nay
SELECT incident_id, camera_id, timestamp, risk_score
FROM paimon.security.bronze_violence_incidents
WHERE timestamp >= CURRENT_DATE
ORDER BY timestamp DESC;

-- Incidents theo camera
SELECT camera_id, COUNT(*) as total, AVG(risk_score) as avg_risk
FROM paimon.security.bronze_violence_incidents
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAY
GROUP BY camera_id
ORDER BY total DESC;
```

### Aggregation Tables SQL
```sql
-- Daily summary by location
SELECT stat_date, location, total_incidents, violent_incidents, avg_risk_score, max_risk_score
FROM paimon.security.daily_incident_stats
WHERE stat_date = CURRENT_DATE;

-- Camera performance today
SELECT stat_date, camera_id, total_incidents, violent_incidents, avg_risk_score, avg_confidence
FROM paimon.security.camera_stats
WHERE stat_date = CURRENT_DATE
ORDER BY violent_incidents DESC;

-- Hourly trend (from base table)
SELECT DATE_TRUNC('HOUR', `timestamp`) as hour, COUNT(*) as count
FROM paimon.security.violence_incidents
WHERE `timestamp` >= CURRENT_DATE
GROUP BY DATE_TRUNC('HOUR', `timestamp`)
ORDER BY hour;
```

### Paimon Table Definitions

**Base Table — `violence_incidents`**
```sql
CREATE TABLE violence_incidents (
    incident_id STRING,
    camera_id STRING,
    `timestamp` TIMESTAMP(3),
    risk_score DOUBLE,
    confidence DOUBLE,
    is_violent BOOLEAN,
    event_type STRING,
    location STRING,
    is_deleted BOOLEAN,
    PRIMARY KEY (incident_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'deduplicate',
    'changelog-producer' = 'input',
    'snapshot.time-retained' = '7d',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '50'
);
```

**Aggregation Table — `daily_incident_stats`**
```sql
CREATE TABLE daily_incident_stats (
    stat_date DATE,
    location STRING,
    total_incidents BIGINT,
    violent_incidents BIGINT,
    avg_risk_score DOUBLE,
    max_risk_score DOUBLE,
    PRIMARY KEY (stat_date, location) NOT ENFORCED
) WITH (
    'merge-engine' = 'deduplicate',
    'changelog-producer' = 'input',
    'snapshot.time-retained' = '30d'
);
```

**Aggregation Table — `camera_stats`**
```sql
CREATE TABLE camera_stats (
    stat_date DATE,
    camera_id STRING,
    total_incidents BIGINT,
    violent_incidents BIGINT,
    avg_risk_score DOUBLE,
    avg_confidence DOUBLE,
    PRIMARY KEY (stat_date, camera_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'deduplicate',
    'changelog-producer' = 'input',
    'snapshot.time-retained' = '30d'
);
```

### Implementation Details
- **Catalog**: Filesystem catalog (không cần Hive Metastore)
- **Warehouse**: `s3://warehouse/paimon` (MinIO)
- **S3 config**: `s3.path.style.access = true` (bắt buộc cho MinIO)
- **JARs**: `paimon-flink-1.18-0.8.2.jar` + `paimon-s3-0.8.2.jar` trong `/opt/flink/lib/`
- **Scripts**:
  - `scripts/transform/init_paimon_tables.py` — Tạo catalog + database + 3 tables (batch mode)
  - `scripts/transform/sink_to_paimon.py` — Flink streaming job Kafka → Paimon (consumer group: `paimon-sink-group`)
  - `scripts/transform/aggregate_paimon.py` — Flink streaming aggregation job (StatementSet: 2 INSERT)
- **Data mapping**: `event_id → incident_id`, `is_deleted = false` (soft-delete support)
- **Aggregation logic**: Đọc CDC changelog từ `violence_incidents`, GROUP BY date+location và date+camera, ghi deduplicate vào 2 bảng stats

---

## 3. COLD LAYER — Apache Iceberg

**Mục đích**: Historical analysis & long-term archival
**Latency**: 10+ minutes (on-demand)
**Retention**: Years

### Đặc Điểm
- File-based lakehouse format
- Parquet columnar storage
- Batch-optimized queries

### Tính Năng
- ACID transactions
- Time travel
- Schema evolution
- Partition pruning
- Broad ecosystem (Spark, Presto, Trino, etc.)

### Khi Nào Dùng
- Monthly incident reports
- Year-over-year trends
- ML training data
- Regulatory compliance
- Cost-effective long-term storage

### Iceberg Table Definition
```sql
CREATE TABLE historical_violence_incidents (
    incident_id STRING,
    camera_id STRING,
    `timestamp` TIMESTAMP(3),
    risk_score DOUBLE,
    confidence DOUBLE,
    is_violent BOOLEAN,
    event_type STRING,
    location STRING,
    incident_date DATE
) PARTITIONED BY (incident_date)
WITH (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
);
```

### SQL Examples
```sql
-- Monthly trend
SELECT
  DATE_TRUNC('MONTH', `timestamp`) as month,
  COUNT(*) as incidents,
  AVG(risk_score) as avg_severity
FROM iceberg.security.historical_violence_incidents
WHERE YEAR(`timestamp`) = 2026
GROUP BY DATE_TRUNC('MONTH', `timestamp`)
ORDER BY month DESC;

-- Time travel query (Iceberg snapshot)
SELECT * FROM iceberg.security.historical_violence_incidents
FOR TIMESTAMP AS OF TIMESTAMP '2026-01-14 14:00:00';

-- Yearly comparison
SELECT
  YEAR(`timestamp`) as year,
  COUNT(*) as total_incidents,
  AVG(risk_score) as avg_severity
FROM iceberg.security.historical_violence_incidents
GROUP BY YEAR(`timestamp`)
ORDER BY year;
```

### Implementation Details
- **Catalog**: Hive Metastore (`thrift://hive-metastore:9083`)
- **Warehouse**: `s3a://warehouse/iceberg_warehouse/` (MinIO)
- **IO Implementation**: `org.apache.iceberg.aws.s3.S3FileIO`
- **JARs**: `iceberg-flink-runtime-1.18-1.5.2.jar` + `flink-shaded-hadoop-2-uber-2.8.3-10.0.jar`
- **Format**: Parquet with Snappy compression, format-version 2
- **Partitioning**: By `incident_date` (DATE) — cho phép efficient partition pruning
- **Scripts**:
  - `scripts/transform/init_iceberg_tables.py` — Tạo Iceberg catalog + database + table (batch mode)
  - `scripts/transform/archive_to_iceberg.py` — Batch archival job Paimon → Iceberg (weekly schedule)
- **Dedup strategy**: `NOT EXISTS` subquery trên `incident_id` để tránh duplicate khi re-run
- **Archive filter**: Chỉ archive data >7 ngày tuổi (`WHERE timestamp < NOW() - INTERVAL '7' DAY`)
- **Dependencies**: Hive Metastore + MySQL phải running trước khi init/archive

---

## Data Flow Between Layers

```
Flink (Source)
    │
    ▼
Fluss (HOT) ──[1-2hr TTL expires]──→ Paimon (WARM) ──[30d]──→ Iceberg (COLD)
    │                                      │                         │
    ▼                                      ▼                         ▼
Real-time alerts              Operational analytics        Historical analysis
<100ms queries                1-10 min queries             Batch queries
Live dashboards               Daily/weekly reports         Monthly/yearly reports
```

### Archival Strategy
- **Fluss → Paimon**: Automatic TTL expiry + Flink sink (continuous) — `sink_to_paimon.py`
- **Paimon → Iceberg**: Scheduled Flink batch job (weekly) — `archive_to_iceberg.py`
- **Iceberg partitioning**: By `incident_date` (DATE) for efficient pruning

### Flink Jobs Summary
| Job | Type | Script | Input | Output |
|-----|------|--------|-------|--------|
| Data Contract Validator | Streaming | `data_contract_validator.py` | Kafka `urban-safety-alerts` | Kafka `hot-violence-alerts-valid` + `urban-safety-quarantine` |
| Fluss Sink | Streaming | `sink_to_fluss.py` | Kafka `hot-violence-alerts-valid` | Fluss `hot_violence_alerts` |
| Paimon Sink | Streaming | `sink_to_paimon.py` | Kafka `hot-violence-alerts-valid` | Paimon `violence_incidents` |
| Paimon Aggregation | Streaming | `aggregate_paimon.py` | Paimon `violence_incidents` (CDC) | Paimon `daily_incident_stats` + `camera_stats` |
| Iceberg Archival | Batch | `archive_to_iceberg.py` | Paimon `violence_incidents` | Iceberg `historical_violence_incidents` |

### Execution Order
```bash
# 1. Init tables (batch — run once)
python /opt/flink/scripts/init_fluss_tables.py
python /opt/flink/scripts/init_paimon_tables.py
python /opt/flink/scripts/init_iceberg_tables.py    # requires hive-metastore + mysql

# 2. Submit streaming jobs (run continuously)
flink run -py /opt/flink/scripts/data_contract_validator.py
flink run -py /opt/flink/scripts/sink_to_fluss.py
flink run -py /opt/flink/scripts/sink_to_paimon.py
flink run -py /opt/flink/scripts/aggregate_paimon.py

# 3. Run archival (batch — weekly schedule)
flink run -py /opt/flink/scripts/archive_to_iceberg.py
```
