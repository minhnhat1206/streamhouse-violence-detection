# Streamhouse Architecture & Chatbot — Technical Document
**Date**: 2026-05-07  
**Author**: Claude (Infrastructure Agent)  
**Status**: Current system state

---

## 1. Kiến trúc Streamhouse 3-Layer

### 1.1 Tổng quan

Hệ thống sử dụng kiến trúc **Streamhouse Trio** — kết hợp 3 storage layer với latency và retention khác nhau, thay vì Lakehouse truyền thống:

```
Camera (RTSP)
    │
    ▼
VioMobileNet (ML Inference) ─────────────────────────► Kafka
    │                                               topic: hot-violence-alerts-valid
    │
    ▼
Apache Flink (Stream Processing)
    │
    ├──► [HOT]  Apache Fluss ──── <100ms ──── 1-2 giờ retention
    │           port: 9123/9094
    │           table: security.hot_violence_alerts
    │
    ├──► [WARM] Apache Paimon ─── 1-10 phút ─ 7-30 ngày retention
    │           via Flink SQL Gateway (port 8083)
    │           tables: violence_incidents, daily_incident_stats, camera_stats
    │
    └──► [COLD] Apache Iceberg ── 10+ phút ── Years retention
                via Trino (port 8082)
                table: historical_violence_incidents
                       (catalog: iceberg, schema: security)
```

### 1.2 Chi tiết từng Layer

#### HOT Layer — Apache Fluss
- **Latency**: <100ms từ khi event xảy ra đến khi query được
- **Retention**: 1-2 giờ (log-based, auto-expire)
- **Use case**: Dashboard real-time, alert ngay lập tức
- **Query**: Trino connector (fluss catalog) hoặc Flink native
- **Schema**: `camera_id`, `timestamp`, `risk_score`, `is_violent`, `confidence`
- **Populate**: Flink job `sink_to_fluss.py` consume từ Kafka → write Fluss

#### WARM Layer — Apache Paimon
- **Latency**: 1-10 phút (Flink checkpoint interval 30s + Paimon commit)
- **Retention**: 7-30 ngày (configurable TTL)
- **Use case**: Analytics hàng ngày, trend analysis, aggregations
- **Query**: Flink SQL Gateway (vì paimon-trino JAR không tồn tại trên Maven)
- **Tables**:
  - `violence_incidents` — raw incidents, deduplicate trên `incident_id`
  - `daily_incident_stats` — aggregation theo ngày (COUNT, AVG risk score)
  - `camera_stats` — aggregation theo camera (total incidents, violent count)
- **Populate**: Flink job `sink_to_paimon.py` + `aggregate_paimon.py`

#### COLD Layer — Apache Iceberg
- **Latency**: 10+ phút (batch commit)
- **Retention**: Vô hạn (object storage MinIO)
- **Use case**: Lịch sử dài hạn, báo cáo, compliance, forensics
- **Query**: Trino (iceberg catalog, schema: security)
- **Table**: `historical_violence_incidents`
  - Columns: `incident_id`, `camera_id`, `timestamp`, `risk_score`, `confidence`, `is_violent`, `event_type`, `location`, `incident_date`
- **Populate**: Flink job `archive_to_iceberg.py` — đọc từ Kafka, write Iceberg

---

## 2. Kiến trúc Chatbot (Agentic RAG)

### 2.1 Stack

| Component | Technology |
|-----------|-----------|
| Framework | LangGraph (multi-node agent graph) |
| LLM | Google Gemini 2.0 Flash |
| Vector DB | ChromaDB (local persistent) |
| Query Engine | Trino + Flink SQL Gateway |
| API | FastAPI (port 5002) |

### 2.2 Agent Graph — 6 Nodes

```
User Query
    │
    ▼
┌─────────────────────────────────────────────┐
│  Node 1: understand                          │
│  • Gemini phân tích intent                   │
│  • Extract: time_period, location, metric    │
│  • Detect language (Vietnamese/English)      │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  Node 2: select_layer                        │
│  • time_period < 1 giờ  → HOT (Fluss)       │
│  • 1 giờ ≤ time ≤ 7 ngày → WARM (Paimon)   │
│  • time > 7 ngày         → COLD (Iceberg)   │
│  • ChromaDB lookup để lấy schema context    │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  Node 3: generate_sql                        │
│  • Gemini generate SQL dựa trên schema       │
│  • Sử dụng layer-appropriate table names    │
│  • ChromaDB cung cấp schema metadata        │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  Node 4: execute                             │
│  • Route tới Fluss / Paimon / Iceberg        │
│  • _adapt_sql_to_iceberg() nếu cần fallback │
│  • Trino hoặc Flink SQL Gateway             │
└─────────────────┬───────────────────────────┘
                  │
            ┌─────┴──────┐
            │ Error?     │
            ▼            ▼
┌───────────────┐   ┌─────────────────────────┐
│ Node 5:       │   │  Node 6: respond         │
│ self_correct  │   │  • Format kết quả        │
│ • Max 3 retry │   │  • Citation (source      │
│ • Fix SQL     │   │    table, time range,    │
│ • Log errors  │   │    layer)                │
│               │   │  • Vietnamese response   │
└───────┬───────┘   └─────────────────────────┘
        │                         ▲
        └─────────────────────────┘
              (retry → execute)
```

### 2.3 Layer Routing Logic

```python
# Numeric regex trước — tránh false positive
pattern = r'(\d+)\s*(hour|giờ|gio|day|ngày|week|tuần|month|tháng|minute|phút)'

# Routing decision:
# 24 giờ qua → 24 hours → 1 day → WARM (Paimon)
# 5 phút qua → 5 minutes → HOT (Fluss)
# 30 ngày qua → 30 days → COLD (Iceberg) nếu > 7 ngày
```

### 2.4 SQL Adaptation (Iceberg Fallback)

Khi query Iceberg, `_adapt_sql_to_iceberg()` tự động:
1. Strip catalog prefix: `paimon.security.` → `iceberg.security.`
2. Remap table names: `violence_incidents` → `historical_violence_incidents`
3. **Rewrite aggregate queries**: Nếu SQL tham chiếu `daily_incident_stats` hoặc `camera_stats` (không tồn tại trong Iceberg), thay bằng inline aggregation trên `historical_violence_incidents`

```sql
-- Input (từ Gemini):
SELECT * FROM iceberg.security.daily_incident_stats
WHERE timestamp >= NOW() - INTERVAL '24' HOUR

-- Output (sau adaptation):
SELECT CAST(timestamp AS DATE) AS stat_date,
       COUNT(*) AS total_incidents,
       SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) AS violent_incidents,
       AVG(risk_score) AS avg_risk_score
FROM iceberg.security.historical_violence_incidents
WHERE timestamp >= NOW() - INTERVAL '24' HOUR
GROUP BY CAST(timestamp AS DATE)
ORDER BY stat_date DESC
LIMIT 30
```

### 2.5 REST API Endpoints

| Endpoint | Method | Mô tả |
|----------|--------|-------|
| `/chat` | POST | Agentic RAG query |
| `/api/recent-incidents` | GET | 20 incidents mới nhất từ Iceberg |
| `/api/stats` | GET | KPI stats cho Analytics dashboard |
| `/api/camera-status` | GET | Camera status từ Kafka (5 phút gần nhất) |
| `/health` | GET | Health check |

---

## 3. Dữ liệu có tự động đổ vào Streamhouse không?

### 3.1 Phân tích hiện tại

**Tóm tắt: Có một phần tự động, nhưng chưa đủ.**

| Bước | Tự động? | Cơ chế | Ghi chú |
|------|----------|--------|---------|
| Camera → Kafka | ✅ Tự động | `inference-mock` container chạy liên tục | Production: cần `producer` + ML model |
| Kafka → HOT (Fluss) | ⚠️ Cần khởi động | Flink job `sink_to_fluss.py` | Phải submit job thủ công |
| Kafka → WARM (Paimon) | ⚠️ Cần khởi động | Flink job `sink_to_paimon.py` | Phải submit job thủ công |
| Kafka → COLD (Iceberg) | ⚠️ Cần khởi động | Flink job `archive_to_iceberg.py` | Phải submit job thủ công |
| WARM → Aggregations | ⚠️ Cần khởi động | Flink job `aggregate_paimon.py` | Phải submit job thủ công |
| Health monitoring | ❌ Không có | Chưa implement | Flink jobs có thể crash |
| Data quality checks | ❌ Không có | Chưa implement | Không có validation report |
| Old data cleanup | ❌ Không có | Chưa implement | Paimon/Iceberg có thể phình to |

### 3.2 Vấn đề cốt lõi

Flink streaming jobs (HOT/WARM/COLD sinks) **không tự khởi động** khi container restart. Nếu Flink jobmanager restart, tất cả jobs bị mất state và phải submit lại.

Hiện tại cũng **không có cơ chế tự phát hiện** khi Flink job bị lỗi hay fail.

---

## 4. Có nên dùng Airflow không?

### 4.1 Phân tích

**Kết luận: Nên dùng Airflow cho một số task cụ thể**, nhưng không phải toàn bộ pipeline.

| Task | Airflow? | Lý do |
|------|----------|-------|
| Flink streaming jobs (HOT/WARM/COLD sink) | ❌ Không | Đây là streaming jobs, không phải batch — nên dùng Flink restart policy |
| Archive Paimon → Iceberg (weekly cleanup) | ✅ Có | Batch job, cần scheduling |
| Flink job health monitoring | ✅ Có | Cần poll Flink REST API định kỳ, restart nếu fail |
| Iceberg data quality check | ✅ Có | Batch validation, cần schedule |
| Paimon partition cleanup | ✅ Có | TTL management |
| Report generation | ✅ Có | Batch report, cần schedule |

**Lý do chọn Airflow**:
- Python-native DAGs, dễ integrate với PyTrino và Flink REST API
- Web UI để monitor, retry, backfill
- Phù hợp với resource limit 16GB (lightweight scheduler)
- Không cần Spark hay Hadoop — khớp với Streamhouse philosophy

### 4.2 Alternative: Không dùng Airflow

Nếu không muốn thêm dependency, có thể dùng:
- **systemd timers** (trên Linux) — đơn giản nhưng không có UI
- **cron jobs** — đơn giản nhưng khó monitor
- **Flink checkpointing + restart policy** — cho streaming jobs đặc biệt

Với thesis/demo scope, Airflow là lựa chọn professional nhất.

---

## 5. Airflow Plan

### 5.1 Architecture

```
Airflow (port 8089)
    │
    ├── DAG 1: flink_jobs_monitor        (every 15 min)
    │   └── Check Flink REST API → restart failed jobs
    │
    ├── DAG 2: streamhouse_archive       (weekly, Sunday 2:00 AM)
    │   └── Trigger archive_to_iceberg.py for old Paimon data
    │
    └── DAG 3: iceberg_data_quality      (daily, 6:00 AM)
        └── Count rows, check nulls, alert if anomaly
```

### 5.2 DAG 1: Flink Jobs Health Monitor

**File**: `airflow/dags/flink_jobs_monitor.py`

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, logging

FLINK_API = "http://jobmanager:8081"

REQUIRED_JOBS = [
    "sink_to_fluss",
    "sink_to_paimon", 
    "archive_to_iceberg",
    "aggregate_paimon",
]

def check_and_restart_flink_jobs():
    resp = requests.get(f"{FLINK_API}/jobs", timeout=10)
    resp.raise_for_status()
    running = {j["name"] for j in resp.json()["jobs"] if j["status"] == "RUNNING"}
    
    for job_name in REQUIRED_JOBS:
        if job_name not in running:
            logging.warning(f"Flink job '{job_name}' not running — attempting restart")
            # Submit JAR or Python job via Flink REST API
            # In production: call /jars/{jar-id}/run with job params
            # For PyFlink: use flink run CLI via subprocess

with DAG(
    dag_id="flink_jobs_monitor",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 5, 7),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
) as dag:
    PythonOperator(
        task_id="check_flink_jobs",
        python_callable=check_and_restart_flink_jobs,
    )
```

### 5.3 DAG 2: Streamhouse Archive (Weekly)

**File**: `airflow/dags/streamhouse_archive.py`

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="streamhouse_archive",
    schedule_interval="0 2 * * 0",  # Sunday 2:00 AM
    start_date=datetime(2026, 5, 7),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
) as dag:

    # Step 1: Archive old Paimon WARM data → Iceberg COLD
    archive_paimon = BashOperator(
        task_id="archive_paimon_to_iceberg",
        bash_command="""
        docker exec flink-jobmanager \
          /opt/flink/bin/flink run \
          -py /opt/flink/usrlib/archive_to_iceberg.py \
          --pyFiles /opt/flink/usrlib/ \
          -Dexecution.runtime-mode=BATCH
        """,
    )

    # Step 2: Expire old Paimon snapshots (>30 days)
    expire_paimon = BashOperator(
        task_id="expire_paimon_snapshots",
        bash_command="""
        docker exec trino-coordinator \
          trino --execute "
            CALL paimon.system.expire_snapshots(
              'security', 'violence_incidents',
              TIMESTAMP '{{ macros.ds_add(ds, -30) }} 00:00:00'
            )
          "
        """,
    )

    # Step 3: Expire old Iceberg snapshots (>90 days)
    expire_iceberg = BashOperator(
        task_id="expire_iceberg_snapshots",
        bash_command="""
        docker exec trino-coordinator \
          trino --execute "
            ALTER TABLE iceberg.security.historical_violence_incidents
            EXECUTE expire_snapshots(retention_threshold => '90d')
          "
        """,
    )

    archive_paimon >> expire_paimon >> expire_iceberg
```

### 5.4 DAG 3: Iceberg Data Quality (Daily)

**File**: `airflow/dags/iceberg_data_quality.py`

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import trino, logging

def run_quality_checks():
    conn = trino.dbapi.connect(
        host="trino-coordinator",
        port=8082,
        user="airflow",
        catalog="iceberg",
        schema="security",
    )
    cur = conn.cursor()

    checks = {
        "total_rows": "SELECT COUNT(*) FROM historical_violence_incidents",
        "null_camera_id": "SELECT COUNT(*) FROM historical_violence_incidents WHERE camera_id IS NULL",
        "null_timestamp": "SELECT COUNT(*) FROM historical_violence_incidents WHERE timestamp IS NULL",
        "rows_last_24h": """
            SELECT COUNT(*) FROM historical_violence_incidents
            WHERE timestamp >= NOW() - INTERVAL '24' HOUR
        """,
        "violent_ratio": """
            SELECT ROUND(
                100.0 * SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) / COUNT(*), 2
            ) FROM historical_violence_incidents
            WHERE timestamp >= NOW() - INTERVAL '7' DAY
        """,
    }

    results = {}
    for name, sql in checks.items():
        cur.execute(sql)
        results[name] = cur.fetchone()[0]
        logging.info(f"Quality check [{name}]: {results[name]}")

    # Alert conditions
    if results["null_camera_id"] > 0:
        logging.error(f"DATA QUALITY ALERT: {results['null_camera_id']} rows with null camera_id")
    if results["rows_last_24h"] == 0:
        logging.warning("DATA QUALITY ALERT: No data in last 24 hours — pipeline may be down")
    if results.get("violent_ratio", 0) > 80:
        logging.warning(f"DATA QUALITY ALERT: Violent ratio {results['violent_ratio']}% seems too high")

    conn.close()
    return results

with DAG(
    dag_id="iceberg_data_quality",
    schedule_interval="0 6 * * *",  # Daily 6:00 AM
    start_date=datetime(2026, 5, 7),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:
    PythonOperator(
        task_id="iceberg_quality_checks",
        python_callable=run_quality_checks,
    )
```

### 5.5 Docker Compose — Airflow Service

Thêm vào `docker/docker-compose.yml` dưới profile `orchestration`:

```yaml
  # ─── Airflow (Orchestration) ───────────────────────────────────────────────
  airflow:
    image: apache/airflow:2.9.1-python3.11
    container_name: airflow
    profiles: [orchestration]
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////opt/airflow/airflow.db
      AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY:-changeme_32char_fernet_key_here_}
      AIRFLOW__WEBSERVER__SECRET_KEY: ${AIRFLOW_SECRET_KEY:-changeme_secret}
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL: "60"
      # Access to other services
      FLINK_API: http://jobmanager:8081
      TRINO_HOST: trino-coordinator
      TRINO_PORT: "8082"
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - /var/run/docker.sock:/var/run/docker.sock  # để gọi docker exec
    ports:
      - "8089:8080"
    networks:
      - violence-detection-net
    command: >
      bash -c "
        airflow db migrate &&
        airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com &&
        airflow scheduler &
        airflow webserver
      "
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    deploy:
      resources:
        limits:
          memory: 512m
          cpus: "0.50"
```

**Khởi động**:
```bash
docker compose -f docker/docker-compose.yml --profile orchestration up -d airflow
# Web UI: http://localhost:8089 (admin/admin)
```

---

## 6. Kết luận & Recommendations

### Trạng thái hiện tại (2026-05-07)

| Thành phần | Trạng thái |
|-----------|-----------|
| Kafka (message broker) | ✅ Hoạt động |
| Flink (stream processing) | ✅ Container chạy, jobs cần submit thủ công |
| HOT — Fluss | ⚠️ Container chạy, data pipeline cần Flink job |
| WARM — Paimon | ✅ Có dữ liệu (14,106+ rows), query qua Flink SQL Gateway |
| COLD — Iceberg | ✅ Có dữ liệu, query qua Trino |
| Chatbot RAG | ✅ Hoạt động với fallback SQL adaptation |
| Airflow | ❌ Chưa deploy |

### Bước tiếp theo

1. **Ngắn hạn**: Submit Flink streaming jobs (sink_to_fluss, sink_to_paimon, archive_to_iceberg) tự động khi stack khởi động — dùng Flink REST API hoặc script init
2. **Trung hạn**: Deploy Airflow với 3 DAGs trên
3. **Dài hạn**: Thêm Flink savepoint management để resume sau restart mà không mất state

---

*Document này mô tả trạng thái thực tế của hệ thống ngày 2026-05-07, không phải trạng thái lý tưởng.*
