# Dashboard & Monitoring Guide

## Tổng Quan

Hệ thống giám sát được xây dựng trên 3 lớp:

| Lớp | Công cụ | Mục đích |
|-----|---------|----------|
| **React UI** | Vigilance AI (port 5173) | Command center cho operator |
| **Grafana** | port 3001 | Time-series metrics, SLA monitoring |
| **Prometheus** | port 9090 | Thu thập metrics từ Flink, chatbot, node |

---

## 1. React UI Dashboard (Vigilance AI)

### Truy cập
```
http://localhost:5173
```

### Các trang chính

#### `/analytics` — Analytics Dashboard
Dữ liệu thực từ Streamhouse (Fluss/Paimon/Iceberg).

**KPI Cards (top)**
| Card | Data Source | Mô tả |
|------|-------------|--------|
| Alerts (24h) | Iceberg via Trino | Tổng số incident 24 giờ qua |
| Peak Risk Score | Iceberg via Trino | Risk score cao nhất trong 24h |
| WARM rows | `/api/layer-counts` | Tổng rows trong Paimon |
| COLD rows | `/api/layer-counts` | Tổng rows trong Iceberg |

**Streamhouse 3-Layer Health Section**
- Hiển thị latency thực đo vs SLA target cho từng layer
- SLA badge (✓ xanh / ✗ đỏ) tự động tính theo ngưỡng:
  - HOT (Fluss): target **100ms**
  - WARM (Paimon): target **10.0s**
  - COLD (Iceberg): target **30.0s**
- SLA progress bar màu sắc theo mức độ (xanh → vàng → đỏ)
- Latency comparison table 3 layer

**Charts (dữ liệu từ `/api/recent-incidents?limit=200`)**

| Chart | Type | Mô tả |
|-------|------|--------|
| Alerts Per Hour | AreaChart (Recharts) | Số alert theo giờ, 24h gần nhất |
| Loại Sự Cố | PieChart donut | Phân phối incident type (7 ngày) |
| Camera Breakdown | Horizontal BarChart | Total vs Violent theo từng camera |
| Risk Score Distribution | BarChart histogram | Phân phối risk score (bucket 25%) |
| Điểm Nóng (Top Locations) | Progress bars | Top địa điểm nhiều incident nhất |
| Radar Chart | RadarChart | Multi-dimension incident type view |

**Auto-refresh intervals**
- `/api/stats` → 60 giây
- `/api/layer-counts` → 30 giây
- `/api/latency` → 30 giây
- `/api/recent-incidents` → 60 giây

---

#### `/status` — Streamhouse Status
Live infrastructure health cho toàn bộ pipeline.

**Flink Cluster Overview** (từ `http://localhost:8081`)
- Running jobs count
- Task slots total / available
- Flink version

**Storage Layer Health** (từ `/api/layer-counts` + `/api/latency`)
| Layer | Metrics hiển thị |
|-------|-----------------|
| HOT · Fluss | Rows stored, Query latency ms, SLA bar |
| WARM · Paimon | Rows stored, Query latency s, SLA bar |
| COLD · Iceberg | Rows stored, Query latency s, SLA bar |

**Flink Jobs List** (từ Flink REST API)
- Job name, job ID, status badge (RUNNING=xanh, FAILED=đỏ, FINISHED=xám)
- Sắp xếp: RUNNING lên đầu

**Service Connectivity** (health check qua fetch)
| Service | Endpoint kiểm tra |
|---------|-------------------|
| Chatbot API | `/api/layer-counts` |
| Flink JobManager | `localhost:8081/overview` |
| Trino Coordinator | `/api/stats` (chatbot proxy) |

**Quick Links** (icon buttons)
- Flink UI → `http://localhost:8081`
- MinIO → `http://localhost:9001`
- Grafana → `http://localhost:3001`
- Prometheus → `http://localhost:9090`
- Kafka UI → `http://localhost:18085`
- Trino → `http://localhost:8082`

**Auto-refresh**: 15 giây

---

## 2. Prometheus Scraping

### Config file
`config/prometheus/prometheus.yml`

### Scrape jobs đang active

| Job | Target | Metrics path | Mô tả |
|-----|--------|-------------|--------|
| `prometheus` | `localhost:9090` | `/metrics` | Self-monitoring |
| `node-exporter` | `node-exporter:9100` | `/metrics` | Host CPU/RAM/Disk/Network |
| `flink-jobmanager` | `jobmanager:9249` | `/` | Flink JM JVM + jobs metrics |
| `flink-taskmanager` | `taskmanager:9250` | `/` | Flink TM throughput metrics |
| `chatbot` | `chatbot:5002` | `/metrics` | RAG query latency + counts |

### Flink Prometheus Reporter
Plugin: `/opt/flink/plugins/metrics-prometheus/`

Cấu hình trong `docker-compose.yml` (FLINK_PROPERTIES):
```
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249   # JobManager
# TaskManager: port 9250
```

### Key Flink Metrics

| Metric | Mô tả |
|--------|--------|
| `flink_jobmanager_numRunningJobs` | Số jobs đang chạy |
| `flink_taskmanager_job_task_numRecordsIn` | Records vào từng task |
| `flink_taskmanager_job_task_numRecordsOut` | Records ra từng task |
| `flink_jobmanager_Status_JVM_Memory_Heap_Used` | JM heap bytes dùng |
| `flink_taskmanager_Status_JVM_Memory_Heap_Used` | TM heap bytes dùng |
| `flink_jobmanager_Status_JVM_Memory_Heap_Max` | JM heap bytes tối đa |

### Key Chatbot Metrics

| Metric | Labels | Mô tả |
|--------|--------|--------|
| `chatbot_queries_total` | `layer={hot,warm,cold}` | Tổng queries theo layer |
| `chatbot_query_duration_seconds_bucket` | `layer`, `le` | Histogram latency |
| `chatbot_query_duration_seconds_sum` | `layer` | Tổng thời gian query |
| `chatbot_query_duration_seconds_count` | `layer` | Số lần đo |

### Key Node-Exporter Metrics

| Metric | Mô tả |
|--------|--------|
| `node_cpu_seconds_total{mode="idle"}` | CPU idle time (dùng để tính % usage) |
| `node_memory_MemFree_bytes` | RAM tự do |
| `node_memory_MemTotal_bytes` | Tổng RAM |
| `node_network_receive_bytes_total` | Network nhận |
| `node_network_transmit_bytes_total` | Network gửi |

### Useful PromQL Queries

```promql
# CPU usage %
100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)

# RAM usage %
100 * (1 - ((node_memory_MemFree_bytes + node_memory_Cached_bytes + node_memory_Buffers_bytes) / node_memory_MemTotal_bytes))

# Flink records throughput (1m rate)
rate(flink_taskmanager_job_task_numRecordsIn[1m])

# Chatbot P95 latency by layer
histogram_quantile(0.95, sum(rate(chatbot_query_duration_seconds_bucket[5m])) by (layer, le))

# Chatbot query rate
rate(chatbot_queries_total[5m])
```

---

## 3. Grafana Dashboards

### Truy cập
```
http://localhost:3001
user: admin / password: admin
```

Folder: **Streamhouse** (auto-provisioned từ `config/grafana/provisioning/`)

### Dashboard 1: Streamhouse Architecture Monitor
**UID**: `streamhouse-arch-001`  
**URL**: `http://localhost:3001/d/streamhouse-arch-001`  
**Datasource**: Prometheus (`prometheus_ds`)  
**Auto-refresh**: 10 giây

#### Row 1: HOT Layer — Apache Fluss (<100ms Real-Time)

| Panel | Type | Metric | Giá trị thực tế |
|-------|------|--------|----------------|
| Flink Running Jobs | `stat` | `flink_jobmanager_numRunningJobs` | **2** |
| Flink Records In/sec | `stat` | `sum(rate(...numRecordsIn[1m]))` | Live |
| JobManager JVM Heap % | `gauge` | `1 - Heap_Available/Heap_Max` | N/A nếu metric variant khác |
| TaskManager JVM Heap % | `gauge` | `1 - TM_Available/TM_Max` | N/A nếu metric variant khác |
| Flink Record Throughput | `timeseries` | rate numRecordsIn/Out [1m] | Max **337/s** |
| Flink JVM Memory Usage | `timeseries` | Heap Used (JM + TM) bytes | Live |

#### Row 2: Chatbot RAG — Query Performance by Layer

| Panel | Type | Mô tả |
|-------|------|--------|
| Query Latency P50/P95/P99 by Layer | `timeseries` | histogram_quantile theo layer |
| P95 Latency per Layer (SLA check) | `bargauge` | HOT/WARM/COLD P95 so với SLA |
| Queries per Layer (10min increments) | `timeseries` | increase(chatbot_queries_total[10m]) |
| Current P50 Latency by Layer | `stat` | P50 latency hiện tại 3 layer |

#### Row 3: System Resources — Node Health

| Panel | Type | Metric | Giá trị thực tế |
|-------|------|--------|----------------|
| Host CPU Usage | `gauge` | node_cpu idle rate | **23.2%** |
| Host RAM Usage | `gauge` | node_memory | **82.2%** |
| Host Disk Usage | `gauge` | node_filesystem `/` | N/A (Docker mountpoint) |
| CPU & Memory Trend | `timeseries` | CPU% + RAM% 1h | Real history |
| Network I/O | `timeseries` | Rx/Tx eth0 bytes/s | **51.2 B/s Rx** |

---

### Dashboard 2: Violence Incidents Analytics
**UID**: `violence-incidents-v2`  
**URL**: `http://localhost:3001/d/violence-incidents-v2`  
**Datasource**: Trino (`trino_security_ds`) → cần Trino datasource plugin  
**Note**: Dashboard này yêu cầu Grafana Trino datasource plugin. Nếu plugin chưa cài, panels sẽ hiển thị "No data source".

#### Panels
- **KPI Row**: Violent incidents 24h, violent incidents 7d, active cameras 24h, avg risk score 24h
- **Trends Row**: Incidents per hour (total vs violent, 48h timeseries), incident type donut chart
- **Hotspot Row**: Camera breakdown bar chart, top locations bar chart, camera risk table
- **Risk Row**: Camera risk gauge, high-risk incidents table (risk ≥ 0.75)

---

## 4. Khởi Động Monitoring Stack

### Khởi động với monitoring profile
```bash
# Core + monitoring (Prometheus + Grafana + Node-Exporter)
docker compose -f docker/docker-compose.yml --profile monitoring up -d

# Core + monitoring + UI (+ Kafka UI + Flink SQL Gateway)
docker compose -f docker/docker-compose.yml --profile monitoring --profile ui up -d

# Full stack
docker compose -f docker/docker-compose.yml --profile monitoring --profile ui --profile streaming up -d
```

### Kiểm tra Prometheus targets
```
http://localhost:9090/targets
```
Kết quả mong đợi:
- `chatbot` → 1/1 UP
- `flink-jobmanager` → 1/1 UP
- `flink-taskmanager` → 1/1 UP (nếu Flink đã warm-up)
- `node-exporter` → 1/1 UP
- `prometheus` → 1/1 UP

### Reload Grafana sau khi sửa dashboard JSON
```bash
docker compose -f docker/docker-compose.yml --profile monitoring restart grafana
```

---

## 5. Troubleshooting

### Grafana "An unexpected error happened"
**Nguyên nhân**: Panel type config không hợp lệ với Grafana version hiện tại (13.x).

**Common mistakes**:
- `gauge` panel có options của `stat` panel (`colorMode`, `graphMode`, `textMode`)
- `bargauge` thiếu `minVizHeight`, `minVizWidth`, `namePlacement`, `valueMode`

**Fix gauge panel options** (đúng cho Grafana 13):
```json
{
  "type": "gauge",
  "options": {
    "orientation": "auto",
    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false},
    "showThresholdLabels": false,
    "showThresholdMarkers": true,
    "minVizHeight": 75,
    "minVizWidth": 75
  }
}
```

**Fix bargauge panel options**:
```json
{
  "type": "bargauge",
  "options": {
    "displayMode": "gradient",
    "minVizHeight": 10,
    "minVizWidth": 0,
    "namePlacement": "auto",
    "orientation": "horizontal",
    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false},
    "showUnfilled": true,
    "valueMode": "color"
  }
}
```

### Flink metrics không hiển thị
```bash
# Kiểm tra Prometheus reporter port mở
docker exec jobmanager curl -s localhost:9249 | head -5

# Kiểm tra FLINK_PROPERTIES
docker exec jobmanager env | grep metrics
```
Nếu không có metrics → restart jobmanager + taskmanager.

### node_filesystem disk metric "No data"
Node-exporter trong Docker không mount `/` của host → metric `mountpoint="/"` không tồn tại.  
Xem danh sách mountpoints thực tế:
```
http://localhost:9090/graph?g0.expr=node_filesystem_avail_bytes
```

### Chatbot metrics trống
Cần chạy ít nhất 1 query qua chatbot để tạo metric series.  
Test nhanh:
```bash
curl -s -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "có bao nhiêu sự cố trong 30 phút qua?"}'
```

### React UI không load data
Kiểm tra chatbot API:
```bash
curl http://localhost:5002/api/layer-counts
curl http://localhost:5002/api/latency
```
Nếu lỗi → chatbot container chưa start. Kiểm tra: `docker ps | grep chatbot`

---

## 6. Cấu Trúc Files

```
config/
├── prometheus/
│   └── prometheus.yml              # Scrape config (Flink JM/TM, chatbot, node)
└── grafana/
    └── provisioning/
        ├── datasources/
        │   └── datasources.yml     # Prometheus datasource (uid: prometheus_ds)
        └── dashboards/
            ├── dashboards.yml      # Provider config (auto-load from this folder)
            ├── streamhouse_architecture.json   # Dashboard 1 (Prometheus)
            └── violence_incidents_v2.json      # Dashboard 2 (Trino)

Violence-Urban-Safety-UI/frontend/src/
├── pages/
│   ├── Analytics.jsx               # Analytics dashboard (Recharts + real API data)
│   ├── StreamhouseStatus.jsx       # Infrastructure status page
│   ├── Home.jsx                    # Command center (LayerBadge + live counts)
│   └── Chatbot.jsx                 # Agentic RAG terminal
├── components/layout/
│   └── SideBar.jsx                 # Nav: LiveStreams/Alerts/Analytics/Status/Assistant
└── routers/
    └── router.jsx                  # React Router v6 config

scripts/chatbot/
└── main.py                         # FastAPI + Prometheus metrics export
                                    # (chatbot_queries_total, chatbot_query_duration_seconds)
```

---

## 7. Giá Trị Thực Tế Đã Kiểm Chứng (2026-05-22)

| Metric | Giá trị | Source |
|--------|---------|--------|
| HOT latency | **77ms** | `/api/latency` (Fluss SQL Gateway) |
| WARM latency | **7.5–10.4s** | `/api/latency` (Paimon Trino) |
| COLD latency | **2.0–5.7s** | `/api/latency` (Iceberg Trino) |
| COLD rows | **15,834** | Iceberg historical table |
| Flink running jobs | **2** | Flink REST API |
| Host CPU | **23.2%** | node-exporter |
| Host RAM | **82.2%** | node-exporter |
| Network Rx | **51.2 B/s** | node-exporter (eth0) |
| Flink peak throughput | **337 records/s** | Prometheus (numRecordsIn) |
| Top incident location | **Đường Nguyễn Bỉnh Khiêm** (1,232) | Iceberg via Trino |

---

*Updated: 2026-05-22 — Session 43*  
*Verified by: Claude Code (Anthropic)*
