# 🤖 Agent Guide to Initialize Streamhouse Data Pipeline
> [!WARNING]
> **Dành riêng cho Agent (Gemini/Claude)**: Máy tính này bị lỗi Environment Variables với lệnh `docker`. Tránh dùng trực tiếp `docker`. Mọi thao tác đều **phải dùng đường dẫn tuyệt đối qua PowerShell**. File này chứa các lệnh đã được cấu hình sẵn đường dẫn tuyệt đối để copy-paste trực tiếp.

This guide follows a step-by-step approach to initialize the Realtime Violence Detection system, focusing on testing the data flow from source to message broker.

## 🛠️ Step 0: Network Initialization
First, ensure the external network required by Docker Compose exists.
```powershell
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" network create violence-detection-net
```

## 📦 Step 1: Core Infrastructure
Initialize the message broker and object storage.
```powershell
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" compose -f docker/docker-compose.yml --env-file docker/.env up -d kafka kafka-ui minio minio_client
```
**Verification:**
- **Kafka UI:** Access `http://localhost:18085` to verify the cluster is healthy.
- **MinIO Console:** Access `http://localhost:9001` (User: `minio`, Pass: `mypassword`).

## 📡 Step 2: Data Source & Simulation
Start the camera simulator and the AI mock inference service.
```powershell
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" compose -f docker/docker-compose.yml --env-file docker/.env up -d producer inference-mock
```
**Verification:**
- Open **Kafka UI** -> Topics.
- Check if topic `urban-safety-alerts` was created.
- Verify messages are flowing into the topic.

## 🛑 Stopping Streaming Services
After testing, **ALWAYS** stop `producer` and `inference-mock` to prevent uncontrolled data flow.

```powershell
# Dừng gracefully bằng stop file
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" exec inference-mock touch /app/tmp/STOP
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" exec producer touch /app/tmp/STOP

# Hoặc dừng bằng docker compose
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" compose -f docker/docker-compose.yml --env-file docker/.env stop inference-mock producer
```

> Chi tiết cơ chế: xem `docs/agent-guides/stop-mechanism.md`

## ⚙️ Step 3: Compute Layer & Data Validation
Initialize the Flink cluster and start the Data Contract validator.

```powershell
# Khởi tạo Flink (sử dụng --build nếu Dockerfile.flink thay đổi)
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" compose -f docker/docker-compose.yml --env-file docker/.env up -d --build jobmanager taskmanager

# Submit Data Contract Validator Job (JARs pre-loaded in /opt/flink/lib/)
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" exec jobmanager flink run -py /opt/flink/scripts/data_contract_validator.py
```

**Verification:**
- **Flink UI:** Access `http://localhost:8081` -> Check `Running Jobs`.
- **Kafka UI:** Check topics `hot-violence-alerts-valid` and `urban-safety-quarantine`.

## 🔥 Step 4: Hot Storage — Apache Fluss
Start the Fluss cluster (ZooKeeper + Coordinator + TabletServer):

```powershell
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" compose -f docker/docker-compose.yml --env-file docker/.env up -d fluss-zookeeper fluss-coordinator fluss-tablet
```

> ⏳ Wait ~10 seconds for the cluster to initialize before the next step.

Then create the Fluss catalog and tables:
```powershell
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" exec jobmanager python /opt/flink/scripts/init_fluss_tables.py
```

**Verification:**
- Expected output: `[SUCCESS] Fluss Catalog and Tables initialized successfully.`

### 📌 Fluss Networking Notes
| Component | INTERNAL Port | CLIENT Port | Purpose |
|-----------|--------------|-------------|---------|
| Coordinator | 9092 | 9123 | 9092 = internal coord-tablet; 9123 = Flink catalog |
| TabletServer | 9093 | 9094 | 9093 = internal replication; 9094 = Flink read/write |

## 🚀 Step 5: Start Kafka → Fluss Sink Job
Submit the Flink job that continuously sinks validated data from Kafka to Fluss:

```powershell
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" exec jobmanager flink run -py /opt/flink/scripts/sink_to_fluss.py
```

**Verification:**
- **Flink UI** (`http://localhost:8081`): Should show 2 running jobs (Validator + Sink).
- Data should now flow: `inference-mock → Kafka → Flink Validator → Kafka (valid) → Flink Sink → Fluss`

## 🌡️ Step 6: Warm Storage — Apache Paimon

### 6.1 — Rebuild Flink Image (includes Paimon JARs)
```powershell
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" compose -f docker/docker-compose.yml --env-file docker/.env up -d --build jobmanager taskmanager
```

### 6.2 — Initialize Paimon Catalog & Warm Table
```powershell
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" exec jobmanager python /opt/flink/scripts/init_paimon_tables.py
```

**Verification:**
- Expected output: `[SUCCESS] Paimon Catalog and Warm table initialized successfully.`
- Paimon uses filesystem catalog with warehouse at `s3://warehouse/paimon` on MinIO.

### 6.3 — Start Kafka → Paimon Sink Job
```powershell
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" exec jobmanager flink run -py /opt/flink/scripts/sink_to_paimon.py
```

**Verification:**
- **Flink UI** (`http://localhost:8081`): Should show 3 running jobs (Validator + Fluss Sink + Paimon Sink).
- Data flow: `Kafka (valid) → Paimon (security.violence_incidents)`
- Consumer group `paimon-sink-group` (separate from Fluss's `fluss-sink-group`).

## 🔜 Next Steps
- **Step 7:** Create **Paimon Aggregation tables** — aggregation jobs.
- **Step 8:** Setup **Cold Storage (Iceberg)** — historical table + archival.
- **Step 9:** Configure **Trino** for federated queries (port `8082`).
- **Step 10:** Start **Agentic RAG** chatbot (`scripts/chatbot/`).
