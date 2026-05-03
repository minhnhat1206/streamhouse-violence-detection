# Trino Quick Reference Guide

**Quick lookup** cho debugging, testing, và daily operations.

---

## Start / Stop

```bash
# Start Trino coordinator only
docker compose -f docker/docker-compose.yml up -d trino-coordinator

# Start with Flink SQL Gateway (for hot layer)
docker compose -f docker/docker-compose.yml --profile ui up -d flink-sql-gateway

# Stop
docker compose -f docker/docker-compose.yml down

# View logs
docker compose -f docker/docker-compose.yml logs -f trino-coordinator
```

---

## Connect to Trino CLI

```bash
docker exec -it trino-coordinator trino
```

### Inside Trino REPL

```sql
-- List all catalogs
SHOW CATALOGS;

-- List schemas in a catalog
SHOW SCHEMAS IN iceberg;
SHOW SCHEMAS IN paimon;

-- List tables in a schema
SHOW TABLES IN iceberg.violence_db;
SHOW TABLES IN paimon.security;

-- Describe table
DESCRIBE iceberg.violence_db.violence_events_for_rag;
DESC paimon.security.violence_incidents;

-- Sample data
SELECT * FROM paimon.security.violence_incidents LIMIT 10;

-- Count rows (quick check)
SELECT COUNT(*) FROM iceberg.violence_db.violence_events_for_rag;

-- Exit
EXIT;
```

---

## Common Queries

### Warm Layer (Paimon) — Recent incidents

```sql
SELECT
  camera_id,
  COUNT(*) as incident_count,
  MAX(risk_score) as max_risk,
  ROUND(AVG(risk_score), 2) as avg_risk,
  MAX("timestamp") as latest_incident
FROM paimon.security.violence_incidents
WHERE "timestamp" >= NOW() - INTERVAL '24' HOUR
GROUP BY camera_id
ORDER BY incident_count DESC;
```

### Cold Layer (Iceberg) — Historical trend

```sql
SELECT
  DATE_TRUNC('day', event_timestamp) as day,
  COUNT(*) as incidents,
  ROUND(AVG(risk_score), 2) as avg_risk
FROM iceberg.violence_db.violence_events_for_rag
WHERE event_timestamp >= NOW() - INTERVAL '30' DAY
GROUP BY 1
ORDER BY 1 DESC;
```

### Federated Query — Compare layers

```sql
-- Last 7 days from both layers
SELECT
  'paimon' as layer,
  COUNT(*) as count,
  ROUND(AVG(risk_score), 3) as avg_risk
FROM paimon.security.violence_incidents
WHERE "timestamp" >= NOW() - INTERVAL '7' DAY

UNION ALL

SELECT
  'iceberg' as layer,
  COUNT(*) as count,
  ROUND(AVG(risk_score), 3) as avg_risk
FROM iceberg.violence_db.violence_events_for_rag
WHERE event_timestamp >= NOW() - INTERVAL '7' DAY;
```

### Cross-layer JOIN — Top cameras

```sql
WITH paimon_stats AS (
  SELECT camera_id, COUNT(*) as recent_count
  FROM paimon.security.violence_incidents
  WHERE "timestamp" >= NOW() - INTERVAL '24' HOUR
  GROUP BY camera_id
),
iceberg_stats AS (
  SELECT camera_id, COUNT(*) as historical_count,
         ROUND(AVG(risk_score), 2) as historical_avg_risk
  FROM iceberg.violence_db.violence_events_for_rag
  WHERE event_timestamp >= NOW() - INTERVAL '90' DAY
  GROUP BY camera_id
)
SELECT
  COALESCE(p.camera_id, i.camera_id) as camera_id,
  p.recent_count,
  i.historical_count,
  i.historical_avg_risk
FROM paimon_stats p
FULL OUTER JOIN iceberg_stats i ON p.camera_id = i.camera_id
ORDER BY recent_count DESC;
```

### Time-travel (Iceberg snapshots)

```sql
-- Current state
SELECT COUNT(*) FROM iceberg.violence_db.violence_events_for_rag;

-- State at specific timestamp
SELECT COUNT(*) FROM iceberg.violence_db.violence_events_for_rag
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-04-20 14:00:00';

-- State at snapshot (if available)
SELECT COUNT(*) FROM iceberg.violence_db.violence_events_for_rag@123;
-- where 123 is snapshot-id
```

---

## Configuration Files

### Coordinator

```
config/trino/coordinator/etc/
├── config.properties       ← Main settings
├── jvm.config             ← JVM tuning
└── catalog/
    ├── iceberg.properties
    └── paimon.properties
```

### Workers

```
config/trino/worker{1,2}/etc/
├── config.properties
├── jvm.config
└── catalog/
    ├── iceberg.properties
    └── paimon.properties
```

---

## Key Settings Explained

### Memory

```properties
query.max-memory=2GB                      # Total per-cluster
query.max-memory-per-node=700MB           # Per single node
query.max-total-memory-per-node=1100MB    # With overhead

# JVM Heap: Coordinator 1200M, Worker 768M
# Must be < container limit
```

### Optimizer

```properties
optimizer.join-reordering-strategy=COST_BASED
# → Trino calculates cheapest join order automatically
# Faster for skewed data (1M + 1K rows)

optimizer.optimize-hash-generation=true
# → Generate hash efficiently
# Important for hash joins
```

### Exchange (Network)

```properties
exchange.compression-codec=LZ4
# → Compress data between tasks
# 4:1 ratio typical, LZ4 ultra-fast
# Saves network bandwidth 4x
```

### Spilling

```properties
spill-enabled=true
spiller-spill-path=/tmp/trino-spill
max-spill-per-node=4GB
# → When query > memory limit
# → Spill intermediate results to disk
# → Allows large queries to complete (slow, but better than OOM)
```

---

## Monitoring Queries

### Show query performance

```sql
-- Check ongoing queries
SHOW PROCESSLIST;

-- View query stats (if available)
SELECT * FROM system.runtime.nodes;

-- Memory usage
SELECT
  query_id,
  user,
  query,
  CAST(memory_usage_mb AS VARCHAR) || ' MB' as memory,
  CAST((CPU_TIME_MS / 1000.0) AS VARCHAR) || ' sec' as cpu_time
FROM system.runtime.tasks
WHERE query_id = 'YOUR_QUERY_ID';
```

---

## Debugging

### Enable query logging

```bash
# Check logs
docker compose -f docker/docker-compose.yml logs trino-coordinator | tail -100

# Grep for errors
docker compose -f docker/docker-compose.yml logs trino-coordinator | grep -i "error\|exception"
```

### Check catalog status

```sql
-- Verify catalog connected
SHOW CATALOGS;

-- Test Iceberg catalog
SELECT * FROM iceberg.information_schema.schemata;

-- Test Paimon catalog
SELECT * FROM paimon.information_schema.schemata;
```

### Verify statistics

```sql
-- Iceberg statistics (for CBO)
SELECT * FROM iceberg.information_schema.column_statistics;

-- Paimon table info
SELECT * FROM paimon.information_schema.tables WHERE schema_name = 'security';
```

---

## Common Errors & Fixes

### Error: "Catalog 'paimon' not found"

```bash
# Check JAR file exists
docker exec trino-coordinator ls /usr/lib/trino/plugin/paimon/

# If missing, rebuild
docker compose -f docker/docker-compose.yml build --no-cache trino-coordinator
docker compose -f docker/docker-compose.yml up -d trino-coordinator
```

### Error: "Connection refused to MinIO"

```bash
# Check MinIO is running
docker compose -f docker/docker-compose.yml ps minio

# Test connectivity from Trino
docker exec trino-coordinator curl -v http://minio:9000/minio/health/live

# If fails, check security group / firewall
```

### Error: "Table not found in Paimon"

```bash
# Ensure Paimon tables were initialized
docker exec jobmanager python /opt/flink/scripts/init_paimon_tables.py

# Verify warehouse path
docker exec trino-coordinator trino
  > DESCRIBE paimon.security.violence_incidents;
```

### Error: "Slow query (timeout)"

```sql
-- Check if spilling enabled
SHOW SESSION WHEN spill_enabled;  -- Should be true

-- Manually tune for current query
SET SESSION query_max_memory = '1GB';
SET SESSION task_concurrency = 4;

-- Then rerun query
```

### Error: "OOM: Java heap space"

```bash
# Check JVM heap configuration
docker exec trino-coordinator cat /etc/trino/jvm.config | grep Xmx

# Expected:
# Coordinator: -Xmx1200M (78% of 1536m container limit)
# Worker: -Xmx768M (75% of 1g container limit)

# If wrong, edit and restart
docker compose -f docker/docker-compose.yml restart trino-coordinator
```

---

## Hot Layer (Fluss) via Flink SQL Gateway

### Start gateway

```bash
docker compose -f docker/docker-compose.yml --profile ui up -d flink-sql-gateway
```

### Run hot queries

```bash
# Method 1: Python router (recommended)
python scripts/setup/federated_queries.py --layer hot \
  --sql "SELECT * FROM hot_violence_alerts LIMIT 10"

# Method 2: Manual Flink SQL CLI
docker exec -it jobmanager /opt/flink/bin/sql-client.sh
```

### Inside Flink SQL Client

```sql
-- Register Fluss catalog
CREATE CATALOG fluss_hot WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'fluss-coordinator:9123'
);

USE CATALOG fluss_hot;
USE `security`;

-- Query hot data
SELECT * FROM hot_violence_alerts LIMIT 5;

-- Stats
SELECT
  camera_id,
  COUNT(*) as count,
  MAX(risk_score) as max_risk
FROM hot_violence_alerts
WHERE `timestamp` > NOW() - INTERVAL '30' MINUTE
GROUP BY camera_id;
```

---

## Performance Tuning Checklist

- [ ] Metadata cache enabled (`iceberg.metadata-cache.enabled=true`)
- [ ] CBO enabled (`optimizer.join-reordering-strategy=COST_BASED`)
- [ ] Exchange compression enabled (`exchange.compression-codec=LZ4`)
- [ ] Spilling enabled (`spill-enabled=true`)
- [ ] JVM heap properly sized (Coordinator 1200M, Worker 768M)
- [ ] Task concurrency tuned (`task.concurrency=4`)
- [ ] Memory limits reasonable per data volume
- [ ] Statistics collected for Iceberg tables

---

## Useful Commands

```bash
# Rebuild Trino image (after config changes)
docker compose -f docker/docker-compose.yml build --no-cache trino-coordinator

# Restart Trino
docker compose -f docker/docker-compose.yml restart trino-coordinator

# Full cluster restart
docker compose -f docker/docker-compose.yml down
docker compose -f docker/docker-compose.yml up -d trino-coordinator

# Check resource usage
docker stats trino-coordinator

# SSH into container
docker exec -it trino-coordinator /bin/bash

# View full config
docker exec trino-coordinator cat /etc/trino/config.properties

# Test S3 connectivity
docker exec trino-coordinator \
  curl -v http://minio:9000/warehouse/paimon/

# Check Hive Metastore
docker exec trino-coordinator \
  curl -v http://hive-metastore:9083  # Thrift, not HTTP
```

---

## Port Reference

| Service | Port | Used for |
|---------|------|----------|
| Trino HTTP | 8082 | JDBC/HTTP client |
| Trino internal | 8080 | Coordinator API |
| Flink SQL Gateway | 8083 | REST API for Fluss queries |
| MinIO S3 | 9000 | Data warehouse |
| Hive Metastore | 9083 | Metadata (Thrift) |
| Fluss Coordinator | 9123 | Hot storage client |

---

## Testing Federated Queries

```bash
# Run full demo (hot + warm + cold + federated)
python scripts/setup/federated_queries.py --demo

# Single layer test
python scripts/setup/federated_queries.py --layer warm \
  --sql "SELECT COUNT(*) FROM paimon.security.violence_incidents"

python scripts/setup/federated_queries.py --layer cold \
  --sql "SELECT COUNT(*) FROM iceberg.violence_db.violence_events_for_rag"

python scripts/setup/federated_queries.py --layer hot \
  --sql "SELECT COUNT(*) FROM hot_violence_alerts"
```

---

## Notes

- **Coordinator is also a worker** (`node-scheduler.include-coordinator=true`)
- **No need for external workers** for thesis demo (data is small)
- **Scaling profile** with 2 workers is optional (showcase architecture)
- **Metadata cache is critical** for repeated queries
- **LZ4 compression** reduces network by 4x
- **CBO** automatically picks best join order

