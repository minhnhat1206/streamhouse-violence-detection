#!/bin/bash
# start-pipeline.sh: Bootstrap Streamhouse pipeline from scratch
# Run this once before the first `docker compose up`
# Usage: bash scripts/setup/start-pipeline.sh [--profile streaming] [--profile monitoring]

set -e

DOCKER_COMPOSE="docker compose -f docker/docker-compose.yml"
EXTRA_PROFILES=""

for arg in "$@"; do
    EXTRA_PROFILES="$EXTRA_PROFILES $arg"
done

echo "=== Step 1: Create Docker network ==="
docker network create violence-detection-net 2>/dev/null && echo "Network created." || echo "Network already exists, skipping."

echo ""
echo "=== Step 2: Start core services ==="
$DOCKER_COMPOSE $EXTRA_PROFILES up -d

echo ""
echo "=== Step 3: Wait for Kafka to be healthy ==="
echo "Waiting up to 60s for kafka..."
timeout 60 bash -c 'until docker compose -f docker/docker-compose.yml ps kafka | grep -q "healthy"; do sleep 3; done'
echo "Kafka is healthy."

echo ""
echo "=== Step 4: Create Kafka topics ==="
docker exec kafka bash /scripts/setup/create-topics.sh

echo ""
echo "=== Step 5: Wait for Flink to be ready ==="
echo "Waiting up to 90s for jobmanager..."
timeout 90 bash -c 'until curl -sf http://localhost:8081/overview > /dev/null 2>&1; do sleep 5; done'
echo "Flink JobManager is up."

echo ""
echo "=== Step 6: Wait for Hive Metastore ==="
echo "Waiting up to 120s for hive-metastore..."
timeout 120 bash -c 'until docker compose -f docker/docker-compose.yml ps hive-metastore | grep -q "healthy"; do sleep 5; done'
echo "Hive Metastore is healthy."

echo ""
echo "=== Step 7: Initialize storage tables ==="
echo "Initializing Paimon tables..."
docker exec jobmanager python /opt/flink/scripts/init_paimon_tables.py && echo "Paimon: OK" || echo "Paimon init failed (may already exist)"

echo "Initializing Fluss tables..."
docker exec jobmanager python /opt/flink/scripts/init_fluss_tables.py && echo "Fluss: OK" || echo "Fluss init failed (may already exist)"

echo "Initializing Iceberg tables..."
docker exec jobmanager python /opt/flink/scripts/init_iceberg_tables.py && echo "Iceberg: OK" || echo "Iceberg init failed (may already exist)"

echo ""
echo "=== Step 8: Submit Flink streaming jobs ==="
echo "Submitting data_contract_validator.py (urban-safety-alerts → hot-violence-alerts-valid / quarantine)..."
docker exec jobmanager flink run -py /opt/flink/scripts/data_contract_validator.py -d && echo "Data contract validator: submitted"

echo "Submitting sink_to_fluss.py..."
docker exec jobmanager flink run -py /opt/flink/scripts/sink_to_fluss.py -d && echo "Fluss sink: submitted"

echo "Submitting sink_to_paimon.py..."
docker exec jobmanager flink run -py /opt/flink/scripts/sink_to_paimon.py -d && echo "Paimon sink: submitted"

echo "Submitting aggregate_paimon.py..."
docker exec jobmanager flink run -py /opt/flink/scripts/aggregate_paimon.py -d && echo "Paimon aggregation: submitted"

echo ""
echo "=== Pipeline bootstrap complete ==="
echo ""
echo "Services:"
echo "  Flink Web UI       -> http://localhost:8081"
echo "  MinIO Console      -> http://localhost:9001"
echo "  Trino              -> http://localhost:8082"
echo "  Chatbot API        -> http://localhost:5002"
echo ""
echo "To start mock inference data:"
echo "  docker compose -f docker/docker-compose.yml up inference-mock"
echo ""
echo "To start RTSP streaming (optional):"
echo "  docker compose -f docker/docker-compose.yml --profile streaming up -d"
echo ""
echo "To stop inference-mock gracefully:"
echo "  docker exec inference-mock touch /app/tmp/STOP"
