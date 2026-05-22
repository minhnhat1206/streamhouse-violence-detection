#!/bin/bash
# =============================================================================
# Streamhouse Stack — Ordered Startup for Oracle Cloud
# Usage: bash deploy/scripts/start-stack.sh
# =============================================================================

set -euo pipefail
COMPOSE="docker compose -f $(dirname "$0")/../docker-compose.cloud.yml"
ENV_FILE="$(dirname "$0")/../.env.cloud"

log() { echo -e "\033[1;32m[START]\033[0m $1"; }
wait_healthy() {
  local service=$1
  local max_wait=${2:-120}
  log "Waiting for $service to be healthy (max ${max_wait}s)..."
  for i in $(seq 1 $max_wait); do
    status=$($COMPOSE ps "$service" --format json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Health',''))" 2>/dev/null || echo "unknown")
    if [ "$status" = "healthy" ]; then
      log "✅ $service is healthy"
      return 0
    fi
    sleep 1
  done
  echo "⚠️  $service did not become healthy in ${max_wait}s — continuing anyway"
}

# Check env file
if [ ! -f "$ENV_FILE" ]; then
  echo "❌ Missing $ENV_FILE — copy .env.cloud.example and fill in values"
  exit 1
fi

log "=== Starting Streamhouse Stack ==="

# Phase 1: Storage foundation
log "Phase 1: Starting storage services..."
$COMPOSE up -d mysql minio minio_client
wait_healthy mysql 90
wait_healthy minio 60
sleep 5

# Phase 2: Metastore
log "Phase 2: Starting Hive Metastore..."
$COMPOSE up -d hive-metastore
wait_healthy hive-metastore 120

# Phase 3: Kafka
log "Phase 3: Starting Kafka..."
$COMPOSE up -d kafka
wait_healthy kafka 90
sleep 5

# Setup Kafka topics
log "Setting up Kafka topics..."
docker exec kafka bash /scripts/setup/create-topics.sh || \
  log "⚠️  Topics may already exist"

# Phase 4: Fluss (HOT layer)
log "Phase 4: Starting Fluss (HOT layer)..."
$COMPOSE up -d fluss-zookeeper
sleep 5
$COMPOSE up -d fluss-coordinator fluss-tablet
sleep 10

# Phase 5: Flink
log "Phase 5: Starting Flink..."
$COMPOSE up -d jobmanager
wait_healthy jobmanager 90
$COMPOSE up -d taskmanager
sleep 15

# Phase 6: Flink SQL Gateway + Trino
log "Phase 6: Starting query layer..."
$COMPOSE up -d flink-sql-gateway trino-coordinator
sleep 20

# Phase 7: Pipeline Manager (submits Flink jobs)
log "Phase 7: Starting Pipeline Manager..."
$COMPOSE up -d pipeline-manager
sleep 10

# Phase 8: Chatbot API
log "Phase 8: Starting Chatbot API..."
$COMPOSE up -d chatbot
wait_healthy chatbot 120

# Phase 9: Monitoring (optional)
log "Phase 9: Starting Monitoring stack..."
$COMPOSE --profile monitoring up -d prometheus grafana node-exporter

# Final status
echo ""
log "=== Stack Status ==="
$COMPOSE ps

echo ""
log "✅ Streamhouse stack started!"
echo ""
echo "  API Health:    curl https://\$API_DOMAIN/health"
echo "  Flink UI:      http://localhost:8081"
echo "  Grafana:       http://localhost:3001"
echo "  MinIO:         http://localhost:9001"
echo "  Kafka UI:      http://localhost:18085 (if UI profile)"
