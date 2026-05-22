#!/bin/bash
# =============================================================================
# Health Check Script — Streamhouse Violence Detection
# Usage: bash deploy/scripts/health-check.sh [--full]
# Returns: 0 if all critical services healthy, 1 otherwise
# =============================================================================

API_BASE="${API_BASE:-http://localhost:5002}"
FLINK_BASE="${FLINK_BASE:-http://localhost:8081}"
MINIO_BASE="${MINIO_BASE:-http://localhost:9001}"
FULL_CHECK="${1:-}"

PASS=0; FAIL=0; WARN=0

ok()   { echo -e "  \033[1;32m✅ OK\033[0m    $1"; ((PASS++)); }
fail() { echo -e "  \033[1;31m❌ FAIL\033[0m  $1"; ((FAIL++)); }
warn() { echo -e "  \033[1;33m⚠️  WARN\033[0m  $1"; ((WARN++)); }

check_http() {
  local name=$1 url=$2 expect=${3:-200}
  status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$url" 2>/dev/null)
  if [ "$status" = "$expect" ]; then ok "$name ($url)";
  else fail "$name — HTTP $status (expected $expect)"; fi
}

check_docker() {
  local name=$1
  state=$(docker inspect --format='{{.State.Health.Status}}' "$name" 2>/dev/null || echo "not_found")
  case "$state" in
    healthy)    ok "Docker: $name" ;;
    unhealthy)  fail "Docker: $name (unhealthy)" ;;
    starting)   warn "Docker: $name (still starting)" ;;
    not_found)  fail "Docker: $name (container not found)" ;;
    *)          warn "Docker: $name (no healthcheck, state: $(docker inspect --format='{{.State.Status}}' "$name" 2>/dev/null))" ;;
  esac
}

echo "======================================================"
echo "  Streamhouse Health Check — $(date '+%Y-%m-%d %H:%M:%S')"
echo "======================================================"

echo ""
echo "── Docker Containers ──────────────────────────────"
for svc in kafka jobmanager taskmanager fluss-coordinator fluss-tablet \
           mysql hive-metastore minio chatbot trino-coordinator; do
  check_docker "$svc"
done

echo ""
echo "── API Endpoints ───────────────────────────────────"
check_http "Chatbot health" "$API_BASE/health"
check_http "Layer counts"   "$API_BASE/api/layer-counts"
check_http "Latency API"    "$API_BASE/api/latency"
check_http "Stats API"      "$API_BASE/api/stats"
check_http "Flink Overview" "$FLINK_BASE/overview"

if [ "$FULL_CHECK" = "--full" ]; then
  echo ""
  echo "── Flink Jobs (Full Check) ────────────────────────"
  jobs=$(curl -s "$FLINK_BASE/jobs" 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
running = [j for j in data.get('jobs', []) if j['status'] == 'RUNNING']
print(f'Running jobs: {len(running)}')
for j in running:
    print(f'  - {j[\"id\"][:8]}... {j[\"status\"]}')
" 2>/dev/null || echo "  Could not reach Flink REST API")
  echo "$jobs"

  echo ""
  echo "── Layer Data (Full Check) ────────────────────────"
  curl -s "$API_BASE/api/layer-counts" 2>/dev/null | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(f'  HOT  rows: {d.get(\"hot\", \"N/A\")}')
    print(f'  WARM rows: {d.get(\"warm\", \"N/A\")}')
    print(f'  COLD rows: {d.get(\"cold\", \"N/A\")}')
except: print('  Could not parse layer counts')
"
  curl -s "$API_BASE/api/latency" 2>/dev/null | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(f'  HOT  latency: {d.get(\"hot_ms\", \"N/A\")} ms')
    print(f'  WARM latency: {d.get(\"warm_s\", \"N/A\")} s')
    print(f'  COLD latency: {d.get(\"cold_s\", \"N/A\")} s')
except: print('  Could not parse latency')
"

  echo ""
  echo "── Disk Usage ─────────────────────────────────────"
  df -h / /var/lib/docker 2>/dev/null | tail -n +2 | \
    awk '{printf "  %-20s used=%-8s avail=%s\n", $6, $3, $4}'
fi

# Summary
echo ""
echo "────────────────────────────────────────────────────"
echo "  PASS: $PASS  |  WARN: $WARN  |  FAIL: $FAIL"
echo "────────────────────────────────────────────────────"

if [ "$FAIL" -gt 0 ]; then
  echo -e "  \033[1;31mStatus: DEGRADED\033[0m"
  exit 1
elif [ "$WARN" -gt 0 ]; then
  echo -e "  \033[1;33mStatus: WARNING\033[0m"
  exit 0
else
  echo -e "  \033[1;32mStatus: ALL OK\033[0m"
  exit 0
fi
