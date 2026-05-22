#!/usr/bin/env bash
# shutdown_save.sh
# Chạy trước khi tắt máy: force-tier HOT→WARM và optionally WARM→COLD,
# sau đó dừng toàn bộ stack.
#
# Usage:
#   ./scripts/shutdown_save.sh           # tier HOT→WARM rồi tắt
#   ./scripts/shutdown_save.sh --cold    # tier HOT→WARM + archive WARM→COLD rồi tắt
#   ./scripts/shutdown_save.sh --dry-run # chỉ tier, không tắt stack
#
# Lý do cần script này:
#   fluss-zookeeper KHÔNG có persistent volume → HOT data mất sau khi tắt máy.
#   Paimon (WARM) và Iceberg (COLD) lưu trong MinIO → sống qua restart.
#   Chạy script này trước khi tắt để data không bị mất.

set -euo pipefail

DOCKER_COMPOSE="docker compose -f docker/docker-compose.yml"
DO_COLD=false
DRY_RUN=false

for arg in "$@"; do
  case $arg in
    --cold)     DO_COLD=true ;;
    --dry-run)  DRY_RUN=true ;;
  esac
done

echo "======================================================"
echo " Streamhouse Shutdown Save"
echo "======================================================"

# ── Step 1: Force-tier HOT → WARM ─────────────────────────────────────────────
echo ""
echo "[1/3] Force-tiering HOT → WARM (TIERING_HOURS=0)..."
echo "      (tier tất cả events hiện tại, bỏ qua điều kiện 2h)"

docker exec jobmanager bash -c \
  "export TIERING_HOURS=0 && flink run -py /opt/flink/scripts/tier_fluss_to_paimon.py" \
  2>&1 | grep -E "Tiering|Phase|cutoff|COMPLETE|PARTIAL|ERROR|INFO\]"

echo "  ✓ HOT → WARM complete"

# ── Step 2 (optional): Archive WARM → COLD ────────────────────────────────────
if [ "$DO_COLD" = true ]; then
  echo ""
  echo "[2/3] Force-archiving WARM → COLD (ARCHIVE_INTERVAL_DAYS=0)..."
  echo "      (archive tất cả WARM data, bỏ qua điều kiện 7 ngày)"

  docker exec jobmanager bash -c \
    "export ARCHIVE_INTERVAL_DAYS=0 && flink run -py /opt/flink/scripts/archive_to_iceberg.py" \
    2>&1 | grep -E "archival|Archive|SUCCESS|ERROR|INFO\]"

  echo "  ✓ WARM → COLD complete"
else
  echo ""
  echo "[2/3] Skipping WARM→COLD archive (add --cold flag to include)"
fi

# ── Step 3: Verify counts ──────────────────────────────────────────────────────
echo ""
echo "[3/3] Layer counts after save:"
python3 -c "
import urllib.request, json, time

def trino(sql):
    req = urllib.request.Request('http://localhost:8082/v1/statement', data=sql.encode(),
        headers={'X-Trino-User': 'admin', 'Content-Type': 'text/plain'})
    with urllib.request.urlopen(req) as r: d = json.load(r)
    rows = []
    for _ in range(20):
        rows += d.get('data') or []
        if not d.get('nextUri'): break
        time.sleep(0.5)
        with urllib.request.urlopen(d['nextUri']) as r: d = json.load(r)
    return (rows + (d.get('data') or []))

warm = trino('SELECT COUNT(*) FROM paimon.security.violence_incidents WHERE is_deleted=false')
cold = trino('SELECT COUNT(*) FROM iceberg.security.historical_violence_incidents')
print(f'  WARM (Paimon):  {warm[0][0] if warm else \"?\"}  rows  [MinIO-persisted, survives restart]')
print(f'  COLD (Iceberg): {cold[0][0] if cold else \"?\"}  rows  [MinIO-persisted, survives restart]')
print(f'  HOT  (Fluss):   will be EMPTY after restart (ZooKeeper no volume)')
" 2>/dev/null || echo "  (could not query — Trino/chatbot may be stopping)"

# ── Step 4: Stop stack (unless dry-run) ───────────────────────────────────────
if [ "$DRY_RUN" = false ]; then
  echo ""
  echo "Stopping stack..."
  $DOCKER_COMPOSE --profile streaming --profile ui down 2>&1 | tail -5
  echo "  ✓ Stack stopped. WARM/COLD data saved in MinIO volumes."
else
  echo ""
  echo "[DRY RUN] Stack NOT stopped. Data saved to WARM/COLD."
fi

echo ""
echo "======================================================"
echo " Done. Khi bật lại:"
echo "   docker compose -f docker/docker-compose.yml --profile streaming --profile ui up -d"
echo " → WARM/COLD có data từ phiên này, HOT tích lũy mới."
echo "======================================================"
