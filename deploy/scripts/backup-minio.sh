#!/bin/bash
# =============================================================================
# MinIO Evidence Frames Backup Script
# Backs up evidence-frames bucket to a local tar archive (rolling 7-day)
# Usage: bash deploy/scripts/backup-minio.sh
# Cron:  0 2 * * *  bash /opt/streamhouse/deploy/scripts/backup-minio.sh
# =============================================================================
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-/opt/streamhouse/backups}"
MINIO_ALIAS="${MINIO_ALIAS:-cloud}"
MINIO_URL="${MINIO_URL:-http://localhost:9000}"
MINIO_USER="${MINIO_ROOT_USER:?Set MINIO_ROOT_USER}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:?Set MINIO_ROOT_PASSWORD}"
BUCKET="${BACKUP_BUCKET:-evidence-frames}"
KEEP_DAYS=7

DATE=$(date '+%Y-%m-%d')
ARCHIVE="${BACKUP_DIR}/evidence-frames-${DATE}.tar.gz"
STAGING="${BACKUP_DIR}/staging-${DATE}"

mkdir -p "$BACKUP_DIR" "$STAGING"

echo "══════════════════════════════════════════════"
echo "  MinIO Backup — ${DATE}"
echo "  Bucket: ${BUCKET}  →  ${ARCHIVE}"
echo "══════════════════════════════════════════════"

# Configure mc alias
mc alias set "$MINIO_ALIAS" "$MINIO_URL" "$MINIO_USER" "$MINIO_PASS" --quiet

# Mirror bucket to staging dir
echo "Syncing s3://${BUCKET} → ${STAGING}..."
mc mirror "${MINIO_ALIAS}/${BUCKET}" "$STAGING" --quiet

# Compress
echo "Compressing..."
tar -czf "$ARCHIVE" -C "$BACKUP_DIR" "staging-${DATE}"
rm -rf "$STAGING"

SIZE=$(du -sh "$ARCHIVE" | cut -f1)
echo "✅ Archive created: ${ARCHIVE} (${SIZE})"

# Prune old backups
echo "Pruning backups older than ${KEEP_DAYS} days..."
find "$BACKUP_DIR" -name "evidence-frames-*.tar.gz" -mtime "+${KEEP_DAYS}" -delete

echo "Remaining backups:"
ls -lh "$BACKUP_DIR"/evidence-frames-*.tar.gz 2>/dev/null || echo "  (none)"

echo "══════════════════════════════════════════════"
echo "  Backup complete — $(date '+%Y-%m-%d %H:%M:%S')"
echo "══════════════════════════════════════════════"
