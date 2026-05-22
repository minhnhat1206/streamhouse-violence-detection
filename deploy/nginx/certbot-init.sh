#!/bin/bash
# =============================================================================
# Certbot — First-time SSL Certificate Setup
# Run ONCE after nginx is up and DNS is pointing to the VM
# Usage: bash deploy/nginx/certbot-init.sh your-domain.com admin@your-domain.com
# =============================================================================
set -euo pipefail

DOMAIN="${1:?Usage: $0 <domain> <email>}"
EMAIL="${2:?Usage: $0 <domain> <email>}"

echo "──────────────────────────────────────────────────────"
echo "  Certbot Init: domain=$DOMAIN, email=$EMAIL"
echo "──────────────────────────────────────────────────────"

# Ensure certbot is installed
if ! command -v certbot &>/dev/null; then
  echo "Installing certbot..."
  sudo apt-get update -qq
  sudo apt-get install -y certbot python3-certbot-nginx
fi

# Obtain certs for all subdomains used in nginx.conf
SUBDOMAINS=(
  "api.${DOMAIN}"
  "grafana.${DOMAIN}"
  "minio.${DOMAIN}"
  "flink.${DOMAIN}"
)

DOMAIN_ARGS=""
for sub in "${SUBDOMAINS[@]}"; do
  DOMAIN_ARGS="$DOMAIN_ARGS -d $sub"
done

echo "Requesting certificates for: ${SUBDOMAINS[*]}"
sudo certbot --nginx \
  $DOMAIN_ARGS \
  --email "$EMAIL" \
  --agree-tos \
  --non-interactive \
  --redirect

echo ""
echo "✅ Certificates issued! Nginx will auto-reload."
echo ""
echo "Auto-renewal is handled by the certbot systemd timer."
echo "Verify with: sudo systemctl status certbot.timer"
echo "Test renewal: sudo certbot renew --dry-run"
