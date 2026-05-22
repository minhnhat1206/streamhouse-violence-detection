#!/bin/bash
# =============================================================================
# Oracle Cloud VM Setup Script — Streamhouse Violence Detection
# Target OS: Ubuntu 22.04 ARM64 (VM.Standard.A1.Flex)
# Run as: ubuntu user (with sudo)
# Usage: curl -fsSL <raw-url>/deploy/oracle-cloud/setup.sh | bash
# =============================================================================

set -euo pipefail
REPO_URL="https://github.com/minhnhat1206/streamhouse-violence-detection.git"
APP_DIR="$HOME/streamhouse-violence-detection"
NETWORK_NAME="violence-detection-net"

log() { echo -e "\033[1;32m[SETUP]\033[0m $1"; }
warn() { echo -e "\033[1;33m[WARN]\033[0m $1"; }
err() { echo -e "\033[1;31m[ERROR]\033[0m $1"; exit 1; }

# ─── 1. System update ────────────────────────────────────────────────────────
log "Updating system packages..."
sudo apt-get update -qq && sudo apt-get upgrade -y -qq

# ─── 2. Install Docker ───────────────────────────────────────────────────────
log "Installing Docker..."
if command -v docker &>/dev/null; then
  warn "Docker already installed: $(docker --version)"
else
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"
  log "Docker installed. NOTE: Log out and back in for group changes to take effect."
fi

# Install Docker Compose plugin
sudo apt-get install -y docker-compose-plugin
log "Docker Compose: $(docker compose version)"

# ─── 3. Install utilities ────────────────────────────────────────────────────
log "Installing utilities..."
sudo apt-get install -y \
  git curl wget unzip htop \
  nginx certbot python3-certbot-nginx \
  fail2ban ufw

# ─── 4. Firewall (UFW) ───────────────────────────────────────────────────────
log "Configuring UFW firewall..."
sudo ufw --force reset
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow 22/tcp comment "SSH"
sudo ufw allow 80/tcp comment "HTTP"
sudo ufw allow 443/tcp comment "HTTPS"
# NOTE: Port 9092 (Kafka) — open only for specific IPs after setup
# sudo ufw allow from <inference-server-ip> to any port 9092
sudo ufw --force enable
log "UFW enabled. Status:"
sudo ufw status

# ─── 5. Oracle iptables fix ──────────────────────────────────────────────────
# Oracle Cloud VMs have iptables rules that block traffic even if UFW allows it
log "Fixing Oracle iptables rules..."
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 80 -j ACCEPT
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 443 -j ACCEPT
sudo netfilter-persistent save 2>/dev/null || \
  (sudo apt-get install -y iptables-persistent && sudo netfilter-persistent save)

# ─── 6. Docker network ───────────────────────────────────────────────────────
log "Creating Docker network: $NETWORK_NAME"
docker network create "$NETWORK_NAME" 2>/dev/null || \
  warn "Network $NETWORK_NAME already exists"

# ─── 7. Clone repository ─────────────────────────────────────────────────────
log "Cloning repository..."
if [ -d "$APP_DIR" ]; then
  warn "Directory exists, pulling latest..."
  git -C "$APP_DIR" pull
else
  git clone "$REPO_URL" "$APP_DIR"
fi

# ─── 8. Create .env.cloud ────────────────────────────────────────────────────
if [ ! -f "$APP_DIR/deploy/.env.cloud" ]; then
  log "Creating .env.cloud from template..."
  cp "$APP_DIR/deploy/.env.cloud.example" "$APP_DIR/deploy/.env.cloud"
  warn "⚠️  IMPORTANT: Edit $APP_DIR/deploy/.env.cloud and fill in:"
  warn "   - GEMINI_API_KEY"
  warn "   - MINIO_ROOT_PASSWORD (change from default!)"
  warn "   - METASTORE_DB_PASSWORD (change from default!)"
  warn "   - DOMAIN_NAME"
fi

# ─── 9. Nginx basic config ───────────────────────────────────────────────────
log "Setting up Nginx..."
sudo cp "$APP_DIR/deploy/nginx/nginx.conf" /etc/nginx/sites-available/vigilance
sudo ln -sf /etc/nginx/sites-available/vigilance /etc/nginx/sites-enabled/vigilance
sudo rm -f /etc/nginx/sites-enabled/default

# Test nginx config
sudo nginx -t && sudo systemctl reload nginx
log "Nginx configured and running"

# ─── 10. Fail2ban ────────────────────────────────────────────────────────────
log "Setting up fail2ban for SSH protection..."
sudo systemctl enable fail2ban
sudo systemctl start fail2ban

# ─── 11. Log rotation for Docker ─────────────────────────────────────────────
log "Configuring Docker log rotation..."
sudo mkdir -p /etc/docker
cat <<'EOF' | sudo tee /etc/docker/daemon.json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m",
    "max-file": "3"
  }
}
EOF
sudo systemctl restart docker

# ─── Done ────────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════"
log "✅ Oracle VM setup complete!"
echo "════════════════════════════════════════════════════════"
echo ""
echo "Next steps:"
echo "  1. Edit $APP_DIR/deploy/.env.cloud with your secrets"
echo "  2. Point your domain DNS to this IP: $(curl -s ifconfig.me)"
echo "  3. Run SSL setup: sudo certbot --nginx -d yourdomain.com"
echo "  4. Start the stack: cd $APP_DIR && bash deploy/scripts/start-stack.sh"
echo ""
warn "Re-login to apply Docker group: exit && ssh back in"
