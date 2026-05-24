#!/usr/bin/env bash
# =============================================================================
# GCP VM Setup Script — Streamhouse Violence Detection
# Run once after SSH into VM: bash setup-vm.sh
# VM: instance-20260524-104630, Ubuntu 22.04, e2-standard-4
# =============================================================================
set -euo pipefail

echo "=== [1/6] Update apt packages ==="
sudo apt-get update -y
sudo apt-get install -y curl git nano netcat-openbsd

echo "=== [2/6] Install Docker ==="
if ! command -v docker &>/dev/null; then
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"
  echo "Docker installed. NOTE: Log out and back in for group to take effect."
  echo "Or run: newgrp docker"
fi
docker --version

echo "=== [3/6] Install Docker Compose plugin ==="
if ! docker compose version &>/dev/null 2>&1; then
  sudo apt-get install -y docker-compose-plugin
fi
docker compose version

echo "=== [4/6] Create Docker network ==="
docker network create violence-detection-net 2>/dev/null || echo "Network already exists — OK"

echo "=== [5/6] Clone project (skip if already exists) ==="
if [ ! -d "$HOME/streamhouse" ]; then
  echo "Cloning project..."
  # Option A: git clone (nếu repo public hoặc đã có SSH key)
  # git clone https://github.com/YOUR_ORG/realtime-violence-detection.git ~/streamhouse
  echo "MANUAL STEP: Copy project code to ~/streamhouse/ using one of:"
  echo "  Option A (git): git clone <repo-url> ~/streamhouse"
  echo "  Option B (scp): gcloud compute scp --recurse <local-path> instance-20260524-104630:~/streamhouse --zone asia-southeast1-b"
else
  echo "~/streamhouse already exists — skipping clone"
fi

echo "=== [6/6] Setup .env.gcp ==="
if [ -d "$HOME/streamhouse/deploy" ]; then
  if [ ! -f "$HOME/streamhouse/deploy/.env.gcp" ]; then
    cp "$HOME/streamhouse/deploy/.env.gcp.example" "$HOME/streamhouse/deploy/.env.gcp"
    echo ""
    echo "⚠️  EDIT .env.gcp before running docker compose:"
    echo "    nano ~/streamhouse/deploy/.env.gcp"
    echo "    → Set GEMINI_API_KEY=<your-key>"
    echo "    → Verify GCP_VM_EXTERNAL_IP=136.110.16.108"
    echo ""
  else
    echo ".env.gcp already exists — skipping"
  fi
fi

echo ""
echo "=== Setup complete! ==="
echo ""
echo "NEXT STEPS:"
echo "  1. Copy project code to ~/streamhouse/ (if not done):"
echo "     gcloud compute scp --recurse <local-path> instance-20260524-104630:~/streamhouse --zone asia-southeast1-b"
echo ""
echo "  2. Edit .env.gcp:"
echo "     nano ~/streamhouse/deploy/.env.gcp"
echo ""
echo "  3. Start core stack (from ~/streamhouse/deploy/):"
echo "     cd ~/streamhouse/deploy"
echo "     docker compose -f docker-compose.gcp.yml --env-file .env.gcp up -d"
echo ""
echo "  4. Check logs:"
echo "     docker compose -f docker-compose.gcp.yml logs -f chatbot"
echo ""
echo "  5. Test API:"
echo "     curl http://localhost:5002/health"
echo ""
