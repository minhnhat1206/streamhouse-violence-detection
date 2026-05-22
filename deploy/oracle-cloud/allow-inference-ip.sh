#!/bin/bash
# =============================================================================
# Allow VioMoViNet Inference Server to Access Kafka External Port (9093)
# Run on Oracle VM whenever the inference machine's IP changes
# Usage: bash deploy/oracle-cloud/allow-inference-ip.sh <inference-machine-ip>
# =============================================================================
set -euo pipefail

INFERENCE_IP="${1:?Usage: $0 <inference-machine-ip>}"
KAFKA_EXTERNAL_PORT=9093

echo "────────────────────────────────────────────────────"
echo "  Allowing Kafka access from: ${INFERENCE_IP}"
echo "  Port: ${KAFKA_EXTERNAL_PORT}/tcp"
echo "────────────────────────────────────────────────────"

# ── UFW rule ──────────────────────────────────────────────────────────────
if command -v ufw &>/dev/null; then
  # Remove any existing rule for this port first (clean state)
  sudo ufw delete allow "$KAFKA_EXTERNAL_PORT/tcp" 2>/dev/null || true

  # Allow only from specific IP
  sudo ufw allow from "$INFERENCE_IP" to any port "$KAFKA_EXTERNAL_PORT" proto tcp
  echo "✅ UFW: allowed ${INFERENCE_IP} → port ${KAFKA_EXTERNAL_PORT}"
else
  echo "⚠️  UFW not found — skipping UFW rule"
fi

# ── Oracle iptables (required on Oracle Cloud VMs) ────────────────────────
# Oracle Cloud's default iptables chain blocks ports even if UFW allows them
if sudo iptables -L INPUT -n | grep -q "REJECT\|DROP"; then
  sudo iptables -I INPUT -s "$INFERENCE_IP" -p tcp --dport "$KAFKA_EXTERNAL_PORT" -j ACCEPT
  echo "✅ iptables: added ACCEPT rule for ${INFERENCE_IP}:${KAFKA_EXTERNAL_PORT}"

  # Persist iptables rules
  if command -v iptables-save &>/dev/null; then
    sudo iptables-save > /etc/iptables/rules.v4 2>/dev/null || \
      sudo sh -c "iptables-save > /etc/iptables.rules"
    echo "✅ iptables rules persisted"
  fi
fi

echo ""
echo "────────────────────────────────────────────────────"
echo "  Also ensure Oracle Security List (in OCI Console)"
echo "  has an Ingress Rule:"
echo "    Source: ${INFERENCE_IP}/32"
echo "    Protocol: TCP, Dest Port: ${KAFKA_EXTERNAL_PORT}"
echo "────────────────────────────────────────────────────"
echo ""

# ── Verify Kafka external listener is up ─────────────────────────────────
if docker inspect kafka &>/dev/null; then
  if docker inspect --format='{{.State.Health.Status}}' kafka | grep -q healthy; then
    echo "✅ Kafka container: healthy"
  else
    echo "⚠️  Kafka container not healthy — check: docker logs kafka"
  fi
fi

echo "Done. VioMoViNet at ${INFERENCE_IP} can now produce to Kafka:${KAFKA_EXTERNAL_PORT}"
