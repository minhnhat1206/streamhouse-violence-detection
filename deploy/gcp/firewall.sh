#!/usr/bin/env bash
# =============================================================================
# GCP Firewall Rules — Streamhouse Ports
# Run từ máy LOCAL (Windows) sau khi gcloud auth login
# =============================================================================
# Thay GCP_PROJECT_ID bằng project ID thật từ GCP Console
GCP_PROJECT_ID="your-gcp-project-id"

echo "=== Creating GCP firewall rules for Streamhouse ==="

# Chatbot API — required for Vercel → GCP communication
gcloud compute firewall-rules create streamhouse-chatbot \
  --project="$GCP_PROJECT_ID" \
  --allow=tcp:5002 \
  --source-ranges=0.0.0.0/0 \
  --description="Chatbot API — Vercel app integration" \
  --network=default

# Kafka External — for VioMoViNet remote inference machine
gcloud compute firewall-rules create streamhouse-kafka-external \
  --project="$GCP_PROJECT_ID" \
  --allow=tcp:9093 \
  --source-ranges=0.0.0.0/0 \
  --description="Kafka external listener — VioMoViNet" \
  --network=default

# Optional dev ports (UI/monitoring — restrict to your IP for security)
# Replace YOUR_IP with your actual public IP (run: curl ifconfig.me)
YOUR_IP="0.0.0.0/0"  # Change to YOUR_IP/32 for tighter security

gcloud compute firewall-rules create streamhouse-dev-ui \
  --project="$GCP_PROJECT_ID" \
  --allow=tcp:8081,tcp:9001,tcp:8082,tcp:8083,tcp:3001,tcp:9090,tcp:5003 \
  --source-ranges="$YOUR_IP" \
  --description="Dev UIs: Flink(8081), MinIO(9001), Trino(8082), SQLGateway(8083), Grafana(3001), Prometheus(9090), Admin(5003)" \
  --network=default

# RTSP streaming (if using streaming profile)
gcloud compute firewall-rules create streamhouse-rtsp \
  --project="$GCP_PROJECT_ID" \
  --allow=tcp:8554,tcp:8888,tcp:8889,udp:8189 \
  --source-ranges=0.0.0.0/0 \
  --description="MediaMTX RTSP streaming" \
  --network=default

echo ""
echo "=== Firewall rules created! ==="
echo ""
echo "Summary:"
echo "  Port 5002 — Chatbot API (public)"
echo "  Port 9093 — Kafka External (public)"
echo "  Port 8081,9001,8082,8083,3001,9090,5003 — Dev UIs (restricted)"
echo "  Port 8554,8888,8889 — RTSP (streaming profile only)"
echo ""
echo "Test:"
echo "  curl http://136.110.16.108:5002/health"
echo "  Expected: {\"ok\": true}"
