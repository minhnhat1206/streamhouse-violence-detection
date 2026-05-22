# Quick Start — 5 Minutes

Get the full Streamhouse pipeline running in 5 minutes.

## Step 1 — Clone

```bash
git clone --recurse-submodules https://github.com/minhnhat1206/realtime-violence-detection.git
cd realtime-violence-detection
```

## Step 2 — Configure

```bash
cp docker/.env.example docker/.env
```

Open `docker/.env` and set your Gemini API key:

```
GEMINI_API_KEY=your_key_here
```

Get a free key at https://aistudio.google.com/app/apikey

## Step 3 — Create Docker network

```bash
docker network create violence-detection-net
```

## Step 4 — Start the stack

```bash
cd docker
docker compose up -d
```

First run pulls images (~2-3 GB). Subsequent starts are instant.

## Step 5 — Wait for initialization (~3 minutes)

```bash
docker logs -f pipeline-manager
# Wait until you see: "All streaming jobs running"
```

The pipeline-manager automatically:
- Creates Kafka topics
- Initializes Fluss, Paimon, Iceberg tables
- Submits 3 Flink streaming jobs
- Starts generating synthetic events via `inference-mock`

## Step 6 — Verify it's working

```bash
# Check all containers are up
docker compose ps

# Check data is flowing
curl http://localhost:5002/api/layer-counts
# Expected: {"hot": N, "warm": 0, "cold": 0}  (warm fills after 30min)
```

## Step 7 — Open the dashboard

```bash
cd Violence-Urban-Safety-UI
npm install && npm run dev
# Open http://localhost:5173
```

## Step 8 — Ask the chatbot

```bash
curl -X POST http://localhost:5002/api/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Trong 30 phút qua có bao nhiêu vụ bạo lực?"}'
```

---

## What's running

| URL | Service |
|-----|---------|
| http://localhost:5173 | React dashboard |
| http://localhost:5002/docs | Chatbot API (Swagger) |
| http://localhost:8081 | Flink Web UI |
| http://localhost:9001 | MinIO Console (minio / mypassword) |
| http://localhost:8082 | Trino (for Iceberg COLD queries) |

---

## Stop everything

```bash
# Graceful stop for streaming services
docker exec inference-mock touch /app/tmp/STOP

# Stop stack (keep data)
docker compose down

# Stop stack + delete all data
docker compose down -v
```

---

For full documentation, see [README.md](README.md).
