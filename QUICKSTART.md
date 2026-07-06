# Quick Start — Hướng dẫn chạy dự án

## Yêu cầu tối thiểu

- Docker Desktop 4.x + WSL2 (Windows) hoặc Docker Engine (Linux/Mac)
- RAM: **12 GB** dành cho containers (16 GB máy total)
- Disk: **30 GB** trống (images ~20 GB sau build)
- CPU: 4 cores trở lên

---

## Bước 1 — Clone repo (~1 phút)

```bash
git clone --recurse-submodules https://github.com/minhnhat1206/realtime-violence-detection.git
cd realtime-violence-detection
```

## Bước 2 — Cấu hình (~2 phút)

```bash
cp docker/.env.example docker/.env
```

Mở `docker/.env` và điền Gemini API key:

```
GEMINI_API_KEY=your_key_here
```

Lấy key miễn phí tại https://aistudio.google.com/app/apikey

## Bước 3 — Tạo Docker network (~5 giây)

```bash
docker network create violence-detection-net
```

## Bước 4 — Build images (~30–45 phút lần đầu)

```bash
cd docker
docker compose build
```

> **Lần đầu tiên** phải build tất cả images từ Dockerfile (PyFlink, Chatbot, Frame-extractor...).
> Tổng ~20 GB images. Sau lần đầu, `docker compose up -d` chỉ mất vài giây.

## Bước 5 — Khởi động core stack (~3 phút)

```bash
# Core services (Kafka, Minio, Flink, Fluss, Trino, Chatbot...)
docker compose up -d
```

Kiểm tra tất cả services healthy:

```bash
docker compose ps
```

## Bước 6 — Khởi động RTSP pipeline (~1 phút)

```bash
# RTSP streaming: mediamtx + rtsp_pusher + rtsp-inference-mock
docker compose --profile streaming up -d

# Flink SQL Gateway (cần cho dim_camera seeding)
docker compose --profile ui up -d flink-sql-gateway
```

## Bước 7 — Chờ pipeline khởi tạo (~8 phút)

```bash
docker logs -f pipeline-manager
```

Chờ đến khi thấy các dòng:
```
✓ dim_camera seeded with 15 cameras via SQL Gateway.
✓ Streaming job 'Contract Validator' submitted successfully.
✓ Streaming job 'hot_violence_alerts' submitted successfully.
✓ Streaming job 'daily_incident_stats' submitted successfully.
```

> pipeline-manager tự động: khởi tạo tables (Fluss/Paimon/Iceberg), seed dim_camera, submit 3 Flink jobs

## Bước 8 — Verify dữ liệu đang chảy (~1 phút)

```bash
# Layer counts — hot sẽ > 0 sau ~2 phút
curl http://localhost:5002/api/layer-counts

# Latency check
curl http://localhost:5002/api/latency
# Expected: hot ~30ms, warm ~1000ms
```

## Bước 9 — Mở dashboard

```bash
cd Violence-Urban-Safety-UI/frontend
npm install && npm run dev
# Mở http://localhost:5173
```

## Bước 10 — Hỏi chatbot

```bash
curl -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Trong 30 phút qua có bao nhiêu vụ bạo lực?"}'
```

---

## Tổng thời gian ước tính

| Bước | Thời gian |
|------|-----------|
| Clone + Config | ~3 phút |
| Build images (lần đầu) | **30–45 phút** |
| Start stack + chờ healthy | ~3 phút |
| Start RTSP + chờ pipeline init | ~8 phút |
| **Tổng lần đầu** | **~50 phút** |
| **Tổng lần sau** (images đã build) | **~12 phút** |

---

## Services & Ports

| URL | Service |
|-----|---------|
| http://localhost:5173 | React dashboard |
| http://localhost:5002/docs | Chatbot API (Swagger) |
| http://localhost:8081 | Flink Web UI |
| http://localhost:9001 | MinIO Console (minio / mypassword) |
| http://localhost:8082 | Trino (COLD queries) |
| http://localhost:8083 | Flink SQL Gateway |

---

## Dừng hệ thống

```bash
# Graceful stop RTSP streaming
docker exec rtsp-inference-mock touch /app/tmp/STOP
docker exec rtsp_pusher touch /app/tmp/STOP

# Dừng stack, giữ dữ liệu
docker compose --profile streaming down

# Dừng stack + xóa dữ liệu (reset hoàn toàn)
docker compose --profile streaming down -v
```

---

Xem thêm chi tiết tại [README.md](README.md).
