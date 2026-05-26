# Demo Script — Hệ thống Phát hiện Bạo lực Thời gian thực

**Khóa luận tốt nghiệp** — Nguyễn Ngọc Minh Nhật & Nguyễn Quốc Huy  
**GCP VM:** `34.87.122.219` | Chatbot API: `http://34.87.122.219:5002`

---

## Chuẩn bị trước khi demo (5 phút trước)

```bash
# 1. Verify GCP VM đang chạy
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"
"$GCLOUD" compute instances list

# 2. Verify chatbot sống
curl -s http://34.87.122.219:5002/health

# 3. Verify 3 Flink jobs RUNNING
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b \
  --command='curl -s http://localhost:8081/jobs/overview | python3 -c "
import sys,json; jobs=json.load(sys.stdin)[\"jobs\"]
[print(j[\"name\"][:55], \"--\", j[\"state\"]) for j in jobs if j[\"state\"]==\"RUNNING\"]
"'

# 4. Start local RTSP (nếu muốn demo live camera feed)
docker compose -f docker/docker-compose.local-stream.yml up -d
```

---

## Kịch bản Demo (15–20 phút)

### Mở đầu — Giới thiệu hệ thống (2 phút)

> "Hệ thống của chúng tôi kết hợp AI phát hiện bạo lực từ camera RTSP với kiến trúc Streamhouse Trio — ba lớp lưu trữ theo thời gian thực: HOT (Fluss), WARM (Paimon), và COLD (Iceberg) — cho phép truy vấn từ mili-giây đến lịch sử năm. Toàn bộ được triển khai trên GCP và truy vấn qua chatbot AI sử dụng Google Gemini + Agentic RAG."

---

### Demo 1 — HOT Layer: Cảnh báo trong 30 phút qua (Fluss)

**Câu hỏi demo:**
> "Camera nào có cảnh báo bạo lực trong 30 phút qua?"

**Lệnh chạy:**
```bash
curl -X POST http://34.87.122.219:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Camera nào có cảnh báo bạo lực trong 30 phút qua?"}' \
  | python3 -m json.tool
```

**Expected output:**
```json
{
  "answer": "Trong 30 phút qua, các camera ghi nhận cảnh báo bạo lực: ...",
  "layer": "Fluss",
  "latency_ms": 100,
  "source_table": "hot_violence_alerts"
}
```

**Điểm nhấn khi thuyết minh:**
- Layer = **Fluss** → dữ liệu real-time, latency **100ms** ở tầng storage
- Dữ liệu đến trực tiếp từ camera RTSP qua Kafka → Flink → Fluss (không qua disk)
- Chatbot E2E ~35s (bao gồm Gemini intent + query + Gemini trả lời)

---

### Demo 2 — WARM Layer: Thống kê 3 giờ qua (Paimon + Trino)

**Câu hỏi demo:**
> "Thống kê số vụ bạo lực theo camera trong 3 giờ qua?"

**Lệnh chạy:**
```bash
curl -X POST http://34.87.122.219:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Thống kê số vụ bạo lực theo camera trong 3 giờ qua?"}' \
  | python3 -m json.tool
```

**Expected output:**
```json
{
  "answer": "Thống kê trong 3 giờ qua: cam_01 - 12 vụ, cam_03 - 8 vụ, ...",
  "layer": "Paimon",
  "latency_ms": 5900,
  "source_table": "violence_incidents"
}
```

**Điểm nhấn khi thuyết minh:**
- Layer = **Paimon** → WARM layer, ACID, LSM-tree, query qua Trino native connector
- Latency storage **5.9s** — nhanh hơn 14–23× so với Flink SQL Gateway cũ (3–5 phút)
- Dữ liệu được tiering tự động từ Fluss mỗi ~5 phút bởi pipeline-manager

---

### Demo 3 — COLD Layer: Dữ liệu lịch sử (Iceberg + Trino)

**Câu hỏi demo:**
> "Tổng số vụ bạo lực được ghi nhận từ trước đến nay?"

**Lệnh chạy:**
```bash
curl -X POST http://34.87.122.219:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Tổng số vụ bạo lực được ghi nhận từ trước đến nay?"}' \
  | python3 -m json.tool
```

**Expected output:**
```json
{
  "answer": "Tổng cộng ... vụ bạo lực được ghi nhận trong hệ thống ...",
  "layer": "Iceberg",
  "latency_ms": 9500,
  "source_table": "historical_violence_incidents"
}
```

**Điểm nhấn khi thuyết minh:**
- Layer = **Iceberg** → COLD layer, Parquet format, time-travel, lưu trữ năm
- Query qua Trino → MinIO (S3-compatible object storage)
- Latency storage **9.5s** cho dữ liệu lịch sử — phù hợp với use case phân tích

---

### Demo 4 — Routing Intelligence (tùy chọn, nếu hội đồng hỏi thêm)

Minh hoạ chatbot tự động chọn đúng layer:

| Câu hỏi | Layer được chọn | Lý do |
|---------|----------------|-------|
| "30 phút qua" | Fluss (HOT) | < 1 giờ → real-time |
| "3 giờ qua" | Paimon (WARM) | 1h–7 ngày → batch |
| "tuần trước" | Paimon (WARM) | 1h–7 ngày → batch |
| "tháng trước" | Iceberg (COLD) | > 7 ngày → historical |
| "từ trước đến nay" | Iceberg (COLD) | > 7 ngày → historical |

---

### Kết — Kiến trúc & Hiệu năng (2 phút)

**Benchmark table (đọc cho hội đồng):**

| Layer | Công nghệ | Storage Latency | Chatbot E2E |
|-------|-----------|----------------|-------------|
| HOT | Apache Fluss | **100ms** | ~35–44s |
| WARM | Apache Paimon + Trino | **5.9s** | ~35–41s |
| COLD | Apache Iceberg + Trino | **9.5s** | ~31–35s |

> "Chatbot E2E bao gồm: phân tích ý định bằng Gemini (~8s) + truy xuất ChromaDB (~1s) + thực thi query + sinh câu trả lời Gemini (~8s). Storage latency thuần đạt mục tiêu thiết kế."

---

## Câu hỏi thường gặp từ hội đồng

**Q: Tại sao không dùng một database duy nhất?**
> Một database không thể tối ưu đồng thời cho real-time ingestion (write-heavy) và historical analytics (read-heavy). Streamhouse Trio dùng công cụ chuyên biệt cho từng use case — Fluss cho write <100ms, Paimon cho ACID analytics, Iceberg cho long-term archival.

**Q: Tiering tự động hoạt động thế nào?**
> `pipeline-manager` chạy watchdog 5 phút/lần. Khi phát hiện data trong Fluss đủ ngưỡng thời gian (>1 giờ), tự trigger job `tier_fluss_to_paimon.py` qua Flink. Data cũ hơn 7 ngày được archive sang Iceberg lúc 2:00 UTC mỗi ngày.

**Q: Độ chính xác model AI?**
> Model VioMobileNet được demo qua `rtsp-inference-mock` trong môi trường GCP. Trong production, model inference chạy trên máy riêng (GPU), kết quả gửi vào Kafka qua port 9093.

**Q: Hệ thống scale thế nào?**
> Kafka partition → Flink TaskManager slot. Hiện tại: 1 TaskManager, 3 slots. Để scale: tăng số TaskManager (Flink cluster horizontal scaling) hoặc thêm Trino worker (đã có profile `scaling` trong docker-compose).

---

## Lệnh dự phòng (nếu chatbot timeout)

```bash
# Query Paimon trực tiếp qua Trino
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b \
  --command='docker exec trino-coordinator trino \
    --server localhost:8080 --catalog paimon --schema security \
    --execute "SELECT camera_id, COUNT(*) as incidents FROM violence_incidents GROUP BY camera_id ORDER BY incidents DESC LIMIT 10"'

# Query HOT layer (Fluss) count
curl -s http://34.87.122.219:5002/api/layer-counts | python3 -m json.tool

# Check latency
curl -s http://34.87.122.219:5002/api/latency | python3 -m json.tool
```

---

## P3 — HLS Live Camera Streams (nếu muốn demo video thật)

### Yêu cầu
- ngrok đã cài: https://ngrok.com/download
- Docker local đang chạy

### Các bước

```bash
# 1. Start local RTSP → GCP Kafka
docker compose \
  -f docker/docker-compose.local-stream.yml \
  -f docker/docker-compose.gcp-stream.yml \
  up -d

# 2. Verify HLS streams có tín hiệu
curl http://localhost:8888/cam_01/index.m3u8 | head -3
# Expected: #EXTM3U

# 3. Expose HLS qua ngrok
ngrok http 8888
# Copy URL dạng: https://xxxx.ngrok-free.app

# 4. Chạy frontend local (không bị mixed-content issue)
cd Violence-Urban-Safety-UI/frontend && npm run dev
# Mở http://localhost:5173 → Settings → paste ngrok URL → Save
# → Live Streams page: 15 camera cards load HLS, badge LIVE xuất hiện
```

### Lưu ý khi dùng Vercel thay vì local frontend

Vercel dùng HTTPS → không gọi được GCP HTTP API (mixed-content).
Cần expose thêm chatbot port qua ngrok:
```bash
ngrok http 5002   # Terminal khác
# Vào Vercel Project Settings → Environment Variables:
#   VITE_API_BASE_URL = https://yyyy.ngrok-free.app
# Redeploy → dùng Preview URL
```

---

## Stop sau demo

```bash
# Dừng local RTSP (nếu đã start)
docker exec rtsp-inference-mock touch /app/tmp/STOP
docker exec rtsp_pusher touch /app/tmp/STOP

# Stop GCP VM (tiết kiệm chi phí — chỉ làm SAU buổi bảo vệ)
"$GCLOUD" compute instances stop instance-20260524-104630 --zone=asia-southeast1-b
```
