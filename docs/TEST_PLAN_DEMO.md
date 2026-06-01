# Test Plan — Full E2E Demo Stack

> **Mục tiêu:** Xác nhận toàn bộ luồng hoạt động trước buổi bảo vệ.
> **Thời gian ước tính:** ~45 phút
> **Thực hiện bởi:** Claude Preview (browser automation) + manual verify

---

## Sơ đồ luồng cần test

```
[Local]                              [GCP VM 34.87.122.219]
  mediamtx (HLS :8888)
  rtsp_pusher → mediamtx (RTSP)
  rtsp-inference-mock ──────────────→ Kafka :9093
                                        ↓
                                      Flink (Contract Validator)
                                        ├─→ Fluss HOT
                                        └─→ Paimon WARM (tiering 30min)
                                        ↓
                                      Chatbot API :5002
                                        ↓
[Local]                             Prometheus :9090 / Grafana :3001
  npm run dev → localhost:5173
    ├─ HLS Player ← localhost:8888
    ├─ Camera Status ← GCP :5002/api/camera-status
    ├─ Layer Counts ← GCP :5002/api/layer-counts
    └─ Chatbot ← GCP :5002/chat
```

---

## Prerequisites

```bash
# Kiểm tra trước khi bắt đầu:
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"

# 1. GCP VM đang chạy
"$GCLOUD" compute instances list | grep instance-20260524

# 2. Docker local đang chạy
docker info > /dev/null && echo "Docker OK"

# 3. npm installed
cd Violence-Urban-Safety-UI/frontend && npm install --silent
```

---

## Phase 0 — Khởi động stack (5 phút)

### 0.1 GCP: Enable monitoring profile

```bash
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b \
  --strict-host-key-checking=no --command='
  cd ~/streamhouse/deploy
  docker compose -f docker-compose.gcp.yml --env-file .env.gcp \
    --profile monitoring up -d prometheus grafana node-exporter
  echo "Monitoring started"
'
```

**Kết quả kỳ vọng:** `prometheus`, `grafana`, `node-exporter` containers started.

### 0.2 Local: Start RTSP stack → GCP Kafka

```bash
# Dùng override file để rtsp-inference-mock gửi lên GCP Kafka
docker compose \
  -f docker/docker-compose.local-stream.yml \
  -f docker/docker-compose.gcp-stream.yml \
  up -d

# Chờ 15s rồi verify
sleep 15
docker logs rtsp-inference-mock --tail 5
```

**Kết quả kỳ vọng:**
```
[cam_01] VIOLENCE   | score=0.9xx | → sent to GCP Kafka
[cam_02] Normal     | score=0.0xx | → sent to GCP Kafka
```

### 0.3 Local: Start frontend

```bash
cd Violence-Urban-Safety-UI/frontend
npm run dev &
# Chờ Vite ready (3-5s)
```

---

## Phase 1 — RTSP Pipeline Verify (5 phút)

### T1.1 — HLS stream có tín hiệu

```bash
curl -s http://localhost:8888/cam_01/index.m3u8 | head -3
```

| Kết quả kỳ vọng | Pass |
|----------------|------|
| `#EXTM3U` ở dòng đầu | ✓ |

### T1.2 — Events đang vào GCP Kafka

```bash
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b \
  --strict-host-key-checking=no --command='
  docker logs pipeline-manager --tail 5 2>&1
'
```

| Kết quả kỳ vọng | Pass |
|----------------|------|
| Flink jobs RUNNING: Contract Validator ✓ | ✓ |
| Flink jobs RUNNING: hot_violence_alerts ✓ | ✓ |

### T1.3 — GCP Kafka có events mới

```bash
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b \
  --strict-host-key-checking=no --command='
  docker exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic hot-violence-alerts-valid \
    --time -1 2>/dev/null | awk -F: "{sum+=\$3} END {print \"total offset:\",sum}"
'
```

| Kết quả kỳ vọng | Pass |
|----------------|------|
| `total offset: > 0` và tăng theo thời gian | ✓ |

---

## Phase 2 — GCP Data Layers Verify (5 phút)

### T2.1 — HOT layer có data mới

```bash
curl -s http://34.87.122.219:5002/api/layer-counts
```

**Kết quả kỳ vọng:**
```json
{
  "hot": { "count": "> 0", "latency_ms": "< 500" },
  "warm": { "count": "> 0" },
  "cold": { "count": 0 }
}
```

| Check | Pass |
|-------|------|
| HOT count > 0 | ✓ |
| WARM count > 0 (Paimon 10k+ rows) | ✓ |
| COLD = 0 (data < 7 ngày — đúng theo thiết kế) | ✓ |

### T2.2 — Camera status API

```bash
curl -s http://34.87.122.219:5002/api/camera-status | python3 -m json.tool
```

**Kết quả kỳ vọng:**
```json
{
  "cameras": {
    "cam_01": "VIOLENCE_DETECTED",
    "cam_02": "NORMAL",
    ...
  },
  "window_seconds": 300
}
```

| Check | Pass |
|-------|------|
| Response trả về JSON với `cameras` key | ✓ |
| Ít nhất 1 camera có `VIOLENCE_DETECTED` (sau 2-3 phút RTSP chạy) | ✓ |

---

## Phase 3 — UI Test với Claude Preview (15 phút)

> Sử dụng Claude Preview browser tool để navigate và screenshot từng page.

### T3.1 — Mở frontend, kiểm tra Command Center (Home)

**Action:** Mở `http://localhost:5173`

**Kiểm tra:**
- [ ] LayerBadge hiển thị HOT/WARM/COLD row counts (không phải 0/0/0)
- [ ] Latency metrics hiển thị (100ms / 5.9s / 9.5s)
- [ ] Không có lỗi console đỏ

### T3.2 — Live Streams page — HLS video

**Action:** Click "Live Streams" → Settings → nhập `http://localhost:8888` → Save → quay lại Live Streams

**Kiểm tra:**
- [ ] 15 camera cards hiển thị
- [ ] Badge "HLS Active" xuất hiện
- [ ] Ít nhất 1-2 camera load được video (badge LIVE màu đỏ nhấp nháy)
- [ ] Camera có `VIOLENCE_DETECTED` → card viền đỏ

### T3.3 — Analytics page

**Action:** Click "Analytics"

**Kiểm tra:**
- [ ] Biểu đồ load dữ liệu từ chatbot API
- [ ] Không có "No data" hoặc error

### T3.4 — Chatbot — HOT layer

**Action:** Click "Chatbot" (Vigilance Terminal) → nhập câu hỏi

**Query:** `"Camera nào có cảnh báo bạo lực trong 30 phút qua?"`

**Kiểm tra:**
- [ ] Response trả về trong < 60s
- [ ] Layer = `Fluss` hoặc `hot`
- [ ] Có ít nhất 1 camera_id trong câu trả lời

### T3.5 — Chatbot — WARM layer

**Query:** `"Thống kê số vụ bạo lực theo camera trong 3 giờ qua?"`

**Kiểm tra:**
- [ ] Layer = `Paimon` hoặc `warm`
- [ ] Response có số liệu thực tế (> 0)

### T3.6 — Chatbot — COLD layer

**Query:** `"Tổng số vụ bạo lực được ghi nhận từ trước đến nay?"`

**Kiểm tra:**
- [ ] Layer = `Iceberg` hoặc `cold`
- [ ] Response thừa nhận nếu COLD = 0 (không hallucinate số)

---

## Phase 4 — Grafana + Prometheus (10 phút)

### T4.1 — Prometheus scraping thành công

**URL:** `http://34.87.122.219:9090/targets`

**Kiểm tra:**
- [ ] `flink-jobmanager` → State: UP
- [ ] `flink-taskmanager` → State: UP
- [ ] `chatbot` → State: UP
- [ ] `node-exporter` → State: UP

### T4.2 — Grafana dashboard load

**URL:** `http://34.87.122.219:3001` (admin/admin)

**Kiểm tra:**
- [ ] Dashboard Flink Overview visible
- [ ] Metric `flink_jobmanager_numRunningJobs` = 3
- [ ] CPU/Memory VM node metrics có dữ liệu

### T4.3 — Flink metrics qua Prometheus query

**URL:** `http://34.87.122.219:9090`

**Query:** `flink_taskmanager_job_task_numRecordsIn`

**Kiểm tra:**
- [ ] Metric tồn tại và có giá trị > 0
- [ ] Tăng theo thời gian (RTSP đang chạy)

---

## Phase 5 — Stop & Cleanup (2 phút)

```bash
# Stop local RTSP
docker exec rtsp-inference-mock touch /app/tmp/STOP
docker exec rtsp_pusher touch /app/tmp/STOP

# Stop local containers
docker compose \
  -f docker/docker-compose.local-stream.yml \
  -f docker/docker-compose.gcp-stream.yml \
  down

# Stop frontend (Ctrl+C terminal npm run dev)
```

> GCP VM: để tiếp tục chạy (pipeline-manager giữ Flink jobs), chỉ stop nếu xong hẳn.

---

## Tổng hợp Pass/Fail

| Phase | Test | Điều kiện Pass |
|-------|------|---------------|
| **P1** | HLS stream | `#EXTM3U` từ localhost:8888 |
| **P1** | GCP Kafka events | offset tăng sau khi RTSP chạy |
| **P2** | HOT layer count | > 0 |
| **P2** | WARM layer count | > 0 (Paimon) |
| **P2** | Camera status API | JSON hợp lệ, có VIOLENCE_DETECTED |
| **P3** | Home LayerBadge | Hiển thị số thực (không phải 0) |
| **P3** | HLS video stream | Ít nhất 1 camera LIVE |
| **P3** | Chatbot HOT | layer=Fluss, < 60s |
| **P3** | Chatbot WARM | layer=Paimon, data > 0 |
| **P3** | Chatbot COLD | Không hallucinate nếu empty |
| **P4** | Prometheus targets | flink-jobmanager UP |
| **P4** | Grafana dashboard | numRunningJobs = 3 |

**Pass tiêu chuẩn bảo vệ:** 10/12 trở lên (có thể chấp nhận COLD = 0 và Grafana thiếu dashboard custom).

---

## Known Issues & Workarounds

| Issue | Workaround |
|-------|-----------|
| Chatbot timeout > 60s (HOT cold-start) | Gửi query trước khi demo 5 phút để warm up session |
| COLD = 0 rows | Đây là đúng theo thiết kế (data < 7 ngày). Giải thích cho hội đồng |
| GCP IP thay đổi sau VM restart | Kiểm tra IP mới bằng `gcloud compute instances list` |
| Camera status NORMAL (không có VIOLENCE) | Chờ thêm 2-3 phút sau khi RTSP bắt đầu, hoặc restart rtsp-inference-mock |
| Grafana dashboard trống | Thêm panel thủ công với query `flink_jobmanager_numRunningJobs` |
