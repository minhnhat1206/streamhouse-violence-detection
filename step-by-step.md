# Streamhouse — Chạy lại sạch (Clean Rerun) với VioMoViNet thật

> Runbook thực tế cho topology **Hybrid**: RTSP mock chạy ở máy **LOCAL** → VioMoViNet (GPU) inference thật → events vào **GCP Kafka** → Fluss/Paimon/Iceberg.
> Dành cho demo / bảo vệ khóa luận. Tất cả lệnh đã được verify trên GCP (session 2026-06-21).

---

## 0. Kiến trúc & vị trí chạy

```
Máy LOCAL (datalab, Tailscale 100.94.25.122):
  mediamtx + rtsp_pusher  ──RTSP (docker bridge / Tailscale)──►
  VioMoViNet (movinet-api, GPU GTX 1650) + MinIO local (evidence)
                                        │ inference thật (KAFKA_ENABLED=true)
                                        ▼ publish Kafka
GCP VM (34.124.131.144):            Kafka :9093
                                        ▼ Flink (Contract Validator)
                                   hot-violence-alerts-valid
                                   ├─► Fluss HOT (hot_violence_alerts)  — real-time <100ms
                                   ├─► Paimon WARM (violence_incidents) — tiering mỗi 30 phút
                                   └─► Iceberg COLD (historical_...)     — archive 02:00 hằng ngày
                                        ▼
                                   Trino → Chatbot (Agentic RAG)
```

| Thành phần | Chạy ở đâu | Code mới cần push GCP? |
|-----------|-----------|----------------------|
| `rtsp_pusher`, `prepare_cameras_*`, `register_streams` | **LOCAL** (có dataset SCVD) | ❌ Không |
| VioMoViNet (`movinet-api`) + MinIO (`minio`) | **LOCAL** (datalab, repo `../VioMoViNet`) | ❌ Không |
| Kafka / Flink / Fluss / Paimon / Iceberg / Trino / Chatbot | **GCP VM** | ❌ Không (core không đổi) |

> VioMoViNet + MinIO chạy trên **chính máy datalab** này (docker compose `../VioMoViNet`).
> Container: `movinet-api` (port 8000), `minio` (9000-9001, creds `minio`/`mypassword`, bucket `inference-results`).
> GPU: GTX 1650 (4GB) — 5 streams MoViNet-a3 chạy OK.

**→ KHÔNG cần push code hay build image trên GCP.** Chỉ cần máy local sẵn sàng + GCP core đang chạy sạch.

---

## 1. Điều kiện tiên quyết (máy LOCAL — dataguy@datalab)

Chạy các lệnh check (từ repo root `streamhouse-violence-detection/`):

```bash
# Dataset SCVD (symlink phải còn dùng được)
readlink -f data/raw/SCVD && ls data/raw/SCVD      # → .../SCVD_converted/{Train,Test}

# Registry + playlist đã sinh
ls data/metadata/camera_registry.csv data/metadata/camera_playlists.json

# Tailscale IP của máy này (phải = 100.94.25.122 hoặc IP GPU box)
tailscale ip -4

# Image rtsp_pusher đã build
docker image ls docker-rtsp_pusher:latest

# Docker đang chạy
docker info >/dev/null && echo "docker OK"
```

> **Lần đầu / sau khi đổi dataset**: chạy `python scripts/prepare_cameras_context.py` để sinh lại
> `camera_registry.csv` + `camera_playlists.json` (chạy trên host, cần numpy/sklearn/cv2/ffmpeg).
> Nếu đổi image: `docker build -f docker/Dockerfile.rtsp-pusher -t docker-rtsp_pusher:latest .`

---

## 2. GCP — DỌN MOCK DATA CŨ (RESET)

> Mục đích: xóa hết data giả mà `rtsp-inference-mock` đã bơm vào Kafka/Fluss/Paimon/Iceberg,
> để khi chạy VioMoViNet thật chỉ còn data thật. **dim_camera (15 camera) được GIỮ NGUYÊN.**

### 2.1 Đảm bảo VM đang chạy

```bash
# Check trạng thái (từ local)
gcloud compute instances describe instance-20260524-104630 \
  --zone=asia-southeast1-b --format='table(name,status)'

# Nếu TERMINATED → start lại
gcloud compute instances start instance-20260524-104630 --zone=asia-southeast1-b
# chờ 1–2 phút
```

### 2.2 SSH vào VM (dùng user `user@`, KHÔNG phải dataguy)

Repo trên VM ở `/home/user/streamhouse/`. SSH vào 1 lần rồi chạy các bước 2.3–2.6:

```bash
gcloud compute ssh user@instance-20260524-104630 --zone=asia-southeast1-b
```

> Lý do phải `user@`: `gcloud ssh` mặc định login bằng username máy local (`dataguy`) →
> `~` = `/home/dataguy` (rỗng), không thấy repo ở `/home/user/streamhouse`.

### 2.3 Stop mock streaming (ngừng bơm data giả)

```bash
docker stop rtsp-inference-mock rtsp_pusher mediamtx
# Verify: không còn mock nào chạy
docker ps --format '{{.Names}}' | grep -iE 'rtsp|mediamtx' || echo "OK — no mock streaming"
```

> ⚠️ **BẮT BUỘC** dừng 3 service này khi chạy VioMoViNet thật — nếu không sẽ **double-publish**
> (mock + real cùng topic `urban-safety-alerts`). Đây là cách chạy mock thuần thay thế VioMoViNet.

### 2.4 Cancel Flink jobs (trước khi clear Kafka)

Lấy job IDs rồi cancel qua **Flink REST** (lệnh `flink cancel` CLI đang lỗi JAAS trên VM này):

```bash
# Lấy danh sách job đang chạy + ID
docker exec jobmanager flink list | grep RUNNING

# Cancel từng job (PATCH /jobs/<id> với body RỖNG — Flink version này không nhận field "mode")
for jid in <job_id_1> <job_id_2>; do
  docker exec jobmanager curl -s -o /dev/null -w "cancel $jid -> %{http_code}\n" \
    -X PATCH "http://localhost:8081/jobs/$jid"
done
# Chờ vài giây rồi verify
docker exec jobmanager flink list | grep -iE 'RUNNING|No running'
# Kỳ vọng: "No running jobs."
```

### 2.5 Clear Kafka topics (xóa data raw đầu vào)

```bash
K=/opt/kafka/bin   # image apache/kafka:4.0.1 — scripts ở /opt/kafka/bin

# Xóa 4 topic (nếu topic chưa tồn tại sẽ báo lỗi — bỏ qua)
for t in urban-safety-alerts hot-violence-alerts-valid urban-safety-quarantine hot-violence-frames-uploaded; do
  docker exec kafka $K/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $t || true
done
sleep 8   # chờ xóa async

# Tạo lại (create-topics.sh dùng --if-not-exists, partitions=3)
docker exec kafka bash /scripts/setup/create-topics.sh

# Verify: các topic tồn tại lại, rỗng
docker exec kafka $K/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -iE 'urban-safety|hot-violence'
```

### 2.6 Drop bảng Paimon + Iceberg (xóa data đã tích lũy)

```bash
# Paimon WARM (3 bảng fact/aggregation)
docker exec trino-coordinator trino --execute "DROP TABLE paimon.security.violence_incidents"
docker exec trino-coordinator trino --execute "DROP TABLE paimon.security.daily_incident_stats"
docker exec trino-coordinator trino --execute "DROP TABLE paimon.security.camera_stats"

# Iceberg COLD
docker exec trino-coordinator trino --execute "DROP TABLE iceberg.security.historical_violence_incidents"
```

> Các bảng này sẽ **tự tạo lại (rỗng)** khi pipeline chạy:
> - `violence_incidents` → `tier_fluss_to_paimon.py` (mỗi 30 phút) + `aggregate_paimon.py`
> - `daily_incident_stats`, `camera_stats` → `aggregate_paimon.py`
> - `historical_violence_incidents` → `archive_to_iceberg.py` (02:00 hằng ngày)
> - `hot_violence_alerts` (Fluss) → tự DROP+CREATE khi `sink_to_fluss_enriched` job start
> - `dim_camera` → **KHÔNG xóa** (dimension data, tự re-seed 15 camera nếu thiếu)

### 2.7 Restart pipeline-manager (resubmit jobs sạch + re-seed dim_camera)

```bash
docker restart pipeline-manager
# Chờ ~2–3 phút cho submit jobs + init catalogs/DDL + seed dim_camera
```

### 2.8 Verify GCP đã sạch

```bash
# Flink jobs chạy lại (kỳ vọng: Contract Validator + hot_violence_alerts RUNNING)
docker exec jobmanager flink list | grep RUNNING

# dim_camera còn nguyên (15 camera)
docker logs pipeline-manager 2>&1 | grep "dim_camera seeded"

# Kafka rỗng (offsets 0:0)
docker exec kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic urban-safety-alerts

# Paimon/Iceberg trống
docker exec trino-coordinator trino --execute "SELECT count(*) FROM paimon.security.violence_incidents"
```

Kỳ vọng sau reset:
- ✅ 2 Flink jobs RUNNING (Contract Validator + Fluss sink)
- ✅ `dim_camera seeded with 15 cameras`
- ✅ Kafka `urban-safety-alerts` offsets = `...:0:0` (rỗng)
- ✅ `violence_incidents` count = 0 (hoặc "table not found" cho đến khi tiering chạy)

---

## 3. LOCAL — Chạy RTSP simulation (chỉ mediamtx + rtsp_pusher)

```bash
# TỪ repo root trên máy local. KHÔNG có rtsp-inference-mock (mock = thay thế VioMoViNet → double-publish)
docker compose -f docker/docker-compose.local-stream.yml up -d mediamtx rtsp_pusher

# Verify 5 luồng RTSP live
docker logs rtsp_pusher --tail 20
# Test 1 luồng (cần ffplay/ffprobe)
ffprobe -v error -show_entries stream=codec_name,width,height rtsp://localhost:8554/cam_01
```

→ 5 luồng RTSP `rtsp://100.94.25.122:8554/cam_01..05` sẵn sàng cho VioMoViNet pull.
> Muốn đổi số camera: set `ACTIVE_CAMERAS=cam_01,cam_02,...` khi `up -d`.

---

## 4. LOCAL — VioMoViNet: bật Kafka + fix producer (BẮT BUỘC)

VioMoViNet mặc định `KAFKA_ENABLED=false` (standalone) + kafka-python 3.0.2 **không tương thích Kafka 4.x** trên GCP → phải làm 2 việc:

### 4.1 Bật Kafka khi (re)start movinet-api
```bash
cd ../VioMoViNet
KAFKA_ENABLED=true docker compose up -d --force-recreate api
# Verify: log phải hiện "KafkaEventProducer connected → 34.124.131.144:9093"
docker logs movinet-api 2>&1 | grep -i "KafkaEventProducer connected"
# Verify: KHÔNG còn "struct.error" hay "InitProducerId" hang
```

### 4.2 Fix producer cho Kafka 4.x (đã áp dụng + commit trong `../VioMoViNet`)
File `../VioMoViNet/app/kafka/producer.py` — KafkaProducer cần **3 thay đổi**:
```python
# (1) acks phải là INT, không phải string — kafka-python pack acks as int16
#     trong ProduceRequest; acks="1" → struct.error "required argument is not
#     an integer". Đây là ROOT CAUSE của lỗi publish.
acks = int(acks_raw) if isinstance(acks_raw, str) and acks_raw.lstrip("-").isdigit() else acks_raw

KafkaProducer(
    ...,
    acks=acks,                   # (1) int, KHÔNG phải string "1"
    api_version=(3, 7, 0),       # (2) tránh struct.error khi broker negotiate API version cao
    enable_idempotence=False,     # (3) tránh hang InitProducerId vs Kafka 4.0 transaction.version=2
)
```
- `../VioMoViNet/docker-compose.yml` mount `./app/kafka/producer.py:/app/app/kafka/producer.py:ro`
  để fix có hiệu lực **không cần rebuild** TF image (rebuild image sau để bake vĩnh viễn).
- **Diagnose**: `struct.error`/`KafkaTimeoutError` trong `docker logs movinet-api` = đang dùng producer
  chưa fix. Sau fix: log hiện `KafkaEventProducer connected`, KHÔNG còn struct.error.
- **Đã verify E2E** (session 2026-06-21): test acks=1 (int) → SENT_OK 3/3; acks="1" (string) → fail.

### 4.3 Connectivity
- GCP Kafka external `34.124.131.144:9093` phải reachable từ datalab:
  `timeout 5 bash -c 'cat </dev/tcp/34.124.131.144/9093 && echo REACHABLE'`
- GCP firewall: cho inbound TCP 9093 từ Tailscale IP datalab (`100.94.25.122`).

---

## 5. LOCAL — Register streams lên VioMoViNet

```bash
# Từ repo streamhouse. VioMoViNet + mediamtx đều trên datalab → default đúng.
ACTIVE_CAMERAS=cam_01,cam_02,cam_03,cam_04,cam_05 \
  VIOMOVINET_API_URL=http://100.94.25.122:8000 \
  RTSP_HOST=100.94.25.122:8554 \
  python3 scripts/streaming/register_streams.py
```

| Biến | Giá trị | Ghi chú |
|------|---------|---------|
| `VIOMOVINET_API_URL` | `http://100.94.25.122:8000` | VioMoViNet chạy trên datalab |
| `RTSP_HOST` | `100.94.25.122:8554` | mediamtx chạy trên datalab |
| `ACTIVE_CAMERAS` | `cam_01,...,cam_05` | Phải khớp luồng đang chạy ở bước 3 |

→ VioMoViNet pull RTSP → inference thật → publish events vào GCP Kafka `34.124.131.144:9093`.

**Verify streams đang chạy + có score:**
```bash
curl -s http://localhost:8000/api/stream/active | python3 -m json.tool   # 5 streams, status=running, score>0
```

---

## 6. Verify E2E (GCP nhận data thật)

```bash
# Chờ ~1–2 phút sau khi VioMoViNet bắt đầu publish. Hỏi chatbot (HOT/Fluss):
curl -X POST http://34.124.131.144:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"camera nao co canh bao trong 30 phut qua?"}'
# Kỳ vọng: "layer": "Fluss" + data thật từ VioMoViNet

# WARM (Paimon) — chỉ có sau khi tiering chạy (≤30 phút):
curl -X POST http://34.124.131.144:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"thong ke bao luc trong 3 gio qua?"}'

# COLD (Iceberg) — chỉ có sau archival 02:00:
curl -X POST http://34.124.131.144:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"du lieu bao luc thang truoc?"}'
```

---

## 7. LỆNH CHECK (tham khảo)

> Chạy trên **GCP VM** (sau khi `gcloud compute ssh user@instance-...`), trừ khi ghi chú.

### Kafka
```bash
K=/opt/kafka/bin
docker exec kafka $K/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka $K/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic urban-safety-alerts
# Offset mỗi partition (topic:partition:earliest:latest)
docker exec kafka $K/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic urban-safety-alerts
# Đọc vài message mới nhất
docker exec kafka $K/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic urban-safety-alerts \
  --from-beginning --max-messages 3 --property print.key=true
```

### Flink
```bash
docker exec jobmanager flink list                 # jobs đang chạy + ID
# Web UI: http://34.124.131.144:8081
```

### Trino (Paimon / Iceberg)
```bash
docker exec -it trino-coordinator trino           # vào shell Trino
# hoặc 1-liner:
docker exec trino-coordinator trino --execute "SHOW TABLES FROM paimon.security"
docker exec trino-coordinator trino --execute "SELECT count(*) FROM paimon.security.violence_incidents"
docker exec trino-coordinator trino --execute "SELECT count(*) FROM iceberg.security.historical_violence_incidents"
docker exec trino-coordinator trino --execute "SELECT * FROM paimon.security.violence_incidents ORDER BY \"timestamp\" DESC LIMIT 5"
```

### Fluss (HOT + dim_camera — KHÔNG có catalog Trino, query qua SQL Gateway hoặc Chatbot)
```bash
# dim_camera count (qua Chatbot RAG hoặc check log pipeline-manager)
docker logs pipeline-manager 2>&1 | grep -i "dim_camera seeded"
```

### Containers
```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'        # tất cả container đang chạy
docker logs pipeline-manager --tail 30                     # trạng thái pipeline
docker logs rtsp_pusher --tail 20                          # (trên LOCAL) luồng RTSP
```

### Tailscale (LOCAL)
```bash
tailscale ip -4                       # IP máy này
tailscale status | grep -i viomovinet # thấy GPU box chưa
```

---

## 8. DỪNG / dọn dẹp khi xong

```bash
# Trên LOCAL — stop graceful RTSP (KHÔNG kill cứng, tránh file STOP tồn đọng)
docker compose -f docker/docker-compose.local-stream.yml down

# Trên GCP — nếu muốn dừng mock/local stream đã lỡ bật: KHÔNG dùng --profile streaming khi VioMoViNet chạy

# Tiết kiệm chi phí: stop VM (data vẫn giữ)
gcloud compute instances stop instance-20260524-104630 --zone=asia-southeast1-b
```

---

## 9. Known issues / lưu ý

1. **`aggregate_paimon.py` (daily_incident_stats) fail to submit** — lỗi `NoSuchFileException /tmp/pyflink/.../aggregate_paimon.py` (pyflink staging). **Đã có từ trước** reset (job này vốn không chạy). Hệ quả: `daily_incident_stats` + `camera_stats` không tự cập nhật đến khi fix. `violence_incidents` vẫn OK qua tiering. → Cần fix riêng nếu demo cần WARM aggregation.

2. **`setup_star_schema.py` fail (non-fatal)** — cùng lỗi pyflink. Không nghiêm trọng: `dim_time`, `fact_violence_incidents` đã tồn tại từ trước; `dim_camera` được seed riêng qua SQL Gateway (thành công).

3. **Iceberg COLD chỉ cập nhật lúc 02:00 hằng ngày** — nếu cần Iceberg ngay, trigger `archive_to_iceberg.py` thủ công hoặc chờ qua đêm.

4. **Paimon WARM (violence_incidents) cập nhật mỗi 30 phút** (tiering) — sau khi VioMoViNet chạy, chờ ≤30 phút thì WARM có data.

5. **`flink cancel` CLI lỗi JAAS** trên VM này — dùng Flink REST `PATCH /jobs/<id>` (body rỗng) thay thế (đã verify HTTP 202).

6. **Trino KHÔNG có catalog Fluss** — query Fluss (`hot_violence_alerts`, `dim_camera`) qua Chatbot RAG hoặc SQL Gateway, không qua Trino.

7. **Username GCP là `user`** (không phải `dataguy`/`ubuntu`). Repo ở `/home/user/streamhouse/`.

---

## 10. Phụ lục — 15 camera + Webapp dashboard

### 10.1 Chạy đủ 15 RTSP (không phải 5 mặc định)
Mặc định `MAX_CAMERAS=5`. Để chạy 15 cần 2 thay đổi trong `docker/docker-compose.local-stream.yml` (rtsp_pusher):
1. `MAX_CAMERAS: "15"` (playlist `camera_playlists.json` đã có đủ cam_01..15).
2. **Bump resource**: `cpus: "6.00"`, `memory: 2g` (mặc định 1cpu/768m → OOM, chỉ 5-7 stream sống).

**Quan trọng — clip SCVD là `mpeg4` 720p → phải transcode H.264 bằng CPU** (rtsp_pusher không có GPU). 15 transcode 720p30 saturate box (load ~57/12 cores). Fix đã apply trong `scripts/streaming/rtsp_pusher.py`: thêm `-vf scale=-2:360 -r 15` (MoViNet infer 256px, grid nhỏ → 360p15 đủ; CPU giảm ~5x, load xuống ~25).

```bash
# Scale 15 (sau khi edit local-stream.yml + rtsp_pusher.py)
docker compose -f docker/docker-compose.local-stream.yml up -d --force-recreate rtsp_pusher
# Register 15 với VioMoViNet
CAMS=$(python3 -c "print(','.join(f'cam_{i:02d}' for i in range(1,16)))")
ACTIVE_CAMERAS="$CAMS" VIOMOVINET_API_URL=http://100.94.25.122:8000 RTSP_HOST=100.94.25.122:8554 \
  python3 scripts/streaming/register_streams.py
# Verify
curl -s http://localhost:9997/v3/paths/list | python3 -c "import sys,json;d=json.load(sys.stdin);print(len([p for p in d['items'] if p['name'].startswith('cam_')]),'streams')"
curl -s http://localhost:8000/api/stream/active | python3 -m json.tool   # 15 running
```
**Trade-off**: 15 stream → fps/camera thấp (~1.7 thay vì 9), load box cao (~25/12 cores). Muốn fps cao hơn → giảm số camera hoặc bump CPU thêm. Đã verify: 15/15 running, 6 violent detected.

### 10.2 Webapp dashboard không hiện stream — fix HLS URL
Webapp (`../Violence-Urban-Safety-UI/frontend`, vite :5173) dùng **HLS (hls.js)**, KHÔNG phải RTSP. URL = `<hls_base_url>/cam_XX/index.m3u8`. `hls_base_url` lấy từ **localStorage** (set qua Settings page).

**Root cause "không thấy stream"**: chưa set `hls_base_url` + help text sai port (8888 thay vì 18888).
**Đã fix (session 2026-06-22)**:
- `frontend/src/pages/LiveStreams.jsx`: default `DEFAULT_HLS='http://100.94.25.122:18888'` (Tailscale IP — works local + remote; host :8888 bị VS Code chiếm → remap :18888).
- `frontend/src/pages/Settings.jsx`: help text "port 8888" → "port 18888".

**Nếu browser đã lưu URL sai trước đó** (localStorage ưu tiên default), clear trong Settings hoặc console:
```js
localStorage.removeItem('hls_base_url'); location.reload();
```
**Verify**: `curl -sL http://localhost:18888/cam_01/index.m3u8` → HTTP 200 + `#EXTM3U`. Refresh browser → grid hiện 15 camera.
> admin-api (:5003, profile `admin`) KHÔNG cần chạy cho grid — frontend fallback hiển thị 15 camera hardcoded từ `constants.js`. Bật admin-api nếu cần filter `active` + điều khiển pipeline.
