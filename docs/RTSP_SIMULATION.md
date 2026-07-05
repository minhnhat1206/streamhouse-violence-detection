# RTSP Simulation — Giả lập luồng camera CCTV

> Tài liệu mô tả cách hệ thống **giả lập nhiều luồng camera RTSP** từ video clip (không cần camera thật) để test/benchmark/demo pipeline Streamhouse.
>
- **Code:** `scripts/streaming/rtsp_pusher.py` (pusher), `scripts/prepare_cameras_dataset.py` (prep registry)
- **Compose:** `docker/docker-compose.yml`, `docker/docker-compose.local-stream.yml`, `deploy/docker-compose.gcp.yml` (profile `streaming`)

---

## 1. Tổng quan

Vì không có camera CCTV thật, hệ thống **mô phỏng camera** bằng cách: lấy các video clip bạo lực/bình thường từ dataset, **loop liên tục** và đẩy vào MediaMTX dưới dạng luồng **RTSP live** (`rtsp://mediamtx:8554/cam_NN`). Pipeline downstream xử lý y như camera thật.

**Dataset hiện tại:** **SCVD** (SmartCity CCTV Violence Detection) — CCTV thực tế, dùng làm dataset **stream/eval**.
> ⚠️ Tách biệt khỏi **RWF-2000** (đã dùng để **train MoViNet**) → tránh test-on-train leakage. Trước đây sim dùng RWF-2000; đã chuyển sang SCVD (Session 2026-06-18 #2).

---

## 2. Kiến trúc (3 service, profile `streaming`)

```
              ffmpeg -re (concat loop)                  capture + inference
  ┌──────────────────────┐   RTSP    ┌─────────────┐   ┌─────────────────────┐
  │   rtsp_pusher        │ ────────► │  mediamtx   │ ─►│ rtsp-inference-mock │ ──► Kafka
  │ (1 thread / camera)  │  :8554    │ (RTSP/HLS/  │   │ (MOCK inference)    │   urban-safety-
  │ 6 clip × 200 loop    │           │  WebRTC)    │   │                     │   alerts
  └──────────────────────┘           └─────────────┘   └─────────────────────┘
        ▲                                                        │
        │ đọc registry                              Kafka topic  ▼
  data/raw/SCVD +                                          Flink pipeline
  data/metadata/camera_registry.csv                        (Fluss/Paimon/Iceberg)
```

| Service | Vai trò | Port |
|---------|---------|------|
| **mediamtx** | RTSP server + HLS + WebRTC (relay luồng) | RTSP `8554`, HLS `8888`, WebRTC `8889` |
| **rtsp_pusher** | Đọc clip → ffmpeg → đẩy RTSP vào mediamtx (**chỉ là nguồn video, KHÔNG gọi AI**) | — |
| **rtsp-inference-mock** | Capture frame từ RTSP → inference **mock** → publish Kafka | — |

> Lưu ý: `rtsp_pusher` **chỉ push video**, không phát hiện bạo lực. Inference do `rtsp-inference-mock` (MOCK) hoặc **VioMoViNet** (producer thật, repo riêng). Xem §8.

---

## 3. Giả lập như thế nào (logic pusher)

`rtsp_pusher.py` chạy **1 thread ffmpeg mỗi camera** (`CameraPusher`), mỗi thread:

1. **Đọc registry** `data/metadata/camera_registry.csv` → danh sách camera (`cam_01`…`cam_15`) + cờ `has_violence` mỗi camera.
2. **Chọn pool clip** theo `has_violence`:
   - `has_violence=True` → pool **violence** (clip bạo lực)
   - `has_violence=False` → pool **non-violence** (clip bình thường)
3. **Sample ngẫu nhiên** `CLIPS_PER_CAM` clip (mặc định **6**) từ pool.
4. **Ghi playlist** ffmpeg concat-demuxer: 6 clip × `repeat=200` (lặp 200 vòng) → tệp temp.
5. **ffmpeg `-re`** (real-time) đọc concat playlist → encode H.264 baseline, `-preset ultrafast`, `-tune zerolatency`, `-rtsp_transport tcp` → đẩy `rtsp://mediamtx:8554/<cam_id>`.
6. Khi ffmpeg exit (hết 200 loop) → **thread tự restart** với playlist shuffle mới → **stream chạy vô tận**.
7. **Graceful stop:** `docker exec rtsp_pusher touch /app/tmp/STOP` (file bị xóa khi restart).

→ Mỗi camera là **1 luồng RTSP liên tục**, xen kẽ clip bạo lực/bình thường tùy `has_violence`.

---

## 4. Bao nhiêu luồng?

| Thông số | Giá trị | Ý nghĩa |
|----------|---------|---------|
| Camera trong registry | **15** (`cam_01`…`cam_15`) | Sinh bởi `prepare_cameras_dataset.py` (`N_CAMERAS=15`) |
| Luồng chạy cùng lúc | **5** (`MAX_CAMERAS=5`) | Giới hạn CPU — pusher chỉ lấy 5 camera đầu (hoặc theo `ACTIVE_CAMERAS`) |
| Clip mỗi camera | **6** (`CLIPS_PER_CAM=6`) | Sample ngẫu nhiên từ pool |
| Số vòng loop | **200** (`repeat=200`) | Mỗi playlist lặp 200 lần trước khi reshuffle |
| Tỷ lệ bạo lực | **~60%** (`prob_include_fight=0.6`) | Sinh ở bước prep → ~60% camera `has_violence=True` |

**→ Mặc định: 5 luồng RTSP song song**, mỗi luồng loop 6 clip × 200 lần. Tăng `MAX_CAMERAS` = thêm luồng (cẩn thận CPU — mỗi luồng = 1 tiến trình ffmpeg re-encode).

Override từng camera: đặt env `ACTIVE_CAMERAS=cam_01,cam_03,cam_07`.

---

## 5. Dataset nào

| | Trước | Hiện tại |
|---|-------|----------|
| Dataset | RWF-2000 (`.avi`, clip 5s) | **SCVD** (`.avi`/`.mp4`, clip CCTV ngắn) |
| Vai trò | train MoViNet | **stream/eval** (tách biệt train) |

**Cấu trúc SCVD** (layout Kaggle unzip ra có thể khác nhau — pusher **auto-detect**):
```
data/raw/SCVD/
├── Train/{Class A, Class B}/   ← layout publish (Class A=Violence, Class B=NonViolence)
├── Test/{Class A, Class B}/    ← pusher pool cả Train + Test
└── (hoặc violence/non_violence, hoặc 3-class Normal/Violent/Weaponized)
```

**Auto-detect** (`discover_scvd_dirs()` trong `rtsp_pusher.py`):
- Quét `SCVD_DATA_ROOT` (`/app/data/raw/SCVD`) tìm split folder `Train`/`Test`/`Val` (**case-insensitive**).
- Phân loại class folder theo **alias**: `Class A / Violence / Fight / Violent / Weaponized` → violence; `Class B / NonViolence / Normal / Safe` → non-violence.
- 3-class fallback: `Normal` → non-violence, `Violent`+`Weaponized` → violence.

**Cấu hình env** (mặc định):
```
SCVD_DATA_ROOT = /app/data/raw/SCVD
FIGHT_DIR      = /app/data/raw/SCVD/Violence        # override nếu muốn
NON_FIGHT_DIR  = /app/data/raw/SCVD/NonViolence
VIDEO_EXTENSIONS = (.avi, .mp4)
```
Nếu `FIGHT_DIR`/`NON_FIGHT_DIR` không tồn tại → pusher auto-discover dưới `SCVD_DATA_ROOT`.

---

## 6. Camera registry (`camera_registry.csv`)

Sinh bởi `prepare_cameras_dataset.py` (chạy 1 lần). Mỗi camera có:
- `camera_id` (`cam_NN`), `rtsp_url` (`rtsp://mediamtx:8554/cam_NN`)
- `has_violence` (True/False) — quyết định pool clip
- **Geo metadata** (giả lập Quận 1, TP.HCM): `city`, `district`, `ward` (15 phường), `street` (15 đường), `latitude`/`longitude` (10.77–10.78, 106.69–106.71)
- `playlist`, `total_clips` (lưu vết prep)

> Geo metadata cho phép pipeline **enrich location** + dashboard vẽ heatmap. Pusher runtime chỉ quan tâm `has_violence` (+ `camera_id`).

---

## 7. Chạy (compose nào?)

| File | Khi nào dùng |
|------|--------------|
| `docker/docker-compose.local-stream.yml` | **Demo local** — chỉ 3 service streaming (mediamtx + pusher + mock), KHÔNG cần Kafka local (mock push thẳng GCP Kafka) |
| `docker/docker-compose.yml --profile streaming` | **Full stack local** — chạy cùng core services (Kafka/Flink/Fluss/...) |
| `deploy/docker-compose.gcp.yml --profile streaming` | **GCP VM** — cần upload SCVD lên `~/streamhouse/data/raw/SCVD/` |

```bash
# Local demo (mock → GCP Kafka)
docker compose -f docker/docker-compose.local-stream.yml up -d

# Full local
docker compose -f docker/docker-compose.yml --profile streaming up -d

# Xem luồng (bất kỳ client nào)
ffplay rtsp://localhost:8554/cam_01
# hoặc HLS: http://localhost:8888/cam_01/index.m3u8

# Dừng (graceful)
docker exec rtsp_pusher touch /app/tmp/STOP
docker exec rtsp-inference-mock touch /app/tmp/STOP
```

> Đảm bảo đã có `data/raw/SCVD/` (tải dataset) + `data/metadata/camera_registry.csv` (chạy `prepare_cameras_dataset.py` nếu chưa).

---

## 8. Mock vs Real producer (QUAN TRỌNG)

- `rtsp-inference-mock` = **MOCK inference** — publish event giả (đánh dấu `mock:true`) lên topic `urban-safety-alerts`. Dùng để test pipeline **không cần GPU**.
- **Producer thật** = **VioMoViNet** (repo riêng, GPU box 2×2080Ti) — inference thật, `mock:false`.

> ⚠️ **Double-publish:** KHÔNG bật `--profile streaming` (mock) khi VioMoViNet thật đang chạy → 2 nguồn cùng publish `urban-safety-alerts`.
> `rtsp_pusher` an toàn dùng chung cho cả 2 (nó chỉ là nguồn video RTSP — cả mock lẫn VioMoViNet đều capture từ cùng RTSP stream). Chi tiết: `.claude/rules/real-producer.md`, `docs/REAL_PRODUCER_INTEGRATION_PLAN.md`.

---

## 9. Resource limits

Theo `.claude/rules/resource-limits.md` (máy 16GB):
- `rtsp_pusher`: **256m RAM / 0.5 CPU** — đủ cho ~5 ffmpeg stream (`MAX_CAMERAS=5`).
- Tăng luồng = tăng CPU (mỗi luồng 1 ffmpeg re-encode). Trên GCP e2-standard-4 (4 vCPU) cẩn thận.

## 10. Config knobs (env trong compose)

| Env | Mặc định | Ý nghĩa |
|-----|----------|---------|
| `MAX_CAMERAS` | `5` | Số luồng RTSP cùng lúc |
| `CLIPS_PER_CAM` | `6` | Số clip mỗi camera |
| `FIGHT_DIR` / `NON_FIGHT_DIR` | SCVD Violence/NonViolence | Override pool (bỏ trống → auto-discover) |
| `SCVD_DATA_ROOT` | `/app/data/raw/SCVD` | Root để auto-discover |
| `ACTIVE_CAMERAS` | (all) | `cam_01,cam_03` — chỉ push camera cụ thể |
| `STOP_FILE` | `/app/tmp/STOP` | Graceful stop |

---

## 11. Troubleshooting

| Triệu chứng | Nguyên nhân / Fix |
|-------------|-------------------|
| `[ERROR] No .avi clips found` | SCVD chưa tải, hoặc folder name không khớp alias → thêm vào `_VIOLENCE_ALIASES`/`_NON_VIOLENCE_ALIASES` trong `rtsp_pusher.py` |
| Camera không có trên `ffplay` | Kiểm tra `docker logs rtsp_pusher`; mediamtx chưa up? (`depends_on: mediamtx`) |
| CPU quá cao | Giảm `MAX_CAMERAS`; ffmpeg đã `-preset ultrafast` |
| Stream đứt/quá ngắn | Bình thường — ffmpeg restart sau 200 loop; thread tự relaunch |
| `ffprobe`/duration sai | SCVD clip ngắn nhưng pusher loop theo số (200), không tính duration → OK, không ảnh hưởng |

---
*Session 2026-06-18 #2 — chuyển RWF-2000 → SCVD. Xem `DEVELOPER_LOG.md` để biết chi tiết thay đổi.*
