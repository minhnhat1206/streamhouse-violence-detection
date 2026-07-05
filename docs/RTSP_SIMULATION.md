# RTSP Simulation — Giả lập luồng camera CCTV

> Tài liệu mô tả cách hệ thống **giả lập nhiều luồng camera RTSP** từ video clip (không cần camera thật) để test/benchmark/demo pipeline Streamhouse.
>
- **Code:** `scripts/streaming/rtsp_pusher.py` (pusher), `scripts/prepare_rtsp_scenarios.py` (5-stream scenario metadata), `scripts/prepare_cameras_dataset.py` / `scripts/prepare_cameras_context.py` (legacy prep)
- **Compose:** `docker/docker-compose.scvd-scenarios.yml` (current 5-stream benchmark), `docker/docker-compose.local-stream.yml`, `docker/docker-compose.yml`, `deploy/docker-compose.gcp.yml`

---

## 1. Tổng quan

Vì không có camera CCTV thật, hệ thống **mô phỏng camera** bằng cách: lấy các video clip bạo lực/bình thường từ dataset, ghép thành timeline deterministic, **loop liên tục** và đẩy vào MediaMTX dưới dạng luồng **RTSP live** (`rtsp://mediamtx:8554/cam_NN`). Pipeline downstream xử lý y như camera thật.

**Dataset hiện tại:** **SCVD** (SmartCity CCTV Violence Detection) — CCTV thực tế, dùng làm dataset **stream/eval**.
> ⚠️ Tách biệt khỏi **RWF-2000** (đã dùng để **train MoViNet**) → tránh test-on-train leakage. Trước đây sim dùng RWF-2000; đã chuyển sang SCVD (Session 2026-06-18 #2).

**Runtime hiện tại:** deterministic 5-stream scenario benchmark:

| Camera | Scenario | Mục đích |
|--------|----------|----------|
| `cam_01` | RTSP-01 baseline | baseline accuracy |
| `cam_02` | RTSP-02 crowd | crowded/occluded scenes |
| `cam_03` | RTSP-03 difficult_conditions | lighting/distance/shake |
| `cam_04` | RTSP-04 hard_negative | false-alarm control |
| `cam_05` | RTSP-05 peak_frequency | dense alert stress test |

Mỗi camera có playlist ordered và annotation CSV riêng để đánh giá model theo timeline.

---

## 2. Kiến trúc hiện tại

```
              ffmpeg -re (concat loop)                  capture + inference
  ┌──────────────────────┐   RTSP    ┌─────────────┐   ┌─────────────────────┐
  │   rtsp_pusher        │ ────────► │  mediamtx   │ ─►│ rtsp-inference-mock │ ──► Kafka
  │ (1 thread / camera)  │  :8554    │ (RTSP/HLS/  │   │ (MOCK inference)    │   urban-safety-
  │ scenario playlists   │           │  WebRTC)    │   │                     │   alerts
  └──────────────────────┘           └─────────────┘   └─────────────────────┘
        ▲                                                        │
        │ đọc registry + playlists + scenarios     Kafka topic  ▼
  data/raw/SCVD + data/metadata/*                         Flink pipeline
                                                          (Fluss/Paimon/Iceberg)
```

| Service | Vai trò | Port |
|---------|---------|------|
| **mediamtx** | RTSP server + HLS + WebRTC (relay luồng) | RTSP `8554`, HLS `8888`, WebRTC `8889` |
| **rtsp_pusher** | Đọc clip → ffmpeg → đẩy RTSP vào mediamtx (**chỉ là nguồn video, KHÔNG gọi AI**) | — |
| **rtsp-inference-mock** | Capture frame từ RTSP → inference **mock** → publish Kafka | — |

> Lưu ý: `rtsp_pusher` **chỉ push video**, không phát hiện bạo lực. Inference do `rtsp-inference-mock` (MOCK) hoặc **VioMoViNet** (producer thật, repo riêng). Xem §8.

---

## 3. Giả lập như thế nào

`rtsp_pusher.py` chạy **1 thread ffmpeg mỗi camera** (`CameraPusher`), mỗi thread:

1. **Đọc registry** `data/metadata/camera_registry.csv` → đúng 5 camera scenario (`cam_01`…`cam_05`).
2. **Đọc ordered playlist** `data/metadata/camera_playlists.json` → danh sách clip theo timeline đã build sẵn.
3. **Đọc scenario metadata** `data/metadata/camera_scenarios.json` → tên scenario, số event, annotation/schedule path.
4. **Ghi playlist** ffmpeg concat-demuxer với `PLAYLIST_REPEAT=1` cho scenario mode.
5. **ffmpeg `-re`** (real-time) đọc concat playlist → encode H.264 baseline, `-preset ultrafast`, `-tune zerolatency`, `-rtsp_transport tcp` → đẩy `rtsp://mediamtx:8554/<cam_id>`.
6. Khi ffmpeg exit → thread tự restart cùng ordered playlist → stream chạy vô tận nhưng timeline vẫn deterministic.
7. **Graceful stop:** `docker exec rtsp_pusher touch /app/tmp/STOP` hoặc `touch /tmp/STOP` khi chạy bare-metal trên Vast.ai.

Pusher vẫn giữ fallback legacy: nếu thiếu `camera_playlists.json`, nó auto-discover pool SCVD và sample ngẫu nhiên như flow cũ. Runtime hiện tại không dùng fallback đó.

---

## 4. Bao nhiêu luồng?

| Thông số | Giá trị | Ý nghĩa |
|----------|---------|---------|
| Camera trong registry | **5** (`cam_01`…`cam_05`) | Sinh bởi `prepare_rtsp_scenarios.py` |
| Luồng source chạy cùng lúc | **5** (`MAX_CAMERAS=5`) | `ACTIVE_CAMERAS=cam_01,cam_02,cam_03,cam_04,cam_05` |
| Clip mỗi camera | **192-268** | Theo scenario schedule, không sample random |
| Số vòng repeat | **1** (`PLAYLIST_REPEAT=1`) | Mỗi scenario playlist chạy một lần rồi ffmpeg restart |
| Annotation events | **17 total** | 3 + 3 + 3 + 1 + 7 events |

**→ Mặc định: 5 luồng RTSP source song song**, cộng thêm 5 luồng `*_result` khi VioMoViNet visualizer chạy.

Override từng camera: đặt env `ACTIVE_CAMERAS=cam_01,cam_03,cam_07`.

---

## 5. Dataset nào

| | Trước | Hiện tại |
|---|-------|----------|
| Dataset | RWF-2000 (`.avi`, clip 5s) | **SCVD** (`.avi`/`.mp4`, clip CCTV ngắn) |
| Vai trò | train MoViNet | **stream/eval** (tách biệt train) |

**Cấu trúc project-local hiện tại:**

```
data/raw/SCVD/
├── SCVD_converted/
│   ├── Train/Normal/
│   ├── Train/Violence/
│   ├── Train/Weaponized/
│   └── Test/...
├── labels/
├── rtsp_scenarios/
├── scripts/
├── requirements.txt
└── README_rtsp_fiftyone.md
```

**Legacy Kaggle layout** vẫn được pusher auto-detect nếu dùng fallback:

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

Sinh bởi `prepare_rtsp_scenarios.py` cho runtime hiện tại. Mỗi camera có:
- `camera_id` (`cam_NN`), `rtsp_url` (`rtsp://mediamtx:8554/cam_NN`)
- `has_violence` (True/False)
- `scenario_id`, `scenario_name`, `difficulty`, `frequency`, `purpose`
- `n_events`, `duration_seconds`, `annotation_file`, `schedule_file`
- **Geo metadata** (giả lập Quận 1, TP.HCM): `city`, `district`, `ward` (15 phường), `street` (15 đường), `latitude`/`longitude` (10.77–10.78, 106.69–106.71)

> Geo metadata cho phép pipeline **enrich location** + dashboard vẽ heatmap. Pusher runtime chủ yếu dùng `camera_id`; ordered clip list nằm trong `camera_playlists.json`.

---

## 7. Chạy (compose nào?)

| File | Khi nào dùng |
|------|--------------|
| `docker/docker-compose.scvd-scenarios.yml` | **Current SCVD benchmark** — 5 deterministic scenario streams + mock publisher |
| `docker/docker-compose.local-stream.yml` | Legacy local streaming — mediamtx + pusher + mock |
| `docker/docker-compose.yml --profile streaming` | **Full stack local** — chạy cùng core services (Kafka/Flink/Fluss/...) |
| `deploy/docker-compose.gcp.yml --profile streaming` | **GCP VM** — cần upload SCVD lên `~/streamhouse/data/raw/SCVD/` |

```bash
# Current SCVD 5-stream benchmark
docker compose -f docker/docker-compose.scvd-scenarios.yml up -d --build

# Full local
docker compose -f docker/docker-compose.yml --profile streaming up -d

# Xem luồng (bất kỳ client nào)
ffplay rtsp://localhost:8554/cam_01
# hoặc HLS: http://localhost:8888/cam_01/index.m3u8

# Dừng (graceful)
docker exec rtsp_pusher touch /app/tmp/STOP
docker exec rtsp-inference-mock touch /app/tmp/STOP
```

> Đảm bảo đã có `data/raw/SCVD/` + `data/metadata/camera_registry.csv`/`camera_playlists.json`/`camera_scenarios.json` (chạy `prepare_rtsp_scenarios.py` nếu chưa).

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
| `ACTIVE_CAMERAS` | `cam_01..cam_05` trong scenario compose | Chỉ push camera cụ thể |
| `CAMERA_PLAYLISTS_FILE` | `/app/data/metadata/camera_playlists.json` | Ordered per-camera clip playlists |
| `SCENARIO_METADATA_FILE` | `/app/data/metadata/camera_scenarios.json` | Scenario metadata + annotation/schedule paths |
| `PLAYLIST_REPEAT` | `1` trong scenario compose, `200` legacy default | Số lần lặp concat playlist |
| `SCVD_DATA_ROOT` | `/app/data/raw/SCVD/SCVD_converted` trong scenario compose | Root để auto-discover fallback |
| `FIGHT_DIR` / `NON_FIGHT_DIR` | SCVD Violence/NonViolence | Legacy fallback override |
| `CLIPS_PER_CAM` | `6` | Legacy fallback random sample size |
| `STOP_FILE` | `/app/tmp/STOP` | Graceful stop |

---

## 11. Troubleshooting

| Triệu chứng | Nguyên nhân / Fix |
|-------------|-------------------|
| `[ERROR] No .avi clips found` | SCVD chưa tải, hoặc folder name không khớp alias → thêm vào `_VIOLENCE_ALIASES`/`_NON_VIOLENCE_ALIASES` trong `rtsp_pusher.py` |
| `camera_playlists.json` load được nhưng camera có `0 valid clips` | Playlist path không khớp runtime mount. Regenerate bằng `prepare_rtsp_scenarios.py` với đúng `--container-scvd-root` |
| Camera không có trên `ffplay` | Kiểm tra `docker logs rtsp_pusher`; mediamtx chưa up? (`depends_on: mediamtx`) |
| CPU quá cao | Giảm `MAX_CAMERAS`; ffmpeg đã `-preset ultrafast` |
| Stream đứt sau một scenario loop | Bình thường — ffmpeg restart sau playlist, thread tự relaunch |
| Pusher dừng ngay sau start | Có stale stop file: xóa `/app/tmp/STOP` trong container hoặc `/tmp/STOP` khi chạy bare-metal |
| MediaMTX chỉ thấy `cam_01..cam_05` nhưng không thấy `*_result` | Source RTSP đã chạy; cần start VioMoViNet visualizer để publish result stream |

---
*Updated 2026-07-06 — deterministic SCVD 5-stream scenario runtime. Xem `docs/NEW_SCVD_RTSP_SCENARIO_PLAN.md` và `DEVELOPER_LOG.md` để biết chi tiết thay đổi.*
