# 🤖 Joint Agent Mission Control

## 🎯 Current Global Objective
Xây dựng hệ thống phát hiện bạo lực thời gian thực (**Streamhouse Trio** — Fluss/Paimon/Iceberg).
Đây là **Khóa luận tốt nghiệp** của Nguyễn Ngọc Minh Nhật & Nguyễn Quốc Huy.

## 📋 Project Context
> **BẮT BUỘC ĐỌC** trước khi bắt đầu: `docs/PROJECT_CONTEXT.md`
> Chứa toàn bộ trạng thái services, ports, tiến độ, phân công, và Docker commands.

## 🤝 Handover Protocol
*Mỗi khi kết thúc phiên làm việc, Agent PHẢI cập nhật phần "Last State" bên dưới.*

---

### 🗺️ Last State — Session 2026-06-21 (GCP clean reset + VioMoViNet runbook) ✅ DONE

**Agent:** Claude
**GCP/Services:** VM RUNNING. Mock streaming STOPPED. Data mock ĐÃ XÓA. Core pipeline chạy sạch.

#### Mục đích phiên
User muốn chạy lại hệ thống sạch với **VioMoViNet thật** (topology Hybrid: RTSP mock local → VioMoViNet GPU → GCP Kafka). Dọn mock data `rtsp-inference-mock` đã tích lũy, viết runbook.

#### Đã làm (verify trên GCP, IP 34.124.131.144)
- **Stop mock streaming**: rtsp-inference-mock, rtsp_pusher, mediamtx (double-publish risk khi VioMoViNet chạy).
- **Cancel 2 Flink jobs** qua REST `PATCH /jobs/<id>` body rỗng (HTTP 202). `flink cancel` CLI lỗi JAAS.
- **Clear + recreate 4 Kafka topics** (`/opt/kafka/bin`, image `apache/kafka:4.0.1`; `create-topics.sh` partitions=3).
- **DROP Paimon** (`violence_incidents` = **366,135 rows mock** + `daily_incident_stats` + `camera_stats`) + **Iceberg** `historical_violence_incidents`. `dim_time`/`fact_violence_incidents` GIỮ.
- **Restart pipeline-manager** → Contract Validator + Fluss sink RUNNING; `dim_camera` re-seed 15 cameras qua SQL Gateway.

#### Runbook
`step-by-step.md` — full clean-rerun (reset GCP → local RTSP → register VioMoViNet → verify E2E) + section **"lệnh check"**.

#### ⚠️ Known issues (cho session kế)
- **`aggregate_paimon.py` fail to submit**: `NoSuchFileException /tmp/pyflink/.../aggregate_paimon.py` (pyflink staging). **PRE-EXISTING** — job vốn không chạy từ trước (KHÔNG do reset). → `daily_incident_stats` + `camera_stats` không tự update. `violence_incidents` OK qua tiering (30ph). **Cần fix nếu demo cần WARM aggregation.**
- `setup_star_schema.py` cũng fail (non-fatal, cùng lỗi pyflink). `dim_camera` seed riêng OK.
- Iceberg COLD chỉ update 02:00 hằng ngày (`archive_to_iceberg` schedule).

#### GCP gotchas (ghi nhớ)
- Username VM = **`user`** (KHÔNG phải dataguy). Repo `/home/user/streamhouse/`. `gcloud ssh` phải dùng `user@instance-...`.
- Trino catalogs: `iceberg`, `paimon` (KHÔNG có `fluss`). Query Fluss qua Chatbot/SQL Gateway.
- jobmanager KHÔNG mount scripts (baked trong image); pipeline-manager là client submit qua `flink run -py`.

#### Chưa làm (defer)
- Chạy local RTSP + register VioMoViNet (chờ user confirm `VIOMOVINET_API_URL` / IP GPU box).
- Fix `aggregate_paimon` pyflink staging.
- Verify GCP firewall TCP 9093 từ GPU box.

---

### 🗺️ Last State — Session 2026-06-18 #3 (RTSP sim: context-continuous + full dataset coverage) ✅ IMPLEMENTED & VERIFIED E2E

**Agent:** Claude (local code — chưa commit, chưa deploy GCP)
**GCP/Services:** KHÔNG thay đổi.

#### Mục đích phiên
RTSP simulator cũ pick clip **ngẫu nhiên + shuffle** mỗi restart → 1 camera nhảy giữa các cảnh không liên quan (giả). User yêu cầu: mỗi luồng RTSP = **1 bối cảnh cố định (chung context)**, **timeline liền mạch + dài**, dùng **hết dataset** (cả normal lẫn violence, đa dạng), vẫn **realistic**.

#### Root cause phát hiện
- `rtsp_pusher.py` **bỏ qua** trường `playlist` của registry → tự `random.sample` + `shuffle` lại → effort của `prepare_cameras_dataset.py` vô dụng.
- Registry cũ **randomize lat/lon** → trượt khỏi `dim_camera` (seed cố định) — bug tiềm ẩn.
- `prepare_cameras_dataset.py` chỉ dùng một phần dataset (random sample 2-4 clip/cam).

#### Files thay đổi
| File | Thay đổi |
|------|---------|
| `scripts/prepare_cameras_context.py` | **MỚI** — clustering + playlist builder. ffmpeg/cv2 rút 1 frame/clip → HSV histogram 130-d → KMeans 15 cluster (1 cluster = 1 camera/bối cảnh) → nearest-neighbor order → `build_playlist` full-coverage. Chạy trên host (zero-install: numpy/sklearn/cv2/ffmpeg). |
| `scripts/streaming/rtsp_pusher.py` | Thêm `load_context_playlists()` + `CAMERA_PLAYLISTS_FILE`; **bỏ shuffle** (ordered, deterministic); context-mode clip selection (fallback random nếu thiếu JSON). Backward-compatible. |
| `docker/docker-compose.local-stream.yml` | Thêm env `CAMERA_PLAYLISTS_FILE`; remap HLS `8888→18888` (VS Code chiếm host :8888). |
| `data/raw/SCVD` | **Symlink** → `../MSA-MoViNet/data/SCVD/SCVD_converted` (absolute; docker follow). |

#### Quyết định thiết kế
- **HSV color histogram + KMeans** (user chọn) — zero-install, ~15-30s. Mỗi cluster = 1 camera location cố định → "chung context".
- **build_playlist full-coverage** (refinement sau khi user hỏi "dùng hết data?"): ALL normal clips lặp `r` lần làm phông nền dài + ALL violence clips rải đều → **dùng 100% dataset** + density ≈ `--target-density` (default 0.12) + timeline dài (25-227 clip/cam ≈ 20 phút). `r = ceil(n_v·(1/d−1)/n_n)`, cap 15.
- **Geo FIXED** mirror `setup_star_schema.py:_seed_dim_camera` (15 tuple) → registry & Fluss dim_camera đồng bộ (fix bug randomize lat/lon).
- Pusher image (`python:3.10-slim`+ffmpeg) không có numpy/sklearn/cv2 → clustering chạy **trên host 1 lần**, không trong container.

#### Verify (tất cả PASS — E2E)
- ✅ Coverage: **481/481 clip dùng (100%)** — normal 246/246 + violence 235/235.
- ✅ Density 11.9% ≈ target 0.12; playlist 25-227 clip/cam.
- ✅ Cluster coherence (visual): cam_01 = 1 cảnh nhà/quán trong nhà nhất quán; cam_12 = cảnh công nghiệp, violence xảy ra **ngay trong cảnh đó** (không phải clip violence nhảy vào từ cảnh khác).
- ✅ 5 luồng RTSP live: `ffprobe` h264/1280×720/30fps, grab frame OK mỗi cam.
- ✅ 5 camera = 5 cảnh khác nhau (quán ăn / cửa hàng / dân cư / đường phố / ngoài trời).

#### ⚠️ Notes cho session/agent kế
- **Dataset SCVD** nằm ở **sibling repo** `../MSA-MoViNet/data/SCVD/SCVD_converted`, symlink vào `data/raw/SCVD` (KHÔNG copy). 481 clip (246 Normal + 111 Violence + 124 Weaponized).
- **Port conflict:** host `:8888` bị VS Code (`code`) chiếm → mediamtx HLS remap `18888:8888` trong local-stream.yml. RTSP `:8554` unaffected. **Đừng "fix" lại 8888.**
- **Reload playlist:** sau khi rerun prep, pusher phải `--force-recreate` mới đọc `camera_playlists.json` mới (`up -d` thường không recreate).
- **Build image:** `docker build -f docker/Dockerfile.rtsp-pusher -t docker-rtsp_pusher:latest .` (image local, không có trên registry).
- Chỉ **5/15 camera** chạy (`MAX_CAMERAS=5`, CPU). Cam_06-15 sẵn sàng trong playlist.
- **0 "calm" camera** — dataset 49% violence → KMeans trộn violence vào mọi cluster. Muốn calm → thêm normal-only cluster option.
- Memory đã update: `rtsp-context-clustering`, `scvd-dataset-location`, `mediamtx-hls-port-conflict`.

#### Chưa làm (defer)
- **Commit** local changes (script mới + sửa pusher + compose + symlink) — đang trên branch `devHuy`.
- **Test downstream** mock→Kafka (chưa chạy `rtsp-inference-mock`).
- **Update docs/RTSP_SIMULATION.md** (vẫn mô tả random sampling cũ).
- **Cameras "calm"** option nếu cần.

---

### 🗺️ Last State — Session 2026-06-18 #2 (RTSP sim: RWF-2000 → SCVD) 🔧 IMPLEMENTED

**Agent:** Claude (local code — chưa deploy, chưa đụng GCP/services)
**GCP/Services:** KHÔNG thay đổi. Toàn bộ thay đổi ở code local repo `streamhouse-violence-detection`.

#### Mục đích phiên
Chuyển **RTSP simulator** từ dataset **RWF-2000 → SCVD** (SmartCity CCTV Violence Detection). Lý do: RWF-2000 đã dùng để **train MoViNet** → streaming RWF-2000 = test-on-train leakage. SCVD (CCTV thực tế) làm dataset **stream/eval** riêng, tách biệt.

#### ⚠️ Lỗi scope đã sửa
Claude ban đầu sửa **nhầm repo legacy** `Smart-Security-Monitoring-System/` (có `simulateRTSP.py` với timeline risk-level). User đính chính repo active là `streamhouse-violence-detection/`. Đã **`git restore`** 3 file ở repo legacy → về trạng thái RWF-2000 ban đầu (sạch).

#### Files thay đổi (streamhouse — 5 file)
| File | Thay đổi |
|------|---------|
| `scripts/streaming/rtsp_pusher.py` | Defaults `FIGHT_DIR`/`NON_FIGHT_DIR` → `/app/data/raw/SCVD/{Violence,NonViolence}`; thêm `SCVD_DATA_ROOT` + `VIDEO_EXTENSIONS=(.avi,.mp4)`; thêm `discover_scvd_dirs()` (auto-detect class folders, **case-insensitive** split match Train/train, pool Train+Test, handle Class A/B + violence/non_violence + 3-class Normal/Violent/Weaponized); `load_clips` → recursive walk + both exts; thêm `load_clips_multi`; `main()` auto-discover khi configured dirs thiếu |
| `scripts/prepare_cameras_dataset.py` | `RAW_ROOT=./data/raw/SCVD`; `rglob` `.avi`+`.mp4`; `_classify_clip()` theo parent folder; `has_violence = any(c in fight_set ...)` |
| `docker/docker-compose.yml` | `rtsp_pusher`: env FIGHT_DIR/NON_FIGHT_DIR → SCVD + mount `../data/raw/SCVD:/app/data/raw/SCVD:ro` |
| `docker/docker-compose.local-stream.yml` | như trên |
| `deploy/docker-compose.gcp.yml` | mount SCVD + header/section comment `RWF-2000→SCVD` (gcp dùng script defaults, không set FIGHT_DIR env) |

#### Quyết định thiết kế
- **Repoint tối thiểu** (user chọn): GIỮ NGUYÊN thiết kế loop đơn giản của streamhouse (`CLIPS_PER_CAM=6` clips × `repeat=200` mỗi camera). KHÔNG port timeline thực tế (risk-level + safe-gap + violence injection 60-120s) — logic đó chỉ thuộc repo legacy.
- Pusher dùng **stdlib only** (csv/os/random/subprocess/sys/tempfile/threading/time) → portable, không cần extra dep.

#### 🐛 Bug bắt + fix khi verify
`discover_scvd_dirs` match split theo `_SCVD_SPLITS=("train",...)` **lowercase**, nhưng SCVD publish là **`Train`/`Test` viết hoa** → case-sensitive trên Linux → trả empty lists. Fix: match case-insensitive qua `_norm_name()`. (Loại bug chỉ phát hiện khi chạy unit test.)

#### Verify (tất cả PASS)
- ✅ `py_compile` cả 2 file Python
- ✅ RWF-2000 leftover: chỉ còn 2 **comment giải thích** cố ý (giải thích lý do tách SCVD) — không còn ref functional
- ✅ 6 unit test `discover_scvd_dirs`/`load_clips`: `{Train,Test}/{Class A,B}` (publish layout), flat `violence/non_violence`, 3-class fallback, recursive load `.mp4+.avi`, pooling, missing-root clean fail

#### ⚠️ Notes cho session/agent kế
- **BLOCKER: SCVD chưa tải.** Trước khi `--profile streaming` chạy được, phải download:
  ```bash
  kaggle datasets download -d toluwaniaremu/smartcity-cctv-violence-detection-dataset-scvd \
    -p data/raw/SCVD --unzip
  ```
  Tương tự trên GCP VM: upload SCVD vào `~/streamhouse/data/raw/SCVD/`.
- Pusher **auto-detect** layout SCVD → không cần biết chính xác cấu trúc Kaggle unzip. Nếu log "No clips found" → folder name không khớp alias → thêm vào `_VIOLENCE_ALIASES`/`_NON_VIOLENCE_ALIASES` ở `rtsp_pusher.py`.
- Repo legacy `Smart-Security-Monitoring-System/` **không dùng** cho project này (đã revert). RTSP sim thật ở streamhouse.
- Memory Claude đã update: `streamhouse-rtsp-scvd` (+ MEMORY.md index).

#### Chưa làm (defer)
- **Verify E2E** (cần SCVD tải + docker run): pusher → MediaMTX → `ffplay rtsp://localhost:8554/cam_01`. Chờ user tải SCVD hoặc cấp `kaggle.json`.
- **Commit** (chờ user).
- Docs khác (QUICKSTART/ROADMAP/README) vẫn ghi RWF-2000 — chưa sửa (cosmetic, để user quyết).

---

### 🗺️ Last State — Session 2026-06-18 (VioMoViNet → Kafka real producer) 🔧 IMPLEMENTED

**Agent:** Claude (local implementation — chưa deploy lên GCP)
**GCP/Services:** KHÔNG thay đổi — không start/stop VM, không đụng pipeline GCP. Toàn bộ thay đổi ở code local (VioMoViNet + note streamhouse).

#### Mục đích phiên
Cài **producer Kafka thật** vào VioMoViNet để nó trở thành nguồn sự kiện duy nhất feed pipeline (thay mock `rtsp_inference_mock.py`). Đóng gap "mock vs real" đã tồn đọng.

#### Vấn đề tồn đọng đã fix
| Vấn đề | Fix |
|--------|-----|
| VioMoViNet không publish Kafka (chỉ MinIO + API) → pipeline GCP toàn chạy trên mock | Thêm `KafkaEventProducer` vào VioMoViNet, publish thẳng topic `urban-safety-alerts` theo đúng data contract |
| GCP Kafka IP mâu thuẫn (`136.110.16.108` ở `.env.gcp.example` vs `34.124.131.144` ở `send_test_events.py`) | **Xác nhận `34.124.131.144` là IP đúng** (PARTNER_GUIDE + history). Fix default trong VioMoViNet compose. `.env.gcp.example` đang stale |
| Lo ngại double-publish (mock + real cùng topic) | `rtsp-inference-mock` đã `profiles:[streaming]` ở cả `docker/docker-compose.yml` + `deploy/docker-compose.gcp.yml` (KHÔNG có standalone `inference-mock` — `resource-limits.md` cũ stale). Thêm note cảnh báo |

#### Files thay đổi
**VioMoViNet repo** (producer thật):
- `app/kafka/producer.py` (MỚI) — `KafkaEventProducer`, lifecycle mirror `EvidenceStorage`, fail không crash, no flush/event
- `app/kafka/__init__.py` (MỚI)
- `app/config.py` — 11 settings `kafka_*` (default `KAFKA_ENABLED=false`)
- `app/main.py` — instantiate + init + shutdown, wire vào `StreamManager`
- `app/stream/manager.py` + `app/stream/worker.py` — truyền `event_producer`, hook publish trong `_do_inference` (gate 0.5s violent / 5s heartbeat), bắt thêm `p_nofight`
- `app/routes/stream.py` — validate `camera_id` `^cam_\d{2}$` → 422
- `requirements.txt` — `kafka-python`; `docker-compose.yml` — env `KAFKA_*`, default broker `34.124.131.144:9093`

**Streamhouse repo** (disable mock):
- `docker/docker-compose.yml` + `deploy/docker-compose.gcp.yml` — note cảnh báo: không bật `--profile streaming` khi VioMoViNet thật chạy (double-publish)
- `docs/REAL_PRODUCER_INTEGRATION_PLAN.md` (MỚI) — plan chi tiết (data contract, hook points, verification)

#### Quyết định thiết kế
- Payload mirror `rtsp_inference_mock.py` (Flink validator **không đổi**): `risk_score`=final_prob, `confidence`=max(p_fight,p_nofight), `event_type`="FIGHTING" khi violent, base64 thumbnail 160×90 (frame-extractor sink cần field này), `is_valid` do Flink set.
- `KAFKA_ENABLED=false` mặc định → VioMoViNet vẫn chạy standalone như cũ nếu chưa config Kafka.

#### ⚠️ Notes cho session/agent kế
- **CHƯA verify E2E** (cần GPU box + GCP lên). Verify tĩnh đã pass: Python compile OK, 3 YAML valid, wiring nhất quán.
- **PREREQUISITE ops**: GCP firewall phải allow inbound TCP **9093** từ GPU box (Session 45 đã verify 9093 reach từ máy local; GPU box riêng cần check).
- Khi chạy platform + producer thật: **KHÔNG** `--profile streaming`, **KHÔNG** dùng `docker-compose.local-stream.yml` (file đó chạy mock ungated).
- `camera_id` bắt buộc `cam_NN` khi `KAFKA_ENABLED=true` (sai → 422 ở VioMoViNet; nếu lọt qua sẽ bị Flink quarantine).
- `evidence_url` trong message trỏ MinIO của VioMoViNet (riêng) — chỉ là metadata phụ; evidence chính thức của platform vẫn do `frame-extractor` sink tạo từ `metadata.thumbnail`.
- Memory Claude đã update: `viomovinet-kafka-producer`, `kltn-project-structure` (sửa 3 repo chính).

#### Chưa làm (defer)
- Verify E2E thật khi có infra (Kafka UI thấy `mock:false` → Flink `hot-violence-alerts-valid` → Trino → UI).
- Commit (chờ user).
- `.env.gcp.example` vẫn ghi IP stale `136.110.16.108` — nên sửa thành `34.124.131.144` (minor, để sau).

---

### 🗺️ Last State — Session 2026-06-17 (Thesis Benchmark Planning + Scope Correction) 📋 PLANNING

**Agent:** Claude (local analysis)
**GCP/Services:** KHÔNG thay đổi — giữ nguyên state từ Session 2026-06-04. Không start/stop VM, không đụng pipeline.

#### Mục đích phiên
Phân tích toàn bộ KLTN (4 repo) + lập bộ plan benchmark cho luận văn.

#### ⚠️ Scope correction (QUAN TRỌNG cho agent sau)
- KLTN = **4 repo** (không phải chỉ training repo):
  1. `MoViNets-...-Streaming/` — train MoViNet A0–A5 (weights)
  2. `VioMoViNet/` — AI inference server (FastAPI, **đã build v2**: MinIO evidence, auto-reconnect, ~40–60 stream)
  3. `Violence-Urban-Safety-UI/` — React + Node dashboard
  4. `streamhouse-violence-detection/` — platform Kafka/Flink/Fluss/Paimon/Iceberg/Trino/RAG (deployed GCP)
- **KHÔNG build lại api/** — VioMoViNet đã có (Claude từng nhầm scope lúc đầu → đã đính chính).

#### Gap thesis phát hiện
Luận văn **nặng platform, nhẹ model**:
- Có: kiến trúc (Ch3) + đánh giá platform (Fluss/Paimon/Iceberg latency, Kafka, Flink, chatbot E2E).
- Thiếu: đánh giá MoViNet (accuracy A0–A5, vì sao chọn A3); số đa phần single-shot (thiếu mean±std); A3 gốc 84.66% **không tái lập** (re-run thật ~79–81%).

#### Artifact tạo — `MoViNets-.../plan/` (7 file, **CHƯA thực thi**)
Bộ benchmark plan 3 trục:
| File | Nội dung |
|---|---|
| `00_overview` | mục lục, scope, nguyên tắc |
| `01_platform_latency` | HOT/WARM/COLD, N=30 → mean±std/p95 (GCP) |
| `02_model_accuracy` | data 35 thí nghiệm **đã có** → write-up; retrain A3×5 tùy chọn |
| `03_inference_throughput` | A0–A5 VRAM/max-stream + Pareto — **việc đo mới chính** |
| `04_methodology_env` | 2 môi trường, statistical rigor, tools |
| `05_figures_roadmap` | tables/figures + phase A–E + risk |
| `06_model_selection` | lý do chọn A3 (draft thesis luôn) |

#### Lưu ý cho session/agent kế
- **Accuracy**: report số reproduce ~79–81%, KHÔNG 84.66.
- **2 môi trường**: platform đo GCP CPU VM; inference+accuracy đo GPU server (2×2080Ti). Demo GCP dùng `rtsp-inference-mock` (GCP không GPU) → **khai Limitation**.
- **Model accuracy data đã có sẵn** (35 thí nghiệm trong training repo) → chỉ write-up, KHÔNG retrain nặng.
- **Việc MỚI thật sự**: trục 3 (throughput benchmark trên VioMoViNet) + deployed detection P/R/F1.
- Memory Claude đã update: `kltn-project-structure`, `kltn-contribution-strategy`, `kltn-benchmark-plan`, `movinet-reproduction-gap`.

#### Chưa làm (defer)
- Chưa chạy benchmark thật, chưa retrain, chưa viết chương thesis, chưa commit (chờ user duyệt plan).

---

### 🗺️ Last State — Session 2026-06-04 (Bug Fixes + Full Test) ✅ COMPLETED

**Branch:** `deploy/hybrid-cloud`
**GCP VM:** `34.124.131.144` (RUNNING), static IP

#### Services Status (GCP)
| Service | Status |
|---------|--------|
| kafka, minio, flink, fluss, chatbot | ✅ UP |
| grafana (port 3001), prometheus | ✅ UP |
| rtsp pipeline (mediamtx + rtsp_pusher + rtsp-inference-mock) | ✅ UP |
| frame-extractor | ✅ UP (MinIO evidence-frames ~7k+ files) |
| pipeline-manager | ✅ UP (2 Flink jobs RUNNING) |

#### Data State
- **HOT (Fluss)**: ~25k rows, 106ms latency
- **WARM (Paimon)**: 366,135 rows (2026-05-25 → 2026-06-04)
- **COLD (Iceberg)**: 10,312 rows (historical)
- **MinIO evidence-frames**: ~7,000+ JPEG thumbnails (cam_01–cam_15, 2026-06-04)

#### Bugs Fixed This Session
| Bug | Fix |
|-----|-----|
| Grafana "Error loading: stat" (Trino plugin unavailable) | Migrate tất cả panels sang Prometheus custom gauges |
| Grafana violence-security-monitor + analytics "No data" | Rebuild với Prometheus datasource |
| Evidence chatbot trả 20 ảnh random | Fix: query MinIO theo camera_id+date từ Paimon, respect count limit |
| Evidence: UUID mismatch (Paimon ≠ MinIO) | Dùng DISTINCT camera_id+DATE, list actual MinIO files |
| Evidence: Deadlock asyncio (HTTP self-call) | Dùng `_trino_client.query_paimon()` trực tiếp |
| GCP firewall block MinIO port 9000 | Tạo rule `streamhouse-minio` allow tcp:9000,9001 |
| MinIO credentials mismatch (minio/mypassword vs minioadmin/minioadmin) | Update `.env.gcp` |
| HOT count null in /api/layer-counts | Fix metric param `0.numRecordsIn` + return 0 thay vì None |
| Chatbot hallucination khi row_count=0 | Guard + anti-hallucination prompt |

#### Grafana Dashboards (All Working)
| Dashboard | UID | Datasource |
|-----------|-----|-----------|
| Violence Incidents Analytics | violence-incidents-v2 | Prometheus + Infinity |
| Security Monitor | violence-security-monitor | Prometheus |
| Violence Analytics | violence_analytics | Prometheus |
| Chatbot Performance | chatbot-metrics | Prometheus |
| Streamhouse Architecture | streamhouse-arch-001 | Prometheus |

**Prometheus metrics refreshed every 5 min:**
- `violence_incidents_24h_total = 160,404`
- `violence_incidents_7d_total = 182,982`
- `violence_cameras_active = 15`
- `violence_incidents_by_type{event_type=...}` (4 types)
- `violence_incidents_by_camera{camera_id=...}` (15 cameras)
- `streamhouse_hot/warm/cold_rows_total`

#### Test Results (5/5 PASS)
| Query | Layer | Result |
|-------|-------|--------|
| "1 ảnh đường Nguyễn Huệ" | WARM | 1 ảnh thật ✅ |
| "5 ảnh Hàm Nghi" | WARM | "Không tìm thấy" (đúng) ✅ |
| "3 ảnh gần đây" | WARM | 3 ảnh thật ✅ |
| "15 phút qua bao nhiêu alert?" | HOT · Fluss | 100 alerts ✅ |
| "Camera nguy hiểm nhất 7 ngày?" | WARM · Paimon | cam_15 ✅ |

#### Important Notes for Next Session
- **MinIO credentials on GCP**: `minioadmin/minioadmin` (NOT `minio/mypassword` như trong `.env.gcp` cũ)
- **Prometheus refresh**: auto mỗi 5 phút. Manual trigger: `POST /api/grafana/refresh-metrics`
- **Evidence images**: MinIO port 9000 đã mở public. Frame URL pattern: `34.124.131.144:9000/evidence-frames/{cam}/{YYYY-MM-DD}/{uuid}.jpg`
- **RTSP pipeline data files** trên GCP: chỉ có ~4 Fight clips (local upload) — đủ để test
- **Grafana dashboard URL**: `http://34.124.131.144:3001/d/violence-incidents-v2`
- **Local UI**: `Violence-Urban-Safety-UI/frontend/` → `npm run dev` (port 5173)

---

### 🗺️ Plan Session 46 — Local RTSP → GCP Kafka ✅ COMPLETED

> **Kết quả:** Toàn bộ P0–P3 hoàn thành. RTSP pipeline local verified E2E.
> HLS player trên Vercel ready (chỉ cần ngrok để dùng).

---

### 🗺️ Plan Session 47 — Vercel HLS Live Demo + Thesis

> **Mục tiêu:** Demo đầy đủ với live video trên Vercel, viết báo cáo thesis.

#### Bước 1 — Bật ngrok expose HLS
```bash
# Cài ngrok nếu chưa có: https://ngrok.com/download
# Expose local MediaMTX HLS port:
ngrok http 8888

# Copy HTTPS URL (e.g., https://xxxx.ngrok-free.app)
# Vào Vercel app → Settings page → dán URL → Save
```

#### Bước 2 — Start local RTSP + ngrok
```bash
# Start local RTSP stack
docker compose -f docker/docker-compose.local-stream.yml up -d

# Start ngrok (cửa sổ riêng)
ngrok http 8888

# Mở Vercel app, vào Settings, paste ngrok URL, Save
# Live Streams page sẽ tự load HLS streams
```

#### Bước 3 — Verify E2E demo
```bash
# Query GCP chatbot
curl -X POST http://34.124.131.144:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"camera nao co canh bao trong 30 phut qua?"}'

# Stop sau demo:
docker exec rtsp-inference-mock touch /app/tmp/STOP
docker exec rtsp_pusher touch /app/tmp/STOP
```

#### Bước 4 — Thesis work (P4)
- Benchmark: GCP vs local latency (HOT <100ms target)
- Architecture diagrams (Streamhouse Trio flow)
- Performance metrics section

#### Lưu ý QUAN TRỌNG
- ngrok URL thay đổi mỗi lần restart → cần update lại trong Settings page
- GCP VM IP: `34.124.131.144` (có thể thay đổi sau VM restart)
- Local RTSP stack: `docker compose -f docker/docker-compose.local-stream.yml up -d`
- KHÔNG dùng `send_test_events.py` để demo (fake data)

---

### 🗺️ Plan Session 45 — Local RTSP Stream → GCP Kafka ✅ COMPLETED

> **Mục tiêu:** Chạy RTSP pipeline trên máy local, gửi inference events lên GCP Kafka.
> Vercel HLS display để session sau.

#### Bối cảnh & phát hiện

| Hạng mục | Trạng thái |
|----------|-----------|
| `config/mediamtx/mediamtx.yml` — HLS port 8888 | ✅ Đã bật (`hlsAlwaysRemux: yes`, segment 1s) |
| `config/mediamtx/mediamtx.yml` — WebRTC port 8889 | ✅ Đã bật |
| `rtsp_inference_mock.py` — timestamp `.isoformat()` | ✅ Đã fix (không cần sửa) |
| `rtsp_inference_mock.py` — `KAFKA_BROKER` env var | ✅ Đọc từ env, default `kafka:9092` |
| GCP Kafka port 9093 accessible từ local | ✅ Đã verify (send_test_events.py) |
| `docker-compose.yml` streaming profile | ⚠️ `rtsp-inference-mock` depends_on `kafka` local |

**Vấn đề chính:** Service `rtsp-inference-mock` trong `docker/docker-compose.yml` có `depends_on: kafka` → không start được nếu không kéo cả local Kafka lên.

#### Kế hoạch implement (Session 45)

**Bước 1 — Tạo `docker/docker-compose.local-stream.yml`**

File compose riêng, chỉ 3 services, không cần local Kafka:
```yaml
services:
  mediamtx:           # RTSP server + HLS (image sẵn có)
  rtsp_pusher:        # ffmpeg đẩy RWF-2000 clips → MediaMTX
  rtsp-inference-mock:
    environment:
      KAFKA_BROKER: 136.110.16.108:9093   # → GCP Kafka trực tiếp
    # KHÔNG có depends_on kafka
```

Lệnh chạy:
```bash
docker compose -f docker/docker-compose.local-stream.yml up -d
# Stop:
docker exec rtsp-inference-mock touch /app/tmp/STOP
docker exec rtsp_pusher touch /app/tmp/STOP
```

**Bước 2 — Start GCP VM + verify pipeline**
```bash
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"
"$GCLOUD" compute instances start instance-20260524-104630 --zone=asia-southeast1-b
# Chờ ~2 phút → start services
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b --command='
  cd ~/streamhouse
  docker compose -f deploy/docker-compose.gcp.yml --env-file deploy/.env.gcp up -d
'
# Fix S3 plugin nếu containers bị recreate (xem bước trong Session 44 state bên dưới)
```

**Bước 3 — Test E2E**
```bash
# Verify events vào GCP Kafka (local terminal)
docker logs rtsp-inference-mock | tail -20
# Expected: [cam_01] VIOLENCE | score=0.92x hoặc Normal | score=0.0xx

# Verify HOT layer trên GCP (sau ~2 phút)
curl -X POST http://136.110.16.108:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"camera nao co canh bao trong 30 phut qua?"}'
# Expected: "layer": "Fluss", data thật từ RTSP stream local
```

**Bước 4 — Verify chatbot fix (session 44 pending)**
- Nếu chatbot vẫn lỗi `HTTPConnectionPool(host='jobmanager')`:
  ```bash
  "$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b --command='
    docker exec chatbot sed -i "s/default=\"jobmanager\"/default=\"flink-sql-gateway\"/" \
      /app/scripts/chatbot/config.py
    docker restart chatbot
  '
  ```

#### Deferred (Session 46+) — Vercel HLS Display
- ngrok expose port 8888 → public HTTPS URL
- React Camera Grid: thêm `hls.js` player, đọc `https://<ngrok>/cam_XX/index.m3u8`
- Config: env var `VITE_HLS_BASE_URL` cho ngrok URL
- Không cần deploy lại Vercel mỗi lần (ngrok URL thay đổi) → có thể dùng settings page

---

### 📍 Last State (Updated: 2026-05-26 — Session 51) ✅ GCP Tiering VERIFIED + Commit DONE

- **Agent vừa làm:** Claude (Session 51 — verify GCP tiering fix, Paimon data confirmed, taskmanager rebuilt)
- **Trạng thái:** GCP pipeline STABLE. Tất cả 3 Flink jobs RUNNING. Paimon có 10,226 rows. Commit `c37fa4b` pushed.
- **Nhánh git:** `deploy/hybrid-cloud` (clean — đã commit `deploy/docker-compose.gcp.yml`)
- **GCP VM:** `instance-20260524-104630` — **ĐANG CHẠY** (IP: `34.124.131.144`)

---

#### ✅ Session 51 — Đã hoàn thành

| Hạng mục | Chi tiết |
|----------|---------|
| Tiering verified ✅ | `✓ Tiering job completed successfully.` @ 03:41:42 UTC (4 phút sau trigger 03:37:55) |
| Paimon data verified ✅ | `SELECT COUNT(*) FROM violence_incidents` = **10,226 rows** |
| Taskmanager image rebuild ✅ | `deploy-taskmanager:latest` built successfully — S3 plugin baked in |
| Taskmanager recreated ✅ | `docker compose up -d --force-recreate taskmanager` — new image active |
| 3 Flink jobs RUNNING ✅ | Contract Validator + hot_violence_alerts (Fluss) + daily_incident_stats (Paimon) |
| Git commit ✅ | `c37fa4b` — `fix(gcp): add shared fluss-remote-data volume for tiering coordinator-tablet-taskmanager` |

#### ✅ Session 50 — Đã hoàn thành

| Hạng mục | Chi tiết |
|----------|---------|
| Root cause GCP tiering fail | `_METADATA` files ở coordinator `/tmp/fluss-remote`; taskmanager đọc từ TMP riêng (không shared) ✅ Đã xác định |
| Fix: Shared Docker volume | Thêm `fluss-remote-data` vào `deploy/docker-compose.gcp.yml` — coordinator + tablet + taskmanager mount cùng `/var/fluss/remote-data` ✅ |
| `_METADATA` files verified | `/var/fluss/remote-data/kv/security/hot_violence_alerts-14/*/snap-0/_METADATA` — tất cả 3 buckets ✅ |
| Secondary: S3 plugin missing | `deploy-taskmanager` image (built 2026-05-24) thiếu S3 plugin trong `/opt/flink/plugins/`. Quick-fix: copy jar + restart taskmanager ✅ |
| Taskmanager rebuild | Completed in Session 51 ✅ |
| 3 Flink jobs RUNNING | Contract Validator + sink_to_fluss_enriched + aggregate_paimon ✅ |
| Tiering triggered | Completed in Session 51 ✅ — 10,226 rows in Paimon |

#### 🔍 Session kế — VIỆC CẦN LÀM TIẾP THEO

> **Tất cả checklist Session 50 đã hoàn thành trong Session 51.** GCP pipeline stable.

**[P1] Thesis writeup — Performance Evaluation chapter:**
- Dùng benchmark table từ Session 49 (bên dưới)
- HOT 100ms native ✅, WARM 5.9s ✅, COLD 9.5s ✅
- Ghi rõ: Chatbot E2E = Gemini intent (~8s) + ChromaDB (~1s) + query + Gemini answer (~8s)

**[P2] Demo script cho buổi bảo vệ:**
```
Q1: "Camera nào có cảnh báo bạo lực trong 30 phút qua?" → HOT (Fluss)
Q2: "Thống kê bạo lực trong 3 giờ qua?" → WARM (Paimon)
Q3: "Dữ liệu tháng trước?" → COLD (Iceberg)
```

---

### 📍 Last State (Updated: 2026-07-06 — Session 52) ✅ E2E Latency Measurement & Dashboard IMPORTED

- **Agent vừa làm:** Antigravity (Session 52 — implement đo E2E latency, import Grafana dashboard, hoàn thành Chapter 4 báo cáo thực nghiệm)
- **Trạng thái:** GCP + Vast.ai pipeline STABLE. Kết quả thực nghiệm đã cập nhật đầy đủ vào luận văn.
- **Nhánh git:** `deploy/hybrid-cloud`
- **GCP VM:** `instance-20260524-104630` — **ĐANG CHẠY** (IP: `34.124.131.144`)

---

#### ✅ Session 52 — Đã hoàn thành

| Hạng mục | Chi tiết |
|----------|---------|
| Đo latency Model Inference ✅ | Thêm logic đo FPS/Inference latency vào `visualize_stream.py`. Thực đo RTX A4000: **Mean = 794ms**, P95 = 861ms (N=250). |
| Đo E2E Pipeline Latency ✅ | Embed `kafka_sent_at` timestamp vào Kafka message payload. Viết script `e2e_latency_benchmark.py` để verify. |
| Import Thesis Grafana Dashboard ✅ | Tạo dashboard `thesis_evaluation.json` với 20 panels. Import thành công vào Grafana GCP (port 3001). |
| Đo Trino & Chatbot E2E Latency ✅ | Thực đo: WARM COUNT = 2.57s (warm: 0.96s), COLD COUNT = 0.22s. Chatbot E2E = 14.6s (simple) / 28.8s (complex). |
| Viết báo cáo thực nghiệm §4.3 ✅ | Cập nhật Bảng 4.11, 4.12, 4.13. Thêm mục §4.3.6 (Sessionization nén 97.6% dữ liệu) và §4.3.7 (E2E Latency). |

**[P3] HLS Live Streams — chạy local, chiếu màn hình (CHỐT):**
```bash
# Terminal 1: Start RTSP → GCP Kafka
docker compose \
  -f docker/docker-compose.local-stream.yml \
  -f docker/docker-compose.gcp-stream.yml \
  up -d

# Terminal 2: Chạy frontend local
cd Violence-Urban-Safety-UI/frontend && npm run dev
# Mở http://localhost:5173 → Settings → HLS URL = http://localhost:8888 → Save
```
> Không dùng Vercel/ngrok. Chiếu màn hình laptop lên projector. Chi tiết: `docs/DEMO_SCRIPT.md`

**[GCP restart] Quy trình nếu VM bị stop:**
```bash
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"
"$GCLOUD" compute instances start instance-20260524-104630 --zone=asia-southeast1-b
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b --strict-host-key-checking=no --command='
  cd ~/streamhouse/deploy
  docker compose -f docker-compose.gcp.yml --env-file .env.gcp up -d
'
# Chờ ~5 phút cho dim_camera seed + Flink jobs start
```

#### ⚠️ Root Cause Analysis (quan trọng — đọc nếu troubleshoot tiếp)

**GCP Fluss KvSnapshotNotExistException (đã fix):**
- `_METADATA` file path: `file:///tmp/fluss-remote/kv/security/hot_violence_alerts-12/0/snap-4/_METADATA`
- Lý do fail: `coordinator` write `_METADATA` vào `/tmp/fluss-remote` của CONTAINER MÌNH; `taskmanager` (Flink) đọc từ `/tmp/fluss-remote` của CONTAINER MÌNH — hai container có `/tmp` riêng biệt, không shared!
- Tại sao local OK: Local dùng named Docker volume `fluss-tablet-remote` mounted tại `/var/fluss/remote-data` cho CẢ coordinator + tablet + taskmanager → share cùng 1 filesystem.
- Fix đã apply: Thêm `fluss-remote-data` named volume vào `deploy/docker-compose.gcp.yml` với mount tại `/var/fluss/remote-data` cho cả 3 containers; đổi `remote.data.dir` từ `file:///tmp/fluss-remote` → `/var/fluss/remote-data`.

**GCP S3 plugin thiếu (ĐÃ FIX PERMANENT — Session 51):**
- `deploy-taskmanager` image rebuilt + recreated trong Session 51 — S3 plugin baked in ✅
- Không cần manual patch nữa

#### 📊 GCP State khi kết thúc Session 51 (UTC ~04:10)

| Component | State |
|-----------|-------|
| GCP VM IP | `34.124.131.144` |
| Kafka topics | `hot-violence-alerts-valid`: growing (continuous) |
| Contract Validator | RUNNING ✅ |
| sink_to_fluss_enriched | RUNNING ✅ |
| aggregate_paimon | RUNNING ✅ |
| Fluss HOT | `_METADATA` files on shared volume ✅ |
| Paimon WARM | **10,226 rows** — verified Session 51 ✅ |
| Taskmanager image rebuild | **deploy-taskmanager:latest** built + recreated — Session 51 ✅ |

#### ✅ Session 49 — Đã hoàn thành

| Hạng mục | Chi tiết |
|----------|---------|
| HOT real data | Fluss `hot_violence_alerts` = **4,995 rows** (sink_to_fluss_enriched đang chạy) ✅ |
| HOT benchmark (chatbot) | warm session: **32–44s** E2E; cold/no-data: ~60s timeout ✅ |
| HOT pipeline verified | rtsp-inference-mock → kafka:9092 → ContractValidator → Fluss ✅ |
| WARM benchmark (Trino direct) | warm: **11–13s**; first call: ~16s ✅ |
| COLD benchmark (Trino direct) | warm: **8–11s**; first call: ~10.5s ✅ |
| `/api/latency` truth | HOT native=**100ms**, WARM=**5.9s**, COLD=**9.5s** ✅ |
| `/api/layer-counts` | HOT=4,995 / WARM=15,834 / COLD=15,834 (duration=7.4s) ✅ |
| Tiering test | Pipeline-manager auto-tiered @ 15:24 (6 min), completed ✅. WARM count = 15,834 (unchanged — same incident_ids, deduplicate upsert — ĐÚNG) |
| GCP pipeline | Kafka rebuilt (KRaft cleared), all 7 topics OK, 3 Flink jobs RUNNING ✅ |
| GCP topics | urban-safety-alerts, hot-violence-alerts-valid, và 5 topic khác ✅ |

#### 📊 Thesis Benchmark Table (Session 49 — final numbers)

| Layer | Công nghệ | Storage Latency (API) | Direct Query (Trino/Gateway) | Chatbot E2E (warm) |
|-------|-----------|----------------------|-----------------------------|--------------------|
| HOT | Fluss | **100ms** | ~8s (SQL Gateway LIMIT scan) | **32–44s** |
| WARM | Paimon + Trino | **5.9s** | 11–16s (cold→warm) | **35–41s** |
| COLD | Iceberg + Trino | **9.5s** | 8–11s | **31–35s** |

> **Ghi chú cho thesis:**
> - "Storage Latency" = thời gian query thuần (không có LLM), đo từ `/api/latency`
> - HOT native 100ms là target thiết kế của Fluss được đạt ✅
> - Chatbot E2E gồm: Gemini intent (~8s) + ChromaDB retrieval (~1s) + query + Gemini answer (~8s)
> - WARM 14–23× faster hơn Flink Gateway cũ (3–5 phút → 6s)

#### ⚠️ Lưu ý quan trọng (Session 48–49)

- **GCP IP mới**: `34.124.131.144` (thay cho `34.124.131.144`). Cập nhật mọi lần VM restart.
- **HOT data issue**: rtsp-inference-mock trỏ về `kafka:9092` (local). Old data trong Kafka có timestamp cũ → chatbot filter "30 phút" sẽ thấy data chỉ khi rtsp mới chạy đủ lâu.
- **WARM latency**: 6s (API level) = ~5s Trino + ~1s overhead. Chatbot E2E 35s = thêm 2× Gemini (8+8s) + ChromaDB.
- **GCP Kafka KRaft issue**: Sau `TERMINATED`, `listTopics` timeout → fix bằng cách clear `/tmp/kafka-logs/` và restart kafka container.
- **dim_camera local**: Seeded 15 cameras qua SQL Gateway REST. GCP seeded tự động bởi pipeline-manager.
- **Paimon data (local)**: 15,834 rows từ Session 38–39. GCP Paimon bắt đầu accumulate sau khi pipeline chạy đủ lâu.

#### 🚀 Bước tiếp theo (Session 50)

1. **Thesis writeup**: Dùng benchmark table bên trên cho chapter Performance Evaluation.
2. **Demo script cho bảo vệ**:
   ```
   Q1: "Camera nào có cảnh báo bạo lực trong 30 phút qua?" → HOT (Fluss)
   Q2: "Thống kê bạo lực trong 3 giờ qua?" → WARM (Paimon)
   Q3: "Dữ liệu tháng trước?" → COLD (Iceberg)
   ```
3. **GCP WARM data**: Để pipeline GCP chạy 1–2 ngày để accumulate Paimon data → verify tiering GCP.
4. **Architecture diagram**: Cập nhật diagram với Trino+Paimon native connector (thay Flink Gateway cho WARM).

---

#### ✅ Đã hoàn thành (session 43–46)

| Hạng mục | Chi tiết |
|----------|---------|
| Kafka external port 9093 | Local → GCP Kafka hoạt động ✅ |
| Contract Validator | Valid events → `hot-violence-alerts-valid` ✅ |
| HOT job Fluss | Enriched location, 15 cameras ✅ |
| dim_camera seeding | 15 cameras, Quận 1 HCM, temporal join ✅ |
| flink-sql-gateway | rest.address + FLINK_GATEWAY_HOST fixed ✅ |
| S3 plugin — PERMANENT | Baked into `docker/Dockerfile.flink` (commit `431c60b`), GCP rebuilt ✅ |
| Chatbot | "30 phut qua" → Fluss → 15 camera locations, layer=Fluss ✅ |
| RTSP E2E test | `docker compose -f docker/docker-compose.local-stream.yml up -d` → 15 cameras → GCP Kafka → Fluss → chatbot 15 rows ✅ |
| GCP VM git sync | `git pull` + `docker compose build jobmanager chatbot` + containers restarted ✅ |
| HLS player (Vercel) | `HLSPlayer.jsx` (hls.js), Settings page ngrok URL input, localStorage persist ✅ |
| Admin API | Standalone `admin-api` service (port 5003, profile admin) for RTSP start/stop ✅ |

**RTSP E2E test PASS (session 46):**
- `docker compose -f docker/docker-compose.local-stream.yml up -d` → 15 cameras live
- Violence events generated by rtsp-inference-mock → GCP Kafka
- Chatbot: "camera nao co canh bao trong 30 phut qua?" → **15 rows, layer=Fluss** ✅

---

#### ⚠️ Trạng thái local

- **Local RTSP stack**: `mediamtx + rtsp_pusher + rtsp-inference-mock` đang chạy (session 46 test)
- **Stop khi xong**: `docker exec rtsp-inference-mock touch /app/tmp/STOP && docker exec rtsp_pusher touch /app/tmp/STOP`

---

#### 🔄 Quy trình khởi động lại GCP VM

```bash
GCLOUD="/c/Users/user/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin/gcloud"

# 1. Start VM
"$GCLOUD" compute instances start instance-20260524-104630 --zone=asia-southeast1-b

# 2. Chờ ~30s rồi SSH
"$GCLOUD" compute ssh instance-20260524-104630 --zone=asia-southeast1-b --strict-host-key-checking=no --command='
  cd ~/streamhouse/deploy
  docker compose -f docker-compose.gcp.yml --env-file .env.gcp up -d
'
# NOTE: S3 plugin đã baked trong image — KHÔNG cần exec-fix nữa
# Chờ ~5 phút để pipeline-manager seed dim_camera và submit Flink jobs

# 3. Verify chatbot (sau ~5 phút)
curl -X POST http://34.124.131.144:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"camera nao co canh bao trong 30 phut qua?"}'
```

---

#### 📂 Files đã thay đổi (session 46, đã commit)

| File | Thay đổi |
|------|---------|
| `scripts/admin/main.py` + `index.html` | MỚI — admin-api service |
| `docker/Dockerfile.admin` | MỚI — admin-api Docker image |
| `docker/docker-compose.yml` | Thêm admin-api service (profile admin) |
| `config/mediamtx/mediamtx.yml` | Auth config cho internal API access |
| `scripts/chatbot/main.py` | Xóa `/api/streaming-status` (moved to admin-api) |
| `Violence-Urban-Safety-UI/frontend/src/common/HLSPlayer.jsx` | MỚI — hls.js player |
| `Violence-Urban-Safety-UI/frontend/src/pages/LiveStreams.jsx` | Replace WebRTC → HLS, fix streaming-status |
| `Violence-Urban-Safety-UI/frontend/src/pages/Settings.jsx` | Add HLS URL config section |

---

#### 🎯 Next Steps (Session 46+)

1. **[P0]** Commit tất cả local changes lên git
2. **[P0]** Sync `deploy/docker-compose.gcp.yml` changes với GCP VM (để sau VM restart lại không cần manual patch)
3. **[P1]** Test local RTSP → GCP pipeline: `docker compose -f docker/docker-compose.local-stream.yml up -d`
4. **[P2]** Vercel HLS Display: ngrok expose port 8888, React Camera Grid với hls.js
5. **[P3]** Rebuild Flink images trên GCP để bake S3 plugin permanently
6. **[P4]** Thesis: benchmark GCP vs local latency, architecture diagrams, performance section

---

#### 📊 Stack state (cuối session 45 — VM ĐANG CHẠY)

```
GCP VM:           RUNNING (instance-20260524-104630, asia-southeast1-b, IP: 34.124.131.144)
HOT layer (Fluss): 60+ rows verified (session 45 test events)
WARM layer (Paimon): chưa có data (cần ~2h data để tier từ Fluss)
COLD layer (Iceberg): chưa có data (archive chỉ chạy 2:00 UTC)
Flink jobs:       3 RUNNING: Contract Validator, HOT sink, daily_incident_stats
dim_camera:       15 cameras, Quận 1 HCM, seeded ✅
Chatbot:          WORKING — Fluss routing verified (15 camera locations returned)
```

---

> **Lịch sử sessions cũ (Session 1–43):** Xem trong git history hoặc file `.claude/projects/.../memory/`.  
> Tóm tắt: Toàn bộ local stack hoàn thiện qua Sessions 18–43.  
> Session 43: Grafana/Prometheus setup, React UI (Analytics + StreamhouseStatus pages), 22/23 E2E tests PASS.  
> Session 40: Hard reset, RTSP pipeline sole data source, 9624+ HOT events, chatbot routing 100% correct.  
> Session 46: RTSP E2E verified (local → GCP → Fluss → chatbot). HLS player deployed to Vercel (hls.js + ngrok URL). S3 plugin baked permanently in Dockerfile.flink.

