# TASK PHASES V2 — Kế hoạch triển khai refactor star-schema-v2

> Đi kèm `docs/REFACTOR_PLAN_V2.md` (phân tích + thiết kế).
> File này là **checklist thi công theo phase** — làm tuần tự, mỗi phase có
> điều kiện vào (entry), việc cần làm (task), và tiêu chí nghiệm thu (exit).
> Cập nhật trạng thái: `[ ]` chưa làm · `[x]` xong · `[~]` đang làm · `[!]` blocked.
>
> Branch: `refactor/star-schema-v2` (cả 3 repo). Cập nhật lần cuối: 2026-07-08.

---

## Tổng quan phase

| Phase | Nội dung | Chạy ở đâu | Cần GPU? | Trạng thái |
|:-----:|----------|------------|:--------:|:----------:|
| 0 | Code refactor (pipeline + chatbot + UI) | local/git | ✗ | ✅ XONG |
| 1 | Fix Trino federation (Paimon + Iceberg) | GCP | ✗ | ⬜ **BLOCKING** |
| 2 | Deploy schema v2 + migrate dữ liệu cũ | GCP | ✗ | ⬜ |
| 3 | Redeploy services (pipeline-manager, chatbot, Grafana) | GCP | ✗ | ⬜ |
| 4 | Producer mới + bboxAPI trên Vast.ai | Vast.ai | ✔ | ⬜ |
| 5 | Nghiệm thu E2E (số vụ đúng, ảnh có bbox, chatbot) | cả 2 | ✔ | ⬜ |
| 6 | Đồng bộ báo cáo + slide + Grafana panel | local | ✗ | ⬜ |

**Thứ tự bắt buộc:** 1 → 2 → 3 → 4 → 5. Phase 6 làm sau khi 5 chốt số liệu.
Phase 1–3 làm được NGAY (không cần bật GPU). Phase 4–5 chờ thuê/bật GPU Vast.

---

## Phase 0 — Code refactor ✅ (xong 2026-07-08)

- [x] Commit + push 3 repo, tạo branch `refactor/star-schema-v2` (3 repo)
- [x] `docs/REFACTOR_PLAN_V2.md` — phân tích hiện trạng + thiết kế v2
- [x] Pipeline: `init_star_schema_v2.py`, `build_incident_facts.py`, `migrate_v2.py` (mới);
      producer incident_uid + frame 640×360; sink 2-bảng HOT; tiering partial-update;
      gold từ fact; archive + `historical_incident_facts`; validator; frame_extractor
      chỉ upload violent; pipeline_manager orchestrate + seed từ CSV
- [x] Chatbot: 1-call Gemini, registry introspect động, metric đo thật (xoá số bịa
      794.71/500ms), xoá rewrite view sessionized, xoá `app.py`, fix SQL injection
- [x] UI backend: alerts/analytics controller → fact v2
- [x] Compose: mount `../data/metadata` cho pipeline-manager

Commit chính: `81c6640` (pipeline), `d9db35e` (chatbot), `03f2734` (UI backend).

---

## Phase 1 — Fix Trino federation trên GCP ⬜ **BLOCKING — làm đầu tiên**

**Hiện trạng (xác nhận 2026-07-08):** `trino-coordinator` (Up ~40h) không đọc được
cả Paimon (`UnsupportedSchemeException: no file io for scheme 's3'`) lẫn Iceberg
(`Failed to read file s3a://...metadata.json`) dù jar `trino-filesystem-s3-440.jar`
có trong plugin và MinIO còn nguyên data. Mọi query WARM/COLD + chatbot metrics
refresh fail. Nghi nguyên nhân: container được recreate với image/config lệch version.

**Entry:** SSH GCP OK (`gcloud compute ssh instance-20260524-104630 --zone=asia-southeast1-b`).

### Tasks
- [ ] 1.1 Chụp trạng thái trước khi sửa (để so + rollback):
  ```bash
  sudo docker exec trino-coordinator trino --version
  sudo docker inspect trino-coordinator --format '{{.Image}} {{.Created}}'
  sudo docker exec trino-coordinator cat /etc/trino/catalog/paimon.properties
  ```
- [ ] 1.2 **Thử A (ít xâm lấn):** đổi `deploy/config/trino/catalog/paimon.properties`
  sang HadoopFileIO — bỏ `fs.native-s3.enabled=true`, đổi
  `warehouse=s3a://warehouse/paimon`, thêm bộ key `hadoop.fs.s3a.*`
  (endpoint/access/secret/path.style). Restart trino-coordinator, test:
  ```bash
  sudo docker exec trino-coordinator trino --execute \
    "SELECT COUNT(*) FROM paimon.security.violence_incidents"
  ```
- [ ] 1.3 Nếu A fail → **Thử B:** kiểm tra `docker/Dockerfile.trino` — pin đúng
  version paimon-trino bundle từng chạy (app.py cũ ghi `paimon-trino-440`);
  rebuild image: `docker compose -f docker-compose.gcp.yml build trino-coordinator`.
- [ ] 1.4 Fix Iceberg cùng lúc (nghi cùng gốc image/env): test
  `SELECT COUNT(*) FROM iceberg.security.historical_violence_incidents` —
  nếu vẫn lỗi, kiểm tra `hive.s3.*` env đã render (`docker exec trino-coordinator env | grep -i minio`)
  và quyền đọc object bằng `mc`/curl trực tiếp.
- [ ] 1.5 Commit thay đổi config/Dockerfile vào branch (KHÔNG sửa tay ngoài git).

### Exit criteria
- [ ] `SELECT COUNT(*)` chạy được trên CẢ `paimon.security.*` VÀ `iceberg.security.*`
- [ ] Log chatbot hết lỗi `_refresh_dashboard_metrics` (chờ 5–10 phút sau restart)

---

## Phase 2 — Deploy schema v2 + migrate dữ liệu ⬜

**Entry:** Phase 1 xong. Pull branch mới trên GCP repo (`/home/user/streamhouse` hoặc
đường dẫn deploy thực tế) + `docker compose up -d` lại pipeline-manager để mount CSV.

### Tasks
- [ ] 2.1 Pull code branch lên GCP VM:
  ```bash
  cd /home/user/streamhouse && git fetch && git checkout refactor/star-schema-v2 && git pull
  ```
- [ ] 2.2 Recreate pipeline-manager (compose đổi mount metadata):
  ```bash
  cd deploy && docker compose -f docker-compose.gcp.yml up -d --force-recreate pipeline-manager
  ```
- [ ] 2.3 Chạy DDL + seed dims (idempotent):
  ```bash
  sudo docker exec pipeline-manager /opt/flink/bin/flink run \
    -Dexecution.runtime-mode=BATCH --python /opt/flink/scripts/init_star_schema_v2.py
  ```
  Verify: `SHOW TABLES FROM paimon.security` có `fact_violence_incident`,
  `dim_camera/dim_date/dim_time/dim_event_type`, `fact_incident_person`;
  `SELECT * FROM paimon.security.dim_camera` = **5 camera** từ CSV (không phải 15).
- [ ] 2.4 Migrate dữ liệu cũ (chạy MỘT lần):
  ```bash
  sudo docker exec pipeline-manager /opt/flink/bin/flink run \
    -Dexecution.runtime-mode=BATCH --python /opt/flink/scripts/migrate_v2.py
  ```
  Verify: `violence_incidents` có cột `incident_uid` (backfill `legacy_*`),
  tồn tại `violence_incidents_v1_backup`, `fact_violence_incidents` (bảng trùng cũ) đã DROP.
- [ ] 2.5 Build fact cho toàn bộ lịch sử:
  ```bash
  sudo docker exec -e BUILD_LOOKBACK_HOURS=8760 pipeline-manager /opt/flink/bin/flink run \
    -Dexecution.runtime-mode=BATCH --python /opt/flink/scripts/build_incident_facts.py
  ```
- [ ] 2.6 **Đối chiếu số vụ** (số quan trọng nhất của refactor):
  ```sql
  SELECT COUNT(*) FROM paimon.security.violence_incidents WHERE is_violent = true; -- raw events
  SELECT COUNT(*) FROM paimon.security.fact_violence_incident;                      -- SỐ VỤ
  ```
  Kỳ vọng: số vụ **nhỏ hơn hàng chục lần** số raw event (1 vụ ≈ 40 event) và khớp
  ± với view sessionized cũ: `SELECT COUNT(*) FROM iceberg.default.violence_incidents_sessionized WHERE is_violent=true`.
- [ ] 2.7 Ghi 2 con số (raw events / số vụ) vào cuối file này để dùng cho báo cáo.

### Exit criteria
- [ ] fact_violence_incident có dữ liệu, số vụ hợp lý, dims đủ 5 camera
- [ ] Không job Flink nào crash-loop (`http://<GCP>:8081` → Running Jobs)

---

## Phase 3 — Redeploy services trên GCP ⬜

**Entry:** Phase 2 xong.

### Tasks
- [ ] 3.1 Rebuild + restart chatbot (code mới):
  ```bash
  cd /home/user/streamhouse/deploy && \
  docker compose -f docker-compose.gcp.yml build chatbot && \
  docker compose -f docker-compose.gcp.yml up -d chatbot
  ```
- [ ] 3.2 Verify chatbot startup log có:
  - `✓ Schema registry: N tables introspected from Trino` (registry động chạy)
  - `[metrics] Dashboard gauges updated` (hết fail)
  - KHÔNG còn giá trị latency cố định 794.x (check `curl localhost:5002/metrics | grep inference`)
- [ ] 3.3 Restart pipeline-manager watchdog để submit sink job mới
  (`sink_to_fluss_enriched` v2 tạo `hot_violence_incidents`):
  ```bash
  docker compose -f docker-compose.gcp.yml restart pipeline-manager
  ```
  Verify Flink UI: 3 streaming job RUNNING (validator, hot sink, aggregate) —
  aggregate giờ đọc `fact_violence_incident`.
- [ ] 3.4 Grafana: mở dashboard chính — KPI `violent_24h/7d` đổ số từ fact
  (có thể = 0 nếu chưa có vụ mới trong cửa sổ; xem panel 7d).
- [ ] 3.5 (Tuỳ chọn) Xoá 2 view `iceberg.default.*_sessionized` SAU khi mọi consumer
  đã chạy ổn 1–2 ngày — hoặc giữ làm tài liệu so sánh trong báo cáo.

### Exit criteria
- [ ] `/health` chatbot ok; `/api/layer-counts`, `/api/recent-incidents`, `/api/stats` trả dữ liệu
- [ ] Grafana không panel nào lỗi datasource

---

## Phase 4 — Vast.ai: producer mới + bboxAPI ⬜ (cần bật GPU)

**Entry:** Phase 3 xong + thuê/bật lại GPU Vast.ai (cập nhật `~/.ssh/config` host `buidAPI`
theo `VASTAI_SETUP_GUIDE.md` mục 2–3).

### Tasks
- [ ] 4.1 Setup lại stack Vast theo guide (MediaMTX → rtsp_pusher → bboxAPI →
  visualize_stream ×5 → buildAPI) — dùng đúng lệnh trong `VASTAI_SETUP_GUIDE.md`.
- [ ] 4.2 Rsync producer MỚI (có incident_uid):
  ```bash
  rsync -avz 'streamhouse-violence-detection/scripts/streaming/rtsp_inference_mock.py' \
    buidAPI:~/streamhouse/scripts/streaming/
  ```
- [ ] 4.3 Start producer, verify log:
  - `Incident OPEN: <uuid>` khi có vụ, `Incident CLOSE` sau ~30s hết violent
  - `bbox_status=ok` (bboxAPI sống, capture từ stream `_bbox`)
- [ ] 4.4 Verify payload Kafka có field mới (chạy trên GCP):
  ```bash
  sudo docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic hot-violence-alerts-valid --max-messages 3 \
    | python3 -c "import sys,json; [print({k: j.get(k) for k in ('incident_uid','people_count','is_violent')}) for j in map(json.loads, sys.stdin)]"
  ```
- [ ] 4.5 Verify ảnh evidence mới trong MinIO: key dạng `{cam}/{date}/{incident_uid}/{event}.jpg`,
  kích thước ~640×360, mở ảnh thấy **bounding box**.

### Exit criteria
- [ ] Event violent nào cũng có `incident_uid`; validator không tăng `MISSING_INCIDENT_UID`
- [ ] Ảnh evidence có bbox nhìn rõ (≥640px)

---

## Phase 5 — Nghiệm thu E2E ⬜

**Entry:** Phase 4 chạy ổn ≥ 1–2 giờ (đủ để tiering + build fact chạy vài vòng).

### Acceptance checklist
- [ ] 5.1 **Số vụ HOT đúng thực tế:** `hot_violence_incidents` khớp kịch bản RTSP
  (baseline ≈ 1 vụ/5 phút/cam; cam_05 cao điểm ≈ 1 vụ/2 phút) — KHÔNG còn "cả ngàn vụ".
- [ ] 5.2 **HOT → WARM khớp:** sau >1h, vụ trong `fact_violence_incident` = vụ đã đóng
  ở HOT (đối chiếu vài incident_uid cụ thể).
- [ ] 5.3 **frame_url không bị NULL-đè:** chọn 5 vụ trong fact → mở `frame_url`
  → ảnh tồn tại + có bbox (bug partial-update đã fix).
- [ ] 5.4 **Gold + Grafana:** `daily_incident_stats`/`camera_stats` = số VỤ;
  KPI Grafana khớp SQL tay.
- [ ] 5.5 **Chatbot đúng + nhanh:**
  - "Hôm nay có bao nhiêu vụ bạo lực?" → số VỤ (khớp fact), citation đúng tầng
  - "Bây giờ camera nào đang có bạo lực?" → HOT incidents
  - "Cho xem ảnh bằng chứng ở đường Nguyễn Huệ" → gallery ảnh CÓ bbox
  - Latency: câu COUNT < ~5s (1 call Gemini; bản cũ 3 call)
  - `duration_ms` ghi lại 5 câu test → bảng so sánh trước/sau cho báo cáo
- [ ] 5.6 **Web UI:** dashboard alerts hiện vụ (không trùng lặp 40 dòng/vụ),
  ảnh có bbox; analytics chart hợp lý; nút xoá alert hoạt động (soft-delete).
- [ ] 5.7 **Metric thật:** `/metrics` → `streamhouse_inference_latency_ms` dao động
  theo tải thật (không quanh 794.71), `streamhouse_e2e_kafka_to_fluss_ms` là số đo.
- [ ] 5.8 Chạy lại bộ test: `scripts/tests/test_pipeline_e2e.py` + chatbot tests —
  cập nhật test nào còn assert theo schema v1.
- [ ] 5.9 Chụp screenshot mọi kết quả (Grafana, chatbot, UI, MinIO) → làm Hình cho báo cáo.

### Exit criteria
- [ ] Toàn bộ checklist trên pass → merge branch (mở PR 3 repo) hoặc giữ branch để bảo vệ

---

## Phase 6 — Đồng bộ báo cáo + slide ⬜

**Entry:** Phase 5 chốt số liệu.

- [ ] 6.1 `finalOfficial_rutgon.docx`: rà §2.8 (đã viết theo star schema v2 — khớp code
  thật rồi, chỉ cần đối chiếu tên bảng/cột lần cuối), cập nhật số liệu Ch4 nếu
  demo lại (số vụ, latency chatbot, test PASS/WARN)
- [ ] 6.2 Thêm/không thêm 1 đoạn ngắn về sessionization producer-side (incident_uid)
  ở §2.x — quyết định sau khi xem lại độ dài (~140 trang)
- [ ] 6.3 Slide `KLTN_BaoVe_StreamViD.pptx`: s09 (star schema) + s21 (platform stats)
  cập nhật số mới nếu thay đổi đáng kể
- [ ] 6.4 Cập nhật `VASTAI_SETUP_GUIDE.md` + `DEVELOPER_LOG.md`: bước chạy
  init_star_schema_v2/migrate_v2, bảng mới, biến env mới
  (`INCIDENT_GAP_SECONDS`, `VIOLENT_FRAME_SCALE`, `CAMERA_REGISTRY_FILE`, `BUILD_LOOKBACK_HOURS`)
- [ ] 6.5 Xoá `setup_star_schema.py` + `seed_dim_camera_gcp.py` (đã thay bằng v2 + CSV)
  sau khi Phase 5 pass

---

## Rollback (nếu Phase 2–4 hỏng)

1. Code: `git checkout dev.VastAI` (streamhouse) / branch cũ từng repo; compose build lại.
2. Dữ liệu WARM: bảng cũ còn nguyên ở `violence_incidents_v1_backup` —
   rename ngược lại là về v1.
3. HOT: ephemeral, mất cũng không sao (tier lại từ Kafka/producer).
4. Trino config: revert commit Phase 1, `up -d --force-recreate trino-coordinator`.

## Số liệu đối chiếu (điền ở Phase 2.7 / 5)

| Chỉ số | v1 (raw events) | v2 (số vụ) | Ghi chú |
|---|---|---|---|
| WARM total violent | _(điền)_ | _(điền)_ | |
| COLD total | _(điền)_ | _(điền)_ | |
| Chatbot latency câu COUNT | _(điền)_ | _(điền)_ | 3 call → 1 call |
