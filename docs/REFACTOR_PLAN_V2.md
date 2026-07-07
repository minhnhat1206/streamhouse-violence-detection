# REFACTOR PLAN V2 — Star Schema chuẩn + Sessionization + BBox Evidence + Chatbot

> Branch: `refactor/star-schema-v2` (cả 3 repo). Ngày lập: 2026-07-08.
> Căn cứ: phân tích code local (scripts/transform, scripts/streaming, scripts/chatbot)
> + kiểm tra live GCP VM `34.124.131.144` (Trino, MinIO, Kafka, chatbot logs).

---

## 0. Hiện trạng — các vấn đề đã xác nhận

### 0.1 Số vụ bạo lực bị thổi phồng (vấn đề #3 của yêu cầu)
- Producer (`rtsp_inference_mock.py`) gửi event mỗi **5s** (bình thường) / **0.5s** (đang bạo lực),
  mỗi lần gửi sinh `event_id = uuid4()` **mới**.
- → 1 vụ bạo lực ~20s = **~40 event** = 40 dòng trong `hot_violence_alerts` (Fluss),
  40 dòng trong `violence_incidents` (Paimon), 40 dòng trong Iceberg.
- `aggregate_paimon.py` đếm `COUNT(*)` trên raw events → `daily_incident_stats`/`camera_stats` sai.
- Đã có **view** `iceberg.default.violence_incidents_sessionized` (gaps-and-islands, gap 30s/60s)
  + regex rewrite trong `trino_client._query_trino` và `main._trino_query` — nhưng đây là **hack ở tầng đọc**:
  HOT không được sửa, bảng gold vẫn sai, COLD archive vẫn raw, và view tính window function
  trên toàn bảng mỗi lần query (chậm, không scale).

### 0.2 Star schema hiện tại sai chuẩn (vấn đề #2)
- **2 fact table trùng nhau**: `fact_violence_incidents` + `violence_incidents` (backward-compat).
- Cột chiều (`location`, `ward_id`, `district`) nằm ngay trong fact (denormalized).
- `dim_time` chỉ có grain NGÀY (thực chất là dim_date), thiếu chiều GIỜ, thiếu `dim_event_type`.
- Metadata camera bị **hardcode 4 chỗ**: `setup_star_schema.py`, `pipeline_manager.py`,
  `seed_dim_camera_gcp.py` (15 cam) và `camera_registry.csv` (5 cam — nguồn đúng, mới nhất).

### 0.3 Bug frame_url bị ghi đè NULL
- `update_frame_url.py` upsert `frame_url` thật vào `violence_incidents` (merge-engine=`deduplicate`).
- 1 giờ sau, `tier_fluss_to_paimon.py` insert lại đúng PK đó với `frame_url=NULL`
  → **deduplicate = bản mới nhất thắng → URL bằng chứng bị xoá**.
- Fix: đổi merge-engine thành **`partial-update`** (NULL không ghi đè giá trị đã có).

### 0.4 BBox trong ảnh bằng chứng (vấn đề #2 — bbox)
- Chuỗi bbox ĐÃ có: bboxAPI (YOLOv8+ByteTrack) vẽ box → publish RTSP `{cam}_bbox`;
  producer capture thumbnail từ stream `_bbox` → ảnh evidence **đã có box** khi bboxAPI chạy.
- Gaps: (a) ảnh chỉ **160×90** → box không nhìn thấy; (b) `capture_jpeg` không bao giờ trả
  `success=False` → nhánh fallback `_bbox → raw` là dead code, lỗi âm thầm;
  (c) `metadata.people[]` (toạ độ bbox) bị **vứt bỏ** ở mọi tầng lưu trữ → không vẽ lại được,
  không đếm được số người.

### 0.5 Chatbot (vấn đề #5)
- **3 lần gọi Gemini / 1 câu hỏi** (intent → SQL → tổng hợp câu trả lời) → chậm (nhiều giây).
- `schema_registry.py` là snapshot **hardcode tĩnh** — sẽ sai ngay khi đổi schema.
- **Metric bịa số cứng**: `_g_e2e_inference_ms = 794.71 ± random`, `kafka_to_fluss = 500 ± random`
  trong `main.py` — phải xoá, thay bằng đo thật từ `metadata.inference_ms`/`kafka_sent_at`.
- Regex rewrite bảng → view sessionized ở 2 nơi (hack, xoá khi có fact chuẩn).
- SQL injection: `/api/recent-incidents` nối chuỗi `camera_id`, `location` thẳng vào SQL.
- `app.py` (1761 dòng) là bản cũ không được deploy (Dockerfile chạy `main:app`) → dead code.
- `_adapt_sql_for_flink_hot` phẫu thuật SQL bằng regex với nhiều rule cứng.

### 0.6 Sự cố production (phát hiện khi kiểm tra GCP 2026-07-08)
- **Trino → Paimon GÃY**: `UnsupportedSchemeException: no file io for scheme 's3'`
  (jar `trino-filesystem-s3-440.jar` có mặt nhưng plugin không nhận `fs.native-s3.enabled`).
  → mọi query WARM + view sessionized + `_refresh_dashboard_metrics` fail mỗi 5 phút (log chatbot).
- Iceberg qua Trino cũng lỗi đọc metadata (`Failed to read file s3a://...metadata.json`)
  dù file tồn tại trong MinIO → nghi cùng gốc (trino-coordinator được tạo lại ~40h trước).
- Kafka topic đã tích ~60k events. MinIO còn nguyên data (evidence-frames rất nhiều file).

---

## 1. Thiết kế đích — transform ở đâu, tầng nào làm gì

**Nguyên tắc: sessionize MỘT LẦN ở nguồn (producer gắn `incident_uid`), các tầng chỉ aggregate
theo `incident_uid` — không tầng nào phải "đoán" lại vụ.**

```
Vast.ai (producer)                GCP (Flink)                    Storage
─────────────────                ────────────                   ────────
visualize_stream ──┐
bboxAPI (_bbox) ───┼─► rtsp_inference_mock                     Kafka: urban-safety-alerts
                   │   + incident_uid (episode id)     ──►     (event 0.5s/5s, có people[])
                   │   + people[], people_count
                   │   + frame 640×360 khi violent
                                    │
                        data_contract_validator                 → hot-violence-alerts-valid
                                    │
              ┌─────────────────────┴──────────────────────┐
   sink_to_fluss_enriched (HOT)                  frame_extractor (sidecar)
   ├─ raw events → fluss.hot_violence_alerts     └─ chỉ upload ảnh khi is_violent
   └─ GROUP BY incident_uid                          → MinIO → hot-violence-frames-uploaded
      → fluss.hot_violence_incidents  ★MỚI                     │
      (đếm vụ realtime = COUNT bảng này)          update_frame_url → paimon (partial-update)
                                    │
        tier_fluss_to_paimon (mỗi 30'): events cũ >1h → paimon.violence_incidents (event grain)
        build_incident_facts ★MỚI (mỗi 30'): GROUP BY incident_uid trên paimon.violence_incidents
            → paimon.fact_violence_incident (grain = 1 VỤ, peak frame_url, people_count)
            → paimon.fact_incident_person (bridge bbox)
                                    │
        aggregate_paimon: đọc fact_violence_incident → daily_incident_stats/camera_stats (đúng số VỤ)
                                    │
        archive_to_iceberg (2h sáng): fact >7 ngày → iceberg.historical_incident_facts ★MỚI (giữ frame_url)
                                       events >7 ngày → iceberg.historical_violence_incidents (giữ nguyên)
```

### 1.1 Star schema v2 (Paimon WARM) — khớp `thesis_report/figures/star_schema_v2.dbml`
- **`fact_violence_incident`** — grain = 1 vụ (sessionized): `incident_id` (PK, = incident_uid),
  FK `camera_id/date_id/time_id/event_type_id`, measures `start_ts, end_ts, duration_sec,
  event_count, max_risk_score, avg_confidence, people_count, frame_url (peak), is_violent`.
  KHÔNG chứa location/ward/district.
- **`dim_camera`** (Paimon, SCD2: `valid_from/valid_to/is_current`) — seed DUY NHẤT từ
  `camera_registry.csv`. Bản Fluss chỉ là copy phục vụ temporal join HOT.
- **`dim_date`** (đổi từ dim_time cũ: + quarter, month_name, day_name, is_holiday).
- **`dim_time`** ★MỚI — 24 giờ, part_of_day, is_peak_hour.
- **`dim_event_type`** ★MỚI — FIGHTING/ASSAULT/STABBING/SHOOTING + severity.
- **`fact_incident_person`** ★MỚI (bridge) — track_id, bbox, det_score per incident.
- `violence_incidents` GIỮ làm bảng **event grain** (drill-down + evidence),
  đổi merge-engine → `partial-update`, thêm cột `incident_uid`, `people_json`, `people_count`.
- Bỏ `fact_violence_incidents` cũ (trùng lặp) — drop sau khi migrate.

### 1.2 Sessionization — quyết định thiết kế
- **Producer-side episode ID** (`incident_uid`): uuid sinh khi score chuyển sang violent,
  giữ nguyên suốt vụ, đóng vụ sau khi hết violent liên tục `INCIDENT_GAP_SECONDS=30`
  (chống flapping, khớp gap 30s của view cũ).
- Ưu điểm so với session window ở Flink: không phụ thuộc watermark với cadence 0.5s,
  không split vụ khi Flink restart, HOT/WARM/COLD dùng chung 1 identity, giải thích đơn giản
  trong báo cáo. View sessionized cũ giữ lại làm fallback cho dữ liệu lịch sử (không có uid).
- Backfill dữ liệu cũ (60k event không có uid): script migrate dùng đúng logic gaps-and-islands
  của view để sinh uid giả `legacy_{camera}_{session_idx}` một lần.

### 1.3 BBox trong evidence (mọi ảnh tra ra đều có box)
1. Producer: capture từ `{cam}_bbox` (đã có), nâng resolution **640×360 khi is_violent**
   (giữ 160×90 cho heartbeat để nhẹ Kafka), sửa bug fallback của `capture_jpeg`.
2. `people[]` từ bboxAPI đi theo event → lưu `people_json` (Fluss + Paimon events),
   `people_count` measure trong fact, chi tiết box trong `fact_incident_person`.
3. `frame_extractor_sink.py`: chỉ upload ảnh khi `is_violent=true` (hiện upload MỌI event
   5s/lần/camera → rác MinIO), đặt key theo `{camera}/{date}/{incident_uid}/{event_id}.jpg`
   để gom ảnh theo vụ.
4. Fact lấy `frame_url` của event có `risk_score` cao nhất trong vụ (peak frame, có box).
5. UI/dashboard/chatbot: không đổi gì nhiều — `frame_url` giờ luôn trỏ ảnh có box, to hơn.

### 1.4 Chatbot — nhanh, đúng, thích ứng, không hardcode
- **1 lần gọi Gemini thay vì 3**: node `understand_query` trả JSON
  `{intent..., sql}` trong 1 call; `generate_sql` chỉ fallback; câu trả lời dạng COUNT/stat
  đơn giản render bằng template (không cần Gemini call thứ 3), chỉ dùng Gemini tổng hợp
  cho kết quả dạng list/phức tạp.
- **Schema registry động**: introspect `SHOW COLUMNS` từ Trino/Flink Gateway lúc startup
  (+ cache + fallback tĩnh) → thêm bảng/cột mới là chatbot tự biết.
- Routing câu hỏi "bao nhiêu vụ" → `fact_violence_incident` (WARM/COLD) /
  `hot_violence_incidents` (HOT); câu hỏi chi tiết/evidence → bảng events.
- **Xoá**: regex rewrite sessionized (2 nơi), metric bịa 794.71/500ms (thay bằng đo thật:
  avg `metadata.inference_ms` + delta `kafka_sent_at`→Fluss từ sample Kafka), `app.py` dead code.
- Fix SQL injection các endpoint REST (escape/parameterize).

### 1.5 Sự cố Trino (phải fix trước khi test)
- Thử theo thứ tự trên GCP: (1) `paimon.properties`: bỏ `fs.native-s3.enabled`, đổi
  `warehouse=s3a://warehouse/paimon` (dùng HadoopFileIO có sẵn trong plugin);
  (2) nếu không được: pin lại image trino có paimon-trino bundle đúng version như trước restart;
  (3) kiểm tra `iceberg` catalog sau khi sửa (nghi cùng gốc).
- Việc này làm ở bước deploy (mục 3), không chặn viết code.

---

## 2. Danh sách file thay đổi (repo streamhouse, branch `refactor/star-schema-v2`)

| # | File | Thay đổi |
|---|------|----------|
| 1 | `scripts/streaming/rtsp_inference_mock.py` | incident_uid episode tracking, frame 640×360 khi violent, fix capture fallback, people_count |
| 2 | `scripts/transform/init_star_schema_v2.py` ★ | DDL đầy đủ v2 (dims + facts + events partial-update) + seed dims từ CSV |
| 3 | `scripts/transform/sink_to_fluss_enriched.py` | +incident_uid/people vào Fluss events; job agg → `hot_violence_incidents` |
| 4 | `scripts/transform/data_contract_validator.py` | rule mới: violent event phải có incident_uid |
| 5 | `scripts/transform/frame_extractor_sink.py` | chỉ upload khi violent; key theo incident_uid |
| 6 | `scripts/transform/update_frame_url.py` | schema mới (incident_uid, people_json), partial-update |
| 7 | `scripts/transform/tier_fluss_to_paimon.py` | tier events với cột mới; bỏ insert `fact_violence_incidents` cũ |
| 8 | `scripts/transform/build_incident_facts.py` ★ | batch GROUP BY incident_uid → fact + bridge (peak frame) |
| 9 | `scripts/transform/aggregate_paimon.py` | gold đọc từ fact (đúng số vụ) |
| 10 | `scripts/transform/archive_to_iceberg.py` | + archive fact → `historical_incident_facts` (giữ frame_url) |
| 11 | `scripts/transform/pipeline_manager.py` | orchestrate job mới, bỏ seed camera hardcode (dùng CSV) |
| 12 | `scripts/transform/migrate_v2.py` ★ | backfill incident_uid cho data cũ + drop bảng trùng |
| 13 | `scripts/chatbot/components/schema_registry.py` | registry động + bảng fact mới |
| 14 | `scripts/chatbot/agent.py` | 1-call Gemini, routing fact/event, bỏ hack |
| 15 | `scripts/chatbot/main.py` | bỏ metric bịa → đo thật; fix injection; SQL endpoint → fact |
| 16 | `scripts/chatbot/components/trino_client.py` | bỏ rewrite sessionized |
| 17 | `scripts/chatbot/app.py` | XOÁ (dead code, không được deploy) |

UI (`Violence-Urban-Safety-UI`): không đổi API shape — chỉ hưởng số liệu đúng + ảnh có bbox.
buildAPI (VioMoViNet): không đổi (visualize_stream giữ nguyên; bbox do bboxAPI đảm nhiệm).

## 3. Trình tự triển khai / kiểm thử
1. Code + review trên branch (mục 2). ✔ = xong trong đợt commit này.
2. Fix Trino paimon connector trên GCP (mục 1.5) — cần VM, làm lúc deploy.
3. Chạy `init_star_schema_v2.py` + `migrate_v2.py` trên GCP (Flink batch).
4. Redeploy pipeline-manager + chatbot (docker compose build). Bật lại GPU Vast.ai
   → producer mới có incident_uid; verify: số vụ HOT = số episode thật (~1 vụ/5 phút/cam
   theo kịch bản RTSP), fact đếm khớp, ảnh evidence có bbox ≥640px.
5. Cập nhật Grafana panel/documentation + đồng bộ lại §2.8/§4.x báo cáo nếu số liệu thay đổi.
