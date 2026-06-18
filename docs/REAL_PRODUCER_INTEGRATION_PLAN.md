# Plan: Cắm VioMoViNet thật vào pipeline Streamhouse (bỏ mock)

## Context
Hiện pipeline Streamhouse chạy với **mock producer** (`scripts/streaming/inference_mock.py`, `rtsp_inference_mock.py`) publish event giả lập vào Kafka topic `urban-safety-alerts`. Server AI thật **VioMoViNet** chạy rời rạc, chỉ ghi evidence xuống MinIO + phục vụ REST API, **chưa publish Kafka** (grep `app/` không thấy code Kafka). Mục tiêu: để VioMoViNet thật trở thành **producer duy nhất** feed pipeline, lấy mock ra khỏi default. Giữ nguyên mọi thứ phía Flink/Trino/UI — producer thật phải tuân thủ **data contract** y hệt mock để validator không thay đổi.

## Quyết định đã chốt (từ user)
- **Publish policy:** Alert + heartbeat — 0.5s/event khi `is_violent`, 5s heartbeat khi bình thường (mirror mock `rtsp_inference_mock.py:53-54,212-214`).
- **Thumbnail:** Có kèm base64 JPEG (~160×90) trong `metadata.thumbnail` (mirror mock `:233-239`).
- **camera_id:** Validate `^cam_\d{2}$` tại `/api/stream/start`, reject 422 khi sai (tránh bị Flink quarantine).
- **Tự quyết (hợp lý):** `confidence = max(p_fight, p_nofight)` (độ chắc chắn về class top — cần bắt thêm `probs[0][1]`); `event_type = "FIGHTING"` khi violent (model chỉ binary fight, không bịa assault/stabbing).

## Data contract — payload producer phải phát (mirror `rtsp_inference_mock.py:216-245`)
```json
{
  "event_id": "<uuid4>",
  "camera_id": "cam_01",                     // phải khớp ^cam_\d{2}$
  "timestamp": "2026-06-18T10:30:45+00:00",  // ISO8601 UTC từ epoch `now`
  "is_violent": true,
  "risk_score": 0.84,                        // = final_prob (fight prob)
  "confidence": 0.91,                        // = max(p_fight, p_nofight)
  "event_type": "FIGHTING",                  // "FIGHTING" khi violent, null khi thường
  "location": {"city":"","district":"","ward":"","street":"","lat":null,"long":null},
  "metadata": {
    "fps": 11.8, "latency_ms": 7, "mock": false,
    "rtsp_connected": true, "thumbnail": "<base64 jpeg 160x90>",
    "evidence_url": "<minio url hoặc null>"   // field phụ, enrich thêm
  }
}
```
- **KHÔNG set `is_valid`** — Flink validator (`scripts/transform/data_contract_validator.py`) tự set + route valid→`hot-violence-alerts-valid`, invalid→`urban-safety-quarantine`.
- Key = `camera_id` (string), serializer = JSON UTF-8, topic = `urban-safety-alerts`, `acks=1`.

## Thay đổi phía VioMoViNet (repo chính cần sửa)

### 1. Mới: `app/kafka/producer.py` — `KafkaEventProducer`
Lifecycle **mirror `app/evidence/storage.py`** (`initialize()/shutdown()`):
- `initialize()`: nếu `settings.kafka_enabled` → kết nối `KafkaProducer(bootstrap_servers, value_serializer=json, acks=1)` với retry 5× (giống mock `:321-332`); đặt `self._connected`. **Fail không crash server** — log ERROR, `_connected=False`.
- `publish_event(camera_id, is_violent, final_prob, p_fight, p_nofight, fps, latency_ms, now_epoch, frame, evidence_url)`: guard `_connected`/`_enabled` (no-op nếu off); build payload ở trên; `try: self._producer.send(topic, key=camera_id, value=payload) except: log warn, swallow`. **Không raise, không flush mỗi event** (chỉ flush ở `shutdown()`) — tăng throughput cho benchmark.
- `_build_thumbnail(frame)`: `cv2.resize` → 160×90 → `cv2.imencode('.jpg')` → base64 (match mock size).
- `shutdown()`: `flush()` + `close()`.

### 2. `app/config.py` — thêm Settings (Pydantic BaseSettings, cùng pattern dòng 41-47)
```
kafka_enabled: bool = False
kafka_bootstrap_servers: str = "localhost:9092"
kafka_topic: str = "urban-safety-alerts"
kafka_alert_interval: float = 0.5
kafka_heartbeat_interval: float = 5.0
kafka_send_thumbnail: bool = True
kafka_thumbnail_width: int = 160
kafka_thumbnail_height: int = 90
kafka_event_type: str = "FIGHTING"
kafka_producer_acks: str = "1"
```
Default `kafka_enabled=False` → backward-compatible (server chạy như cũ nếu chưa config Kafka).

### 3. `app/main.py` — lifespan (mirror cách inject EvidenceStorage, dòng 72-77, 95-97)
- Startup: `event_producer = KafkaEventProducer(settings); event_producer.initialize()`; `app.state.kafka_producer = event_producer`; truyền vào `StreamManager(model_manager, evidence_storage, event_producer, settings)`.
- Shutdown (finally): `event_producer.shutdown()`.

### 4. `app/stream/manager.py` — thêm param `event_producer` vào `__init__` + truyền cho `StreamWorker(...)` (dòng 78-85).

### 5. `app/stream/worker.py` — lõi tích hợp
- `__init__`: thêm `event_producer`, thêm `_last_publish_time = 0.0`.
- `_do_inference` (dòng 205-262): bắt thêm `p_nofight = float(probs[0][1])` tại dòng 221; sau khi build `self._result` (~dòng 262), thêm gate throttle + publish:
```python
interval = (self._settings.kafka_alert_interval if is_violent
            else self._settings.kafka_heartbeat_interval)
if self._event_producer is not None and (now - self._last_publish_time) >= interval:
    self._event_producer.publish_event(
        self.camera_id, is_violent, final_prob,
        float(raw_prob), p_nofight,                 # p_fight, p_nofight
        self._current_fps, self._latency_ms, now, frame, evidence_url)
    self._last_publish_time = now
```
Lưu ý: `raw_prob` (fight prob gốc) dùng cho `confidence = max(raw_prob, p_nofight)`; `final_prob` (smoothed) làm `risk_score`.

### 6. `app/routes/stream.py` — validate camera_id tại `start_stream` (dòng 29-48)
Thêm đầu hàm: nếu `request.app.state.settings.kafka_enabled` và `not re.fullmatch(r'cam_\d{2}', body.camera_id)` → `raise HTTPException(422, "camera_id phải dạng cam_NN (vd cam_01) để Flink chấp nhận")`.

### 7. `requirements.txt` — thêm `kafka-python` (pin cùng version với streamhouse để đồng bộ).

### 8. `docker-compose.yml` (VioMoViNet) — thêm env cho service `api`:
```yaml
KAFKA_ENABLED: "true"
KAFKA_BOOTSTRAP_SERVERS: "${GCP_KAFKA_BROKER}:9093"   # IP GCP, xác nhận ở Prerequisites
KAFKA_TOPIC: "urban-safety-alerts"
```
(Không có secret — bootstrap là IP:port, Kafka đang PLAINTEXT. Nếu sau này thêm SASL → credential vào `.env`, không hardcode.)

## Thay đổi phía Streamhouse (disable mock)
- Audit `docker/docker-compose.yml` và `deploy/docker-compose.gcp.yml`: đảm bảo **không mock producer nào chạy ở profile default/GCP** khi VioMoViNet thật active (tránh double-publish vào cùng topic).
- Cụ thể: gán `profiles: ["mock"]` cho `inference-mock` (hiện always-on core — agent báo "no profile") và `rtsp-inference-mock`. Giữ code mock nguyên làm fallback cho dev offline; chỉ không tự-start.
- **Không sửa** Flink validator, data contract, hay bảng downstream (producer đã khớp contract).

## Prerequisites (ops — user cần xác nhận, không phải code)
1. **GCP external IP:** ✅ ĐÃ XÁC NHẬN = `34.124.131.144` (PARTNER_GUIDE + DEVELOPER_LOG + `send_test_events.py`). Đã fix default ở `VioMoViNet/docker-compose.yml`, `deploy/.env.gcp.example`, `streamhouse/CLAUDE.md`. Port `9093`, PLAINTEXT.
2. **Firewall GCP (CHƯA làm):** cho inbound TCP 9093 từ public IP của GPU box. GPU box: outbound tới `34.124.131.144:9093`. (Session 45 đã verify 9093 reach từ máy local; GPU box riêng cần check.)
3. **dim_camera** (streamhouse, `seed_dim_camera_gcp.py`): phải có sẵn các `camera_id` thật (cam_01…) — location được enrich downstream bằng join camera_id, không cần ở producer.
4. GPU box chạy VioMoViNet + source RTSP reachable.

## Verification (end-to-end)
1. Build & start VioMoViNet với `KAFKA_ENABLED=true`, broker = `<gcp>:9093`. `curl localhost:8000/api/system/health` OK; log ghi "Connected to Kafka".
2. `POST /api/stream/start {"camera_id":"cam_01","rtsp_url":"rtsp://…"}` → 200. Thử `camera_id:"cam01"` → **422**.
3. Chạy video fight qua RTSP → log VioMoViNet thấy `[PUBLISH] cam_01 VIOLENCE score=…`.
4. **Kafka UI** (GCP :18085): topic `urban-safety-alerts` có message `metadata.mock=false`, key `cam_01`.
5. **Flink:** event vào `hot-violence-alerts-valid` (KHÔNG vào `urban-safety-quarantine` khi camera_id hợp lệ).
6. **Trino:** `SELECT * FROM fluss.security.hot_violence_alerts WHERE camera_id='cam_01' ORDER BY timestamp DESC LIMIT 5;` → event thật.
7. **UI dashboard:** incident thật hiện với thumbnail.
8. **Degrade test:** chặn/Kafka down → stream VioMoViNet vẫn chạy, log warn, không crash; Kafka lên lại → event tiếp tục.
9. `docker compose ps` (streamhouse): không có `inference-mock`/`rtsp-inference-mock` đang chạy.

## Out of scope
- Không đụng weights/training, không đổi Flink validator hay schema downstream.
- Kafka PLAINTEXT hiện OK cho demo thesis trên IP private; production sẽ thêm SASL_SSL (ghi chú, không làm).
- Giữ code mock (sau profile) làm fallback.

---

## Next Steps (chưa làm — resume checklist)

> **Trạng thái hiện tại:** code HOÀN CHỈNH + verify tĩnh pass (compile/YAML/wiring). **CHƯA verify E2E** vì cần GPU box + GCP lên. Khi làm tiếp, duyệt từng mục với user, tick `[x]` khi xong.

### ✅ Đã làm (đừng làm lại)
- [x] Producer thật: `app/kafka/producer.py` + wiring (`main.py` → `StreamManager` → `StreamWorker`) + hook publish trong `_do_inference` (throttle 0.5s/5s).
- [x] `kafka_*` settings (`config.py`, default `KAFKA_ENABLED=false`), `camera_id` validation 422 (`routes/stream.py`), `kafka-python` dep, `docker-compose.yml` env.
- [x] Gate mock: xác nhận `rtsp-inference-mock` đã `profiles:[streaming]` ở 2 compose; thêm note cảnh báo double-publish.
- [x] Fix stale GCP IP `34.124.131.144` ở 3 chỗ (`VioMoViNet/docker-compose.yml`, `deploy/.env.gcp.example`, `streamhouse/CLAUDE.md`).
- [x] Doc: plan này + `DEVELOPER_LOG.md` (Session 2026-06-18) + `VioMoViNet/CLAUDE.md` (section 13) + memory.

### 🔴 P0 — Verify E2E (cần infra lên) — *chờ user mở GCP + GPU box*
- [ ] Start GCP VM + services: `cd ~/streamhouse/deploy && docker compose -f docker-compose.gcp.yml --env-file .env.gcp up -d` (**KHÔNG** `--profile streaming`).
- [ ] Confirm firewall GCP cho inbound TCP **9093** từ GPU box (GCP console / `gcloud compute firewall-rules`).
- [ ] Build + start VioMoViNet (GPU box):
      `KAFKA_ENABLED=true KAFKA_BOOTSTRAP_SERVERS=34.124.131.144:9093 docker compose up --build -d`
- [ ] `curl localhost:8000/api/system/health` OK + log ghi `KafkaEventProducer connected`.
- [ ] Chạy 9 bước ở mục **Verification** (Kafka UI `mock:false` → Flink `hot-violence-alerts-valid` → Trino → UI → degrade test).
- [ ] Nếu `camera_id` không khớp `cam_NN` → kiểm tra trả 422.

### 🟡 P1 — Sau khi verify pass
- [ ] **Commit** (chờ user duyệt branch/message): VioMoViNet changes + streamhouse note/plan. Gợi ý message: `feat(viomovinet): real Kafka producer to Streamhouse (replace mock)`.
- [ ] Update `DEVELOPER_LOG.md`: đổi status `IMPLEMENTED` → `VERIFIED`, ghi số liệu thật (latency, throughput, count events).

### 🟢 P2 — Optional / thesis
- [ ] Viết smoke-test script local (gọi `/api/stream/start` + đọc Kafka) để test nhanh không cần GCP.
- [ ] Benchmark throughput producer thật (trục 3 thesis: events/s, GPU/stream) so với mock — ghi vào `THESIS_PERFORMANCE_EVALUATION.md`.
- [ ] Cân nhắc SASL_SSL cho Kafka nếu demo ra Internet công khai.
- [ ] (minor) `deploy/.env.gcp.example` đã fix IP; rà lại các doc khác còn nhắc IP cũ không.

### ⏸ Chờ xác nhận user trước khi làm
- Mở GCP VM + GPU box để verify E2E.
- Có nên tạo branch + commit ngay không, hay đợi verify xong.

