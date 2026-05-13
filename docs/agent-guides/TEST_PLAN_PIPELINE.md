# Test Plan — Full Streamhouse Pipeline

**Mục tiêu**: Xác nhận toàn bộ pipeline từ RTSP thật → Data Contract → Fluss/Paimon/Iceberg → Chatbot hoạt động đúng theo thiết kế Streamhouse.

**Điều kiện hoàn thành**: Tất cả các mục Pass/Fail ở cuối mỗi phase đều PASS.

---

## Chuẩn Bị Trước Khi Test

### Yêu cầu môi trường
- Docker Desktop đang chạy
- `docker/.env` đã được tạo từ `docker/.env.example`
- Dataset RWF-2000 có tại `data/raw/RWF-2000/norTrain/{Fight,NonFight}/`
- `GEMINI_API_KEY` đã set trong `docker/.env`

### Kiểm tra nhanh trước khi bắt đầu
```bash
# Xác nhận dataset có clip
ls data/raw/RWF-2000/norTrain/Fight/ | head -5

# Xác nhận env file
grep GEMINI_API_KEY docker/.env

# Xác nhận Docker daemon
docker info | grep "Server Version"
```

---

## Phase 0 — Khởi Động Stack

### Bước 0.1: Tạo network và bootstrap
```bash
bash scripts/setup/start-pipeline.sh --profile streaming
```

Script chạy khoảng 3–5 phút. Theo dõi output và đảm bảo không có bước nào báo lỗi nghiêm trọng.

### Bước 0.2: Dừng inference-mock (tránh duplicate)
```bash
docker exec inference-mock touch /app/tmp/STOP
```

### Bước 0.3: Verify tất cả services healthy
```bash
docker compose -f docker/docker-compose.yml ps
```

**Kết quả kỳ vọng** — các service sau phải `healthy` hoặc `running`:

| Service | Trạng thái cần có |
|---------|------------------|
| kafka | healthy |
| minio | healthy |
| jobmanager | running |
| taskmanager | running |
| fluss-coordinator | running |
| fluss-tablet | running |
| mysql | healthy |
| hive-metastore | healthy |
| trino-coordinator | healthy |
| chatbot | healthy |
| mediamtx | running |
| rtsp_pusher | running |
| rtsp-inference-mock | running |

### Bước 0.4: Verify 4 Flink jobs đang chạy

Mở **http://localhost:8081** → tab "Running Jobs".

**Kết quả kỳ vọng:**
```
Data Contract Validator Job          RUNNING
Flink job: Kafka to Fluss Sink       RUNNING
Flink job: Kafka to Paimon Warm Sink RUNNING
Flink job: Paimon Aggregation        RUNNING
```

Nếu thiếu job nào, submit lại thủ công:
```bash
# Tên script tương ứng với job bị thiếu
JOB=data_contract_validator   # hoặc sink_to_fluss / sink_to_paimon / aggregate_paimon
docker exec jobmanager flink run -d -py /opt/flink/scripts/${JOB}.py
```

### ✅ Phase 0 Pass Criteria
- [ ] Tất cả 13 core services up
- [ ] 4 Flink jobs RUNNING tại http://localhost:8081

---

## Phase 1 — Verify RTSP Data Vào Kafka

### Bước 1.1: Confirm RTSP streams có trên MediaMTX

Mở trình duyệt hoặc dùng VLC: `rtsp://localhost:8554/cam_01`

Hoặc kiểm tra qua HTTP:
```bash
curl -s http://localhost:8888/cam_01/index.m3u8 | head -5
```

**Kết quả kỳ vọng**: Có stream, không bị "404 Not Found".

### Bước 1.2: Kafka raw topic nhận data

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic urban-safety-alerts \
  --max-messages 5 \
  --from-beginning
```

**Kết quả kỳ vọng** — mỗi message là JSON với cấu trúc:
```json
{
  "event_id": "uuid-...",
  "camera_id": "cam_01",
  "timestamp": "2026-05-13T...",
  "is_violent": false,
  "risk_score": 0.0843,
  "confidence": 0.512,
  "event_type": null,
  "location": {"city": "TP. Hồ Chí Minh", "district": "Quận 1", ...},
  "metadata": {"mock": true, "rtsp_connected": true, ...}
}
```

Kiểm tra: `camera_id` phải có dạng `cam_01`..`cam_15`, `risk_score` phải trong [0,1].

### Bước 1.3: Đếm offset topic raw

```bash
docker exec kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic urban-safety-alerts
```

Ghi lại số này. Chờ 30 giây, chạy lại — con số phải tăng (chứng tỏ data đang chảy liên tục).

### ✅ Phase 1 Pass Criteria
- [ ] RTSP stream có tại MediaMTX
- [ ] `urban-safety-alerts` nhận message liên tục
- [ ] `camera_id` đúng format `cam_XX`, `risk_score` ∈ [0,1]

---

## Phase 2 — Verify Data Contract Validator

### Bước 2.1: Topic validated có data

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hot-violence-alerts-valid \
  --max-messages 5 \
  --from-beginning
```

**Kết quả kỳ vọng**: Nhận được message (mock data luôn hợp lệ → 100% qua validator).

### Bước 2.2: Inject record vi phạm data contract

```bash
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic urban-safety-alerts << 'EOF'
{"event_id":"test-bad-001","camera_id":"INVALID_CAM","timestamp":"2026-05-13T00:00:00Z","is_violent":true,"risk_score":1.5,"confidence":0.9,"event_type":null,"location":{},"metadata":{}}
EOF
```

Record này vi phạm 3 rules: `INVALID_CAMERA_ID`, `RISK_SCORE_OUT_OF_RANGE`, `MISSING_EVENT_TYPE`.

### Bước 2.3: Xác nhận record xuất hiện trong quarantine

```bash
# Chờ 10-15 giây để Flink xử lý, rồi:
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic urban-safety-quarantine \
  --max-messages 5 \
  --from-beginning
```

**Kết quả kỳ vọng**: Record `test-bad-001` xuất hiện với field `violations` chứa ít nhất `["INVALID_CAMERA_ID", "RISK_SCORE_OUT_OF_RANGE", "MISSING_EVENT_TYPE"]`.

### Bước 2.4: Inject record hợp lệ — phải vào validated topic

```bash
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic urban-safety-alerts << 'EOF'
{"event_id":"test-good-001","camera_id":"cam_01","timestamp":"2026-05-13T10:00:00Z","is_violent":true,"risk_score":0.87,"confidence":0.94,"event_type":"FIGHTING","location":{"city":"TP. Hồ Chí Minh","district":"Quận 1","ward":"Phường Bến Nghé","street":"Đường Nguyễn Huệ","lat":10.77845,"long":106.70014},"metadata":{"mock":false}}
EOF
```

```bash
# Xác nhận xuất hiện trong validated, không xuất hiện trong quarantine
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hot-violence-alerts-valid \
  --max-messages 3 \
  --timeout-ms 10000
```

### ✅ Phase 2 Pass Criteria
- [ ] `hot-violence-alerts-valid` nhận data liên tục
- [ ] Record lỗi `test-bad-001` xuất hiện trong `urban-safety-quarantine` với đúng `violations`
- [ ] Record hợp lệ `test-good-001` xuất hiện trong `hot-violence-alerts-valid`
- [ ] Record lỗi KHÔNG xuất hiện trong `hot-violence-alerts-valid`

---

## Phase 3 — Verify HOT Layer (Fluss)

Fluss không query được trực tiếp qua Trino (connector chưa có). Verify gián tiếp qua Flink job status.

### Bước 3.1: Flink job "Kafka to Fluss Sink" đang RUNNING

```bash
curl -s http://localhost:8081/jobs/overview | python -m json.tool | grep -A3 "Fluss"
```

**Kết quả kỳ vọng**: Job state = `RUNNING`, `duration` tăng theo thời gian.

### Bước 3.2: Kiểm tra records processed

Tại Flink Web UI → Running Jobs → "Kafka to Fluss Sink" → tab "Metrics".

Tìm metric `numRecordsOut` — phải > 0 và tăng theo thời gian.

### Bước 3.3: (Nếu có Flink SQL Gateway — profile `ui`) Query trực tiếp

```bash
# Bật SQL Gateway nếu chưa có
docker compose -f docker/docker-compose.yml --profile ui up -d flink-sql-gateway

# Tạo session
SESSION=$(curl -s -X POST http://localhost:8083/v1/sessions \
  -H "Content-Type: application/json" -d '{}' | python -c "import sys,json; print(json.load(sys.stdin)['sessionHandle'])")

# Query HOT layer
curl -s -X POST http://localhost:8083/v1/sessions/$SESSION/statements \
  -H "Content-Type: application/json" \
  -d '{
    "statement": "SELECT COUNT(*) FROM fluss.security.hot_violence_alerts"
  }' | python -m json.tool
```

### ✅ Phase 3 Pass Criteria
- [ ] Job "Kafka to Fluss Sink" RUNNING với `numRecordsOut` > 0
- [ ] (Nếu SQL Gateway) COUNT(*) trả về số > 0

---

## Phase 4 — Verify WARM Layer (Paimon)

### Bước 4.1: Paimon snapshot tồn tại trong MinIO

```bash
docker exec minio_client mc ls minio/warehouse/paimon/security.db/violence_incidents/snapshot/
```

**Kết quả kỳ vọng**: Phải có ít nhất `snapshot-1`. Nếu chưa có, chờ 30-60 giây (Flink checkpoint interval = 30s).

### Bước 4.2: ORC data files tồn tại

```bash
docker exec minio_client mc ls minio/warehouse/paimon/security.db/violence_incidents/ --recursive | head -10
```

**Kết quả kỳ vọng**: Có file `.orc` trong các partition.

### Bước 4.3: Aggregation tables có data

```bash
docker exec minio_client mc ls minio/warehouse/paimon/security.db/daily_incident_stats/snapshot/
docker exec minio_client mc ls minio/warehouse/paimon/security.db/camera_stats/snapshot/
```

### Bước 4.4: Query Paimon qua Flink SQL Gateway

> Lưu ý: Query Paimon mất 3–5 phút do Flink phải scan ORC files từ MinIO.

```bash
# Tạo session (nếu chưa có từ Phase 3)
SESSION=$(curl -s -X POST http://localhost:8083/v1/sessions \
  -H "Content-Type: application/json" -d '{}' | python -c "import sys,json; print(json.load(sys.stdin)['sessionHandle'])")

# Gửi query đếm records
OP=$(curl -s -X POST http://localhost:8083/v1/sessions/$SESSION/statements \
  -H "Content-Type: application/json" \
  -d '{
    "statement": "SELECT COUNT(*) FROM paimon.security.violence_incidents"
  }' | python -c "import sys,json; print(json.load(sys.stdin)['operationHandle'])")

echo "Operation: $OP"
echo "Chờ 3-5 phút rồi poll kết quả..."

# Poll kết quả (chạy lại đến khi không còn NOT_READY)
curl -s http://localhost:8083/v1/sessions/$SESSION/operations/$OP/result/0 | python -m json.tool
```

**Kết quả kỳ vọng**: COUNT(*) > 0 (phụ thuộc vào thời gian chạy rtsp-inference-mock).

### ✅ Phase 4 Pass Criteria
- [ ] Paimon snapshot tồn tại tại MinIO sau 60s
- [ ] ORC data files tồn tại
- [ ] `daily_incident_stats` và `camera_stats` có snapshot
- [ ] COUNT(*) từ Flink SQL Gateway > 0

---

## Phase 5 — Verify COLD Layer (Iceberg via Trino)

### Bước 5.1: Trino catalog iceberg available

```bash
docker exec trino-coordinator trino --execute "SHOW CATALOGS"
```

**Kết quả kỳ vọng**: `iceberg` xuất hiện trong danh sách.

### Bước 5.2: Schema và table tồn tại

```bash
docker exec trino-coordinator trino --execute \
  "SHOW TABLES IN iceberg.security"
```

**Kết quả kỳ vọng**: `historical_violence_incidents` xuất hiện.

### Bước 5.3: Query dữ liệu lịch sử

```bash
docker exec trino-coordinator trino --execute \
  "SELECT COUNT(*) FROM iceberg.security.historical_violence_incidents"
```

> Iceberg chỉ có data sau khi chạy `archive_to_iceberg.py` hoặc Airflow DAG.
> Nếu count = 0, chạy archive thủ công:
```bash
docker exec jobmanager flink run -py /opt/flink/scripts/archive_to_iceberg.py
```

### Bước 5.4: Time travel query

```bash
docker exec trino-coordinator trino --execute \
  "SELECT snapshot_id, committed_at, record_count
   FROM iceberg.security.\"historical_violence_incidents\$snapshots\"
   ORDER BY committed_at DESC LIMIT 5"
```

### ✅ Phase 5 Pass Criteria
- [ ] Catalog `iceberg` available trong Trino
- [ ] Table `historical_violence_incidents` tồn tại
- [ ] COUNT(*) >= 0 (0 là OK nếu chưa archive; test archive thủ công để xác nhận)

---

## Phase 6 — Chatbot: Layer Routing

> Chatbot phải đang chạy: `docker compose -f docker/docker-compose.yml ps chatbot` → healthy

### Bước 6.1: Health check

```bash
curl -s http://localhost:5002/health | python -m json.tool
```

**Kết quả kỳ vọng**:
```json
{"status": "ok", "agent_initialized": true}
```

### Bước 6.2: Test routing HOT layer

```bash
curl -s -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "15 phút qua có bao nhiêu alert bạo lực?"}' \
  | python -m json.tool
```

**Kết quả kỳ vọng**: `"layer": "hot"` hoặc `"data_layer": "FLUSS"` trong response.

```bash
curl -s -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "30 phut qua co bao nhieu su co?"}' \
  | python -m json.tool
```

**Kết quả kỳ vọng**: Vẫn route `hot` dù không có dấu tiếng Việt.

### Bước 6.3: Test routing WARM layer

```bash
# Các query này phải route về WARM (Paimon)
QUERIES=(
  "Hôm nay camera nào ghi nhận nhiều sự cố nhất?"
  "24 giờ qua tổng cộng bao nhiêu vụ bạo lực?"
  "7 ngày qua xu hướng bạo lực như thế nào?"
  "hom nay co bao nhieu vu bao luc?"
)

for q in "${QUERIES[@]}"; do
  echo "--- Query: $q"
  curl -s -X POST http://localhost:5002/chat \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"$q\"}" \
    | python -c "import sys,json; r=json.load(sys.stdin); print(f'layer={r.get(\"layer\",r.get(\"citations\",{}).get(\"data_layer\",\"?\"))} rows={r.get(\"citations\",{}).get(\"row_count\",\"?\")} answer_len={len(r.get(\"answer\",\"\"))}')"
  echo ""
  sleep 2
done
```

**Kết quả kỳ vọng**: `layer=warm` hoặc `layer=PAIMON` cho tất cả queries.

> Lưu ý: Paimon query mất 3–5 phút mỗi câu. Đây là đặc tính kiến trúc, không phải bug.

### Bước 6.4: Test routing COLD layer

```bash
COLD_QUERIES=(
  "Tháng trước có bao nhiêu vụ bạo lực lịch sử?"
  "90 ngày qua tổng số sự cố là bao nhiêu?"
  "Cho tôi xem lịch sử sự cố tháng 4"
)

for q in "${COLD_QUERIES[@]}"; do
  echo "--- Query: $q"
  curl -s -X POST http://localhost:5002/chat \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"$q\"}" \
    | python -c "import sys,json; r=json.load(sys.stdin); print(f'layer={r.get(\"layer\",r.get(\"citations\",{}).get(\"data_layer\",\"?\"))} answer_len={len(r.get(\"answer\",\"\"))}')"
  echo ""
done
```

**Kết quả kỳ vọng**: `layer=cold` hoặc `layer=ICEBERG`.

### Bước 6.5: Kiểm tra chất lượng response

Mỗi response từ chatbot phải thỏa mãn:

```bash
curl -s -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Hôm nay camera nào nhiều sự cố nhất?"}' \
  | python -c "
import sys, json
r = json.load(sys.stdin)
answer = r.get('answer', '')
citations = r.get('citations', {})

checks = {
  'answer không rỗng': len(answer) > 20,
  'answer tiếng Việt': any(c in answer for c in 'àáảãạăắằẳẵặâấầẩẫậèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữựỳýỷỹỵđ'),
  'có citations': bool(citations),
  'có source_table': bool(citations.get('source_table')),
  'có data_layer': bool(citations.get('data_layer')),
}

for check, result in checks.items():
  status = '✅' if result else '❌'
  print(f'{status} {check}')

print()
print('Answer preview:', answer[:200])
"
```

### ✅ Phase 6 Pass Criteria
- [ ] Chatbot healthy (`agent_initialized: true`)
- [ ] HOT routing: queries < 1 giờ → `layer=hot`
- [ ] WARM routing: queries hôm nay/24h/7 ngày → `layer=warm`
- [ ] COLD routing: queries tháng trước/90 ngày → `layer=cold`
- [ ] Queries không dấu vẫn route đúng
- [ ] Response có `answer` > 20 ký tự, có `citations.source_table`, có `citations.data_layer`

---

## Phase 7 — Chatbot: Chất Lượng Trả Lời

### Bước 7.1: Anti-hallucination — không bịa khi không có data

```bash
curl -s -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Tháng 1 năm 2020 có bao nhiêu vụ bạo lực?"}' \
  | python -c "import sys,json; r=json.load(sys.stdin); print(r.get('answer',''))"
```

**Kết quả kỳ vọng**: Trả lời dạng "Không tìm thấy dữ liệu..." — KHÔNG bịa con số.

### Bước 7.2: Self-correction — query ambiguous

```bash
curl -s -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "có bao nhiêu camera?"}' \
  | python -c "import sys,json; r=json.load(sys.stdin); print('answer:', r.get('answer','')[:200])"
```

**Kết quả kỳ vọng**: Trả lời có ý nghĩa, không crash với 500 error.

### Bước 7.3: Multi-turn conversation

```bash
SESSION_QUERIES=(
  "24 giờ qua có bao nhiêu sự cố bạo lực?"
  "Camera nào nguy hiểm nhất trong khoảng thời gian đó?"
  "Risk score trung bình của camera đó là bao nhiêu?"
)

HISTORY="[]"
for q in "${SESSION_QUERIES[@]}"; do
  echo "=== Turn: $q ==="
  RESULT=$(curl -s -X POST http://localhost:5002/chat \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"$q\", \"history\": $HISTORY}")
  echo "$RESULT" | python -c "import sys,json; r=json.load(sys.stdin); print('Answer:', r.get('answer','')[:200])"
  echo ""
done
```

**Kết quả kỳ vọng**: Mỗi câu trả lời có liên quan, không crash giữa chừng.

### ✅ Phase 7 Pass Criteria
- [ ] Query không có data → trả "không tìm thấy", không bịa số
- [ ] Query ambiguous → trả lời có ý nghĩa, không 500 error
- [ ] Multi-turn 3 câu không crash

---

## Phase 8 — Kiểm Tra Độ Bền (Stability)

### Bước 8.1: Để pipeline chạy 10 phút, đếm lại Paimon records

```bash
# Ghi lại count ban đầu (từ Phase 4)
echo "Chờ 10 phút..."
sleep 600

# Đếm lại — phải lớn hơn lần trước
docker exec minio_client mc ls minio/warehouse/paimon/security.db/violence_incidents/snapshot/ | tail -3
```

**Kết quả kỳ vọng**: Snapshot number tăng (Flink checkpoint mỗi 30s → ~20 snapshots sau 10 phút).

### Bước 8.2: Flink jobs vẫn RUNNING (không tự restart)

```bash
curl -s http://localhost:8081/jobs/overview | python -c "
import sys, json
jobs = json.load(sys.stdin)['jobs']
for j in jobs:
    print(f\"{j['name'][:50]:50} {j['state']} uptime={j.get('duration',0)//1000}s\")
"
```

**Kết quả kỳ vọng**: Tất cả 4 jobs RUNNING với uptime > 600s.

### Bước 8.3: Không có container restart

```bash
docker compose -f docker/docker-compose.yml ps --format "table {{.Name}}\t{{.Status}}"
```

**Kết quả kỳ vọng**: Không service nào có `Restarting` hoặc `Exit`.

### ✅ Phase 8 Pass Criteria
- [ ] Paimon snapshot tăng sau 10 phút
- [ ] 4 Flink jobs vẫn RUNNING, uptime liên tục
- [ ] Không có container restart

---

## Dừng Pipeline Sau Khi Test

```bash
# Dừng streaming producers gracefully trước
docker exec rtsp-inference-mock touch /app/tmp/STOP

# Đợi 5 giây cho producer flush
sleep 5

# Tắt toàn bộ stack
docker compose -f docker/docker-compose.yml --profile streaming down
```

---

## Tổng Kết Pass/Fail

| Phase | Mô tả | Status |
|-------|-------|--------|
| 0 | Stack khởi động, 4 Flink jobs RUNNING | ☐ |
| 1 | RTSP data vào Kafka `urban-safety-alerts` | ☐ |
| 2 | Data contract validator route đúng valid/invalid | ☐ |
| 3 | HOT layer (Fluss) nhận records | ☐ |
| 4 | WARM layer (Paimon) có snapshots, query được | ☐ |
| 5 | COLD layer (Iceberg) accessible qua Trino | ☐ |
| 6 | Chatbot routing HOT/WARM/COLD đúng | ☐ |
| 7 | Chatbot chất lượng: anti-hallucination, multi-turn | ☐ |
| 8 | Pipeline bền vững sau 10 phút | ☐ |

**Pipeline PASS khi tất cả 9 phases đều ☑.**

---

## Xử Lý Sự Cố Thường Gặp

| Triệu chứng | Nguyên nhân | Cách fix |
|-------------|-------------|---------|
| Flink job FAILED ngay sau submit | Kafka topic chưa tồn tại | `docker exec kafka bash /scripts/setup/create-topics.sh` |
| Flink job FAILED sau vài phút | OOM (taskmanager hết RAM) | Giảm parallelism: `env.set_parallelism(1)` đã set, restart taskmanager |
| Paimon không có snapshot sau 2 phút | Flink checkpoint chưa trigger | Check `sink_to_paimon.py` có `enable_checkpointing(30000)` |
| Chatbot trả về 500 | `agent_initialized: false` | `docker compose restart chatbot`, đợi 90s |
| Chatbot routing sai layer | `_extract_time_context()` bug | Check `scripts/chatbot/app.py` hoặc `agent.py` routing logic |
| RTSP stream không có | `rtsp_pusher` chưa chạy hoặc RWF-2000 trống | Kiểm tra `docker logs rtsp_pusher` |
| Quarantine trống khi inject record lỗi | Validator job chưa chạy | Submit lại `data_contract_validator.py` |
| Trino `catalog not found` | Hive Metastore chưa healthy | Chờ thêm 60s, `docker compose restart hive-metastore` |
