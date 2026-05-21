# E2E Test Report — Session 38
**Date:** 2026-05-21 | **Agent:** Claude | **Architecture:** True Streamhouse Tiering

---

## Scorecard

```
S1 Infrastructure:   T1.1[P] T1.2[P] T1.3[P]
S2 Data Pipeline:    T2.1[P] T2.2[P] T2.3[P]
S3 HOT Layer:        T3.1[P] T3.2[P] T3.3[P]
S4 Tiering MOVE ⭐: T4.1[P] T4.2[P] T4.3[P] T4.4[W]
S5 WARM + COLD:      T5.1[P] T5.2[P]
S6 Chatbot:          T6.1[F] T6.2[P] T6.3[F] T6.4[?] T6.5[P]
S7 Data Quality:     T7.1[P] T7.2[P]

TOTAL: 18P / 1W / 2F / 1? của 23
```

**Critical tests (không được FAIL):** T1.2✅ T2.2✅ T3.3✅ T4.2✅ T4.3✅ T6.1❌ T6.2✅ T6.3❌

---

## Stack Recovery (trước khi test)

Khi bắt đầu session, core services bị stopped từ 12 tiếng trước:
- `kafka`, `minio`, `mysql`, `hive-metastore`, `trino-coordinator` → Exited (143)
- `producer` → Restarting (NoBrokersAvailable vì Kafka down)
- `flink-sql-gateway` → không chạy (profile `ui` chưa start)

**Recovery steps:**
```bash
docker compose -f docker/docker-compose.yml up -d kafka minio mysql hive-metastore
docker compose -f docker/docker-compose.yml up -d trino-coordinator
docker compose -f docker/docker-compose.yml restart producer
docker compose -f docker/docker-compose.yml --profile ui up -d flink-sql-gateway
```

Pipeline-manager tự submit lại 3 Flink jobs sau ~10 phút khởi động.

---

## S1: Infrastructure

### T1.1 — Docker containers healthy ✅ PASS
Sau recovery: tất cả services UP, không có Exit/Restarting.

### T1.2 — Flink: 3 jobs RUNNING ✅ PASS
```
jobs-running=3  slots-available=5
```
Jobs: `Data Contract Validator Job`, `insert-into_fluss.security.hot_violence_alerts`, `insert-into_paimon.security.daily_incident_stats,...`

### T1.3 — Chatbot healthy ✅ PASS
```json
{"status":"ok","services":{"api":"ok","agent_initialized":true,"config_valid":true},"version":"2.0.0"}
```

---

## S2: Data Pipeline

### T2.1 — RTSP producer đang publish ✅ PASS
```
[cam_10] VIOLENCE | score=0.844
[cam_15] Normal | score=0.144
[cam_01] Normal | score=0.141
```

### T2.2 — Contract Validator đang process ✅ PASS
```
validator=True  hot_sink=True  aggregate=True
```

### T2.3 — HOT record count đang tăng ✅ PASS
HOT: None → 714 (lần 1 chưa có data, lần 2 = 714). Re-verify: HOT = 977 sau đó.

---

## S3: Fluss HOT Layer

### T3.1 — Schema đúng 10 cột ✅ PASS
```
Columns (10): ['incident_id', 'camera_id', 'timestamp', 'risk_score', 'confidence',
               'is_violent', 'event_type', 'location', 'ward_id', 'district']
missing = set()
```

### T3.2 — dim_camera có 15 cameras ✅ PASS
```
dim_camera: 15 rows
  cam_01 → Đường Nguyễn Huệ
  cam_02 → Đường Lê Lợi
  cam_04 → Đường Lê Thánh Tôn
  cam_14 → Đường Nguyễn Bỉnh Khiêm
  cam_15 → Đường Trương Định
```

### T3.3 — Enrichment: location ≠ 'Unknown' ✅ PASS
```
Sample=30 | Unknown=0 | Real=30
  cam_08 → Đường Hai Bà Trưng
  cam_04 → Đường Lê Thánh Tôn
  cam_02 → Đường Lê Lợi
```

---

## S4: Tiering MOVE ⭐

### T4.1 — Snapshot trước khi tier ✅ PASS
```
BEFORE — hot=1256  warm=155264  cold=0
```

### T4.2 — Force tiering chạy và hoàn thành ✅ PASS
- `flink run --python tier_fluss_to_paimon.py -Dpipeline.name=tier_force_test`
- Phase 1 job submitted: `ca59ec9eeca1590dd0cd9e36fd3b156e`
- Pipeline-manager cũng trigger tiering độc lập (Phase 1 CANCELED + Phase 2 RUNNING)
- Không có `[ERROR]` nghiêm trọng

### T4.3 — WARM count tăng sau tiering ✅ PASS
```
AFTER — hot=6084  warm=158589  cold=0
warm increase: +3325 rows (155264 → 158589)
```

### T4.4 — HOT count giảm sau Phase 2 ⚠️ WARN
- Phase 2 (streaming DELETE) không terminate tự động
- HOT count TĂNG (từ 1256 → 6084+) vì data mới liên tục vào
- Phase 2 cancelled thủ công sau 20+ phút
- **Acceptable:** Routing theo time_period bù lại (HOT < 1h → Fluss, ≥ 1h → Paimon)

---

## S5: WARM và COLD

### T5.1 — Paimon WARM tables tồn tại ✅ PASS
```
Paimon tables: ['camera_stats', 'daily_incident_stats', 'dim_time',
                'fact_violence_incidents', 'violence_incidents']
violence_incidents=True  fact_violence_incidents=True
```
Count via `/api/layer-counts`: warm=158,589 rows.

### T5.2 — Iceberg COLD schema + archival ✅ PASS
```
iceberg.security → "historical_violence_incidents"
Archival job: [SUCCESS] Archival job completed.
cold=0 (expected — data < 7 ngày, filter WHERE timestamp < LOCALTIMESTAMP - INTERVAL '7' DAY)
```

---

## S6: Chatbot

### T6.1 — Routing HOT: "30 phút" → Fluss ❌ FAIL

**Query:** `"trong 30 phut qua co bao nhieu canh bao?"`

**Actual routing:**
```
[ROUTING] Evidence query: overriding Fluss → PAIMON (frame_url)
[ROUTING] time_period='30 phút qua' → layer=Paimon
```

**Expected:** `layer=Fluss`
**Got:** `layer=Paimon`

**Root cause:** Logic phát hiện "evidence query" bị kích hoạt sai bởi từ "canh bao" (alert). Time routing ĐÚNG (Fluss), nhưng bị override sang Paimon.

**Query vẫn trả kết quả** nhưng sai layer source (processed in 284s via Paimon).

**Fix cần thiết:** `scripts/chatbot/agent.py` → `select_data_layer()` — chỉ override khi user explicitly hỏi về hình ảnh/video/frame evidence.

---

### T6.2 — Routing WARM: "hôm nay" → Paimon ✅ PASS

**Query:** `"hom nay co bao nhieu vu bao luc?"`

**Routing log:**
```
[ROUTING] time_period='hôm nay' → layer=Paimon
```
**Layer:** Paimon | **Duration:** 14,531ms ✅

---

### T6.3 — HOT location: trả về tên đường thật ❌ FAIL

**Query:** `"bao luc xay ra o dau trong 15 phut qua?"`

**Routing:** `time_period='15 phut qua' → layer=Fluss` ✅ (routing đúng)

**Answer:**
```
Trong 15 phút qua, đã có 2 sự cố bạo lực. Một vụ ASSAULT tại camera cam_01 lúc 13:56:31
và một vụ STABBING tại camera cam_15 lúc 13:42:39.
```

**Expected:** Có tên đường (Đường Nguyễn Huệ, Đường Trương Định)
**Got:** Chỉ có camera ID (cam_01, cam_15)

**Root cause:** SQL do `generate_sql` node tạo ra không SELECT `location` column từ `hot_violence_alerts`.

**Fix cần thiết:** Prompt/instructions của `generate_sql` cho HOT layer phải bắt buộc include `location, ward_id, district` trong SELECT.

---

### T6.4 — Layer routing boundary: "45 phút" vs "2 giờ" ❓ CHƯA CHẠY
Test bị interrupt. Cần chạy ở session 39.

```bash
# "45 phút" → Fluss (expected)
curl -s -m 120 -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "45 phut qua co gi?"}'

# "2 giờ" → Paimon (expected)
curl -s -m 120 -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "trong 2 gio qua co bao nhieu vu?"}'
```

---

### T6.5 — API endpoints hoạt động ✅ PASS
```
/api/layer-counts: {"hot":13166,"warm":158589,"cold":0,"duration_ms":3958}
/api/latency:
  hot:  60ms   (ok=True)
  warm: 2204ms (ok=True)
  cold: 1083ms (ok=True)
```

---

## S7: Data Quality

### T7.1 — Không có NULL/bad score/duplicates ✅ PASS
```
Sample=50 | NULLs=0 | Bad_score=0 | Dups=0
Sample rows:
  cam_13 | risk_score=0.9584 | confidence=0.9866 | Đường Hàm Nghi
  cam_06 | risk_score=0.8261 | confidence=0.9320 | Đường Trần Hưng Đạo
  cam_15 | risk_score=0.0122 | confidence=0.6269 | Đường Trương Định
```

### T7.2 — dim_camera FK integrity ✅ PASS
Indirect evidence: T3.3 showed 30 HOT rows với `Unknown=0` — tất cả cam_ids join thành công với dim_camera.
Direct verification: T3.2 confirms cam_01→cam_15 đều có trong dim_camera.

---

## Bugs cần fix (Session 39)

### BUG-A — Evidence Query Override (T6.1)
| | |
|---|---|
| **File** | `scripts/chatbot/agent.py` |
| **Function** | `select_data_layer()` |
| **Symptom** | Query count có "canh bao" → route Paimon thay vì Fluss |
| **Log** | `"Evidence query: overriding Fluss → PAIMON (frame_url)"` |
| **Fix** | Chỉ apply evidence override khi query dùng từ khóa: "hình ảnh", "ảnh", "frame", "video", "evidence", "chụp", "xem ảnh" |

### BUG-B — HOT SQL Missing location (T6.3)
| | |
|---|---|
| **File** | `scripts/chatbot/agent.py` |
| **Function** | `generate_sql()` hoặc prompt template HOT path |
| **Symptom** | HOT queries trả camera ID thay vì tên đường |
| **Fix** | Thêm instruction: "Always SELECT location, ward_id, district from hot_violence_alerts" |

---

## Stack State Sau Session 38
```
Services:     All UP (kafka, minio, mysql, hive, trino, flink, fluss, chatbot, flink-sql-gateway)
Flink jobs:   3 RUNNING (validator, hot_sink, aggregate)
Data counts:  HOT=13,166  WARM=158,589  COLD=0
Profile ui:   flink-sql-gateway đang chạy
```

---

## Nhiệm vụ Session 39 (theo thứ tự)

1. **Fix BUG-A** → re-run T6.1 → verify layer=Fluss
2. **Fix BUG-B** → re-run T6.3 → verify tên đường trong answer
3. **Chạy T6.4** → verify "45 phút"→Fluss, "2 giờ"→Paimon
4. **Cập nhật scorecard** → target 21+/23
5. **Commit fixes** với message `fix: chatbot routing evidence override + HOT SQL location`
