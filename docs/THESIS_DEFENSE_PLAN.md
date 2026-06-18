# THESIS DEFENSE PLAN — Kế hoạch bảo vệ Khóa luận tốt nghiệp

**Dự án:** Realtime Violence Detection — Streamhouse Architecture
**Tác giả:** Nguyễn Ngọc Minh Nhật & Nguyễn Quốc Huy
**Cập nhật:** 2026-06-13
**Mục đích:** Nguồn tham chiếu duy nhất từ lúc này đến khi bảo vệ — tình trạng thật, kế hoạch hành động, câu hỏi defense, cách nói đúng.

---

## 0. Phân công (ai làm gì, ai bảo vệ gì)

| Thành phần | Người làm | Người bảo vệ chính |
|---|---|---|
| Streamhouse pipeline (Kafka/Flink/Fluss/Paimon/Iceberg/Trino) | Huy | Huy |
| VioMoViNet (MoViNet đa-GPU, inference server) | Huy | Huy |
| Chatbot (Text-to-SQL + RAG) | Nhật | Nhật |
| Violence-Urban-Safety-UI (React) | Chung | Chung |

> ⚠️ **Quy tắc:** Cả hai cùng bảo vệ chung → mỗi người phải hiểu được phần người kia ở mức giải thích vanh vách. Đừng để tình trạng "phần đó bạn tui làm, tui không rành".

---

## 1. Tình trạng thật của dự án (đánh giá trung thực)

### 1.1 Điểm mạnh thật sự (nơi điểm đến từ)

- **⭐⭐⭐⭐ Kiến trúc Streamhouse tiering** (Fluss→Paimon→Iceberg, không dual-write) — SOTA 2024–2025, hiếm ở trình độ cử nhân. **Đây là đóng góp chính.**
- **⭐⭐⭐ VioMoViNet redesign v1→v2** — model load 1 lần/GPU + thread share → từ 6–8 stream lên ~40–60. Quyết định *không dùng MirroredStrategy* (vì streaming-state mỗi stream riêng) có lý lẽ kỹ thuật rõ.
- **⭐⭐⭐ Data contract + quarantine** — schema-on-write, route sự kiện lỗi — practice production-grade.
- **Tài liệu tốt**, E2E tests (22/23 PASS — nhưng xem §1.3).

### 1.2 Điểm cần trung thực ghi nhận

- **Chatbot là standard self-correcting Text-to-SQL**, KHÔNG phải Agentic RAG. **Không có ChromaDB** (các file `ingest.py`/`rag_store.py`/`download_model.py` được claim trong docs nhưng **không tồn tại**; `schema_registry.py` chỉ là dict cứng). Đây là lớp consumption phía trên, không phải tâm điểm.
- **MoViNet accuracy reproduced = 79–81%**, không phải "best 84.66%" đang ghi trong docs. Phải báo cáo mean±std (xem P1b).
- **Chatbot dùng đúng kỹ thuật** cho loại dữ liệu của nó: Text-to-SQL cho warehouse có cấu trúc là *đúng hơn* RAG vector (RAG chỉ hợp cho tài liệu không cấu trúc). Không phải "thua RAG" — là "lựa chọn đúng".

### 1.3 ⚠️ RỦI RO LỚN NHẤT: Gap mock ↔ model thật

- Pipeline streamhouse mặc định chạy `inference-mock` (sự kiện tổng hợp). 22/23 E2E test PASS **nhờ mock**.
- VioMoViNet (model thật) **chưa nối** vào luồng Kafka → Fluss → dashboard.
- **Nếu giám khảo yêu cầu demo camera thật → MoViNet → dashboard thật, mà không có** → claim "realtime violence detection" sụp đổ.
- → Đây là **P0 tuyệt đối** (xem §3).

---

## 2. Kiến trúc tích hợp mong muốn (target)

```
Camera RTSP
   │
   ▼
VioMoViNet (MoViNet-A3, 2× RTX 2080 Ti)
   │  inference result {camera_id, is_violent, score, fight_probability, timestamp(epoch)}
   ▼
[ADAPTER — CHƯA CÓ]  ← P0: transform + Kafka produce
   │  contract-compliant event
   ▼
Kafka: urban-safety-raw (hoặc alerts — CHỐT 1 tên)
   ▼
Flink: data_contract_validator
   ├── valid   → hot-violence-alerts-valid
   └── invalid → urban-safety-quarantine
   ▼
Fluss HOT → (tier 30min) → Paimon WARM → (archive 02:00) → Iceberg COLD
   ▼
Chatbot (Text-to-SQL) + React Dashboard
```

---

## 3. KẾ HOẠCH HÀNH ĐỘNG (theo độ ưu tiên)

### 🔴 P0 — Adapter luồng thật (CẤP BÁCH, làm đầu tiên)

**Ai:** Huy · **Thời gian:** ~1 ngày · **Lý do:** không có luồng thật = claim realtime là giấy.

**Vấn đề đã verify (2026-06-13):**
1. VioMoViNet **KHÔNG có Kafka producer** (grep 0 hit, requirements.txt không có kafka lib). Là server request/response thuần.
2. Schema output lệch Data Contract nặng → nếu ném raw vào Kafka, validator **REJECT 100%** vào quarantine.

**Bảng map schema (VioMoViNet → Data Contract):**

| Trường Contract | Bắt buộc | VioMoViNet có | Xử lý trong adapter |
|---|---|---|---|
| `event_id` | ✅ | ❌ | sinh `uuid4` |
| `camera_id` (`^cam_\d{2}$`) | ✅ | free-form string | normalize → `cam_01`..`cam_16`, map với `dim_camera` |
| `timestamp` (ISO-8601 UTC) | ✅ | epoch float | convert `datetime.utcfromtimestamp().isoformat()+"+00:00"` |
| `is_violent` (bool) | ✅ | `is_violent` | pass-through |
| `risk_score` [0,1] | ✅ | `score`/`fight_probability` | map = `fight_probability` |
| `confidence` [0,1] | ✅ | ❌ | derive = `fight_probability` (hoặc heuristic) |
| `event_type` (enum nếu violent) | ✅ | ❌ (model binary!) | `is_violent=true` → `"FIGHTING"`; heartbeat → `null` |

**Lựa chọn trigger adapter (cần quyết):**
- **`push` (KHUYẾN):** thêm `KafkaProducer` vào `StreamWorker` của VioMoViNet, emit ngay sau mỗi inference → real-time đúng tinh thần streamhouse. Cần sửa VioMoViNet + thêm `kafka-python`.
- **`poll`:** service riêng gọi `GET /api/stream/status/{cam}` mỗi N giây → forward. Không đụng VioMoViNet, nhưng trễ N giây.

**Gap ngữ nghĩa cần xử lý (giám khảo sẽ hỏi):**
Contract yêu cầu `event_type ∈ {FIGHTING, ASSAULT, STABBING, SHOOTING}` nhưng MoViNet chỉ binary. **Quyết định:** map `is_violent=true → "FIGHTING"`, ghi rõ trong luận văn "model phân loại nhị phân, phân loại loại bạo lực là hướng phát triển".

**Tên topic cần chốt:** `data-contracts.md` ghi `urban-safety-raw`, `README.md` ghi `urban-safety-alerts`. Pin 1 tên + verify nó đúng tên mà Flink validator đang source.

**Checklist hoàn thành P0:**
- [ ] Quyết định trigger: push / poll
- [ ] Chốt tên topic đúng (check `scripts/setup/create-topics.sh`)
- [ ] Viết adapter map đủ 7 trường
- [ ] `camera_id` khớp `dim_camera` (cam_01–cam_15) để temporal join không rớt
- [ ] **Test 1 event thật → ra `hot-violence-alerts-valid` (KHÔNG quarantine)**
- [ ] Test 1 event sai cố ý → vào quarantine (xác nhận validator còn chạy)
- [ ] Verify `api/layer-counts` → `hot > 0` tăng theo thời gian

---

### 🟡 P1a — Sửa docs chatbot (gỡ claim giả)

**Ai:** Nhật · **Thời gian:** ~2–3 giờ · **Lý do:** tránh bị bắt bẻ chữ "RAG".

- Gỡ "RAG / Agentic RAG / ChromaDB" khỏi: `README.md`, `CLAUDE.md`, `.claude/rules/chatbot-rag.md`, docstring `scripts/chatbot/components/sql_generator.py`.
- Đổi nhãn → **"Self-correcting Text-to-SQL with schema grounding"**.
- Xóa/xóa note các file claim nhưng không tồn tại: `ingest.py`, `rag_store.py`, `download_model.py`.

---

### 🟡 P1b — Báo cáo accuracy MoViNet trung thực

**Ai:** Huy · **Thời gian:** ~nửa ngày · **Lý do:** liêm chính học thuật.

- Chạy nhiều seed/runs MoViNet-A3 → báo cáo **mean±std** (reproduced ≈ 79–81%).
- **Không** dùng "best 84.66%" làm con số chính.
- Cập nhật `VioMoViNet/CLAUDE.md` + phần đánh giá trong luận văn.
- Câu biện luận: chọn MoViNet-A3 (mobile, 7.6M params) để đổi throughput đa-stream real-time trên 2080 Ti — tradeoff có ý nghĩa.

---

### 🟢 P2 — Benchmark cho luận văn (nơi điểm thật sự đến)

**Ai:** Huy · **Thời gian:** vài ngày · **Lý do:** đóng góp chính cần số liệu. **Phụ thuộc P0.**

Đổ số liệu vào `docs/THESIS_PERFORMANCE_EVALUATION.md`:
- **Latency query theo layer:** HOT (Fluss, <100ms), WARM (Paimon), COLD (Iceberg/Trino).
- **Throughput MoViNet:** streams/GPU, FPS/stream, latency/stream trên 2× 2080 Ti.
- **Accuracy** mean±std (liên kết P1b).
- Bảng so sánh A0 vs A3 (accuracy vs latency vs throughput).

---

### ⚪ P3 — Chatbot few-shot retrieval (TÙY THỜI GIAN)

**Chỉ nếu** rảnh sau P0–P2 + Nhật đồng ý. Là nâng cấp ROI cao nhất để chatbot "đúng nhãn RAG":
- Lưu cặp *câu hỏi→SQL*, retrieve tương tự (DAIL-SQL skeleton similarity), inject few-shot vào prompt sinh SQL.
- **KHÔNG** chunking tài liệu (sai kỹ thuật với data có cấu trúc).
- Bỏ qua nếu deadline eo hẹp.

---

## 4. Câu hỏi defense có thể gặp + trả lời mẫu

| Giám khảo hỏi | Trả lời đúng |
|---|---|
| *"Cho demo camera thật → MoViNet → dashboard, không phải mock"* | (P0 xong) bật luồng thật, chỉ `hot-violence-alerts-valid` có data + dashboard alert |
| *"Chatbot RAG của em chunk thế nào, embedding gì?"* | "Em **không** dùng vector RAG. Dữ liệu em truy vấn là warehouse có cấu trúc → Text-to-SQL là kỹ thuật đúng hơn. Em có self-correction loop khi SQL lỗi. RAG vector chỉ hợp cho tài liệu không cấu trúc." |
| *"Tại sao gọi là Agentic?"* | "Là workflow agent 6 node với loop tự sửa lỗi (execute→correct→execute, max 3), không phải agent tự chủ. Em chọn workflow determinism cho hệ thống an ninh real-time — routing phải predictable." |
| *"Accuracy MoViNet thấp (79–81%)?"* | "Tradeoff: MoViNet-A3 mobile (7.6M params) để đạt throughput đa-stream real-time trên 2080 Ti. Bảng so sánh accuracy vs latency vs throughput đi kèm." |
| *"Tại sao Fluss+Paimon+Iceberg, không dùng 1 DB?"* | "Tiering theo latency/retention: HOT<100ms, WARM CDC/ACID, COLD time-travel. Tránh dual-write bằng tiering tự động." |
| *"Paimon query qua Trino được không?"* | "Paimon connector JAR không có trên Maven → fallback Flink SQL Gateway. Đây là glue kỹ thuật." |
| *"Nếu DB trả 0 dòng mà chatbot vẫn báo có bạo lực?"* | "Anti-hallucination guard: regex phát hiện Gemini bịa số khi row_count=0 → override 'Không tìm thấy dữ liệu'." |
| *"Camera_id từ VioMoViNet khác dim_camera thì sao?"* | "Adapter normalize sang `cam_XX` + map dim_camera, không thì temporal join rớt." |

---

## 5. Cách nói đúng cho bảo vệ (positioning)

**KHÔNG nói:** "Chatbot của em là Agentic RAG xịn" (bị bắt bẻ).
**KHÔNG nói:** "Nó đơn giản" (tự hạ).
**NÓI:**
> *"Chatbot là lớp giao tiếp tự nhiên phía trên kiến trúc Streamhouse — lõi là Text-to-SQL có self-correction, dùng kỹ thuật đúng cho dữ liệu warehouse có cấu trúc. Đóng góp chính của em nằm ở **kiến trúc streaming 3-layer** bên dưới mà chatbot truy vấn tới, và **inference MoViNet đa-GPU**."*

Đặt chatbot đúng vị trí (consumption layer) → kiến trúc + inference lên ngôi.

---

## 6. Rủi ro & xử lý

| Rủi ro | Xử lý |
|---|---|
| Luồng thật không chạy được lúc demo | P0 + có video quay sẵn demo offline (phòng net/hardware fail) |
| Bị hỏi chữ "RAG" mà bí | P1a sửa docs + câu trả lời §4 |
| Accuracy bị chất vấn | P1b mean±std + biện luận tradeoff |
| Net lag / VM GCP chậm demo | Quay video trước, demo local nếu cần |
| Hỏi sâu phần người kia không rành | Mỗi người đọc code phần bạn kia đến mức giải thích được |

---

## 7. Thứ tự thực thi tuần tự

1. **P0** (luồng thật) — không gì quan trọng hơn.
2. **P1a** (docs chatbot) + **P1b** (accuracy) — song song.
3. **P2** (benchmark) — cần P0.
4. **P3** (few-shot) — nếu còn thời gian.

---

*Tiến độ theo dõi: dùng `/tasks` trong Claude Code (5 task P0–P3 đã setup).*
