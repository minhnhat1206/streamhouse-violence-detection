# Chatbot Architecture — Agentic RAG với LangGraph

> **Tài liệu kỹ thuật chi tiết** về kiến trúc chatbot trong hệ thống Smart Security Monitoring.
> Cập nhật lần cuối: 2026-05-02 (Phiên 22)

---

## 1. Tổng quan

Chatbot là lớp truy vấn ngôn ngữ tự nhiên (NLQ) cho toàn bộ Streamhouse pipeline. Người dùng đặt câu hỏi bằng **tiếng Việt**, chatbot tự động:

1. Hiểu ý định → trích xuất thời gian, địa điểm, loại metric
2. Chọn đúng layer dữ liệu (Fluss / Paimon / Iceberg)
3. Sinh SQL phù hợp với layer đó
4. Thực thi query → tự sửa nếu lỗi (tối đa 3 lần)
5. Trả lời bằng tiếng Việt + trích dẫn nguồn bắt buộc

```
Người dùng (tiếng Việt)
        ↓
  FastAPI /chat
        ↓
  LangGraph Agent (6 nodes)
        ↓
  ┌─────────────────────────────┐
  │  Fluss (HOT)    <1 giờ     │ ← Flink SQL Gateway :8083
  │  Paimon (WARM)  1h–7 ngày  │ ← Flink SQL Gateway :8083
  │  Iceberg (COLD) >7 ngày    │ ← Trino :8082
  └─────────────────────────────┘
        ↓
  Câu trả lời tiếng Việt + Citations
```

---

## 2. Tech Stack

| Thành phần | Công nghệ | Phiên bản |
|-----------|-----------|-----------|
| API Framework | FastAPI + Uvicorn | Python 3.11 |
| Agent Orchestration | LangGraph | 0.1.x |
| LLM (NLP + SQL gen) | Google Gemini 2.5 Flash | `gemini-2.5-flash` |
| Vector DB (RAG) | ChromaDB | 0.4.x |
| Query Engine (COLD) | PyTrino → Trino | 0.337.0 |
| Query Engine (WARM/HOT) | REST → Flink SQL Gateway | 1.18 |
| Object Store | MinIO (S3-compatible) | RELEASE.2024 |
| Container | Docker (1536m RAM, 1 CPU) | — |

---

## 3. Kiến trúc 6-Node LangGraph

```
START
  │
  ▼
┌─────────────────────┐
│  Node 1             │
│  understand_query   │  ← Gemini 2.5 Flash: parse Vietnamese intent
│                     │    → time_period, location, metric, intent_type
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Node 2             │
│  select_data_layer  │  ← Rule-based: map time_period → Fluss/Paimon/Iceberg
│                     │    → trino_catalog, table_name, data_layer
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Node 3             │
│  generate_sql       │  ← ChromaDB schema context + Gemini SQL generation
│                     │    → SQL string (Trino dialect or Flink SQL)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Node 4             │
│  execute_query      │  ← TrinoClient.route_query() → layer-specific execution
│                     │    → QueryResult(success, data, row_count, error)
└──────────┬──────────┘
           │
     ┌─────┴──────┐
     │ success?   │
     │            │
   YES            NO (retry_count < 3)
     │            │
     │            ▼
     │  ┌─────────────────────┐
     │  │  Node 5             │
     │  │  self_correct       │  ← Gemini error analysis → fix SQL
     │  │                     │    → loop back to execute_query
     │  └─────────────────────┘
     │
     ▼
┌─────────────────────┐
│  Node 6             │
│  generate_response  │  ← Gemini: format results → Vietnamese answer
│                     │    → final_answer + mandatory citations
└──────────┬──────────┘
           │
          END → ChatResponse JSON
```

### AgentState (LangGraph State Dict)

```python
class AgentState(TypedDict):
    # Input
    user_query: str           # "Hôm nay có bao nhiêu vụ bạo lực?"
    context: str
    request_id: str

    # Node 1 output
    intent: IntentSchema      # time_period, location, metric, confidence

    # Node 2 output
    selected_layer: LayerChoice   # FLUSS / PAIMON / ICEBERG
    trino_catalog: str            # "fluss" | "paimon" | "iceberg"
    table_name: str               # "hot_violence_alerts" | "violence_incidents" | "historical_..."

    # Node 3 output
    generated_sql: str

    # Node 4 output
    query_result: QueryResult     # success, data, row_count, error

    # Node 5
    retry_count: int              # 0–3
    retry_errors: list[str]

    # Node 6 output
    final_answer: str
    source_table: str
    data_layer: str
    time_period: str
    row_count: int
    response_confidence: float    # 0.0–1.0

    # Frame evidence (single-incident queries)
    incident_id: Optional[str]
    frame_url: Optional[str]
    frame_base64: Optional[str]

    duration_ms: int
```

---

## 4. Node Chi Tiết

### Node 1 — `understand_query` (Vietnamese NLP)

**Mục đích:** Trích xuất ý định từ câu hỏi tiếng Việt.

**Cơ chế:**
1. Gọi Gemini 2.5 Flash với prompt yêu cầu trả về JSON 5 trường
2. Parse JSON response → `IntentSchema`
3. Fallback: `_parse_intent_keywords()` nếu Gemini không khả dụng

**Gemini Prompt:**
```
Phân tích câu hỏi tiếng Việt và trích xuất ý định.
Trả về JSON: {time_period, location, metric, intent_type, query_confidence}

Câu hỏi: "24 giờ qua có bao nhiêu vụ bạo lực?"
→ {"time_period": "24 giờ qua", "location": null, "metric": "count",
   "intent_type": "statistics", "query_confidence": 0.95}
```

**Keyword Fallback (không cần Gemini):**
```python
# _parse_intent_keywords() — rule-based Vietnamese parser
"tháng trước/qua/này" → "tháng trước"
"tuần trước/qua" → "tuần trước"
"hôm qua" → "hôm qua"
"hôm nay/today" → "hôm nay"
"giờ trước/real-time" → "1 giờ qua"
```

---

### Node 2 — `select_data_layer` (Layer Routing)

**Mục đích:** Quyết định query vào layer nào dựa trên `time_period`.

**Thuật toán routing (quan trọng — thứ tự ưu tiên):**

```python
# Bước 1: Numeric regex TRƯỚC (độ tin cậy cao nhất)
match = re.search(r'(\d+)\s*(hour|giờ|gio|day|ngày|ngay|week|tuần|tuan|month|tháng|thang)s?', time_period)
if match:
    num, unit = match groups
    days = convert_to_days(num, unit)
    if days < 1:    → PAIMON  (Fluss workaround — sub-hour routed to warm)
    elif days <= 7: → PAIMON
    else:           → ICEBERG

# Bước 2: Keyword patterns (khi không có số)
"tháng trước/năm/quý/30 ngày" → ICEBERG
"hôm qua/tuần/7 ngày/hôm nay" → PAIMON
"vừa rồi/bây giờ/real-time"   → FLUSS (only explicit "right now")
```

**Ví dụ routing thực tế:**

| time_period (Gemini output) | days | Layer | Lý do |
|-----------------------------|------|-------|-------|
| "24 giờ qua" | 1.0 | PAIMON | numeric: 24 giờ = 1 ngày ≤ 7 ngày |
| "hôm nay" | keyword | PAIMON | keyword match |
| "7 ngày qua" | 7.0 | PAIMON | numeric: 7 ngày ≤ 7 ngày |
| "tháng trước" | keyword | ICEBERG | keyword match |
| "ngay bây giờ" | — | PAIMON | không match explicit "bây giờ" → default PAIMON |

> **Bug đã fix (Session 19):** "24 giờ qua" substring "giờ qua" từng match HOT pattern → routed sai sang Fluss. Fix: dùng numeric regex trước.

**Output state sau Node 2:**
```python
state["selected_layer"] = LayerChoice.PAIMON
state["trino_catalog"]  = "paimon"           # → Flink SQL Gateway
state["table_name"]     = "violence_incidents"
state["data_layer"]     = "Paimon"
```

---

### Node 3 — `generate_sql` (Text-to-SQL)

**Mục đích:** Sinh câu lệnh SQL từ intent + schema context từ ChromaDB.

**ChromaDB RAG Flow:**
```
user_query → ChromaDB semantic search (cosine similarity)
           → top-3 schema chunks (table definitions, column descriptions)
           → context injected vào SQL generation prompt
```

**SQL Generation (Gemini):**
```python
# Template + Gemini refinement
sql_generator.generate_from_intent(
    intent=intent,              # time_period, metric, location
    schema_context=chroma_results,  # RAG context
    table_name="violence_incidents"
)
```

**Time Period → WHERE clause:**
```python
"hôm nay"     → WHERE "timestamp" >= TIMESTAMP '2026-05-02 00:00:00'
"hôm qua"     → WHERE "timestamp" BETWEEN TIMESTAMP '...' AND TIMESTAMP '...'
"24 giờ qua"  → WHERE "timestamp" >= TIMESTAMP '2026-05-01 15:00:00'
"tháng trước" → WHERE "timestamp" >= TIMESTAMP '2026-04-01 00:00:00'
               AND    "timestamp" < TIMESTAMP '2026-05-01 00:00:00'
```

> **Note:** Trino dùng double-quote `"timestamp"` cho reserved keyword. Flink dùng backtick `` `timestamp` ``. Conversion xảy ra trong `_adapt_sql_for_flink()`.

---

### Node 4 — `execute_query` (Query Execution)

**Mục đích:** Thực thi SQL trên layer phù hợp.

```python
results = _trino_client.route_query(sql=sql, layer=selected_layer, timeout=180)
```

**TrinoClient.route_query() flow:**

```
FLUSS  → query_fluss()   → Flink SQL Gateway (port 8083)
PAIMON → query_paimon()  → Flink SQL Gateway (port 8083) + per-session Paimon catalog DDL
         ↳ fallback → Trino paimon catalog (if JAR available)
         ↳ fallback → Iceberg (if infrastructure error)
ICEBERG → query_iceberg() → PyTrino → Trino (port 8082)
```

**Paimon query execution detail (WARM layer):**

```python
# 1. Tạo Flink SQL Gateway session
POST /v1/sessions → sessionHandle

# 2. Init catalog (per-session)
CREATE CATALOG paimon_warm WITH (
  'type'='paimon',
  'warehouse'='s3://warehouse/paimon',
  's3.endpoint'='http://minio:9000',
  's3.access-key'='minioadmin',
  's3.secret-key'='minioadmin123',
  's3.path.style.access'='true'
)
USE CATALOG paimon_warm
USE `security`

# 3. Execute user SQL (Flink SQL dialect)
SELECT COUNT(*) FROM violence_incidents WHERE `timestamp` >= ...

# 4. Poll /result/{token} với pagination (nextResultUri)
#    → Kết quả xuất hiện sau 78–346s (Flink batch job trên MinIO)
#    → result_type = "EOS" → done
#    → streaming aggregate → poll cho đến khi stable (3 empty polls)

# 5. Cleanup session
DELETE /v1/sessions/{sessionHandle}
```

**Iceberg query execution (COLD layer):**
```python
# PyTrino → Trino coordinator:8082
conn = trino.dbapi.connect(host="trino-coordinator", port=8080, ...)
cursor.execute("SELECT * FROM iceberg.security.historical_violence_incidents ...")
# Latency: 1-3s (Parquet files trên MinIO qua Iceberg catalog)
```

---

### Node 5 — `self_correct` (Error Recovery)

**Mục đích:** Tự sửa SQL khi query thất bại, tối đa 3 lần.

**Trigger condition:**
```python
if not state["query_result"].success and state["retry_count"] < 3:
    → go to self_correct
else:
    → go to generate_response
```

**Self-correction strategies:**
```python
# Strategy 1: Gemini analyzes error + rewrites SQL
fixed_sql = sql_generator.fix_sql_error(
    sql=current_sql,
    error_msg=error_msg,
    schema_context=chromadb_context,
    retry_count=retry_count
)

# Strategy 2: Rule-based fallback
if "column" in error:    → remove ORDER BY
if "timeout" in error:   → reduce LIMIT 100 → LIMIT 10
else:                    → simple COUNT(*) query
```

**Retry log example:**
```
[attempt 1/3] Error: Column 'ts' not found → Gemini fix: use 'timestamp'
[attempt 2/3] Error: Type mismatch TIMESTAMP_LTZ → Gemini fix: CAST(NOW() AS TIMESTAMP(3))
[attempt 3/3] Success: 195,642 rows
```

---

### Node 6 — `generate_response` (Vietnamese Answer)

**Mục đích:** Tổng hợp kết quả query thành câu trả lời tiếng Việt + citations bắt buộc.

**Case 1: No data (row_count = 0)**
```
"Không tìm thấy dữ liệu cho câu hỏi của bạn trong khoảng thời gian 'hôm nay'.
 Vui lòng thử mở rộng phạm vi thời gian hoặc điều chỉnh bộ lọc.
 Nguồn: violence_incidents (Paimon)"
```

**Case 2: Has data → Gemini synthesis**
```python
prompt = f"""
Tổng hợp kết quả truy vấn thành câu trả lời tự nhiên bằng tiếng Việt.
Câu hỏi gốc: "{user_query}"
Dữ liệu kết quả (JSON): {results[:5]}
Tổng số hàng: {row_count}
Bảng nguồn: {source_table}

Yêu cầu:
1. Viết câu trả lời tự nhiên bằng tiếng Việt
2. Nêu các con số cụ thể từ kết quả
3. Thêm citation: "Nguồn: {source_table} ({data_layer}), {row_count} hàng"
4. KHÔNG bịa dữ liệu
"""
```

**Anti-hallucination guards:**
- Gemini chỉ nhận **tối đa 5 rows đầu tiên** của kết quả thực tế
- Citation bắt buộc: `source_table`, `data_layer`, `time_period`, `row_count`
- Nếu `row_count = 0` → trả lời "không tìm thấy" thay vì bịa số

**Frame evidence (single-incident queries):**
```python
if row_count == 1 and incident_id:
    frame_b64 = evidence_service.get_frame(
        incident_id=incident_id,
        camera_id=camera_id,
        incident_date=incident_date
    )
    # MinIO: s3://evidence-frames/{camera_id}/{YYYY-MM-DD}/{incident_id}.jpg
```

---

## 5. SQL Adaptation Layer

Một trong những điểm phức tạp nhất: SQL dialect khác nhau giữa Trino và Flink.

### `_adapt_sql_for_flink()` — Trino → Flink SQL

```python
# 1. Strip catalog prefix (gateway dùng USE CATALOG)
"paimon.security.violence_incidents" → "violence_incidents"
"iceberg.security.historical_..."   → stripped

# 2. Table name remapping
"hot_violence_alerts"            → "violence_incidents"
"historical_violence_incidents"  → "violence_incidents"

# 3. Reserved keyword quoting
"timestamp"  →  `timestamp`  (double-quote → backtick)

# 4. TIMESTAMP type fix
NOW()              → CAST(NOW() AS TIMESTAMP(3))
CURRENT_TIMESTAMP  → CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
```

### `_adapt_sql_to_iceberg()` — Fallback to Iceberg

Khi Paimon/Fluss unavailable, SQL được rewrite để query Iceberg thay thế:

```python
# Pass 1: Fully-qualified references (specific → general)
"paimon.security.violence_incidents"    → "iceberg.security.historical_violence_incidents"
"fluss.security.hot_violence_alerts"    → "iceberg.security.historical_violence_incidents"
"iceberg.security.violence_incidents"   → "iceberg.security.historical_violence_incidents"

# Pass 2: Unqualified table names (negative lookbehind regex)
r"(?<![.\w])violence_incidents\b"  → "iceberg.security.historical_violence_incidents"
```

> **Bug đã fix (Session 21):** Double-prefix issue — `historical_violence_incidents` bị match lại bởi regex `violence_incidents`, tạo ra `historical_historical_violence_incidents`. Fix: negative lookbehind `(?<![.\w])` ngăn match nếu đứng sau word char hoặc dot.

---

## 6. ChromaDB RAG Store

**Mục đích:** Cung cấp schema context cho SQL generation → ngăn hallucination table/column names.

**Schema ingested:**
```python
schemas = {
    "violence_incidents": {
        "columns": [
            {"name": "incident_id",  "description": "Unique incident identifier (UUID)"},
            {"name": "camera_id",    "description": "Camera identifier (cam_01 to cam_15)"},
            {"name": "timestamp",    "description": "Event timestamp (UTC)"},
            {"name": "risk_score",   "description": "AI risk score [0.0-1.0]"},
            {"name": "is_violence",  "description": "Boolean: violent event detected"},
            {"name": "location",     "description": "Physical location (Quận 1, TP.HCM...)"},
            {"name": "event_type",   "description": "FIGHTING/ASSAULT/STABBING/UNKNOWN"},
            ...
        ]
    },
    "daily_incident_stats": { ... },
    "camera_stats": { ... }
}
```

**Search flow:**
```python
# User query → semantic search → top-3 relevant chunks
results = chromadb.search_schema(user_query, top_k=3)
# Injected vào Gemini SQL generation prompt
```

**Auto-refresh:** `DataIngestor` cập nhật ChromaDB mỗi 5 phút từ live schema.

---

## 7. API Endpoints

### `POST /chat` — Main endpoint

```http
POST http://localhost:5002/chat
Content-Type: application/json

{
  "query": "24 giờ qua có bao nhiêu vụ bạo lực?"
}
```

```json
{
  "answer": "Trong 24 giờ qua, đã ghi nhận tổng cộng 5 vụ việc bạo lực...",
  "sql_used": "SELECT COUNT(*) FROM violence_incidents WHERE `timestamp` >= ...",
  "citations": {
    "source_table": "violence_incidents",
    "data_layer": "Paimon",
    "time_period": "24 giờ qua",
    "row_count": 142111
  },
  "layer": "Paimon",
  "confidence": 0.9025,
  "duration_ms": 279700,
  "frame_base64": null,
  "frame_url": null
}
```

### `GET /health`
```json
{
  "status": "ok",
  "services": {
    "api": "ok",
    "agent_initialized": true,
    "config_valid": true
  },
  "version": "2.0.0"
}
```

### `GET /api/evidence/{incident_id}/frame?format=base64`
```json
{
  "incident_id": "inc_001",
  "frame_base64": "/9j/4AAQSkZJRgAB...",
  "content_type": "image/jpeg"
}
```

### `POST /webhook/chat` — n8n integration
Same as `/chat`, designed for automated workflows.

---

## 8. Query Latency Profile

| Layer | Query Type | Latency | Backend |
|-------|-----------|---------|---------|
| Fluss (HOT) | SELECT * LIMIT 5 | 12-30s via Gateway | Flink SQL Gateway |
| Paimon (WARM) | SELECT LIMIT | ~78s | Flink batch on MinIO |
| Paimon (WARM) | COUNT(*) | ~122s | Flink batch on MinIO |
| Paimon (WARM) | SUM/GROUP BY/AGGREGATE | 275–346s | Flink batch on MinIO |
| Iceberg (COLD) | Any | 1–5s | Trino + Parquet on MinIO |
| Chatbot total (PAIMON) | NLP + SQL + query + format | **280–310s** | LangGraph pipeline |
| Chatbot total (ICEBERG) | NLP + SQL + query + format | **20–30s** | LangGraph pipeline |

> **Bottleneck:** Paimon queries require Flink to spawn a batch job, read ORC files from MinIO, process data, and return results. This is inherent to Paimon's LSM-tree architecture on object storage. Total: 5min per query.

---

## 9. Startup Sequence

```
Docker container starts
    ↓
FastAPI lifespan(@asynccontextmanager)
    ↓
1. validate_config()           ← Check env vars (GEMINI_API_KEY, TRINO_HOST, etc.)
2. ChromaDBWrapper()           ← Load/create persistent collection
3. ingest_schema(tables)       ← Load 3 table schemas into ChromaDB
4. TrinoClient()               ← Connect to Trino + Flink SQL Gateway
5. SQLGenerator(gemini_key)    ← Init Gemini model
6. EvidenceService(minio)      ← MinIO client + LRU frame cache (100 items)
7. DataIngestor(chromadb, 300) ← Start background schema refresh (every 5min)
8. set_components(...)         ← Inject all components into agent module globals
9. create_agent_graph()        ← Compile LangGraph StateGraph
    ↓
app_state["initialized"] = True
    ↓
Ready to serve requests (~90s startup thanks to cached ONNX model volume)
```

---

## 10. Error Handling Matrix

| Lỗi | Xảy ra ở | Xử lý |
|-----|---------|-------|
| Gemini API không khả dụng | Node 1, 3, 6 | Fallback keyword parser / template SQL / format_response_fallback |
| Paimon SQL Gateway 500 | Node 4 | Retry 3x → fallback Iceberg |
| Paimon SQL timeout (>240s) | Node 4 | Return partial results hoặc empty |
| Iceberg/Trino connection refused | Node 4 | Retry 3x → "Lỗi: không thể truy vấn" |
| SQL syntax error | Node 5 | Gemini fix → re-execute (max 3x) |
| row_count = 0 | Node 6 | "Không tìm thấy dữ liệu" (không bịa) |
| Frame not found in MinIO | Node 6 | Log warning, tiếp tục (frame = null) |
| ChromaDB search fail | Node 3 | Empty context, vẫn generate SQL |

---

## 11. Files & Responsibilities

```
scripts/chatbot/
├── main.py                    ← FastAPI app, lifespan, routes (/chat, /health, /evidence)
├── agent.py                   ← LangGraph 6-node graph definition + AgentState
├── config.py                  ← Settings (env vars: GEMINI_API_KEY, TRINO_HOST, etc.)
├── logger.py                  ← Structured logging helpers
├── app.py                     ← Alternative entry point (legacy)
├── ingest.py                  ← Schema ingestion script (run once)
├── rag_store.py               ← ChromaDB collection management
├── download_model.py          ← ONNX model download on first run
└── components/
    ├── chromadb_wrapper.py    ← ChromaDB persistent client + ingest + search
    ├── trino_client.py        ← Layer routing, Flink Gateway REST, PyTrino, SQL adaptation
    ├── sql_generator.py       ← Gemini SQL generation + time period parsing + validation
    ├── evidence_service.py    ← MinIO frame retrieval + LRU cache
    └── data_ingest.py         ← Background schema refresh (every 5 min)
```

---

## 12. Known Limitations & Future Work

### Giới hạn hiện tại

| Vấn đề | Nguyên nhân | Impact |
|--------|-------------|--------|
| Paimon query ~300s | Flink batch job trên MinIO mỗi lần query | Chatbot WARM queries chậm |
| Fluss không query được qua SQL | Plugin JAR không có trong Gateway | HOT layer chỉ verify qua job RUNNING |
| TC1/TC2 "no data" cho "hôm nay" | Timestamp data là ngày hôm qua/trước | Data freshness issue |
| Chatbot sequential queue | FastAPI single-threaded agent invoke | Concurrent users phải chờ nhau |

### Future Work

1. **Query Caching** — Cache kết quả Paimon trong Redis/memory (TTL 5 phút) → lần query thứ 2 trả về ngay
2. **Fluss Catalog Plugin** — Deploy `fluss-connector-*.jar` vào Flink SQL Gateway → enable Fluss HOT queries
3. **Async Agent** — Chạy LangGraph agent trong separate thread pool → không block FastAPI event loop
4. **Pre-aggregation** — Chạy Paimon queries theo schedule (mỗi 5 phút) → materialise kết quả phổ biến
5. **Circuit Breaker** — Detect Paimon Gateway unavailable sớm → fallback ngay sang Iceberg thay vì timeout 240s

---

## 13. Ví dụ End-to-End

**Query:** `"Tong cong co bao nhieu su co trong 7 ngay qua?"`

```
Node 1 (understand_query):
  → Gemini: {"time_period": "7 ngày", "metric": "count", "intent_type": "statistics"}

Node 2 (select_data_layer):
  → "7 ngày" → numeric: 7 days ≤ 7 → PAIMON
  → table: violence_incidents, catalog: paimon

Node 3 (generate_sql):
  → ChromaDB: finds columns timestamp, is_violence, location
  → Gemini SQL:
     SELECT COUNT(*) as total,
            SUM(CASE WHEN is_violence THEN 1 ELSE 0 END) as violent_count
     FROM violence_incidents
     WHERE `timestamp` >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '7' DAY

Node 4 (execute_query):
  → _adapt_sql_for_flink(): strip prefix, fix timestamp type
  → Flink SQL Gateway session created
  → CREATE CATALOG paimon_warm WITH (...)
  → USE CATALOG paimon_warm; USE security
  → Execute query... (polling 285s)
  → Result: [{total: 213906, violent_count: 6}]

Node 5 (self_correct): SKIPPED (success=True)

Node 6 (generate_response):
  → Gemini synthesis:
     "Trong 7 ngày qua, đã ghi nhận tổng cộng 6 sự cố.
      Các sự cố chủ yếu xảy ra tại TP. Hồ Chí Minh, Quận 1..."
  → Citations: source=violence_incidents, layer=Paimon, rows=213,906

API Response:
  {
    "answer": "Trong 7 ngày qua, đã ghi nhận tổng cộng 6 sự cố...",
    "layer": "Paimon",
    "duration_ms": 285400,
    "citations": {"row_count": 216068, "data_layer": "Paimon", ...}
  }
```

**Đây là query thực tế đã chạy thành công ngày 2026-05-02, 285 giây, 213,906–216,068 rows scanned.**

---

*Tài liệu này được tạo bởi Claude (Session 22) — 2026-05-02*
*Chi tiết implementation: `scripts/chatbot/agent.py`, `scripts/chatbot/components/trino_client.py`*
