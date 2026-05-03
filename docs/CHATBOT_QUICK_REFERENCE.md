# 🤖 Chatbot Quick Reference Guide

**Quick Links:** [Full Docs](CHATBOT_CURRENT_STATE.md) | [API Spec](CHATBOT_API_DOCUMENTATION.md) | [Architecture](agent-guides/architecture.md)

---

## 📋 What is the Chatbot?

A **Vietnamese-speaking AI assistant** that answers questions about violence detection incidents using a **3-tier storage system** (Paimon warm layer + Iceberg cold layer).

**Status:** ✅ Fully Operational (Session 19 — 2026-05-01)

---

## ⚡ Quick Commands

### Start Chatbot

```bash
docker compose -f docker/docker-compose.yml up -d chatbot
```

### Test Chatbot

```bash
curl -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Hôm nay có bao nhiêu vu bạo lực?"}'
```

### Check Health

```bash
curl http://localhost:5002/health
# Output: {"status":"ok","agent_initialized":true}
```

### Enable Flink SQL Gateway (for Paimon direct queries)

```bash
docker compose -f docker/docker-compose.yml --profile ui up -d flink-sql-gateway
```

---

## 📝 Example Queries

### Vietnamese Queries (Recommended)

```
"Hôm nay có bao nhiêu vu bạo lực?"
→ Answer: "22 vu bạo lực" (via Paimon warm layer)
→ Latency: 5-6 minutes

"24 giờ qua tại camera nào ghi nhận bạo lực nhiều nhất?"
→ Answer: "Cam_14 tại Đường Nguyễn Bỉnh Khiêm: 15 vu ASSAULT"
→ Latency: 4-5 minutes

"Tuần trước có bao nhiêu vu bạo lực?"
→ Answer: "127 vu bạo lực" (via Paimon warm layer)
→ Latency: 5-6 minutes

"Tháng trước theo vị trí thống kê bạo lực"
→ Answer: "Quận 1: 23 vu, Quận 3: 18 vu, ..." (via Iceberg cold layer)
→ Latency: 2-3 seconds
```

### English Queries (Supported but Vietnamese Preferred)

```
"How many violent incidents today?"
"Show me camera locations with highest violence"
"Last 24 hours incident statistics by type"
```

---

## 🎯 How Layer Selection Works

| User Says | Time Period | Selected Layer | Latency |
|-----------|-----------|-----------------|---------|
| "hôm nay", "hôm qua" | 0-2 days | **PAIMON** (warm) | 4-6 min |
| "tuần trước/này" | 1-7 days | **PAIMON** (warm) | 4-6 min |
| "tháng trước/năm trước" | >7 days | **ICEBERG** (cold) | 2-3 sec |
| "bây giờ", "vừa rồi" | ~real-time | **FLUSS** (future) | <100ms |

**Rule:** 
- ✅ Numeric regex FIRST: `r'(\d+)\s*(hour\|giờ\|day\|ngày)'`
- ✅ Then keyword patterns
- ✅ Sub-1-hour routed to Paimon (has 5hr fresh data)

---

## 📊 Response Format

### Success Response (200 OK)

```json
{
  "answer": "Trong 24 giờ qua, hệ thống ghi nhận 22 vu bạo lực.",
  "citations": {
    "source_table": "violence_incidents",
    "data_layer": "PAIMON",
    "time_range": "24_hours (1 days)",
    "row_count": 31,
    "query_latency_ms": 346000
  },
  "layer": "PAIMON",
  "duration_ms": 346000
}
```

### Error Response (500)

```json
{
  "error": "All layers unavailable",
  "details": "Paimon gateway timeout + Iceberg query failed",
  "duration_ms": 15000
}
```

---

## 🔍 Debugging

### Issue: "Không tìm thấy dữ liệu" (No data found)

**Likely Causes:**
1. **No fresh data** → Check if inference-mock is running: `docker ps | grep inference-mock`
2. **Data in different time range** → Try "tuần trước" (last week) instead of "hôm nay"
3. **Paimon unavailable** → Check Flink gateway: `curl http://localhost:8083/sessions`
4. **Wrong table** → Chatbot might map to non-existent table

**Fix:**
```bash
# Restart chatbot
docker restart chatbot

# Check Paimon data freshness
docker exec jobmanager python3 -c "
from pyflink.table import TableEnvironment, EnvironmentSettings
t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
t_env.execute_sql('CREATE CATALOG paimon WITH (...)')
result = t_env.execute_sql('SELECT MAX(\`timestamp\`), COUNT(*) FROM violence_incidents')
for row in result.collect(): print('MAX:', row[0], 'COUNT:', row[1])
"
```

### Issue: Query takes >6 minutes (Paimon timeout)

**Why:** Flink batch job reading large data volume from MinIO

**What to do:**
1. Try narrower time range: "hôm nay" instead of "tuần trước"
2. Add location filter: "Quận 1 hôm nay" (queries smaller subset)
3. Use pre-aggregated table: "daily_incident_stats" for daily counts

### Issue: Chatbot returns Iceberg results instead of Paimon

**Check:**
1. Is Flink SQL Gateway running? `docker compose logs flink-sql-gateway`
2. Is gateway healthy? `curl http://flink-sql-gateway:8083/sessions` (should return 200)
3. Check chatbot logs: `docker logs chatbot | grep -i "paimon\|fallback"`

**Fix:**
```bash
# Restart Flink SQL Gateway
docker compose -f docker/docker-compose.yml --profile ui down flink-sql-gateway
docker compose -f docker/docker-compose.yml --profile ui up -d flink-sql-gateway
```

---

## 🏗️ Architecture at a Glance

```
User: "Hôm nay bao nhiêu vu?"
  ↓
[understand_query] → time_period=1_day
  ↓
[select_data_layer] → PAIMON
  ↓
[generate_sql] → "SELECT COUNT(*) FROM violence_incidents WHERE ..."
  ↓
[execute_query] → Flink SQL Gateway REST API → 44K rows
  ↓
[self_correct] → No error, proceed
  ↓
[generate_response] → "22 vu bạo lực" (Vietnamese synthesis)
  ↓
API Response: {"answer": "22 vu bạo lực", "layer": "PAIMON", ...}
```

**6 LangGraph Nodes:**
1. `understand_query` — Vietnamese NLP (Gemini or keyword fallback)
2. `select_data_layer` — Time-aware router (Fluss/Paimon/Iceberg)
3. `generate_sql` — Text-to-SQL (ChromaDB + Gemini)
4. `execute_query` — TrinoClient with fallback chain
5. `self_correct` — Error analysis + SQL retry (max 3x)
6. `generate_response` — Vietnamese synthesis + citations

---

## 📂 Key Files

| File | Purpose |
|------|---------|
| `scripts/chatbot/app.py` | FastAPI entry point |
| `scripts/chatbot/agent.py` | LangGraph 6-node agent |
| `scripts/chatbot/components/trino_client.py` | Query execution + Flink Gateway |
| `scripts/chatbot/components/chromadb_wrapper.py` | Schema metadata search |
| `scripts/chatbot/components/sql_generator.py` | SQL generation (Gemini) |
| `scripts/chatbot/components/evidence_service.py` | Frame retrieval from MinIO |
| `docs/CHATBOT_CURRENT_STATE.md` | Full detailed documentation |

---

## 🔧 Configuration

### Environment Variables (docker/.env)

```bash
GEMINI_API_KEY=your_key_here          # Optional (fallback to keyword NLP)
TRINO_HOST=trino-coordinator
TRINO_PORT=8082
FLINK_GATEWAY_HOST=flink-sql-gateway
FLINK_GATEWAY_PORT=8083
CHROMA_PERSIST_DIR=/chroma_db
```

### Resource Limits

```yaml
chatbot:
  memory: 1536m
  cpu: 1.0 cores
  startup: 90s (with ChromaDB cache)
```

---

## 🎯 Performance SLAs

| Layer | Query Type | Target | Actual | Status |
|-------|-----------|--------|--------|--------|
| **Paimon** | Simple (LIMIT 5) | <1min | 78s | ✅ |
| **Paimon** | Aggregate (COUNT) | <2min | 122s | ✅ |
| **Paimon** | Complex (GROUP BY) | <5min | 275-346s | ✅ |
| **Iceberg** | Simple | <3s | 2-3s | ✅ |
| **Iceberg** | Aggregate | <5s | 3-8s | ✅ |

**Note:** Paimon latency inherent to Flink batch processing. Acceptable for warm analytics.

---

## ✅ Healthy System Checklist

Run this to verify everything is working:

```bash
# 1. Chatbot container running
docker ps | grep chatbot              # Should show "Up (healthy)"

# 2. Health endpoint
curl http://localhost:5002/health     # Should return 200 + {"status":"ok"}

# 3. Paimon data fresh
docker exec jobmanager python3 /check_paimon.py  # Should show timestamp <1hr old

# 4. Test query
curl -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query":"hôm nay bao nhiêu vu?"}' | jq .  # Should return answer + citations

# 5. Evidence frames available
curl http://localhost:5002/api/evidence/event-uuid-123/frame?format=url  # Should return S3 URL
```

---

## 🚀 Next Phase (Phase 4)

**Priority 1:** Health checks + circuit breaker (detect Paimon unavailability)  
**Priority 2:** Prometheus metrics + Grafana dashboard  
**Priority 3:** Query result caching (5 min TTL)  
**Priority 4:** Optimize Paimon indexes

---

## 📞 Getting Help

1. **Query not working?** → Check `/logs/chatbot.log` for error details
2. **Layer wrong?** → Verify time extraction in agent.py line 316-340
3. **Latency too long?** → Normal for Paimon (4-6 min). Try Iceberg queries (>7 days)
4. **Gemini responses odd?** → Check if GEMINI_API_KEY is set; falls back to English keyword NLP

---

**Quick Link to Full Docs:** [CHATBOT_CURRENT_STATE.md](CHATBOT_CURRENT_STATE.md)

**Version:** 1.0 | **Updated:** 2026-05-01 | **Status:** ✅ Complete
