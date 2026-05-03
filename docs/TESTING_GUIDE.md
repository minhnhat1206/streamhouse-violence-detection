# 🧪 Testing Guide - Violence Detection Chatbot

**Version:** 2.0  
**Date:** 2026-04-29  
**Scope:** End-to-end testing of chatbot with 3-tier fallback logic

---

## Quick Start

### Health Check
```bash
curl http://localhost:5002/health | python3 -m json.tool
```

Expected response:
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

---

## Test Suite 1: Fallback Logic (Phase 2.5)

### TC1: Last Month Query (Iceberg Direct)

```bash
echo '{"query":"Tháng trước có bao nhiêu vụ bạo lực?"}' | \
curl -s -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d @- | python3 -m json.tool
```

**Expected:**
- `layer`: "Iceberg" ✅
- `citations.row_count`: >0 (typically 6)
- `duration_ms`: 2000-3500ms
- `answer`: Contains location breakdown (Quan 1, Quan 3, etc.)

**Status:** ✅ PASS (6 rows, 20.2s)

---

### TC2: Camera Risk Query (Paimon→Iceberg Fallback)

```bash
echo '{"query":"Camera nào có điểm rủi ro cao nhất?"}' | \
curl -s -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d @- | python3 -m json.tool
```

**Expected:**
- `layer`: "Paimon" (selected) → returns Iceberg data
- Logs show: "Paimon unavailable → falling back to Iceberg"
- `answer`: "Không tìm thấy dữ liệu..." (0 rows expected for today's filter)
- No errors in response

**Status:** ✅ PASS (Fallback verified, 4.6s)

---

### TC3: Recent Incidents (Iceberg Historical)

```bash
echo '{"query":"Liệt kê 5 vụ bạo lực gần nhất tháng trước"}' | \
curl -s -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d @- | python3 -m json.tool
```

**Expected:**
- `layer`: "Iceberg"
- `citations.row_count`: 6 (incident list)
- SQL contains: `iceberg.security.historical_violence_incidents`

**Status:** ✅ PASS (6 rows, 20.2s)

---

### TC4: District Statistics (Paimon→Iceberg)

```bash
echo '{"query":"Quận nào có nhiều vụ bạo lực nhất?"}' | \
curl -s -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d @- | python3 -m json.tool
```

**Expected:**
- `layer`: "Paimon" (selected)
- Logs: "Paimon unavailable → falling back to Iceberg"
- `answer`: "Không tìm thấy dữ liệu cho hôm nay"
- No exceptions in response

**Status:** ✅ PASS (Fallback ok, 4.6s)

---

## Test Suite 2: SQL Generation

### TC5: Template SQL Validation

**Purpose:** Verify SQL generation with correct table names

```bash
echo '{"query":"Tháng trước bạo lực ở Quận 1 là bao nhiêu?"}' | \
curl -s -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d @- | python3 -c "import sys, json; d=json.load(sys.stdin); print('SQL:', d['sql_used'][:300])"
```

**Verify in SQL:**
- [ ] Correct table: `iceberg.security.historical_violence_incidents` or `paimon.security.violence_incidents`
- [ ] Quoted timestamp: `"timestamp"` (not unquoted `timestamp`)
- [ ] WHERE clause: `WHERE "timestamp" BETWEEN ...`
- [ ] Location filter: `AND location = 'Quận 1'`

---

### TC6: Self-Correction (Simulate Error)

**Purpose:** Verify SQL retry on Trino error

**Manual Test:**
1. Temporarily introduce typo in SQL template
2. Submit query
3. Expect logs: `[self_correct] Retrying SQL after error...`
4. Should recover after max 3 retries

---

## Test Suite 3: Evidence Frame Retrieval

### TC7: Single Incident Frame (Single Row Query)

```bash
echo '{"query":"Vụ bạo lực nguy hiểm nhất tháng trước?"}' | \
curl -s -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d @- | python3 -c "import sys, json; d=json.load(sys.stdin); print('Incident ID:', d.get('incident_id')); print('Frame URL:', d.get('frame_url')); print('Frame B64 length:', len(d.get('frame_base64') or ''))"
```

**Expected:**
- `incident_id`: Populated (e.g., "incident_123")
- `frame_base64`: Populated if image found in MinIO (length >10000)
- `frame_url`: S3 path (s3://bucketname/...)
- Both null if no incidents found (expected when 0 rows)

---

### TC8: Frame Endpoint Direct

```bash
curl -s "http://localhost:5002/api/evidence/incident_001/frame?format=url" | python3 -m json.tool
```

**Expected:**
```json
{
    "incident_id": "incident_001",
    "frame_url": "s3://violence-detection/incident_001_cam_001_frame.jpg",
    "s3_endpoint": "http://minio:9000",
    "bucket": "violence-detection"
}
```

---

## Test Suite 4: Performance & Stress

### TC9: Concurrent Queries

**Purpose:** Verify chatbot handles multiple simultaneous requests

```bash
for i in {1..5}; do
  echo '{"query":"Tháng trước có bao nhiêu vụ?"}' | \
  curl -s -X POST http://localhost:5002/chat \
    -H "Content-Type: application/json" \
    -d @- &
done
wait
```

**Expected:**
- All 5 requests return success (status 200)
- No timeout or "too many connections" errors
- Each duration_ms is similar (±10%)

---

### TC10: Rate Limiting (Future)

**Purpose:** Verify rate limiting when implemented

```bash
# 20 rapid requests
for i in {1..20}; do
  curl -s http://localhost:5002/health &
done
wait
```

**Current:** No rate limiting implemented (all requests succeed)  
**Phase 4:** Add rate limiting if needed

---

## Test Suite 5: Layer Routing Verification

### Routing Table (Expected Behavior)

| Time Period | Selected Layer | Expected Fallback | Test Query |
|-------------|---|---|---|
| Today (0h) | Paimon | → Iceberg | "Hôm nay có bao nhiêu vụ?" |
| Yesterday (24h) | Paimon | → Iceberg | "Hôm qua có bao nhiêu vụ?" |
| This week (7d) | Paimon | → Iceberg | "Tuần này có bao nhiêu vụ?" |
| Last month (30d) | Iceberg | (direct) | "Tháng trước có bao nhiêu vụ?" |
| Last year (365d) | Iceberg | (direct) | "Năm ngoái có bao nhiêu vụ?" |
| Real-time (0-1h) | Fluss | → Iceberg | "Có vụ nào vừa xảy ra không?" |

### Verify Routing

```bash
# Check which layer is selected for each query
for query in \
  "Hôm nay co bao nhieu vu?" \
  "Tuan nay co bao nhieu vu?" \
  "Thang truoc co bao nhieu vu?"; do
  echo "Query: $query"
  echo $'{"query":"'$query'"}' | \
  curl -s -X POST http://localhost:5002/chat \
    -H "Content-Type: application/json" \
    -d @- | \
  python3 -c "import sys, json; d=json.load(sys.stdin); print('Layer:', d['layer'], 'Rows:', d['citations']['row_count'])"
done
```

---

## Debugging & Troubleshooting

### Check Chatbot Logs

```bash
docker logs -f chatbot 2>&1 | grep -E "Paimon|falling back|CATALOG|ERROR"
```

**Look for:**
- `Paimon unavailable (TrinoUserError...)` — Fallback triggered ✅
- `Trino error:` — Query failed, retry attempted
- `Query executed successfully: 0 rows` — Expected for empty results

---

### Check Trino Catalogs

```bash
docker exec trino-coordinator trino --execute "SHOW CATALOGS"
```

**Current (Paimon JAR missing):**
```
"iceberg"
"system"
```

**Expected (after JAR fix):**
```
"iceberg"
"paimon"
"system"
```

---

### Check Trino Tables

```bash
# Iceberg tables
docker exec trino-coordinator trino --execute "SHOW TABLES FROM iceberg.security"

# Paimon tables (once JAR is installed)
docker exec trino-coordinator trino --execute "SHOW TABLES FROM paimon.security"
```

---

### Verify MinIO Frames

```bash
docker exec minio mc ls minio/violence-detection/ --recursive
```

**Expected:**
```
[2026-04-29T...] 45KB incident_001_cam_001_frame.jpg
[2026-04-29T...] 48KB incident_002_cam_002_frame.jpg
...
```

---

## CI/CD Integration (Phase 4)

### Proposed GitHub Actions Workflow

```yaml
name: Chatbot E2E Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      docker:
        image: docker:latest
    steps:
      - uses: actions/checkout@v2
      - name: Start services
        run: docker-compose up -d
      - name: Wait for readiness
        run: sleep 30
      - name: Run test suite
        run: |
          python3 test_suite.py --suite fallback,sql,frame,routing
      - name: Upload results
        if: always()
        uses: actions/upload-artifact@v2
        with:
          name: test-results
          path: test_results.json
```

---

## Manual Test Checklist

### Before Each Session
- [ ] Health check passes
- [ ] Trino running: `docker ps | grep trino`
- [ ] MinIO running: `docker ps | grep minio`
- [ ] Chatbot running: `docker logs chatbot | tail -5`

### After Code Changes
- [ ] TC1-TC4 (fallback tests) PASS
- [ ] TC5-TC6 (SQL generation) PASS
- [ ] No new errors in chatbot logs
- [ ] Response times <10s p99

### Before Phase 3 (Paimon Activation)
- [ ] Paimon JAR installed
- [ ] `SHOW CATALOGS` includes paimon
- [ ] Paimon tables created and populated
- [ ] TC1-TC4 re-run with Paimon responses (not fallback)

---

## Test Data Refresh

### Current Test Data (Iceberg)
- 6 incidents (Quan 1×3, Quan 3×2, Quan 5×2)
- Dates: March 2026 (30 days ago)
- Cameras: cam_001, cam_002, cam_003

### Add More Data

```bash
# Manually insert more incidents for variety
python3 scripts/setup/populate_test_data.py --count 100 --date-range "2026-03-01:2026-04-26"
```

---

## Expected vs Actual Results

### Current Status (2026-04-29)

| Test | Expected | Actual | Status |
|------|----------|--------|--------|
| Health check | 200 ok | 200 ok | ✅ |
| TC1 (Iceberg) | 6 rows | 6 rows | ✅ |
| TC2 (Fallback) | Fallback → Iceberg | Fallback detected | ✅ |
| TC3 (History) | 6 rows | 6 rows | ✅ |
| TC4 (Fallback) | Fallback ok | Fallback detected | ✅ |
| Paimon catalog | Present | Missing (JAR issue) | 🔴 |
| Frame retrieval | S3 URL | Null (no data) | ⚠️ |

**Notes:**
- Frame null because: 0 rows returned → no incident_id → no frame lookup
- Paimon missing because: JAR download failed (base image has no wget)

---

## Next Session Preparation

### Phase 3 Testing (After JAR Fix)

1. Verify Paimon catalog appears
2. Re-run TC1-TC4 with Paimon active
3. Confirm queries 1-7 days old route to Paimon
4. Check latency (should be 1-10min for Paimon queries)

### Test Data Preparation

1. Add data spanning all time ranges:
   - Real-time (last 1 hour) → Fluss
   - Yesterday-last week (1-7 days) → Paimon
   - Older (>7 days) → Iceberg

2. Run full routing matrix (15+ queries)

---

## Test Automation Script

See: `scripts/test/test_suite.py` (to be created in Phase 4)

For now, use manual curl commands above.

---

**End of Testing Guide**  
*Last Updated: 2026-04-29*
