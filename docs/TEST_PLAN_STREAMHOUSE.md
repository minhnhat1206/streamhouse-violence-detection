# Streamhouse Test Plan
**Date:** 2026-05-21 | **Target:** True Tiering Architecture

---

## Thứ tự chạy & thời gian ước tính

```
S1 Infrastructure (5 phút)
  → S2 Data Pipeline (10 phút)
      → S3 HOT Layer (15 phút)
          → S4 Tiering MOVE ⭐ (25 phút — QUAN TRỌNG NHẤT)
              → S5 WARM + COLD (15 phút)
                  → S6 Chatbot (30 phút)
                      → S7 Data Quality (10 phút)
```

**Tổng: ~2 giờ** | Critical path (bỏ qua S7): ~1.5 giờ

---

## Helper: Query Fluss qua SQL Gateway

```bash
# Chạy từ bên trong pipeline-manager container
docker exec pipeline-manager python3 - <<'EOF'
import requests, time

BASE = 'http://flink-sql-gateway:8083/v1'
sid = requests.post(f'{BASE}/sessions', json={}, timeout=30).json()['sessionHandle']

def q(sql, to=60):
    op = requests.post(f'{BASE}/sessions/{sid}/statements',
                       json={'statement': sql}, timeout=30).json()['operationHandle']
    deadline = time.time() + to
    while time.time() < deadline:
        if requests.get(f'{BASE}/sessions/{sid}/operations/{op}/status',
                        timeout=10).json().get('status') in ('FINISHED','ERROR','CLOSED','CANCELED'):
            break
        time.sleep(2)
    rr = requests.get(f'{BASE}/sessions/{sid}/operations/{op}/result/0', timeout=15).json()
    cols = rr.get('results', {}).get('columns', [])
    return [{cols[i]['name']: (row.get('fields', row) if isinstance(row, dict) else row)[i]
             for i in range(len(cols))}
            for row in rr.get('results', {}).get('data', [])]

q("CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123')")
q("USE CATALOG fluss")
q("USE security")

# ---- Thay dòng này bằng query cần chạy ----
print(q("SELECT COUNT(*) FROM dim_camera"))
EOF
```

---

## S1: Infrastructure
*Điều kiện tiên quyết — tất cả test còn lại cần S1 PASS*

### T1.1 — Docker containers healthy
```bash
docker compose -f docker/docker-compose.yml ps --format "table {{.Name}}\t{{.Status}}" \
  | grep -E "Exit|Restarting" && echo "FAIL" || echo "PASS"
```
**Pass:** Không có dòng nào in ra (0 container lỗi).

---

### T1.2 — Flink: 3 jobs RUNNING, đủ slots
```bash
curl -s http://localhost:8081/overview | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'jobs-running={d[\"jobs-running\"]}  slots-available={d[\"slots-available\"]}')
print('PASS' if d['jobs-running'] == 3 and d['slots-available'] >= 1 else 'FAIL')
"
```
**Pass:** `jobs-running=3`, `slots-available ≥ 1`.

---

### T1.3 — Chatbot healthy
```bash
curl -s http://localhost:5002/health | python3 -c "
import sys, json; d = json.load(sys.stdin)
ok = d.get('status') == 'ok' and d.get('services', {}).get('agent_initialized')
print('PASS' if ok else 'FAIL', '|', d)
"
```
**Pass:** `status=ok`, `agent_initialized=true`.

---

## S2: Data Pipeline (Kafka → Flink)

### T2.1 — RTSP producer đang publish
```bash
docker logs rtsp-inference-mock --tail=5 2>&1 | grep -E "PUBLISH|VIOLENCE|Normal" \
  && echo "PASS" || echo "FAIL"
```
**Pass:** Thấy ít nhất 1 dòng log.

---

### T2.2 — Contract Validator đang process
```bash
python3 -c "
import urllib.request, json
d = json.loads(urllib.request.urlopen('http://localhost:8081/jobs/overview').read())
jobs = [j['name'][:50] for j in d['jobs'] if j['state'] == 'RUNNING']
print('Running jobs:')
for j in jobs: print(' ', j)
has_validator = any('Contract' in j for j in jobs)
has_hot_sink  = any('hot_violence_alerts' in j for j in jobs)
has_aggregate = any('daily_incident_stats' in j for j in jobs)
print('PASS' if has_validator and has_hot_sink and has_aggregate else 'FAIL')
"
```
**Pass:** Cả 3 jobs RUNNING (Contract Validator, hot_violence_alerts sink, daily_incident_stats).

---

### T2.3 — HOT record count đang tăng
```bash
# Lấy count lần 1, đợi 15s, lấy lần 2 → phải tăng
C1=$(curl -s http://localhost:5002/api/layer-counts | python3 -c "import sys,json; print(json.load(sys.stdin).get('hot',0))")
sleep 15
C2=$(curl -s http://localhost:5002/api/layer-counts | python3 -c "import sys,json; print(json.load(sys.stdin).get('hot',0))")
echo "HOT: $C1 → $C2"
[ "$C2" -gt "$C1" ] && echo "PASS" || echo "FAIL (không tăng)"
```
**Pass:** C2 > C1.

---

## S3: Fluss HOT Layer

### T3.1 — Schema đúng 10 cột (có location/ward_id/district)
```bash
docker exec pipeline-manager python3 - <<'EOF'
import requests, time
BASE = 'http://flink-sql-gateway:8083/v1'
sid = requests.post(f'{BASE}/sessions', json={}, timeout=30).json()['sessionHandle']
def q(sql, to=30):
    op = requests.post(f'{BASE}/sessions/{sid}/statements', json={'statement':sql}, timeout=30).json()['operationHandle']
    deadline = time.time()+to
    while time.time()<deadline:
        if requests.get(f'{BASE}/sessions/{sid}/operations/{op}/status',timeout=10).json().get('status') in ('FINISHED','ERROR','CLOSED','CANCELED'): break
        time.sleep(2)
    rr = requests.get(f'{BASE}/sessions/{sid}/operations/{op}/result/0',timeout=15).json()
    cols = rr.get('results',{}).get('columns',[])
    return [{cols[i]['name']:(row.get('fields',row) if isinstance(row,dict) else row)[i] for i in range(len(cols))} for row in rr.get('results',{}).get('data',[])]
q("CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123')")
q("USE CATALOG fluss"); q("USE security")
rows = q("DESCRIBE hot_violence_alerts")
cols = [list(r.values())[0] for r in rows]
required = {'incident_id','camera_id','timestamp','risk_score','confidence','is_violent','event_type','location','ward_id','district'}
missing = required - set(cols)
print(f"Columns ({len(cols)}): {cols}")
print("PASS" if not missing else f"FAIL — missing: {missing}")
EOF
```
**Pass:** 10 cột, `missing = set()`.

---

### T3.2 — dim_camera có 15 cameras (không empty)
```bash
docker exec pipeline-manager python3 - <<'EOF'
import requests, time
BASE = 'http://flink-sql-gateway:8083/v1'
sid = requests.post(f'{BASE}/sessions', json={}, timeout=30).json()['sessionHandle']
def q(sql, to=30):
    op = requests.post(f'{BASE}/sessions/{sid}/statements', json={'statement':sql}, timeout=30).json()['operationHandle']
    deadline = time.time()+to
    while time.time()<deadline:
        if requests.get(f'{BASE}/sessions/{sid}/operations/{op}/status',timeout=10).json().get('status') in ('FINISHED','ERROR','CLOSED','CANCELED'): break
        time.sleep(2)
    rr = requests.get(f'{BASE}/sessions/{sid}/operations/{op}/result/0',timeout=15).json()
    cols = rr.get('results',{}).get('columns',[])
    return [{cols[i]['name']:(row.get('fields',row) if isinstance(row,dict) else row)[i] for i in range(len(cols))} for row in rr.get('results',{}).get('data',[])]
q("CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123')")
q("USE CATALOG fluss"); q("USE security")
rows = q("SELECT camera_id, location FROM dim_camera LIMIT 20")
print(f"dim_camera: {len(rows)} rows")
for r in rows[:5]: print(f"  {r.get('camera_id')} → {r.get('location')}")
print("PASS" if len(rows) == 15 else f"FAIL (got {len(rows)}, expected 15)")
EOF
```
**Pass:** 15 rows, không có `location = 'Unknown'`.

---

### T3.3 — Enrichment: location ≠ 'Unknown' trong HOT records
```bash
docker exec pipeline-manager python3 - <<'EOF'
import requests, time
BASE = 'http://flink-sql-gateway:8083/v1'
sid = requests.post(f'{BASE}/sessions', json={}, timeout=30).json()['sessionHandle']
def q(sql, to=30):
    op = requests.post(f'{BASE}/sessions/{sid}/statements', json={'statement':sql}, timeout=30).json()['operationHandle']
    deadline = time.time()+to
    while time.time()<deadline:
        if requests.get(f'{BASE}/sessions/{sid}/operations/{op}/status',timeout=10).json().get('status') in ('FINISHED','ERROR','CLOSED','CANCELED'): break
        time.sleep(2)
    rr = requests.get(f'{BASE}/sessions/{sid}/operations/{op}/result/0',timeout=15).json()
    cols = rr.get('results',{}).get('columns',[])
    return [{cols[i]['name']:(row.get('fields',row) if isinstance(row,dict) else row)[i] for i in range(len(cols))} for row in rr.get('results',{}).get('data',[])]
q("CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123')")
q("USE CATALOG fluss"); q("USE security")
rows = q("SELECT camera_id, location FROM hot_violence_alerts LIMIT 30")
unknown = [r for r in rows if r.get('location') in ('Unknown', None, '')]
print(f"Sample: {len(rows)} | Unknown: {len(unknown)} | Real: {len(rows)-len(unknown)}")
for r in rows[:5]: print(f"  {r.get('camera_id')} → {r.get('location')}")
print("PASS" if rows and not unknown else ("FAIL (Unknown locations)" if unknown else "WARN (no rows)"))
EOF
```
**Pass:** `Unknown = 0`, ít nhất 1 row có tên đường thật.

---

## S4: Tiering MOVE ⭐ (Quan trọng nhất — chưa verify)

> **Setup:** Force tiering ngay với `TIERING_HOURS=0` thay vì đợi 2 giờ.

### T4.1 — Snapshot trước khi tier
```bash
python3 -c "
import urllib.request, json
counts = json.loads(urllib.request.urlopen('http://localhost:5002/api/layer-counts').read())
print(f'BEFORE — hot={counts[\"hot\"]}  warm={counts[\"warm\"]}')
print('Lưu lại để so sánh ở T4.3')
"
```

---

### T4.2 — Force tiering chạy và hoàn thành
```bash
docker exec pipeline-manager bash -c "
  TIERING_HOURS=0 \
  TIERING_PHASE1_WAIT_SECS=90 \
  TIERING_PHASE2_WAIT_SECS=60 \
  /opt/flink/bin/flink run \
    --python /opt/flink/scripts/tier_fluss_to_paimon.py \
    -Dpipeline.name=tier_force_test 2>&1 | grep -E 'Phase|Tiering|ERROR|error'
"
```
**Expected output:**
```
Phase 1: INSERT Fluss aged data ... → Paimon WARM
Phase 1 job submitted: <job_id>
Waiting 90s for Paimon checkpoint commits...
Phase 1 job cancelled.
Phase 2: DELETE from Fluss HOT...
Tiering COMPLETE|PARTIAL
```
**Pass:** Không có `[ERROR]`. `Tiering COMPLETE` hoặc `PARTIAL (Phase 1 OK)`.

---

### T4.3 — WARM count tăng sau tiering
```bash
# Đợi 30s cho Paimon commit
sleep 30
python3 -c "
import urllib.request, json
counts = json.loads(urllib.request.urlopen('http://localhost:5002/api/layer-counts').read())
print(f'AFTER — hot={counts[\"hot\"]}  warm={counts[\"warm\"]}')
print('So sánh với snapshot T4.1 — warm phải tăng')
"
```
**Pass:** `warm_after > warm_before` (từ T4.1).

---

### T4.4 — Phase 2: HOT count giảm (hoặc WARN nếu DELETE unsupported)
```bash
docker exec pipeline-manager python3 - <<'EOF'
import requests, time
BASE = 'http://flink-sql-gateway:8083/v1'
sid = requests.post(f'{BASE}/sessions', json={}, timeout=30).json()['sessionHandle']
def q(sql, to=90):
    op = requests.post(f'{BASE}/sessions/{sid}/statements', json={'statement':sql}, timeout=30).json()['operationHandle']
    deadline = time.time()+to
    while time.time()<deadline:
        if requests.get(f'{BASE}/sessions/{sid}/operations/{op}/status',timeout=10).json().get('status') in ('FINISHED','ERROR','CLOSED','CANCELED'): break
        time.sleep(2)
    rr = requests.get(f'{BASE}/sessions/{sid}/operations/{op}/result/0',timeout=15).json()
    cols = rr.get('results',{}).get('columns',[])
    return [{cols[i]['name']:(row.get('fields',row) if isinstance(row,dict) else row)[i] for i in range(len(cols))} for row in rr.get('results',{}).get('data',[])]
q("CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123')")
q("USE CATALOG fluss"); q("USE security")

# Lấy một incident_id từ HOT
rows = q("SELECT incident_id, timestamp FROM hot_violence_alerts LIMIT 5")
print(f"HOT còn {len(rows)} rows trong mẫu")
# Nếu Phase 2 hoạt động → count giảm đáng kể
# Nếu Phase 2 fail → WARN (data vẫn được tier, chỉ không xóa khỏi HOT)
print("CHECK: HOT count sau tiering?")
# Ghi nhận: nếu hot count đã giảm so với T4.1 → Phase 2 OK
# Nếu không giảm → Phase 2 unsupported (WARN, không phải FAIL)
EOF
```
**Pass (hard):** HOT count giảm đáng kể (Phase 2 DELETE hoạt động).
**Pass (soft):** Phase 1 OK + WARM tăng — Phase 2 WARN acceptable (routing theo time không cần DELETE).

---

## S5: WARM và COLD Layers

### T5.1 — Paimon WARM: bảng tồn tại và có data
```bash
# Trino query Paimon (thông qua Flink SQL Gateway)
docker exec pipeline-manager python3 - <<'EOF'
import requests, time
BASE = 'http://flink-sql-gateway:8083/v1'
sid = requests.post(f'{BASE}/sessions', json={}, timeout=30).json()['sessionHandle']
def q(sql, to=120):
    op = requests.post(f'{BASE}/sessions/{sid}/statements', json={'statement':sql}, timeout=30).json()['operationHandle']
    deadline = time.time()+to
    while time.time()<deadline:
        if requests.get(f'{BASE}/sessions/{sid}/operations/{op}/status',timeout=10).json().get('status') in ('FINISHED','ERROR','CLOSED','CANCELED'): break
        time.sleep(2)
    rr = requests.get(f'{BASE}/sessions/{sid}/operations/{op}/result/0',timeout=15).json()
    cols = rr.get('results',{}).get('columns',[])
    return [{cols[i]['name']:(row.get('fields',row) if isinstance(row,dict) else row)[i] for i in range(len(cols))} for row in rr.get('results',{}).get('data',[])]
q("""CREATE CATALOG paimon WITH (
  'type'='paimon','warehouse'='s3://warehouse/paimon',
  's3.endpoint'='http://minio:9000','s3.access-key'='minio',
  's3.secret-key'='mypassword','s3.path.style.access'='true')""")
q("USE CATALOG paimon"); q("USE security")
tables = q("SHOW TABLES")
print("Paimon tables:", [list(r.values())[0] for r in tables])
# Check violence_incidents
count_rows = q("SELECT COUNT(*) as cnt FROM violence_incidents", to=120)
print("violence_incidents count:", count_rows)
has_vi = any('violence_incidents' in list(r.values())[0] for r in tables)
has_fact = any('fact_violence_incidents' in list(r.values())[0] for r in tables)
print("PASS" if has_vi and has_fact else f"FAIL — missing tables")
EOF
```
**Pass:** `violence_incidents` và `fact_violence_incidents` tồn tại, count > 0.

---

### T5.2 — Iceberg COLD: schema tồn tại, force archival
```bash
# Check schema trước
docker exec trino-coordinator trino \
  --execute "SHOW TABLES FROM iceberg.security" 2>/dev/null \
  && echo "Iceberg schema accessible" || echo "WARN: check trino catalog"

# Force archival
docker exec pipeline-manager bash -c "
  /opt/flink/bin/flink run \
    --python /opt/flink/scripts/archive_to_iceberg.py \
    -Dexecution.runtime-mode=BATCH \
    -Dpipeline.name=force_archive_test 2>&1 | tail -5
"

# Đợi 30s rồi check COLD count
sleep 30
curl -s http://localhost:5002/api/layer-counts | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(f'COLD count: {d.get(\"cold\",0)}')
print('NOTE: cold=0 là ĐÚNG nếu stack mới (data < 7 ngày). Chỉ FAIL nếu archival job lỗi.')
"
```
**Pass:** Archival job FINISHED không lỗi. `cold=0` acceptable với fresh stack (data < 7 ngày).

---

## S6: Chatbot

### T6.1 — Routing HOT: "30 phút" → Fluss
```bash
curl -s -m 120 -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "trong 30 phut qua co bao nhieu canh bao?"}' \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(f'Layer: {d.get(\"layer\")} | Duration: {d.get(\"duration_ms\")}ms')
print(f'Answer: {(d.get(\"answer\") or d.get(\"response\",\"\"))[:200]}')
print('PASS' if d.get('layer')=='Fluss' else 'FAIL')
"
```
**Pass:** `layer = "Fluss"`.

---

### T6.2 — Routing WARM: "hôm nay" → Paimon
```bash
curl -s -m 120 -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "hom nay co bao nhieu vu bao luc?"}' \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(f'Layer: {d.get(\"layer\")} | Duration: {d.get(\"duration_ms\")}ms')
print(f'Answer: {(d.get(\"answer\") or d.get(\"response\",\"\"))[:200]}')
print('PASS' if d.get('layer')=='Paimon' else 'FAIL')
"
```
**Pass:** `layer = "Paimon"`.

---

### T6.3 — HOT location: trả về tên đường thật
```bash
curl -s -m 120 -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "bao luc xay ra o dau trong 15 phut qua?"}' \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
answer = d.get('answer') or d.get('response','')
streets = ['Nguyễn Huệ','Lê Lợi','Nguyễn Thái Học','Pasteur','Trần Hưng Đạo',
           'Đồng Khởi','Hai Bà Trưng','Nguyễn Du','Võ Văn Kiệt','Hàm Nghi']
has_street = any(s in answer for s in streets)
has_unknown = 'Unknown' in answer
print(f'Layer: {d.get(\"layer\")}')
print(f'Answer: {answer[:300]}')
print('PASS' if has_street and not has_unknown else 'FAIL')
"
```
**Pass:** Có tên đường Việt Nam thực, không có 'Unknown'.

---

### T6.4 — Layer routing boundary: "45 phút" vs "2 giờ"
```bash
# 45 phút → HOT
R=$(curl -s -m 90 -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "45 phut qua co gi?"}' 2>/dev/null)
echo "45 phút → Layer: $(echo $R | python3 -c 'import sys,json; print(json.load(sys.stdin).get("layer","?"))')"

# 2 giờ → WARM
R=$(curl -s -m 90 -X POST http://localhost:5002/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "trong 2 gio qua co bao nhieu vu?"}' 2>/dev/null)
echo "2 giờ → Layer: $(echo $R | python3 -c 'import sys,json; print(json.load(sys.stdin).get("layer","?"))')"
```
**Pass:** "45 phút" → `Fluss`, "2 giờ" → `Paimon`.

---

### T6.5 — API endpoints hoạt động
```bash
echo "=== layer-counts ==="
curl -s http://localhost:5002/api/layer-counts

echo -e "\n=== latency ==="
curl -s http://localhost:5002/api/latency | python3 -c "
import sys,json
d=json.load(sys.stdin)
for layer, info in d.items():
    print(f'  {layer}: {info.get(\"latency_ms\")}ms (ok={info.get(\"ok\")})')
"
```
**Pass:** `hot > 0`, `warm > 0`, tất cả layers `ok=true`.

---

## S7: Data Quality (nhanh)

### T7.1 — Không có NULL trong required fields + risk_score hợp lệ
```bash
docker exec pipeline-manager python3 - <<'EOF'
import requests, time
BASE = 'http://flink-sql-gateway:8083/v1'
sid = requests.post(f'{BASE}/sessions', json={}, timeout=30).json()['sessionHandle']
def q(sql, to=30):
    op = requests.post(f'{BASE}/sessions/{sid}/statements', json={'statement':sql}, timeout=30).json()['operationHandle']
    deadline = time.time()+to
    while time.time()<deadline:
        if requests.get(f'{BASE}/sessions/{sid}/operations/{op}/status',timeout=10).json().get('status') in ('FINISHED','ERROR','CLOSED','CANCELED'): break
        time.sleep(2)
    rr = requests.get(f'{BASE}/sessions/{sid}/operations/{op}/result/0',timeout=15).json()
    cols = rr.get('results',{}).get('columns',[])
    return [{cols[i]['name']:(row.get('fields',row) if isinstance(row,dict) else row)[i] for i in range(len(cols))} for row in rr.get('results',{}).get('data',[])]
q("CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123')")
q("USE CATALOG fluss"); q("USE security")

rows = q("SELECT incident_id, camera_id, risk_score, confidence FROM hot_violence_alerts LIMIT 50")
nulls = sum(1 for r in rows if None in (r.get('incident_id'), r.get('camera_id')))
bad_score = sum(1 for r in rows if r.get('risk_score') is not None and not (0 <= r['risk_score'] <= 1))
dups = len(rows) - len({r.get('incident_id') for r in rows})
print(f"Sample: {len(rows)} | NULLs: {nulls} | Bad score: {bad_score} | Duplicates: {dups}")
print("PASS" if nulls == 0 and bad_score == 0 and dups == 0 else "FAIL")
EOF
```
**Pass:** `NULLs=0`, `Bad score=0`, `Duplicates=0`.

---

### T7.2 — dim_camera FK integrity: mọi cam_id trong HOT tồn tại trong dim_camera
```bash
docker exec pipeline-manager python3 - <<'EOF'
import requests, time
BASE = 'http://flink-sql-gateway:8083/v1'
sid = requests.post(f'{BASE}/sessions', json={}, timeout=30).json()['sessionHandle']
def q(sql, to=30):
    op = requests.post(f'{BASE}/sessions/{sid}/statements', json={'statement':sql}, timeout=30).json()['operationHandle']
    deadline = time.time()+to
    while time.time()<deadline:
        if requests.get(f'{BASE}/sessions/{sid}/operations/{op}/status',timeout=10).json().get('status') in ('FINISHED','ERROR','CLOSED','CANCELED'): break
        time.sleep(2)
    rr = requests.get(f'{BASE}/sessions/{sid}/operations/{op}/result/0',timeout=15).json()
    cols = rr.get('results',{}).get('columns',[])
    return [{cols[i]['name']:(row.get('fields',row) if isinstance(row,dict) else row)[i] for i in range(len(cols))} for row in rr.get('results',{}).get('data',[])]
q("CREATE CATALOG fluss WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123')")
q("USE CATALOG fluss"); q("USE security")
hot_cams = {r.get('camera_id') for r in q("SELECT DISTINCT camera_id FROM hot_violence_alerts LIMIT 20")}
dim_cams = {r.get('camera_id') for r in q("SELECT camera_id FROM dim_camera LIMIT 20")}
orphaned = hot_cams - dim_cams
print(f"HOT cam_ids: {sorted(hot_cams)}")
print(f"Orphaned (not in dim_camera): {orphaned}")
print("PASS" if not orphaned else f"FAIL: {orphaned}")
EOF
```
**Pass:** `orphaned = set()`.

---

## Scorecard

```
S1 Infrastructure:   T1.1[  ] T1.2[  ] T1.3[  ]
S2 Data Pipeline:    T2.1[  ] T2.2[  ] T2.3[  ]
S3 HOT Layer:        T3.1[  ] T3.2[  ] T3.3[  ]
S4 Tiering MOVE ⭐: T4.1[  ] T4.2[  ] T4.3[  ] T4.4[  ]
S5 WARM + COLD:      T5.1[  ] T5.2[  ]
S6 Chatbot:          T6.1[  ] T6.2[  ] T6.3[  ] T6.4[  ] T6.5[  ]
S7 Data Quality:     T7.1[  ] T7.2[  ]

TOTAL: ___/23   [P]=PASS  [W]=WARN  [F]=FAIL
```

**Critical (không thể FAIL):** T1.2, T2.2, T3.3, T4.2, T4.3, T6.1, T6.2, T6.3
