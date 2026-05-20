#!/bin/bash
echo "=== CRITICAL TESTS (T01-T06, T11-T12) ==="

# T01: All 4 jobs RUNNING
echo -e "\n[T01] Flink jobs status:"
curl -s http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
running = [j for j in d['jobs'] if j['state']=='RUNNING']
print(f'  RUNNING: {len(running)}/4')
for j in running: print(f'    {j[\"name\"][:45]}')
" 2>/dev/null || echo "  (Flink still loading...)"

# T02: Kafka messages ready
echo -e "\n[T02] Kafka messages (first 1):"
docker exec kafka bash -c "
  timeout 5 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic hot-violence-alerts-valid \
    --max-messages 1 --timeout-ms 3000 2>/dev/null
" | python3 -c "
import json, sys
try:
    msg = json.loads(sys.stdin.read())
    print(f'  camera_id: {msg.get(\"camera_id\")}')
    print(f'  is_valid: {msg.get(\"is_valid\")}')
    print(f'  risk_score: {msg.get(\"risk_score\", \"N/A\"):.2f}')
except: print('  (No messages yet)')
" 2>/dev/null || echo "  (Waiting for Contract Validator to process)"

# T11: Union read structure
echo -e "\n[T11] Union read test:"
curl -s http://localhost:5002/api/union-read 2>/dev/null | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    rows = d.get('rows', []) if isinstance(d, dict) else d
    if rows:
        print(f'  Rows returned: {len(rows)}')
        layers = {}
        for r in rows: layers[r.get('layer','?')] = layers.get(r.get('layer','?'),0) + 1
        for layer, count in sorted(layers.items()): print(f'    {layer}: {count} rows')
    else: print('  (No data yet)')
except: print('  (API not ready)')
" 2>/dev/null || echo "  (API error)"

# T12: Analytics
echo -e "\n[T12] Analytics stats:"
curl -s http://localhost:5002/api/stats 2>/dev/null | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    alerts = d.get('alertsPerHour', [])
    total = sum(a.get('alerts',0) for a in alerts)
    print(f'  24h alerts: {total}')
    print(f'  Top locations: {len(d.get(\"topLocations\", []))}')
except: print('  (API not ready)')
" 2>/dev/null || echo "  (API error)"

echo -e "\n=== END CRITICAL TESTS ==="
