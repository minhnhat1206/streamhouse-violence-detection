# Data Contracts — Quality Control at Source

## Khái Niệm
Data Contracts = hợp đồng dữ liệu: quy tắc kiểm tra chất lượng **ngay khi dữ liệu đến** (shift-left).
Thay vì kiểm tra cuối pipeline (schema-on-read), ta kiểm tra đầu pipeline (schema-on-write).

## Contract Definition

```json
{
  "contract_name": "ai_violence_alerts_v1",
  "fields": {
    "timestamp": {
      "type": "timestamp (ISO 8601 UTC)",
      "constraints": {
        "min": "2025-01-01T00:00:00+00:00",
        "max": "now + 1 minute"
      }
    },
    "camera_id": {
      "type": "string",
      "pattern": "^cam_\\d{2}$",
      "allowed_values": ["cam_01", "cam_02", "...", "cam_16"]
    },
    "risk_score": {
      "type": "double",
      "constraints": {"min": 0, "max": 1}
    },
    "confidence": {
      "type": "double",
      "constraints": {"min": 0, "max": 1}
    },
    "event_type": {
      "type": "string | null",
      "enum": ["FIGHTING", "ASSAULT", "STABBING", "SHOOTING"],
      "note": "null when is_violent=false (heartbeat)"
    }
  }
}
```

## Quality Rules

| Rule ID | Check | Severity |
|---------|-------|----------|
| `no_future_timestamps` | `timestamp <= now + 1 minute` | REJECT |
| `valid_camera_id` | `camera_id matches ^cam_\d{2}$` | REJECT |
| `risk_score_range` | `0 <= risk_score <= 1` | REJECT |
| `confidence_range` | `0 <= confidence <= 1` | REJECT |
| `high_confidence_for_critical` | If event_type IN ('STABBING', 'SHOOTING') THEN confidence >= 0.85 | WARN |
| `event_type_required_when_violent` | If is_violent = true THEN event_type IS NOT NULL | REJECT |

## Validation Flow

```
VioMobileNet Output
    ↓
Kafka Topic (urban-safety-raw)
    ↓
Flink Source → Data Contract Validator
    ├─ Valid ✓ → Fluss (hot_violence_alerts)
    │            → Paimon (violence_incidents)
    └─ Invalid ✗ → Quarantine Topic (urban-safety-quarantine)
                   → Quarantine Paimon table (quarantine_violations)
```

## Flink Validator Implementation (Pseudo-code)

```python
class DataContractValidator:
    def validate(self, record: dict) -> tuple[bool, list[str]]:
        violations = []

        # Timestamp check
        if record['timestamp'] > datetime.now(UTC) + timedelta(minutes=1):
            violations.append("FUTURE_TIMESTAMP")

        # Camera ID check
        if not re.match(r'^cam_\d{2}$', record.get('camera_id', '')):
            violations.append("INVALID_CAMERA_ID")

        # Risk score range
        if not (0 <= record.get('risk_score', -1) <= 1):
            violations.append("RISK_SCORE_OUT_OF_RANGE")

        # Confidence range
        if not (0 <= record.get('confidence', -1) <= 1):
            violations.append("CONFIDENCE_OUT_OF_RANGE")

        # Event type required when violent
        if record.get('is_violent') and not record.get('event_type'):
            violations.append("MISSING_EVENT_TYPE")

        # High confidence for critical events
        if record.get('event_type') in ('STABBING', 'SHOOTING'):
            if record.get('confidence', 0) < 0.85:
                violations.append("LOW_CONFIDENCE_CRITICAL")

        is_valid = len([v for v in violations if v != "LOW_CONFIDENCE_CRITICAL"]) == 0
        return is_valid, violations
```

## Monitoring Contracts

### Metrics to Track
- `contract_violations_total` (by rule_id) — Prometheus counter
- `contract_validation_latency_ms` — Prometheus histogram
- `quarantine_records_total` — Prometheus counter
- `valid_records_ratio` — Grafana dashboard gauge

### Alert Rules
```yaml
# Alert khi tỷ lệ vi phạm > 10%
- alert: HighContractViolationRate
  expr: rate(contract_violations_total[5m]) / rate(records_processed_total[5m]) > 0.1
  for: 2m
  labels:
    severity: warning
```
