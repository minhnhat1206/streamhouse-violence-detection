# Frame Evidence Storage System

## Overview

The Frame Evidence Storage system captures and preserves digital evidence frames from violence detection incidents. Evidence frames are stored in MinIO S3 (`evidence-frames` bucket) with forensic-grade organization and metadata preservation.

**Key Features:**
- On-demand frame capture (1 keyframe per incident, not continuous)
- S3 storage partitioned by camera_id/date/incident_id
- Automatic cleanup with 30-day retention policy
- Frame metadata indexed in Paimon warm layer
- REST API for forensic retrieval

---

## Architecture

### Data Flow

```
RTSP Stream (MediaMTX)
    ↓
Inference Mock (rtsp_inference_mock.py)
    ├─ Captures 160x90 JPEG thumbnail
    ├─ Base64 encodes
    └─ Sends to Kafka: urban-safety-alerts
        ↓
    Data Contract Validator
        ├─ Validates message schema
        └─ Sends to: hot-violence-alerts-valid
            ↓
    Frame Extractor (frame_extractor_sink.py)
        ├─ Reads metadata.thumbnail (base64)
        ├─ Decodes JPEG
        ├─ Uploads to S3: evidence-frames/{camera_id}/{YYYY-MM-DD}/{incident_id}.jpg
        ├─ Publishes enriched record: hot-violence-frames-uploaded
        └─ Failed uploads → frame-extraction-dlq (DLQ)
            ↓
    Sink to Paimon (sink_to_paimon.py)
        ├─ Reads from hot-violence-alerts-valid
        ├─ Stores to violence_incidents table (with frame_url, thumbnail_b64, frame_capture_ts)
        └─ Updates via CDC from frame-extractor intermediary
            ↓
    Chatbot / Frontend
        ├─ Query incident + frame_url from Paimon
        ├─ Call /api/evidence/{incident_id}/frame
        └─ Display frame in UI
```

### Storage Tiers

| Tier | Storage | Retention | Latency | Use Case |
|------|---------|-----------|---------|----------|
| **HOT (Fluss)** | Real-time in-memory | 1-2 hours | <100ms | Command center live view |
| **WARM (Paimon)** | `violence_incidents` table | 7-30 days | 1-10 min | Operational analytics, 24-48hr forensics |
| **COLD (S3)** | `evidence-frames` bucket | 30 days | 10+ min | Long-term evidence preservation |

---

## S3 Path Convention

### Pattern

```
s3://evidence-frames/{camera_id}/{YYYY-MM-DD}/{incident_id}.jpg
```

### Example Paths

```
s3://evidence-frames/cam_01/2026-04-28/evt_abc123def456.jpg
s3://evidence-frames/cam_02/2026-04-28/evt_xyz789uvw012.jpg
s3://evidence-frames/cam_01/2026-04-27/evt_old_incident_789.jpg
```

### Benefits

- **Camera-partitioned**: Forensic queries for specific camera streams efficient
- **Date-partitioned**: Time-range filtering (e.g., "all incidents from cam_01 on Apr 28")
- **Incident-named**: Direct S3 lookup without table join
- **S3-inventory ready**: `SELECT * FROM s3://evidence-frames/cam_01/2026-04-28/`

---

## Frame Quality & Storage

### Thumbnail (Embedded in Paimon)

- **Resolution**: 160x90 pixels
- **Format**: JPEG, base64-encoded
- **Location**: `violence_incidents.thumbnail_b64` column
- **Use**: Quick preview in dashboards, minimal storage
- **Size**: ~2-3 KB per frame
- **Retention**: 7-30 days (with Paimon)

### Evidence Frame (S3 Archive)

- **Resolution**: 640x480 pixels
- **Format**: JPEG, quality 85 (balance quality ↔ storage)
- **Location**: `s3://evidence-frames/{camera_id}/{date}/{incident_id}.jpg`
- **Use**: Forensic analysis, court evidence, detailed review
- **Size**: ~60-80 KB per frame
- **Retention**: 30 days (S3 lifecycle policy)
- **Expected**: 1 keyframe per incident (on-demand, not continuous)

### Storage Estimates

**Assumptions:**
- 50-100 actual violence incidents per month
- 1 keyframe per incident (on-demand capture)

**Monthly Cost:**
- 75 incidents × 70 KB/incident = 5.25 MB/month
- 7-30 day retention = 5-20 MB peak storage
- **Year 1 total**: ~63 MB (negligible)

**Comparison:** Continuous 1 fps capture would be 7 MB/day/camera = **210 MB/month** (100x worse)

---

## Paimon Schema

### `violence_incidents` Table Columns

```sql
CREATE TABLE violence_incidents (
    incident_id STRING,         -- Primary key (event_id from Kafka)
    camera_id STRING,           -- Camera identifier
    timestamp TIMESTAMP(3),     -- Incident timestamp
    risk_score DOUBLE,          -- AI confidence (0.0-1.0)
    confidence DOUBLE,          -- Model confidence
    is_violent BOOLEAN,         -- Whether violent activity detected
    event_type STRING,          -- FIGHTING, ASSAULT, STABBING, SHOOTING
    location STRING,            -- Location metadata
    is_deleted BOOLEAN,         -- Soft-delete flag
    frame_url STRING,           -- S3 path: s3://evidence-frames/cam_01/2026-04-28/evt_xyz.jpg
    thumbnail_b64 STRING,       -- Base64-encoded 160x90 JPEG (inline)
    frame_capture_ts BIGINT,    -- Frame upload timestamp (milliseconds)
    PRIMARY KEY (incident_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'deduplicate',
    'changelog-producer' = 'input',
    'snapshot.time-retained' = '7d',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '50'
)
```

---

## Forensic Queries

### Query 1: Retrieve Frame URL for Incident

```sql
SELECT 
  incident_id, 
  camera_id, 
  timestamp, 
  risk_score, 
  frame_url
FROM paimon.security.violence_incidents
WHERE incident_id = 'evt_abc123def456'
LIMIT 1;
```

**Result:**
```
incident_id      | camera_id | timestamp           | risk_score | frame_url
evt_abc123def456 | cam_01    | 2026-04-28T14:30:45Z | 0.95      | s3://evidence-frames/cam_01/2026-04-28/evt_abc123def456.jpg
```

### Query 2: All Incidents from Camera on Specific Date

```sql
SELECT 
  incident_id, 
  timestamp, 
  risk_score, 
  frame_url
FROM paimon.security.violence_incidents
WHERE camera_id = 'cam_01' 
  AND CAST(timestamp AS DATE) = '2026-04-28'
  AND is_violent = true
ORDER BY timestamp DESC;
```

### Query 3: Incidents with Frame Metadata

```sql
SELECT 
  incident_id, 
  camera_id, 
  timestamp, 
  frame_url,
  CASE WHEN frame_url IS NOT NULL THEN 'yes' ELSE 'no' END AS has_frame,
  frame_capture_ts
FROM paimon.security.violence_incidents
WHERE is_violent = true
  AND timestamp >= '2026-04-28'::TIMESTAMP
ORDER BY timestamp DESC
LIMIT 100;
```

---

## REST API for Frame Retrieval

### Endpoint: `GET /api/evidence/{incident_id}/frame`

**Query Parameters:**
- `format` (optional): `image` (default) or `url`

### Example 1: Retrieve Actual Frame Image

```bash
curl -X GET "http://localhost:5002/api/evidence/evt_abc123def456/frame" \
  --output frame.jpg \
  -H "Accept: image/jpeg"
```

**Response:** JPEG image file (640x480, ~70 KB)

### Example 2: Get Frame S3 URL Only

```bash
curl -X GET "http://localhost:5002/api/evidence/evt_abc123def456/frame?format=url"
```

**Response:**
```json
{
  "incident_id": "evt_abc123def456",
  "camera_id": "cam_01",
  "incident_date": "2026-04-28",
  "frame_url": "s3://evidence-frames/cam_01/2026-04-28/evt_abc123def456.jpg",
  "s3_endpoint": "http://minio:9000"
}
```

### Error Responses

**Incident Not Found:**
```json
{
  "error": "No incident found with ID: evt_notfound",
  "hint": "Ensure incident_id is correct and has been processed"
}
```

**Frame Not Found in S3:**
```json
{
  "error": "Frame not found in S3: cam_01/2026-04-28/evt_abc123def456.jpg",
  "s3_bucket": "evidence-frames",
  "s3_key": "cam_01/2026-04-28/evt_abc123def456.jpg"
}
```

---

## Frame Cleanup Job

### Purpose

Automatic deletion of frames older than retention window (30 days) to maintain bounded storage.

### Configuration

```bash
FRAME_RETENTION_DAYS=30        # Default: 30 days
S3_ENDPOINT=http://minio:9000
S3_BUCKET=evidence-frames
```

### Manual Execution

```bash
python /app/scripts/transform/frame_cleaner.py
```

### Scheduled Execution (Cron)

```bash
# Run cleanup weekly on Sundays at 2 AM
0 2 * * 0  cd /app/scripts/transform && python frame_cleaner.py >> /var/log/frame_cleaner.log 2>&1
```

### Output

```
[2026-04-28 02:00:01] [INFO] Frame Evidence Cleanup Job
  Bucket: evidence-frames
  Endpoint: http://minio:9000
  Retention: 30 days
  Cutoff date (delete before): 2026-03-29

[2026-04-28 02:05:42] [RESULTS]
  Scanned: 1,234 frames
  Deleted: 156 frames
  Size freed: 10.92 MB
  Errors: 0

[2026-04-28 02:05:42] [KAFKA] Published cleanup event to frame-cleanup-events topic
```

### Monitoring

Cleanup events are published to Kafka topic `frame-cleanup-events`:

```json
{
  "event_type": "frame_cleanup",
  "timestamp": "2026-04-28T02:05:42.123Z",
  "retention_days": 30,
  "deleted_count": 156,
  "deleted_size_mb": 10.92,
  "scanned_count": 1234,
  "errors": 0
}
```

---

## Integration with Frontend

### Incident Data Viewer

**Display:** Expand incident row → show thumbnail + frame URL link

```javascript
const getFrameThumbnail = (incident) => {
  if (incident.thumbnail_b64) {
    return `data:image/jpeg;base64,${incident.thumbnail_b64}`;
  }
  return null;
};

const downloadFrame = async (incident_id) => {
  const response = await fetch(`/api/evidence/${incident_id}/frame`);
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${incident_id}.jpg`;
  a.click();
};
```

### Vigilance Terminal (Chatbot)

**Flow:**
1. User asks: "Show evidence for incident evt_abc123"
2. RAG retrieves incident metadata + frame_url from ChromaDB
3. Chatbot responds with frame_url + thumbnail
4. User clicks frame → calls `/api/evidence/{incident_id}/frame`

**Example Response:**
```
"Tôi tìm thấy sự cố bạo lực tại cam_01 vào 14:30:45 với điểm rủi ro 0.95.

Ảnh chứng cứ:
[Thumbnail image displayed]

Link tải xuống: /api/evidence/evt_abc123def456/frame
```

### Analytics Dashboard

**Display:** Click on incident in chart → popup modal with:
- Risk score, timestamp, location
- Thumbnail preview
- "Download Full Evidence" button

---

## Error Handling & Retry Logic

### Frame Extraction Failures

**Scenario 1: Missing Thumbnail in Kafka Message**
- Log warning
- Publish to `frame-extraction-dlq` (dead-letter topic)
- Incident still stored in Paimon without frame_url

**Scenario 2: S3 Upload Failure**
- Retry up to 3 times with exponential backoff (2s, 4s, 8s)
- On final failure, publish to DLQ
- Incident still stored in Paimon without frame_url

**Scenario 3: Base64 Decoding Failure**
- Log error
- Publish to DLQ with original record + error message
- Skip incident

**Example DLQ Record:**
```json
{
  "original": {
    "event_id": "evt_bad_frame",
    "camera_id": "cam_01",
    "timestamp": "2026-04-28T14:30:45Z",
    "metadata": {
      "thumbnail": "invalid-base64-!!!"
    }
  },
  "error": "Incorrect padding",
  "timestamp": "2026-04-28T14:30:50.123Z"
}
```

### Monitoring Dead-Letter Topic

```bash
# View DLQ messages
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic frame-extraction-dlq \
  --from-beginning
```

---

## Security & Forensics

### Evidence Integrity

- **Immutable storage**: S3 objects cannot be modified (only deleted by lifecycle policy)
- **Metadata preserved**: S3 object metadata includes incident_id, camera_id, risk_score
- **Audit trail**: Frame capture timestamps in Paimon `frame_capture_ts`

### Access Control

- **Chatbot API**: Requires authentication (future: OAuth/JWT)
- **S3 Bucket**: MinIO ACLs (currently public download for dev)
- **Paimon**: Row-level security via Trino (future enhancement)

### Compliance

- **Retention**: 30 days (configurable per jurisdiction)
- **Encryption**: MinIO can be configured with TLS/encryption at rest
- **Audit logs**: Flink job logs all frame uploads

---

## Troubleshooting

### Issue: No Frames Being Uploaded

**Check:**
1. Is `frame-extractor` service running?
   ```bash
   docker ps | grep frame-extractor
   ```
2. Are incidents flowing through Kafka?
   ```bash
   docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic hot-violence-alerts-valid \
     --max-messages 5
   ```
3. Check frame-extractor logs:
   ```bash
   docker logs frame-extractor -f
   ```

### Issue: S3 Upload Timeout

**Symptoms:** Frames not appearing in S3, lots of retries in logs

**Causes:**
- MinIO service down
- Network latency
- S3 endpoint misconfigured

**Fix:**
```bash
# Test MinIO connectivity
docker exec -it frame-extractor python -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000', 
                   aws_access_key_id='minio', 
                   aws_secret_access_key='mypassword')
s3.head_bucket(Bucket='evidence-frames')
print('S3 OK')
"
```

### Issue: DLQ Growing (Many Failures)

**Check DLQ messages:**
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic frame-extraction-dlq \
  --from-beginning | jq .
```

**Common Errors:**
- `No thumbnail found` → Check if rtsp_inference_mock is producing thumbnails
- `S3 upload failed` → Check MinIO health & storage quota
- `Incorrect padding` → Check base64 encoding in producer

---

## Performance Tuning

### Frame Extractor Service

- **Parallelism**: 1 (sequential to prevent S3 race conditions)
- **Batch size**: Process 1 message at a time (streaming)
- **Checkpoint**: Every 30 seconds
- **Memory**: 256 MB
- **CPU**: 0.50 cores

**Optimization:** If latency > 5 seconds, increase memory & CPU allocation.

### Frame Cleanup Job

- **Frequency**: Weekly (can be adjusted via cron)
- **Batch size**: 100 objects per S3 delete request
- **Retention window**: 30 days (configurable)

**Optimization:** If cleanup takes > 1 hour, consider running daily.

---

## Future Enhancements

- [ ] Frame compression (JPEG → WebP for smaller size)
- [ ] Video clip extraction (not just keyframe)
- [ ] Frame fingerprinting (duplicate detection)
- [ ] ML-based frame quality scoring
- [ ] Integration with video player (timeline scrubber)
- [ ] Evidence chain-of-custody logs
- [ ] S3 encryption at rest
- [ ] Immutable backups (AWS Glacier, Azure Archive)

---

## References

- [Evidence Frame Storage Plan](../agent-guides/frame-evidence-storage.md)
- [S3 Path Convention](#s3-path-convention)
- [REST API Endpoints](#rest-api-for-frame-retrieval)
- [Kafka Topics Used](#kafka-topics)

### Kafka Topics

| Topic | Producer | Consumer | Purpose |
|-------|----------|----------|---------|
| `urban-safety-alerts` | rtsp_inference_mock | data_contract_validator | Raw frames from inference |
| `hot-violence-alerts-valid` | data_contract_validator | frame-extractor, sink_to_paimon | Validated incidents |
| `hot-violence-frames-uploaded` | frame-extractor | sink_to_paimon | Frame-enriched incidents |
| `frame-extraction-dlq` | frame-extractor | monitoring | Failed frame uploads |
| `frame-cleanup-events` | frame_cleaner | monitoring | Cleanup job events |
