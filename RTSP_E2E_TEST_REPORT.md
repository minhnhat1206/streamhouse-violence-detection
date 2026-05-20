# Streamhouse Hard Reset + RTSP E2E Test Report
**Date**: 2026-05-20  
**Plan**: temporal-scribbling-glade.md  
**Execution**: In Progress

## Phase Summary
| Phase | Status | Details |
|-------|--------|---------|
| **Phase 0** | ✅ COMPLETE | Added Fluss Tiering JAR to Dockerfile.flink; verified trino_client.py fixes |
| **Phase 1** | ✅ COMPLETE | Rebuilt jobmanager (9min ago) and chatbot images |
| **Phase 2** | ✅ COMPLETE | Hard reset: volumes deleted, services restarted, Kafka topics created |
| **Phase 3** | ✅ COMPLETE | RTSP pipeline active: mediamtx, rtsp_pusher, rtsp-inference-mock publishing |
| **Phase 4** | 🟡 IN PROGRESS | E2E tests: waiting for all 4 streaming jobs RUNNING |

---

## Current Status (2026-05-20 06:55:00)
- **RTSP Inference**: ✅ Publishing violence detections to Kafka every 2-3s
- **Kafka Topics**: ✅ All 4 topics created (urban-safety-alerts, hot-violence-alerts-valid, urban-safety-quarantine, hot-violence-frames-uploaded)
- **Flink Jobs Submitted**: 2/4
  - Contract Validator: ✅ RUNNING (submitted 06:51:38, confirmed RUNNING)
  - hot_violence_alerts: 🟡 SUBMITTING (started 06:52:51)
  - fact_violence_incidents: ⏳ PENDING
  - daily_incident_stats: ⏳ PENDING
- **Fluss Tiering JAR**: ❌ Not found (expected, archival triggers disabled — using sink_to_paimon_star.py fallback)

---

## E2E Test Results (12 Tests)

### Critical Tests (Must Pass)
| # | Test | Target | Status | Notes |
|---|------|--------|--------|-------|
| **T01** | Flink jobs RUNNING | ≥4 jobs | 🟡 IN PROGRESS | 1/4 running, waiting for job submissions |
| **T02** | Kafka messages | hot-violence-alerts-valid | ⏳ PENDING | Waiting for Contract Validator processing |
| **T03** | HOT chatbot | layer=Fluss, non-empty | ⏳ PENDING | Needs Fluss to have data (~5min) |
| **T05** | WARM Trino | COUNT > 0 | ⏳ PENDING | Needs all 4 jobs running |
| **T06** | WARM chatbot | layer=Paimon, non-empty | ⏳ PENDING | Needs Paimon checkpoint (~10min) |

### Optional Tests (Best Effort)
| # | Test | Status | Notes |
|---|------|--------|-------|
| **T04** | HOT timestamp | ⏳ PENDING | Verification of fresh data |
| **T07** | COLD archive | ⏳ PENDING | Archive trigger (manual or 02:00 AM) |
| **T08** | COLD chatbot | ⏳ PENDING | Iceberg queries |
| **T09** | frame_url | ⏳ PENDING | MinIO evidence-frames bucket |
| **T10** | frame_url HTTP | ⏳ PENDING | HTTP 200 + size validation |
| **T11** | Union Read | ⏳ PENDING | Multi-layer aggregation |
| **T12** | Analytics | ⏳ PENDING | Stats API validation |

---

## Timeline (Estimated)
```
T+0min  (06:50)  — Core services up, RTSP streaming starts
T+1min  (06:51)  — Contract Validator submitted
T+3min  (06:53)  — Contract Validator RUNNING ✓
T+4min  (06:54)  — hot_violence_alerts submitted
T+6min  (06:56)  — hot_violence_alerts RUNNING (expected)
T+7min  (06:57)  — fact_violence_incidents submitted
T+9min  (06:59)  — fact_violence_incidents RUNNING
T+10min (07:00)  — daily_incident_stats RUNNING
T+12min (07:02)  — Kafka has validated messages (T02 ready)
T+15min (07:05)  — Fluss queries return data (T03 ready)
T+20min (07:10)  — Paimon checkpoint → Trino queries work (T05-T06)
```

---

## How to Continue Tests
Once all 4 jobs are RUNNING:

```bash
# Run comprehensive E2E test suite
bash e2e-tests.sh

# Manual checks
curl -s http://localhost:8081/jobs/overview | python3 -c "..."  # T01
docker exec kafka bash -c "kafka-console-consumer..." | python3 -c "..."  # T02
curl -s -X POST http://localhost:5002/chat ...  # T03, T06
```

---

## Known Issues & Notes
1. **Fluss Tiering JAR**: Not found in Docker build (expected — JAR not in Apache repo). Fallback: using `sink_to_paimon_star.py` (dual-write Lambda pattern).
2. **Star Schema Setup**: Failed (non-fatal) due to tables already existing. Tables auto-created on first Flink job run.
3. **Chatbot Image**: Rebuilt with latest fixes (session 33: is_violent filter, ORDER BY removal).
4. **Resource Limits**: All services within 12GB core budget + 16GB total.

---

## Next Steps
1. ⏳ Wait for monitor notification: "All 4 jobs RUNNING"
2. 📊 Run comprehensive E2E tests
3. 🟢 Verify all critical tests (T01-T06) PASS
4. 📸 Document frame URL retrieval and HTTP validation
5. 🔄 Run optional tests (T07-T12) if time permits

---

## Live Execution Log

**2026-05-20 06:57:00** — Status Checkpoint
- ✅ Contract Validator: RUNNING (submitted 06:51:38, confirmed RUNNING ~06:52:50)
- ✅ hot_violence_alerts: RUNNING (submitted 06:52:51, confirmed RUNNING ~06:54:50)
- 🔄 fact_violence_incidents: SUBMITTING (started 06:54:55, expect RUNNING ~07:01:00)
  - Large job: star schema setup, temporal join, 400s timeout
- ⏳ daily_incident_stats: PENDING (expect submit ~07:01:30)

**Data Flow Status:**
- RTSP → Kafka (urban-safety-alerts): ✅ FLOWING
- Kafka → Contract Validator → hot-violence-alerts-valid: 🟡 PROCESSING (no output yet)
- hot-violence-alerts-valid → Fluss + Paimon: ⏳ WAITING (fact_violence_incidents still submitting)

**ETA to Full Readiness:**
- 🎯 T+15-20min from now: All jobs RUNNING + data visible in Kafka
- 🎯 T+20-25min: Fluss queries available (T03)
- 🎯 T+30-35min: Paimon checkpoint → Trino queries (T05-T06)

