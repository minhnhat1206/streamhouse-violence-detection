# Real Producer Integration (VioMoViNet ↔ Streamhouse)

Rule cross-repo: producer Kafka **thật** cho topic `urban-safety-alerts` là **VioMoViNet server** (repo riêng: `../VioMoViNet`), không phải mock.

## Bắt buộc (KHÔNG vi phạm)
- **KHÔNG bật `--profile streaming`** và **KHÔNG dùng `docker/docker-compose.local-stream.yml`** khi VioMoViNet thật đang chạy → sẽ double-publish (real + mock cùng topic `urban-safety-alerts`).
- **KHÔNG sửa Flink data-contract validator** (`scripts/transform/data_contract_validator.py`) để "nuông" producer. Producer (mock hay thật) PHẢI conform contract. Tham khảo contract: `docs/REAL_PRODUCER_INTEGRATION_PLAN.md` + `VioMoViNet/.claude/rules/kafka-producer.md`.
- Mock `rtsp-inference-mock` (trong `docker/docker-compose.yml` + `deploy/docker-compose.gcp.yml`) đã `profiles:[streaming]` — giữ yên, KHÔNG đưa về core/always-on. (Không có standalone `inference-mock` — `resource-limits.md` cũ stale.)

## Topology
- **GCP Kafka external:** `34.124.131.144:9093`, PLAINTEXT (IP confirmed — PARTNER_GUIDE/DEVELOPER_LOG; `.env.gcp` `GCP_VM_EXTERNAL_IP` drives advertised listener).
- VioMoViNet chạy **riêng** (GPU box 2×2080Ti), reach Kafka qua host network → `<gcp-ip>:9093`. Không join docker network `violence-detection-net`.
- VioMoViNet config `KAFKA_ENABLED=true` + `KAFKA_BOOTSTRAP_SERVERS=34.124.131.144:9093` thì mới publish (default false → standalone).

## Payload (producer thật emit) — đã khớp mock
`event_id, camera_id(cam_NN), timestamp(ISO8601 UTC), is_violent, risk_score, confidence, event_type("FIGHTING"|null), location(empty→enrich bằng dim_camera join), metadata{fps, latency_ms, mock:false, rtsp_connected, thumbnail(base64), evidence_url}`. `is_valid` do Flink set.

## Khi làm việc ở streamhouse
- Muốn test pipeline không cần VioMoViNet → tạm dùng mock: `docker compose -f docker/docker-compose.yml --profile streaming up -d rtsp-inference-mock` (nhưng nhận biết đây là **mock**, đánh dấu rõ trong kết quả/benchmark).
- Producer thật đã tích hợp xong (Session 2026-06-18, status IMPLEMENTED — chưa verify E2E). Chi tiết: `docs/REAL_PRODUCER_INTEGRATION_PLAN.md`, `DEVELOPER_LOG.md`.
