# Nhiệm vụ: Sửa bug + Test lại Time Travel Queries

> **Ngày:** 2026-04-19 | **Agent:** Gemini
> **File cần sửa:** `scripts/transform/time_travel_queries.py`

---

## Kết quả test lần trước — FAIL

```
[PAIMON] 1. Listing Paimon Snapshots:
[WARNING] General Paimon query failed. Error: An error occurred while calling o8.executeSql.
```

**Query 1 fail → outer try/catch bắt → query 2, 3, 4 bị SKIP hoàn toàn.**
Iceberg cũng fail tương tự — lỗi ở SQL execution level, không phải "no data".

---

## Bug #1: Mỗi query phải có try/catch riêng

Hiện tại 4 query Paimon nằm chung 1 try/catch block → query 1 fail thì 2, 3, 4 không chạy.

**Sửa:** Mỗi query wrap try/except riêng, ví dụ:

```python
# Query 1
try:
    t_env.execute_sql("... $snapshots ...").print()
except Exception as e:
    print(f"[SKIP] Snapshots query failed: {e}")

# Query 2 — vẫn chạy dù query 1 fail
try:
    t_env.execute_sql("... scan.snapshot-id ...").print()
except Exception as e:
    print(f"[SKIP] Snapshot-id query failed: {e}")

# ... tương tự cho query 3, 4
```

## Bug #2: Error message quá chung

`str(e).splitlines()[0]` chỉ in `An error occurred while calling o8.executeSql` — vô nghĩa.

**Sửa:** In full exception để debug được:

```python
except Exception as e:
    print(f"[SKIP] Query failed: {e}")  # in toàn bộ, không splitlines
```

---

## Quy trình test lại (QUAN TRỌNG)

### Hiện tại TẤT CẢ services đều TẮT. Phải bật lại theo đúng thứ tự:

```bash
DOCKER="/c/Program Files/Docker/Docker/resources/bin/docker.exe"

# Bước 1 — Core services
"$DOCKER" compose -f docker/docker-compose.yml up -d kafka minio minio_client mysql hive-metastore
# Chờ ~40s cho kafka + mysql + hive-metastore healthy

# Bước 2 — Flink cluster
"$DOCKER" compose -f docker/docker-compose.yml up -d jobmanager taskmanager
# Chờ ~30s, verify: http://localhost:8081

# Bước 3 — Fluss cluster
"$DOCKER" compose -f docker/docker-compose.yml up -d fluss-zookeeper fluss-coordinator fluss-tablet
# Chờ ~15s

# Bước 4 — Init tables
MSYS_NO_PATHCONV=1 "$DOCKER" exec jobmanager python /opt/flink/scripts/init_fluss_tables.py
MSYS_NO_PATHCONV=1 "$DOCKER" exec jobmanager python /opt/flink/scripts/init_paimon_tables.py
MSYS_NO_PATHCONV=1 "$DOCKER" exec jobmanager python /opt/flink/scripts/init_iceberg_tables.py

# Bước 5 — Start producer + submit 4 streaming jobs
"$DOCKER" compose -f docker/docker-compose.yml up -d inference-mock
sleep 10
MSYS_NO_PATHCONV=1 "$DOCKER" exec jobmanager flink run -py /opt/flink/scripts/data_contract_validator.py -d
MSYS_NO_PATHCONV=1 "$DOCKER" exec jobmanager flink run -py /opt/flink/scripts/sink_to_fluss.py -d
MSYS_NO_PATHCONV=1 "$DOCKER" exec jobmanager flink run -py /opt/flink/scripts/sink_to_paimon.py -d
MSYS_NO_PATHCONV=1 "$DOCKER" exec jobmanager flink run -py /opt/flink/scripts/aggregate_paimon.py -d

# Bước 6 — Verify 4 jobs RUNNING
MSYS_NO_PATHCONV=1 "$DOCKER" exec jobmanager flink list

# Bước 7 — CHỜ 2-3 PHÚT cho Paimon tạo nhiều snapshots (checkpoint mỗi 30s)

# Bước 8 — Chạy time travel script
MSYS_NO_PATHCONV=1 "$DOCKER" exec jobmanager python /opt/flink/scripts/time_travel_queries.py

# Bước 9 — Dừng producer sau khi test
MSYS_NO_PATHCONV=1 "$DOCKER" exec inference-mock touch /app/tmp/STOP
```

### Lưu ý
- **Không chạy time travel script khi chưa bật services** — sẽ fail 100%
- Paimon cần **ít nhất 2-3 checkpoint cycles** (mỗi 30s) để có snapshots
- Iceberg có thể không có data nếu chưa chạy `archive_to_iceberg.py` — OK, chỉ cần skip gracefully

---

## Sau khi test thành công

Cập nhật `DEVELOPER_LOG.md` Last State với kết quả output thực tế.
