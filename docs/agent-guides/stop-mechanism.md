# Graceful Stop Mechanism — Streaming Services

## Tổng quan
Các streaming service (`producer`, `inference-mock`) chạy **vòng lặp vô tận** để giả lập camera real-time. Khi `docker compose up -d`, chúng sẽ đẩy data vào Kafka **liên tục không dừng** cho đến khi bị dừng thủ công.

Để tránh data tràn không kiểm soát, mỗi script tích hợp cơ chế **stop file**: tạo một file trên filesystem → script tự phát hiện → thoát gracefully.

## Cách hoạt động

```
┌────────────────────��────────────────────────────────┐
│  Container khởi động                                │
│  ├─ Xóa stop file cũ (nếu có từ lần trước)         │
│  ├─ Kết nối Kafka, load camera registry             │
│  └─ Bắt đầu vòng lặp chính                         │
│       │                                             │
│       ▼                                             │
│  ┌─── while not os.path.exists(STOP_FILE): ───┐     │
│  │  • Check trạng thái camera                  │     │
│  │  • Gửi message vào Kafka                    │     │
│  │  • Sleep 0.1s rồi lặp lại                   │     │
│  └─────────────────────────────────────────────┘     │
│       │                                             │
│       ▼  (STOP file xuất hiện)                      │
│  Log "Stop file detected. Shutting down..."         │
│  Log "Total messages sent: N"                       │
│  Đóng Kafka producer → Container exit               │
└─────────────────────────────────────────────────────┘
```

## Lệnh sử dụng

### Dừng service
```bash
DOCKER="/c/Program Files/Docker/Docker/resources/bin/docker.exe"

# Dừng inference-mock
"$DOCKER" exec inference-mock touch /app/tmp/STOP

# Dừng producer
"$DOCKER" exec producer touch /app/tmp/STOP
```

### Khởi động lại
Không cần xóa stop file thủ công — script **tự xóa stop file cũ** khi khởi động lại:
```bash
COMPOSE="$DOCKER compose -f docker/docker-compose.yml --env-file docker/.env"

# Restart sẽ tự clear stop file
$COMPOSE restart inference-mock
$COMPOSE restart producer
```

### Kiểm tra trạng thái
```bash
# Xem script đang chạy hay đã dừng
"$DOCKER" ps --format "table {{.Names}}\t{{.Status}}" | grep -E "producer|inference"

# Xem logs (số messages đã gửi)
"$DOCKER" logs --tail 20 inference-mock
"$DOCKER" logs --tail 20 producer
```

## Cấu hình

| Biến môi trường | Mặc định | Mô tả |
|---|---|---|
| `STOP_FILE` | `/app/tmp/STOP` | Đường dẫn file dừng trong container |

Stop file được mount qua Docker volume:
- `producer` → volume `producer-tmp` → `/app/tmp`
- `inference-mock` → volume `inference-tmp` → `/app/tmp`

## Files liên quan
- `scripts/streaming/inference_mock.py` — Mock AI inference (line 17: `STOP_FILE`, line 91: while loop)
- `scripts/streaming/producerRTSP.py` — RTSP producer (line 18: `STOP_FILE`, line 136: while loop)
- `docker/docker-compose.yml` — Volume mounts (`producer-tmp`, `inference-tmp`)

## Lưu ý cho Agent
- **Khi test pipeline**: Nhớ dừng `inference-mock` sau khi test xong để tránh data thừa trong Kafka.
- **Khi debug**: Dừng → kiểm tra logs → sửa code → restart. Stop file tự clear khi restart.
- **restart policy** là `on-failure`: nếu script exit bình thường (qua stop file), container sẽ **không** tự restart. Phải dùng `docker compose restart` hoặc `up -d` để chạy lại.
