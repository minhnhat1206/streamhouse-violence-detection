# Alert Bot (Telegram) — Design Document

**Tính năng:** Bot Telegram cảnh báo bạo lực real-time. Khi pipeline phát hiện bạo lực → bot push tin nhắn + ảnh bằng chứng về nhóm/cán bộ an ninh.
**Trạng thái:** Design (chưa implement) · **Phụ thuộc:** P0 (luồng thật phải chạy trước)
**Repo:** `streamhouse-violence-detection` — thêm 1 service Docker mới.

---

## 1. Mục tiêu & không mục tiêu

**Mục tiêu**
- Real-time: sự kiện valid đến Kafka → Telegram trong < 5 giây.
- Message tiếng Việt, kèm ảnh bằng chứng (frame từ MinIO).
- Demo-friendly: có endpoint `/alert/test` để giả lập cảnh báo trên sân khấu.

**Không mục tiêu (out of scope)**
- Hai chiều (user reply, command parsing) — sau này mới cần.
- Gửi cho nhiều người/nhóm khác nhau theo quyền — hardcode 1 `CHAT_ID` cho KLTN.
- Alert qua SMS/Zalo/email — chỉ Telegram cho gọn.

---

## 2. Tại sao là consumer riêng, KHÔNG phải Flink side-effect?

| Cách | Vấn đề |
|---|---|
| Flink job gọi Telegram API trong `ProcessFunction` | ❌ HTTP side-effect trong streaming job = block task slot, vi phạm exactly-once, retry khó, crash job khi Telegram lỗi |
| **Consumer Kafka riêng (chọn)** | ✅ Tách biệt: job streaming không biết Telegram tồn tại. Bot lỗi/không ổn định không ảnh hưởng pipeline. At-least-once qua consumer group |

→ Bot là **sink riêng**, độc lập với compute Flink. Đây là pattern đúng.

---

## 3. Kiến trúc & luồng dữ liệu

```
Flink validator
      │ valid event (Kafka)
      ▼
Kafka topic: hot-violence-alerts-valid
      │ subscribe (group: alert-bot-group)
      ▼
┌─────────────────────────────────────────────┐
│ alert-bot service (FastAPI, Docker)         │
│                                             │
│  1. Load camera_registry.csv → lookup map   │
│  2. Filter: is_violent AND risk_score≥thr   │
│  3. Cooldown: per-camera TTL (tránh spam)   │
│  4. Format msg VN + gắn location            │
│  5. POST Telegram Bot API (sendPhoto/sendMessage) │
│  6. Manual commit offset (at-least-once)    │
└─────────────────────────────────────────────┘
      │ HTTPS
      ▼
Telegram → điện thoại cán bộ
```

**Topic đọc:** `hot-violence-alerts-valid` (output của `data_contract_validator`, post-validation).
**Consumer group:** `alert-bot-group` (theo convention `{service-name}-group`).

---

## 4. Logic cảnh báo (tránh spam)

| Quy tắc | Giá trị mặc định | Env var |
|---|---|---|
| Chỉ alert khi `is_violent=true` | — | — |
| Và `risk_score >= threshold` | `0.70` | `ALERT_MIN_RISK_SCORE` |
| Cooldown mỗi camera | `60` giây | `ALERT_COOLDOWN_SECONDS` |
| Bỏ qua nếu thiếu `camera_id` / `timestamp` | — | — |

**Cooldown** = trong memory dict `{camera_id: last_alert_epoch}`. Đơn giản, đủ cho KLTN (không cần Redis). Mất khi restart — chấp nhận được.

---

## 5. Enrichment location

`camera_registry.csv` đã có 15 camera HCMC với district/location. Load 1 lần lúc startup:

```python
# In-memory map: cam_01 → {"district": "Quận 1", "location": "Đường Nguyễn Huệ"}
CAMERAS = load_camera_registry("data/metadata/camera_registry.csv")
```

→ Message có location mà **không cần query Fluss** (giữ bot độc lập với storage layer).

---

## 6. Format message Telegram

```
🚨 *CẢNH BÁO BẠO LỰC*

📍 _Camera:_ cam_03 — Đường Nguyễn Huệ, Quận 1
⏰ _Thời gian:_ 14:32:07 — 13/06/2026
📊 *Mức rủi ro:* 0.91 (91%)
🏷️ *Loại:* FIGHTING
🆔 `evt_a1b2c3`

🔗 [Xem dashboard](http://<DASH_HOST>/alerts?id=evt_a1b2c3)
```
+ ảnh bằng chứng (frame) đính kèm qua `sendPhoto`.

---

## 7. Ảnh bằng chứng — 2 kịch bản (quan trọng)

MinIO bucket `inference-results` ghi là *public read*. Nhưng Telegram server phải **fetch được** URL đó từ internet công cộng.

| Trường hợp | Cách gửi |
|---|---|
| MinIO expose public (GCP có public IP/URL) | `sendPhoto` với `photo=<url>` — Telegram tự fetch. Đơn giản. |
| MinIO chỉ nội bộ (không public) | Bot **download** frame từ MinIO → **upload multipart** lên Telegram. Mạnh hơn, thêm ~15 dòng. |

→ **Implement kịch bản 2 (download-then-upload)** vì ổn định hơn cho demo (không phụ thuộc MinIO public). Nếu frame không có → fallback `sendMessage` (chỉ text).

---

## 8. Cấu hình (env vars — tất cả trong `docker/.env`)

> ⚠️ Theo `.claude/rules/secrets-security.md`: `TELEGRAM_BOT_TOKEN` **chỉ** trong `docker/.env`, KHÔNG hardcode.

| Biến | Ví dụ | Mô tả |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | `123456:ABC-DEF...` | Token từ @BotFather |
| `TELEGRAM_CHAT_ID` | `-1001234567890` | ID nhóm/cá nhân nhận (dùng `@userinfobot` lấy) |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Bootstrap trong network Docker |
| `ALERT_TOPIC` | `hot-violence-alerts-valid` | Topic subscribe |
| `ALERT_CONSUMER_GROUP` | `alert-bot-group` | Consumer group |
| `ALERT_MIN_RISK_SCORE` | `0.70` | Ngưỡng alert |
| `ALERT_COOLDOWN_SECONDS` | `60` | Cooldown/camera |
| `MINIO_ENDPOINT` | `minio:9000` | Để download frame |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | … | Creds MinIO |
| `MINIO_BUCKET` | `inference-results` | Bucket evidence |
| `DASHBOARD_URL` | `http://136.110.16.108:5173` | Link trong message |

Thêm vào `docker/.env.example` (placeholder, không giá trị thật) đúng convention.

---

## 9. Docker service spec

Thêm vào `docker/docker-compose.yml`. Tuân thủ `resource-limits.md` + `docker-config.md`:

```yaml
  alert-bot:
    build:
      context: ..
      dockerfile: docker/Dockerfile.alert-bot
    container_name: alert-bot
    restart: unless-stopped
    env_file: .env
    depends_on:
      kafka:
        condition: service_healthy
      minio:
        condition: service_healthy
    networks: [streamhouse-net]
    deploy:
      resources:
        limits:
          memory: 128m
          cpus: "0.25"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:5010/health"]
      interval: 30s
      timeout: 5s
      retries: 3
    # profiles: ["alerting"]   # bỏ comment nếu muốn tắt mặc định
```

`Dockerfile.alert-bot`: base `python:3.10-slim`, `pip install fastapi uvicorn kafka-python requests minio`, copy app.

---

## 10. Cấu trúc file

```
streamhouse-violence-detection/
├── alert-bot/
│   ├── __init__.py
│   ├── main.py              ← FastAPI app + lifespan (start consumer thread)
│   ├── config.py            ← Settings (pydantic-settings, đọc env)
│   ├── consumer.py          ← KafkaConsumer loop + filter + cooldown + commit
│   ├── notifier.py          ← Telegram send (sendPhoto/sendMessage + download frame)
│   ├── cameras.py           ← load camera_registry.csv → lookup dict
│   └── formatter.py         ← format message VN
├── docker/
│   ├── Dockerfile.alert-bot
│   └── .env.example         ← thêm các biến §8
└── tests/
    └── test_alert_bot.py    ← test formatter + cooldown logic
```

---

## 11. Code skeleton (minh họa, chưa hoàn chỉnh)

**`consumer.py`** — consumer loop trong daemon thread:
```python
import json, time
from kafka import KafkaConsumer
from .config import settings
from .notifier import send_alert
from .cameras import CAMERAS

_last_alert: dict[str, float] = {}

def should_alert(camera_id: str) -> bool:
    now = time.time()
    if now - _last_alert.get(camera_id, 0.0) < settings.alert_cooldown_seconds:
        return False
    _last_alert[camera_id] = now
    return True

def run_consumer():
    consumer = KafkaConsumer(
        settings.alert_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.alert_consumer_group,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )
    for msg in consumer:
        evt = msg.value
        try:
            if not evt.get("is_violent"):
                continue
            if float(evt.get("risk_score", 0)) < settings.alert_min_risk_score:
                continue
            if not should_alert(evt["camera_id"]):
                continue
            location = CAMERAS.get(evt["camera_id"], {})
            send_alert(evt, location)        # raises nếu Telegram lỗi
            consumer.commit()                # at-least-once: commit sau khi gửi OK
        except Exception as e:
            # KHÔNG commit → event sẽ được xử lý lại (at-least-once)
            logging.error(f"Alert failed (will retry): {e}")
            time.sleep(2)
```

**`notifier.py`** — gửi Telegram (download-then-upload):
```python
import io, requests
from minio import Minio
from .config import settings
from .formatter import format_message

def send_alert(evt: dict, location: dict):
    caption = format_message(evt, location)
    photo = _download_evidence_frame(evt)   # bytes or None
    if photo:
        url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendPhoto"
        files = {"photo": ("evidence.jpg", photo, "image/jpeg")}
        data = {"chat_id": settings.telegram_chat_id, "caption": caption, "parse_mode": "Markdown"}
        r = requests.post(url, files=files, data=data, timeout=10)
    else:
        url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
        r = requests.post(url, json={"chat_id": settings.telegram_chat_id,
                                     "text": caption, "parse_mode": "Markdown"}, timeout=10)
    r.raise_for_status()
```

**`main.py`** — FastAPI + lifespan + test endpoint:
```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from threading import Thread
from .consumer import run_consumer

@asynccontextmanager
async def lifespan(app: FastAPI):
    t = Thread(target=run_consumer, daemon=True)
    t.start()
    yield

app = FastAPI(title="Alert Bot", lifespan=lifespan)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/alert/test")
def test_alert():
    """Giả lập 1 cảnh báo — DÙNG CHO DEMO."""
    from .notifier import send_alert
    send_alert({"camera_id": "cam_03", "is_violent": True, "risk_score": 0.91,
                "event_id": "test", "event_type": "FIGHTING",
                "timestamp": "2026-06-13T14:32:07+00:00"}, {"location": "Đường Nguyễn Huệ", "district": "Quận 1"})
    return {"sent": True}
```

---

## 12. Resilience & xử lý lỗi

| Tình huống | Xử lý |
|---|---|
| Telegram API lỗi (network/rate-limit) | KHÔNG commit → retry event tiếp theo vòng lặp + sleep backoff |
| MinIO download frame lỗi | Fallback `sendMessage` (text only) — vẫn alert, chỉ thiếu ảnh |
| Kafka consumer crash | `restart: unless-stopped` + consumer group → tiếp tục từ offset đã commit |
| Spam khi phát hiện liên tục | Cooldown per-camera 60s |
| Event trùng (replay) | Chấp nhận gửi lại đôi khi (at-least-once) — hoặc check `event_id` đã gửi (TÙY CHỌN, set Redis/set in-memory) |

**Graceful stop** (theo convention STOP file của dự án): kiểm tra `/app/tmp/STOP` mỗi vòng poll, thoát sạch. Thêm `SIGTERM` handler cho `docker compose down`.

---

## 13. Security checklist

- [ ] `TELEGRAM_BOT_TOKEN` chỉ trong `docker/.env`, KHÔNG log ra stdout.
- [ ] `docker/.env.example` chỉ có placeholder.
- [ ] `.gitignore` đã có `.env`.
- [ ] KHÔNG hardcode token trong `notifier.py`/`docker-compose.yml`.
- [ ] MinIO creds lấy từ env (dùng lại biến đã có của hệ thống).

---

## 14. Test plan

| Test | Cách | Kết quả mong đợi |
|---|---|---|
| Bot nhận message | `POST /alert/test` | Telegram nhận tin + ảnh (hoặc text) |
| End-to-end real | P0 xong → VioMoViNet detect bạo lực → Kafka | Telegram nhận alert < 5s |
| Cooldown | 2 alert liên tiếp cùng camera trong 60s | Chỉ 1 tin gửi |
| Filter ngưỡng | Event `risk_score=0.5` (< 0.7) | Không gửi |
| Non-violent | `is_violent=false` (heartbeat) | Không gửi |
| Telegram down | Tắt network tạm | Bot retry, không crash, pipeline vẫn chạy |
| MinIO down | Dừng minio | Bot fallback sendMessage text-only |

---

## 15. Implementation checklist

- [ ] Tạo bot qua @BotFather → lấy `TELEGRAM_BOT_TOKEN`
- [ ] Tạo nhóm, add bot, lấy `TELEGRAM_CHAT_ID` (qua `@userinfobot`)
- [ ] Thêm env vars vào `docker/.env` + `.env.example`
- [ ] Tạo `alert-bot/` (6 file §10)
- [ ] Tạo `docker/Dockerfile.alert-bot`
- [ ] Thêm service `alert-bot` vào `docker-compose.yml` (§9)
- [ ] Implement consumer + cooldown + filter
- [ ] Implement notifier (sendPhoto download-upload + sendMessage fallback)
- [ ] `/health` + `/alert/test`
- [ ] Graceful stop (STOP file + SIGTERM)
- [ ] Test §14

**Ước tính:** ~1 ngày làm việc sau khi P0 xong.

---

## 16. Câu hỏi mở (quyết định trước khi code)

1. **Nhận alert cá nhân hay nhóm?** (chat_id của 1 người hay 1 group Telegram)
2. **MinIO có public URL không?** → quyết định kịch bản §7 (nếu có, đơn giản hơn).
3. **Có dedup `event_id` không?** (tránh gửi trùng khi replay) — khuyên CÓ, in-memory set.
4. **Đặt service ở profile `alerting` hay core?** → khuyên **core** (luôn chạy, 128m nhỏ).

---

*Tham chiếu: `THESIS_DEFENSE_PLAN.md` (P3-adjacent), `.claude/rules/{secrets-security,resource-limits,docker-config,streaming-scripts}.md`.*
