# Daily Security Report (Telegram) — Design Document

**Tính năng:** Mỗi ngày (hoặc on-demand) bot Telegram gửi **1 báo cáo an ninh hoàn chỉnh** — tổng hợp toàn bộ sự cố trong ngày thành narrative tiếng Việt + biểu đồ.
**Trạng thái:** Design (chưa implement) · **Phụ thuộc:** P0 (luồng thật) + data đã có trong Paimon WARM (`daily_incident_stats`, `camera_stats`)
**Repo:** `streamhouse-violence-detection` — mở rộng từ service `alert-bot` (hoặc module riêng).

---

## 1. Khác biệt với Alert Bot (RẤT QUAN TRỌNG — đừng nhầm)

Đây là **2 output khác hẳn nhau** qua cùng kênh Telegram:

| | Alert Bot | Daily Report (doc này) |
|---|---|---|
| **Mục đích** | Cảnh báo tức thời 1 sự cố | Tổng hợp 1 khoảng thời gian |
| **Trigger** | Event-driven (Kafka) | Lịch (cron) / on-demand |
| **Độ trễ** | < 5s real-time | Không quan trọng (batch) |
| **Dữ liệu** | 1 event | N sự cố đã aggregate |
| **LLM?** | ❌ KHÔNG (deterministic) | ✅ **CÓ** (Gemini tổng hợp) |
| **Output** | Tin ngắn + 1 ảnh | Báo cáo dài + biểu đồ |

→ Alert Bot = "điện thoại rung khi có cháy". Daily Report = "bản tin cuối ngày cho chỉ huy". **Hai thứ khác nhau, dùng LLM ở đúng chỗ.**

---

## 2. Tại sao đây MỚI là chỗ LLM hợp lý

LLM mạnh ở việc **biến bảng số liệu thành insight ngôn ngữ** — không phải ở format 1 tin nhắn.

| Đầu vào (deterministic, từ Paimon) | Đầu ra (LLM sinh) |
|---|---|
| `{today: 12, yesterday: 10, top_district: "Quận 1": 5, peak_hour: "20-22h": 6}` | "Hôm nay ghi nhận **12 vụ bạo lực**, tăng 20% so với hôm qua. Tình hình tập trung tại **Quận 1** (5 vụ), cao điểm khung giờ **20–22h**. Khuyến nghị tăng cường tuần tra khu vực Nguyễn Huệ vào buổi tối." |

→ Đây là giá trị thật: **rút insight + khuyến nghị** mà deterministic không làm được. LLM ở đây *đáng đồng tiền*.

---

## 3. Kiến trúc & luồng dữ liệu

```
┌─ Trigger ─────────────────────────────┐
│  • Cron hàng ngày (08:00)             │
│  • POST /report/daily (on-demand)     │
│  • Telegram cmd /baocao (sau này)     │
└───────────────┬───────────────────────┘
                ▼
┌─ 1. Thu thập số liệu (Trino/Flink SQL Gateway) ─┐
│  Query Paimon: daily_incident_stats, camera_stats│
│  → JSON aggregation {total, by_district,         │
│     by_camera, by_hour, vs_yesterday}            │
└───────────────┬──────────────────────────────────┘
                ▼
┌─ 2. Sinh biểu đồ (matplotlib) ───────────────────┐
│  • Bar: sự cố theo giờ                           │
│  • Bar: sự cố theo quận                          │
│  → 2 PNG                                         │
└───────────────┬──────────────────────────────────┘
                ▼
┌─ 3. LLM tổng hợp (Gemini 2.5-flash) ─────────────┐
│  Prompt: số liệu JSON → báo cáo VN (5 phần)      │
│  Anti-hallucination: chỉ dùng số đã cho          │
└───────────────┬──────────────────────────────────┘
                ▼
┌─ 4. Gửi Telegram ────────────────────────────────┐
│  sendMessage (báo cáo markdown, split nếu >4096) │
│  + sendMediaGroup (2 biểu đồ)                    │
└──────────────────────────────────────────────────┘
```

---

## 4. Nguồn dữ liệu (tái dùng hạ tầng đã có)

- Bảng **Paimon WARM** đã có từ job `aggregate_paimon.py`:
  - `daily_incident_stats` — tổng hợp theo ngày
  - `camera_stats` — theo camera
- Query qua **Flink SQL Gateway** (Paimon không query được qua Trino — *reused logic từ chatbot `trino_client.py`*).
- Nếu muốn báo cáo tuần/tháng → query **Iceberg COLD** qua Trino.

→ **Không invent thêm data**, dùng đúng aggregation đã tính sẵn. Điều này reinforce đóng góp Streamhouse của bạn (report dùng output của layer WARM).

---

## 5. Nội dung báo cáo "hoàn chỉnh" (template)

```
📊 *BÁO CÁO AN NINH HẰNG NGÀY*
📅 Thứ Sáu, 13/06/2026

━━━━━━━━━━━━━━━━━━━━
*1. TÓM TẮT*  (LLM — 2-3 câu)
Hôm nay ghi nhận 12 vụ bạo lực, tăng 20% so với hôm qua.
Tình hình tập trung tại Quận 1, cao điểm buổi tối.

*2. SỐ LIỆU CHÍNH*
• Tổng vụ hôm nay: 12  (hôm qua: 10, ↑20%)
• Camera phát hiện: 5/15
• Mức rủi ro TB: 0.78

*3. PHÂN BỐ ĐỊA ĐIỂM*  (top cameras)
• cam_03 — Đường Nguyễn Huệ, Q1: 4 vụ
• cam_07 — Bến Bách Đăng, Q1: 3 vụ
• cam_11 — Đường Lê Lợi, Q1: 2 vụ

*4. CAO ĐIỂM THỜI GIAN*
• 20–22h: 6 vụ (50%)

*5. NHẬN ĐỊNH & KHUYẾN NGHỊ*  (LLM)
Các vụ tập trung vào khung giờ tối tại khu trung tâm
Quận 1. Khuyến nghị tăng cường tuần tra cam_03 và
cam_07 trong 20–22h ngày mai.

━━━━━━━━━━━━━━━━━━━━
Nguồn: Paimon WARM (daily_incident_stats) · 15 camera HCMC
```
+ 2 biểu đồ (theo giờ, theo quận) đính kèm.

---

## 6. LLM synthesis — prompt & anti-hallucination

**Prompt** (Gemini 2.5-flash):
```python
prompt = f"""
Bạn là trợ lý an ninh. Dựa trên số liệu JSON, viết báo cáo tiếng Việt theo 5 phần:
1. Tóm tắt (2-3 câu), 2. Số liệu chính, 3. Phân bố địa điểm,
4. Cao điểm thời gian, 5. Nhận định & khuyến nghị.

Chỉ dùng số liệu trong JSON, KHÔNG bịa. Dùng Markdown in đậm số quan trọng.

Số liệu:
{aggregation_json}
"""
```

**Anti-hallucination** (reused pattern từ chatbot `generate_response`):
- Nếu Gemini nhắc số không có trong JSON → override bằng số thật từ JSON.
- Luôn kèm dòng `Nguồn: Paimon WARM (daily_incident_stats)` (citation).

---

## 7. Stack & delivery

| Thành phần | Chọn | Lý do |
|---|---|---|
| Ngôn ngữ | Python 3.10 | Khớp stack |
| Query layer | **Reuse chatbot `trino_client.py`** | Đã có logic Paimon→Flink SQL Gateway |
| LLM | `google.generativeai` (Gemini 2.5-flash) | Cùng chatbot |
| Biểu đồ | `matplotlib` | Sinh PNG, nhẹ |
| Scheduler | `APScheduler` (cron trong service) | Đơn giản. *Hoặc Airflow DAG* (đã có profile `orchestration`) |
| Telegram | raw HTTP (như Alert Bot) | One-way push |
| Delivery format | **Markdown message + 2 PNG** (khuyên). *Hoặc PDF qua reportlab* nếu muốn "tài liệu" thật | md đủ "hoàn chỉnh", ít code |

**Telegram giới hạn 4096 ký tự/message** → nếu báo cáo dài, split thành 2–3 message liên tiếp, hoặc gửi 1 file `.md`.

---

## 8. Trigger

| Trigger | Khi nào | Dùng cho |
|---|---|---|
| **Cron 08:00 mỗi ngày** | Tự động | Production |
| `POST /report/daily?date=2026-06-13` | On-demand | **DEMO bảo vệ** — sinh báo cáo ngay trên sân khấu |
| `/report/weekly` | Theo nhu cầu | Mở rộng |
| Telegram `/baocao` | Khi user gõ | Nếu làm 2 chiều sau này |

---

## 9. Cấu hình (env — thêm vào `docker/.env`)

| Biến | Mặc định | Mô tả |
|---|---|---|
| `REPORT_CRON_HOUR` | `8` | Giờ chạy báo cáo daily |
| `GEMINI_API_KEY` | (đã có) | Reuse key của chatbot |
| `TRINO_HOST/PORT` | (đã có) | Query Paimon/Iceberg |
| `TELEGRAM_BOT_TOKEN` / `CHAT_ID` | (shared với alert bot) | Cùng bot, cùng nhóm |
| `REPORT_TIMEZONE` | `Asia/Ho_Chi_Minh` | Cron theo giờ VN |

---

## 10. Cấu trúc file (mở rộng alert-bot thành "messaging service")

```
alert-bot/                      ← đổi tên concept thành "comms service"
├── main.py                     ← FastAPI + lifespan (consumer + scheduler)
├── consumer.py                 ← Alert Bot (realtime, no LLM)   [doc ALERT_BOT]
├── report/
│   ├── collector.py            ← query Paimon aggregations (reuse trino_client)
│   ├── charts.py               ← matplotlib → PNG (by hour, by district)
│   ├── synthesizer.py          ← Gemini tổng hợp báo cáo VN
│   └── scheduler.py            ← APScheduler cron 08:00
├── notifier.py                 ← Telegram send (shared: alert + report)
├── cameras.py                  ← camera_registry lookup (shared)
└── config.py
```
→ **1 service, 2 output**: realtime alert (deterministic) + daily report (LLM). Share notifier + config + cameras.

---

## 11. Code skeleton

**`report/collector.py`** — lấy số liệu:
```python
def collect_daily(date_str: str) -> dict:
    """Query Paimon daily_incident_stats + camera_stats → aggregation JSON."""
    today = query_paimon("SELECT count(*) FROM security.daily_incident_stats WHERE dt = %(d)s", d=date_str)
    yesterday = query_paimon(... "WHERE dt = %(d)s", d=prev_day)
    by_district = query_paimon("SELECT district, count(*) ... GROUP BY district ORDER BY 2 DESC LIMIT 5")
    by_hour = query_paimon("SELECT hour, count(*) ... GROUP BY hour")
    return {"today": today, "yesterday": yesterday,
            "by_district": by_district, "by_hour": by_hour}
```

**`report/synthesizer.py`** — Gemini:
```python
import google.generativeai as genai
def synthesize(data: dict, date_str: str) -> str:
    model = genai.GenerativeModel("gemini-2.5-flash")
    prompt = f"Báo cáo an ninh {date_str} theo 5 phần... Chỉ dùng số liệu:\n{json.dumps(data, ensure_ascii=False)}"
    return model.generate_content(prompt).text
```

**`main.py`** — thêm endpoint on-demand + scheduler:
```python
@app.post("/report/daily")
def report_daily(date: str | None = None):
    d = date or today_vn()
    data = collect_daily(d)
    charts = render_charts(data)            # list[PNG bytes]
    text = synthesize(data, d)
    send_long_message(text)                 # split if >4096
    send_media_group(charts)
    return {"sent": True, "incidents": data["today"]}

# Scheduler (APScheduler) — cron 08:00 Asia/Ho_Chi_Minh
scheduler.add_job(lambda: report_daily(), "cron", hour=8,
                  timezone="Asia/Ho_Chi_Minh")
```

---

## 12. Resilience & lỗi

| Tình huống | Xử lý |
|---|---|
| Paimon chưa có data (tiering chưa chạy) | Báo cáo "Không có dữ liệu cho ngày X" (đừng để Gemini bịa) |
| Gemini lỗi | Fallback: báo cáo template điền số liệu thô (không narrative) |
| 0 sự cố trong ngày | Vẫn gửi: "Hôm nay không ghi nhận vụ bạo lực nào" + biểu đồ rỗng |
| Telegram >4096 ký tự | Split message |
| Service restart giữa lịch | Cron chạy lại lần kế tiếp (không retry lỡ) |

---

## 13. Test plan

| Test | Cách | Mong đợi |
|---|---|---|
| On-demand | `POST /report/daily` | Telegram nhận báo cáo + 2 biểu đồ |
| Không có data | Query ngày rỗng | "Không có dữ liệu cho ngày X" |
| Anti-hallucination | Ngày 0 vụ | KHÔNG bịa số |
| Cron | Đợi 08:00 (hoặc test cron `* * * * *`) | Tự gửi |
| So sánh hôm qua | 2 ngày có data | Hiện ↑/↓ % đúng |
| Biểu đồ | Check PNG | 2 ảnh đúng nội dung |

---

## 14. Implementation checklist

- [ ] P0 xong (có data thật trong Paimon)
- [ ] Verify `daily_incident_stats` / `camera_stats` có data (`api/layer-counts` → warm > 0)
- [ ] Reuse `trino_client.py` cho query Paimon
- [ ] `report/collector.py` + `charts.py` + `synthesizer.py`
- [ ] `send_long_message` (split 4096) + `send_media_group`
- [ ] `POST /report/daily` on-demand
- [ ] APScheduler cron 08:00
- [ ] Test §13

**Ước tính:** ~1.5–2 ngày sau khi alert-bot xong (share nhiều code).

---

## 15. Câu hỏi mở

1. **Daily hay weekly hay cả hai?** (khuyên daily trước)
2. **Báo cáo dạng markdown message hay PDF file?** (khuyên markdown + chart, đơn giản)
3. **Scheduler trong service (APScheduler) hay Airflow DAG?** (APScheduler đơn giản hơn; Airflow "production" hơn nếu bạn muốn demo orchestration)
4. **Có phần "Khuyến nghị" (LLM) không?** — khuyên CÓ, đó là chỗ LLM thêm giá trị rõ nhất.

---

## 16. Tóm tắt cho defense

> *"Hệ thống có 2 kênh cảnh báo qua Telegram: **alert real-time** (deterministic, per-event, không LLM — vì cần nhanh/ổn định) và **báo cáo daily** (LLM tổng hợp N sự cố thành narrative + biểu đồ — đây là chỗ LLM thực sự tạo giá trị, biến số liệu thành insight). Cả hai tái dùng hạ tầng: alert đọc Kafka HOT, report đọc Paimon WARM aggregations."*

→ Đây là cách trả lời đẹp cho câu *"LLM dùng ở đâu, có hợp không"*: **LLM ở đúng 2 chỗ** (chatbot + daily report), không nhét khắp nơi.

---

*Tham chiếu: `ALERT_BOT_DESIGN.md` (output song song), `THESIS_DEFENSE_PLAN.md`, `.claude/rules/*`.*
