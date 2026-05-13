# Streamhouse Migration Roadmap
_Cập nhật: 2026-05-05_

---

## Kiến trúc mục tiêu

```
RTSP (cam_01~15)
    ↓ rtsp_pusher (simulateRTSP.py)
MediaMTX (WebRTC/HLS)          ← Dashboard Live Streams
    ↓ rtsp_inference_mock.py
Kafka [urban-safety-alerts]
    ↓ Flink streaming job
 ┌──────────────────────────┐
 │  Fluss  (HOT  <1min)     │  ← Dashboard Alerts (real-time)
 │  Paimon (WARM 1-10min)   │  ← Dashboard Analytics (aggregated)
 │  Iceberg(COLD 10min+)    │  ← Dashboard Historical / Chatbot
 └──────────────────────────┘
    ↓
Backend API (port 3000)
    ↓
Violence-Urban-Safety-UI
```

---

## Trạng thái hiện tại (2026-05-05)

### DONE ✅
| Component | Trạng thái |
|-----------|-----------|
| MediaMTX RTSP/HLS/WebRTC | Running — cam_01~08 live |
| `simulateRTSP.py` | Created — streams RWF-2000 clips |
| `rtsp_inference_mock.py` | Running — 15 camera threads |
| `inference_mock.py` | Running — backup generator |
| Kafka `urban-safety-alerts` | **361,664+ messages** flowing |
| Fluss HOT catalog + table | Schema created |
| Flink → Fluss pipeline | Script ready (`submit_pipeline.py`) |
| Dashboard Live Streams UI | **WebRTC video playing** cam_01~08 |
| `webrtcAllowOrigin: '*'` | Fixed — CORS resolved |
| `@babel/core` install | Fixed — vite error resolved |

### IN PROGRESS / BROKEN ⚠️
| Component | Vấn đề | Ưu tiên |
|-----------|---------|---------|
| Flink job → Fluss | **FAILED** — cần re-submit | P0 |
| Paimon WARM layer | `ClassNotFoundException: S3AFileSystem` — thiếu `hadoop-aws` JAR | P0 |
| cam_09~cam_15 livestream | Chưa có playlist → WHEP 404 | P1 |

### TODO — Migration từ Medallion → Streamhouse 🔲
| Task | Mô tả | Ưu tiên |
|------|-------|---------|
| **Backend API port 3000** | Hiện tại gọi kiến trúc cũ (Medallion/Spark). Cần viết lại backend dùng Fluss + Paimon + Iceberg thay thế | P0 |
| **Alerts page** (`/alertsdashboard`) | Đang fetch `localhost:3000/alerts` → cần đổi sang Fluss HOT real-time query | P1 |
| **Analytics page** (`/analytics`) | Đang fetch `localhost:3000/analytics` → cần đổi sang Paimon WARM aggregations | P1 |
| **Assistant/Chatbot** | Kiểm tra xem đang dùng Gemini trực tiếp hay backend RAG (port 5002) | P2 |
| **Paimon hadoop-aws JAR** | Thêm `hadoop-aws` + `aws-java-sdk-bundle` vào Flink Docker image | P0 |
| **Flink → Paimon pipeline** | Submit `INSERT INTO paimon_cat.security.violence_incidents` streaming job | P1 |
| **cam_09~cam_15 playlists** | Tạo playlist files cho 7 camera còn thiếu | P2 |
| **Iceberg COLD layer** | Flink job ghi dữ liệu cũ hơn 10 phút vào Iceberg cho Trino query | P3 |
| **Settings page** | Kiểm tra chức năng | P3 |

---

## Kế hoạch thực hiện

### Sprint 1 — Đảm bảo data chảy đầy đủ (P0)
1. Re-submit Flink → Fluss job (auto-restart on failure)
2. Fix Paimon S3AFileSystem: add `hadoop-aws` JAR vào Flink image
3. Submit Flink → Paimon job sau khi Paimon fix xong

### Sprint 2 — Backend API mới (P0-P1)
4. Viết `backend/server.js` (Express, port 3000):
   - `GET /alerts` → query Fluss HOT via Flink SQL Gateway
   - `GET /analytics` → query Paimon WARM aggregations
   - `GET /health` → service status
5. Update `AlertsDashboard.jsx` và `Analytics.jsx` nếu cần

### Sprint 3 — Hoàn thiện (P2-P3)
6. Tạo playlist cam_09~cam_15
7. Connect Chatbot/Assistant đến backend RAG (port 5002)
8. Iceberg COLD layer pipeline
