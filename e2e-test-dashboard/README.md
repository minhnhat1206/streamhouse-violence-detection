# 🔒 E2E Pipeline Test Dashboard

Interactive web dashboard để test end-to-end pipeline của hệ thống **Smart Security Monitoring — Streamhouse Architecture**.

## 🚀 Khởi động

### Bước 1: Chạy Backend Server
Backend server xử lý việc thực thi Docker commands và stream output real-time.

```powershell
# Trong thư mục e2e-test-dashboard
$env:PROJECT_CWD="C:\Users\user\Desktop\Khoa_Luan\realtime-violence-detection"
npm run server
```

### Bước 2: Chạy Frontend (terminal mới)
```powershell
npm run dev
```

Mở trình duyệt tại **http://localhost:5173**

---

## 📁 Cấu trúc Project

```
e2e-test-dashboard/
├── server/
│   ├── index.js          # Express + WebSocket server (CommonJS)
│   └── package.json      # Override type: commonjs
├── src/
│   ├── main.jsx          # React entry point
│   ├── App.jsx           # Main app with state management
│   ├── index.css         # Global styles (dark theme)
│   ├── data/
│   │   └── pipeline-steps.js   # 🔧 All phases & commands (edit here)
│   ├── components/
│   │   ├── Sidebar.jsx          # Phase navigation stepper
│   │   ├── StepDetail.jsx       # Main step view
│   │   ├── TerminalOutput.jsx   # Real-time command output
│   │   ├── CommandBox.jsx       # Command display + copy
│   │   ├── StatusBar.jsx        # Footer: RAM, Jobs, Services
│   │   └── ActionButtons.jsx    # Run/Stop/Copy/Re-run
│   └── hooks/
│       └── useCommandRunner.js  # WebSocket hook
├── vite.config.js
└── package.json
```

---

## 🔧 Thêm/Sửa Pipeline Steps

Mở file `src/data/pipeline-steps.js` và chỉnh sửa mảng `PIPELINE_PHASES`.

Mỗi step có cấu trúc:
```js
{
  id: "2.1",                    // Step ID (string)
  title: "Start Kafka + MinIO", // Tên hiển thị
  command: "docker compose ...", // Command sẽ chạy
  description: "...",           // Giải thích chi tiết
  verification: ["..."],        // Checklist sau khi chạy
  estimatedTime: "~30s",        // Thời gian dự kiến
  dependencies: ["0.1", "1.1"], // Các step phải hoàn thành trước
  tags: ["kafka", "minio"],     // Tags hiển thị
  optional: false,              // Là optional hay required
}
```

---

## ⚙️ Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROJECT_CWD` | `process.cwd()` | Thư mục root của project (để resolve docker compose paths) |
| `PORT` | `3001` | Backend server port |

---

## 🎨 Tech Stack

- **Frontend**: React 19 + Tailwind CSS v4 + Lucide Icons
- **Backend**: Node.js + Express 5 + ws (WebSocket)
- **Build**: Vite 8

---

## 📊 Features

| Feature | Status |
|---------|--------|
| Phase navigation sidebar | ✅ |
| Real-time terminal output (WebSocket) | ✅ |
| Step dependency validation | ✅ |
| Run / Stop / Copy / Re-run | ✅ |
| System status bar (RAM, Flink, Services) | ✅ |
| Toast notifications | ✅ |
| Export terminal log | ✅ |
| Color-coded output (error/warn/info) | ✅ |
| Auto-scroll terminal | ✅ |
| Backend connection status | ✅ |
