# 🚀 Hybrid Deployment Roadmap — Streamhouse Violence Detection

> **Strategy**: Oracle Cloud Free Tier (backend) + Vercel (frontend) + RunPod On-Demand (GPU inference)  
> **Target cost**: ~$0–15/tháng  
> **Timeline**: 5–6 tuần (song song với thesis nếu cần)

---

## 🗺️ Tổng Quan Kiến Trúc

```
┌─────────────────── ON-PREMISE / LOCAL ────────────────────────────────┐
│                                                                        │
│  📷 Cameras (RTSP)                                                     │
│      └──→ VioMoViNet Inference Server (máy có GPU NVIDIA)              │
│               └──→ Kafka Producer → Oracle Cloud Kafka                 │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ HTTPS / Kafka over TLS
                                    ▼
┌─────────── ORACLE CLOUD FREE TIER (Permanent Free) ────────────────────┐
│  VM: 4 ARM OCPU + 24GB RAM + 200GB Disk                                │
│                                                                        │
│  ┌─ Core Pipeline ─────────────────────────────────────────────────┐   │
│  │  Kafka (KRaft)  →  Flink (JM+TM)  →  Fluss (HOT)               │   │
│  │                              └──→  Paimon (WARM)                │   │
│  │                              └──→  Iceberg (COLD, via MinIO)    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  ┌─ Query & API ───────────────────────────────────────────────────┐   │
│  │  MySQL (Hive Metastore) → Trino (Federation)                    │   │
│  │  Chatbot API (FastAPI + Gemini + ChromaDB)                      │   │
│  │  MinIO (Evidence frames, S3-compatible)                         │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  ┌─ Monitoring ────────────────────────────────────────────────────┐   │
│  │  Prometheus + Grafana + Node-Exporter                           │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  Nginx (Reverse Proxy + SSL/TLS via Let's Encrypt)                     │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
                    │                           │
          API calls │                           │ Grafana / MinIO (admin)
                    ▼                           ▼
┌─── VERCEL (Free) ──────────┐    ┌── Cloudflare DNS (Free) ──────────┐
│  React Dashboard            │    │  vigilance-ai.yourdomain.com      │
│  (Static + SSR)             │    │  api.yourdomain.com               │
│  → api.yourdomain.com       │    │  grafana.yourdomain.com           │
└────────────────────────────┘    └───────────────────────────────────┘

┌─────── GPU INFERENCE — ON-DEMAND (Pay-per-use) ───────────────────────┐
│  RunPod Spot / Vast.ai                                                 │
│  RTX 3080 Spot: ~$0.12/hr → chỉ bật khi cần                          │
│  Hoặc: Máy teammate (local GPU) khi test / demo                       │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 📋 Chi Phí Ước Tính

| Thành phần | Service | Chi phí/tháng | Ghi chú |
|-----------|---------|:-------------:|---------|
| Backend (Kafka+Flink+Fluss+Paimon+Iceberg+Trino+Chatbot+MinIO) | Oracle Cloud Free Tier | **$0** | 4 ARM OCPU, 24GB RAM, 200GB disk — always free |
| Frontend (React UI) | Vercel Hobby | **$0** | Unlimited static deploys |
| DNS + CDN + DDoS protection | Cloudflare Free | **$0** | Proxied domains |
| SSL Certificate | Let's Encrypt (auto-renew) | **$0** | Via Certbot |
| Object Storage (evidence frames) | Oracle Object Storage Free (20GB) | **$0** | Thêm vào MinIO bucket |
| LLM API (Chatbot Gemini) | Google AI Studio Free | **$0** | 1500 req/day, 1M tokens/min |
| GPU Inference (VioMoViNet) | RunPod Spot RTX 3080 | **~$5–15** | Chỉ trả khi chạy |
| **TỔNG** | | **~$5–15** | GPU là chi phí duy nhất |

> **Zero-cost option**: Dùng máy teammate (có RTX 4060 Ti) làm inference server local, push Kafka events lên Oracle Cloud → **$0/tháng tuyệt đối**.

---

## 🗓️ Phases Chi Tiết

### Phase 0 — Chuẩn Bị (Tuần 1) ⏱️ 3–5 ngày

**Mục tiêu**: Setup tài khoản, tên miền, review code cho production

#### 0.1 Đăng ký dịch vụ cloud
```
□ Oracle Cloud Account
  URL: https://cloud.oracle.com/free
  Lưu ý: Cần credit card xác minh (không bị charge nếu chỉ dùng Always Free)
  → Tạo VM.Standard.A1.Flex: 4 OCPU + 24GB RAM + 200GB

□ Vercel Account
  URL: https://vercel.com (đăng nhập bằng GitHub)
  
□ Cloudflare Account (nếu có domain)
  URL: https://cloudflare.com (free plan)
  Hoặc: Dùng subdomain miễn phí từ Oracle (xxx.compute.oracle.com)

□ DockerHub Account (push images)
  URL: https://hub.docker.com (free: unlimited public, 1 private)
```

#### 0.2 Review production readiness
```
□ Đổi tất cả passwords mặc định trong .env
  - MINIO_ROOT_PASSWORD: đổi từ "mypassword" → strong password
  - METASTORE_DB_PASSWORD: đổi từ "root" → strong password
  
□ Audit ports: chỉ expose những gì cần thiết ra ngoài
  - Public: 80, 443 (Nginx), 5002 (Chatbot API via Nginx)
  - Internal only: 9092 (Kafka), 8081 (Flink UI), 9001 (MinIO), 3001 (Grafana)
  
□ Kiểm tra Gemini API key hạn mức
  - Free: 1,500 requests/day, 1M tokens/minute (đủ cho demo)
  
□ Build Docker images và push lên DockerHub
  - docker-chatbot → yourdockeruser/vigilance-chatbot:latest
  - docker-flink → yourdockeruser/vigilance-flink:latest
  - docker-hive → yourdockeruser/vigilance-hive:latest
```

#### 0.3 Tạo docker-compose.cloud.yml
```
□ Copy từ docker-compose.yml
□ Thêm platform: linux/arm64 cho các services cần
□ Giảm memory limits phù hợp ARM (xem deploy/docker-compose.cloud.yml)
□ Đổi host ports: không expose nội bộ ra ngoài
□ Thêm restart: unless-stopped cho tất cả services
```

**Deliverable**: Tài khoản ready + images pushed + compose file chuẩn

---

### Phase 1 — Oracle Cloud VM Setup (Tuần 1–2) ⏱️ 1–2 ngày

**Mục tiêu**: VM sẵn sàng chạy Docker, network secure

```bash
# Chạy script tự động: deploy/oracle-cloud/setup.sh
# Script này sẽ:
# 1. Cài Docker + Docker Compose
# 2. Tạo docker network violence-detection-net
# 3. Cấu hình firewall (iptables + Oracle Security List)
# 4. Cài Nginx + Certbot
# 5. Clone repo
```

#### Checklist Oracle VM
```
□ Tạo VM trên Oracle Console
  - Compartment: root
  - Shape: VM.Standard.A1.Flex → 4 OCPU, 24GB RAM
  - Image: Ubuntu 22.04 Minimal (ARM64)
  - Boot volume: 200GB
  - VCN: Tạo mới hoặc dùng default
  - Subnet: Public subnet
  - SSH key: Upload public key của bạn

□ Cấu hình Security List (quan trọng!)
  Ingress rules cần mở:
  - Port 22  (SSH)
  - Port 80  (HTTP → redirect to HTTPS)
  - Port 443 (HTTPS)
  - Port 5002 (Chatbot API, tạm thời, sau đó qua Nginx)
  
  Ports NÊN đóng với internet (chỉ internal):
  - 9092 (Kafka) — chỉ mở cho IP của inference server
  - 8081 (Flink UI)
  - 9001 (MinIO Console)
  - 3001 (Grafana)

□ SSH vào VM và chạy setup:
  ssh -i ~/.ssh/id_rsa ubuntu@<oracle-public-ip>
  curl -fsSL https://raw.githubusercontent.com/.../deploy/oracle-cloud/setup.sh | bash
```

**Deliverable**: VM online, Docker running, Nginx installed

---

### Phase 2 — Deploy Streamhouse Stack (Tuần 2–3) ⏱️ 3–5 ngày

**Mục tiêu**: Toàn bộ pipeline chạy trên Oracle Cloud

#### 2.1 Khởi động core services
```bash
# Trên Oracle VM
cd ~/streamhouse-violence-detection/deploy

# Tạo .env.cloud từ template
cp .env.cloud.example .env.cloud
nano .env.cloud  # Điền các giá trị thực

# Khởi động theo thứ tự
docker compose -f docker-compose.cloud.yml up -d \
  mysql hive-metastore minio minio_client

# Chờ healthy
docker compose -f docker-compose.cloud.yml ps

# Khởi động Kafka + Fluss
docker compose -f docker-compose.cloud.yml up -d \
  kafka fluss-zookeeper fluss-coordinator fluss-tablet

# Khởi động Flink
docker compose -f docker-compose.cloud.yml up -d \
  jobmanager taskmanager

# Khởi động Pipeline Manager + Trino + Chatbot
docker compose -f docker-compose.cloud.yml up -d \
  flink-sql-gateway pipeline-manager trino-coordinator chatbot
```

#### 2.2 Seed dữ liệu ban đầu
```bash
# Setup Kafka topics
docker exec kafka bash /scripts/setup/create-topics.sh

# Setup Flink star schema (dim_camera, fact tables)
# Pipeline manager tự động chạy khi start
# Kiểm tra: http://<oracle-ip>:8081 → Jobs running
```

#### 2.3 Cấu hình Nginx reverse proxy
```bash
# Copy nginx config
cp ~/streamhouse-violence-detection/deploy/nginx/nginx.conf /etc/nginx/sites-available/vigilance

# Cấu hình SSL
sudo certbot --nginx -d yourdomain.com -d api.yourdomain.com

# Test
sudo nginx -t && sudo systemctl reload nginx
```

#### 2.4 Kiểm tra từ internet
```
□ https://api.yourdomain.com/health → {"status": "ok"}
□ https://api.yourdomain.com/api/layer-counts → JSON data
□ https://api.yourdomain.com/api/latency → latency data
□ Grafana: https://grafana.yourdomain.com (xem qua Nginx auth)
```

**Deliverable**: API online, dashboard data flowing

---

### Phase 3 — Deploy Frontend (Tuần 3) ⏱️ 1 ngày

**Mục tiêu**: React UI deploy lên Vercel, trỏ API về Oracle Cloud

#### 3.1 Cấu hình frontend cho production
```bash
# Violence-Urban-Safety-UI/frontend/.env.production
VITE_API_BASE=https://api.yourdomain.com
VITE_GRAFANA_URL=https://grafana.yourdomain.com
VITE_MINIO_BASE=https://minio.yourdomain.com
```

#### 3.2 Deploy lên Vercel
```bash
cd Violence-Urban-Safety-UI/frontend

# Option A: Vercel CLI
npm i -g vercel
vercel login
vercel --prod

# Option B: GitHub Auto-Deploy
# 1. Vào vercel.com → New Project
# 2. Import từ GitHub repo Violence-Urban-Safety-UI
# 3. Build command: npm run build
# 4. Output dir: dist
# 5. Thêm Environment Variables: VITE_API_BASE, etc.
# → Mỗi lần push main → tự động redeploy
```

#### 3.3 Cấu hình CORS trên Chatbot API
```python
# scripts/chatbot/main.py — thêm Vercel domain vào CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://vigilance-ai.vercel.app",
        "https://yourdomain.com",
        "http://localhost:5173",  # dev
    ],
    ...
)
```

**Deliverable**: https://vigilance-ai.vercel.app online và kết nối được backend

---

### Phase 4 — VioMoViNet Integration (Tuần 3–4) ⏱️ 3–5 ngày

**Mục tiêu**: Kết nối model inference thực với Streamhouse pipeline

#### 4.1 Option A — Máy Local với GPU (khuyến nghị)
```
Yêu cầu: Máy teammate có NVIDIA RTX 4060 Ti
Cách làm:
  1. Clone VioMoViNet trên máy GPU
  2. Sửa Kafka endpoint: kafka:9092 → <oracle-public-ip>:9092
  3. Chạy VioMoViNet docker với NVIDIA runtime
  4. Mở port 9092 trên Oracle Security List CHỈ cho IP của máy GPU

Chi phí: $0 (dùng máy sẵn có)
Nhược điểm: Cần máy GPU luôn bật khi demo
```

```bash
# Trên máy có GPU, sửa VioMoViNet/config.py hoặc .env:
KAFKA_BOOTSTRAP_SERVERS=<oracle-public-ip>:9092
KAFKA_TOPIC=urban-safety-alerts

# Chạy
docker run --gpus all \
  -e KAFKA_BOOTSTRAP_SERVERS=<oracle-ip>:9092 \
  viomovinet:latest
```

#### 4.2 Option B — RunPod Serverless (nếu không có GPU local)
```
Bước 1: Build và push VioMoViNet image
  docker build -t yourdockeruser/viomovinet:latest .
  docker push yourdockeruser/viomovinet:latest

Bước 2: Tạo RunPod endpoint
  - Đăng ký tại runpod.io
  - New Serverless Endpoint → Custom Container
  - Image: yourdockeruser/viomovinet:latest
  - GPU: RTX 3080 Spot (cheapest)
  - Env: KAFKA_BOOTSTRAP_SERVERS=<oracle-ip>:9092

Bước 3: Start khi cần demo
  - Cost: ~$0.12/hr
  - Dừng sau khi demo xong
```

#### 4.3 Verify pipeline thực
```
□ Camera RTSP → VioMoViNet → Kafka (Oracle) → Flink → Fluss HOT
□ Latency đo được: Camera → Dashboard alert < 500ms
□ Evidence frames: lưu vào MinIO trên Oracle
□ Chatbot query: "sự cố trong 10 phút qua" → kết quả thực từ camera
```

**Deliverable**: E2E pipeline thực sự hoạt động với video camera

---

### Phase 5 — Production Hardening (Tuần 4–5) ⏱️ 3–4 ngày

**Mục tiêu**: Hệ thống ổn định, bảo mật, có thể demo cho hội đồng

#### 5.1 Security
```
□ Đổi tất cả default passwords (đã làm ở Phase 0)
□ Nginx: rate limiting (100 req/min cho /api/chat)
□ Nginx: block direct port access (9092, 8081, 9001 không accessible từ internet)
□ Environment variables: không có secret nào trong git
□ MinIO: tạo read-only access key cho frontend (frames chỉ đọc)
□ Oracle Security List: review lần cuối
```

#### 5.2 Reliability
```
□ Tất cả Docker services: restart: unless-stopped
□ Flink jobs: restart-strategy: exponential-delay
□ Health check script: deploy/scripts/health-check.sh
□ Log rotation: configure Docker log driver (json-file, max 100MB)
□ Monitoring: Grafana alerts nếu Flink jobs chết
```

#### 5.3 Performance tuning cho ARM
```
□ Flink TaskManager slots: giảm từ 8 → 4 (ARM có 4 OCPU)
□ Kafka retention: 2 giờ (thay vì default 7 ngày) để tiết kiệm disk
□ MinIO: enable compression cho evidence frames
□ Trino: max memory per query = 4GB (thay vì 8GB)
```

#### 5.4 Demo script
```
□ Viết demo walkthrough: docs/DEMO_SCRIPT.md
□ Chuẩn bị video mẫu (20-30 giây có violence)
□ Test chatbot queries bằng tiếng Việt cho thesis demo
□ Screenshot tất cả pages cho thesis document
```

**Deliverable**: Hệ thống production-ready, demo được trước hội đồng

---

### Phase 6 — Monitoring & Operations (Ongoing) ⏱️

**Mục tiêu**: Duy trì hệ thống sau khi deploy

#### Daily operations
```bash
# Health check nhanh
curl https://api.yourdomain.com/health

# Xem logs nếu có vấn đề
ssh oracle "docker compose -f .../docker-compose.cloud.yml logs --tail=50 chatbot"

# Kiểm tra Flink jobs
open https://api.yourdomain.com:8081  # Flink UI (qua Nginx)
```

#### Weekly operations
```bash
# Kiểm tra disk usage (200GB limit)
ssh oracle "df -h && docker system df"

# Prune old containers/images
ssh oracle "docker system prune -f"

# Xem Grafana dashboards
open https://grafana.yourdomain.com
```

#### Cost monitoring
```
□ Oracle Cloud: Console → Billing → check Always Free usage
□ RunPod: Dashboard → billing → stop idle instances
□ Vercel: Dashboard → usage (free tier limits)
```

---

## 🖥️ Plan B — Split Architecture: Local Admin + Vercel Public

> **Quyết định kiến trúc (2026-05-23)**: WebRTC WHEP không thể proxy qua Vercel → tách thành 2 app từ 1 codebase.

### Tổng quan

```
┌──────────────────────────────────┐   ┌──────────────────────────────────┐
│  LOCAL ADMIN (npm run dev)       │   │  VERCEL (public URL)             │
│  VITE_APP_MODE=admin             │   │  VITE_APP_MODE=public            │
│  localhost:5174                  │   │  vigilance-ai.vercel.app         │
│                                  │   │                                  │
│  ✅ /admin/streaming             │   │  ✅ /alerts                      │
│     Pipeline control             │   │  ✅ /analytics                   │
│     Start/Stop RTSP pipeline     │   │  ✅ /chatbot                     │
│  ✅ / (LiveStreams)              │   │  ✅ /status                      │
│     WebRTC WHEP live grid        │   │  ❌ / (no WebRTC)                │
│  ✅ /status                      │   │     Placeholder thumbnails       │
│     Kafka lag, Flink jobs        │   │     + evidence frame clips       │
└──────────────────────────────────┘   └──────────────────────────────────┘
         │ same network Docker                   │ HTTPS
         └──── Chatbot API :5002 ────────────────┘
               (local hoặc Oracle VM)
```

### Phân chia feature

| Feature | Local Admin | Vercel Public |
|---------|:-----------:|:-------------:|
| WebRTC live streams | ✅ | ❌ |
| Pipeline start/stop | ✅ | ❌ |
| Kafka / Flink health | ✅ | ❌ |
| Alerts Dashboard | ✅ | ✅ |
| Analytics / Charts | ✅ | ✅ |
| Chatbot RAG | ✅ | ✅ |
| Streamhouse Status | ✅ | ✅ (read-only) |

### Build config

```bash
# Local Admin (dev / build)
npm run dev              # loads .env.admin → VITE_APP_MODE=admin
npm run build:admin      # vite build --mode admin

# Vercel Public (CI/CD auto-deploy)
npm run build            # vite build → loads .env.production → VITE_APP_MODE=public
```

### Roadmap Plan B — 3 Phases

| Phase | Mô tả | Ưu tiên | Phụ thuộc |
|-------|-------|:-------:|-----------|
| **Phase 1** | Local Admin — RTSP Streaming Management | 🔴 Cao | Docker running |
| **Phase 2** | Vercel Public — mode config + placeholder LiveStreams | 🟡 Trung | Oracle VM up |
| **Phase 3** | nginx CORS + SSL cho Vercel↔Oracle | 🟢 Thấp | Domain sẵn |

> Chi tiết Phase 1: xem `docs/internal/PLAN_B_PHASE1_STREAMING_ADMIN.md`

---

## 🔧 Files Trong Branch deploy/hybrid-cloud

```
deploy/
├── docker-compose.cloud.yml      # Cloud-optimized compose (ARM64, no streaming profile)
├── .env.cloud.example             # Cloud-specific env vars template
├── oracle-cloud/
│   └── setup.sh                   # VM initialization script
├── nginx/
│   ├── nginx.conf                 # Reverse proxy + SSL config
│   └── nginx-api.conf             # API-specific config
├── scripts/
│   ├── health-check.sh            # Check all services alive
│   ├── start-stack.sh             # Ordered startup script
│   └── backup-minio.sh            # Backup evidence frames
└── viomovinet/
    └── kafka-bridge.env           # VioMoViNet Kafka config for cloud
```

---

## ⚠️ Những Điều Cần Chú Ý

### ARM64 Compatibility
| Service | ARM64 Support | Ghi chú |
|---------|:------------:|---------|
| Kafka (apache/kafka:4.0.1) | ✅ | Official multi-arch |
| Flink 1.18.1 | ✅ | Official multi-arch từ 1.17+ |
| MinIO | ✅ | Official multi-arch |
| MySQL 8.4 | ✅ | Official multi-arch |
| Fluss 0.9.0 | ⚠️ | Cần test — có thể cần build từ source |
| Trino 476 | ✅ | Official multi-arch |
| Prometheus/Grafana | ✅ | Official multi-arch |
| Chatbot (custom) | ⚠️ | Cần build image với `--platform linux/arm64` |
| Flink (custom, với JARs) | ⚠️ | Cần rebuild Dockerfile.flink cho ARM |

> **Fluss là rủi ro lớn nhất**: Apache Fluss 0.9.0 là incubating project, cần kiểm tra ARM build availability. Nếu không có → fallback: dùng Paimon làm HOT layer (chấp nhận latency ~1s thay vì <100ms).

### Memory Limits cho ARM (24GB total)
```yaml
kafka:          memory: 1g      # tăng từ 512m (ARM cần nhiều hơn)
jobmanager:     memory: 1g
taskmanager:    memory: 3g      # giảm từ 2g slots
fluss:          memory: 512m × 3 services
mysql:          memory: 512m
hive:           memory: 512m
trino:          memory: 2g      # giảm từ 1.5g
chatbot:        memory: 2g
# Tổng: ~12g / 24g → còn buffer 12g cho OS và spikes
```

---

## 🔗 Links Quan Trọng

| Service | URL sau khi deploy |
|---------|-------------------|
| React Dashboard | https://vigilance-ai.vercel.app |
| Chatbot API | https://api.yourdomain.com |
| Grafana | https://grafana.yourdomain.com |
| Flink Web UI | https://flink.yourdomain.com |
| MinIO Console | https://minio.yourdomain.com |
| Prometheus | https://prometheus.yourdomain.com |

---

## 📊 Decision Matrix — Khi Nào Dùng Gì

| Scenario | Recommendation |
|----------|---------------|
| Thesis demo (1 lần) | Máy local teammate GPU → $0 |
| Portfolio online (24/7) | Oracle Cloud + RunPod on-demand → $5-15/tháng |
| Development/testing | Local machine, không cần cloud |
| Scale lên 50+ cameras | Thêm Oracle Paid VM (~$50/tháng) |
| GPU always-on | RunPod reserved: ~$80/tháng (RTX 3080) |

---

*Branch: `deploy/hybrid-cloud`*  
*Created: 2026-05-22 — Session 43*  
*Author: Claude Code (Anthropic)*
