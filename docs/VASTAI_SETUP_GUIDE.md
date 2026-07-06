# Vast.ai GPU Server Setup Guide

Tài liệu này hướng dẫn chi tiết cách thiết lập (setup) từ đầu hệ thống xử lý AI (MoViNet Inference, MediaMTX Streaming, React UI) trên một máy chủ GPU mới được thuê từ Vast.ai.

---

## 1. Yêu cầu cấu hình khi thuê máy trên Vast.ai

* **GPU:** Tối thiểu 1x RTX 3090, RTX 4090 hoặc tương đương (đủ VRAM chạy song song 5 luồng MoViNet-A3).
* **Disk Space:** Tối thiểu **30 GB** (cho weights, venv, và cached video segments).
* **Vast.ai Template:** Chọn template **TensorFlow** sẵn có (ví dụ: `tensorflow/tensorflow:latest-gpu` hoặc `pytorch/pytorch` có cài sẵn CUDA).
* **Cổng mở (Port Mapping):** Khi thuê, cấu hình mở các cổng sau hoặc lấy các cổng public tương ứng:
  * `5173` (Vite Frontend Dashboard)
  * `8000` (FastAPI /vio status endpoint)
  * `8888` (MediaMTX HLS stream)
  * `8889` (MediaMTX WebRTC stream - tùy chọn)
  * `8554` (MediaMTX RTSP stream)

---

## 2. Các thành phần chạy trên Vast.ai GPU Instance

Một GPU instance chạy các thành phần chính sau:
1. **MediaMTX:** Làm RTSP/HLS gateway nhận luồng camera raw và phân phối.
2. **FastAPI (`main.py`):** Expose API đọc trạng thái rủi ro thời gian thực cho React UI.
3. **Vite Frontend:** React Dashboard hiển thị lưới camera.
4. **Visualizer Inference (`visualize_stream.py`):** Chạy MoViNet trên GPU xử lý camera.
5. **Kafka Mock Publisher (`rtsp_inference_mock.py`):** Đẩy dữ liệu cảnh báo bạo lực về GCP VM.

---

## 3. Quy trình thiết lập từng bước (Setup Steps)

### Bước 3.1 — Chuẩn bị môi trường & Virtual Env

SSH vào máy chủ Vast.ai vừa thuê:
```bash
# 1. Cập nhật thư viện hệ thống
apt-get update && apt-get install -y ffmpeg libsm6 libxext6 git curl

# 2. Tạo virtual environment chính cho python
mkdir -p /venv
python3 -m venv /venv/main
source /venv/main/activate

# 3. Cài đặt các thư viện Python cần thiết
pip install --upgrade pip
pip install tensorflow==2.15.0 tensorflow-intel==2.15.0  # Hoặc bản tương ứng CUDA
pip install opencv-python Pillow kafka-python fastapi uvicorn requests
```

### Bước 3.2 — Clone mã nguồn và weights

```bash
# Clone mã nguồn dự án vào thư mục root
cd /root
git clone --recurse-submodules https://github.com/minhnhat1206/realtime-violence-detection.git streamhouse
cd streamhouse
git checkout dev.VastAI

# Clone UI và checkout nhánh devHuy
cd Violence-Urban-Safety-UI
git checkout devHuy
```

> [!IMPORTANT]
> Tải file weights mô hình MoViNet-A3 (`best_weights` của backbone và classifier) và lưu vào đúng đường dẫn:
> * `/root/buildAPI/weights/a3_backbone/best_weights`
> * `/root/buildAPI/weights/sva_03/best_weights`

---

## 4. Cấu hình và Khởi chạy các Service

### 4.1 — Khởi chạy MediaMTX
Đảm bảo file cấu hình `/root/streamhouse/config/mediamtx/mediamtx.yml` có sẵn và được khai báo đúng cổng HLS (8888) và RTSP (8554).
```bash
# Tải và cài đặt MediaMTX nếu chưa có
wget https://github.com/bluenviron/mediamtx/releases/download/v1.6.0/mediamtx_v1.6.0_linux_amd64.tar.gz
tar -zxf mediamtx_v1.6.0_linux_amd64.tar.gz -C /usr/local/bin/

# Khởi chạy MediaMTX ngầm
nohup mediamtx /root/streamhouse/config/mediamtx/mediamtx.yml > /root/mediamtx.log 2>&1 &
```

### 4.2 — Khởi chạy API trạng thái (/vio - Port 8000)
```bash
cd /root/buildAPI
nohup /venv/main/bin/python main.py > /root/api_vio.log 2>&1 &
```
*Kiểm tra API hoạt động:* `curl http://localhost:8000/api/stream/status/cam_01`

### 4.3 — Khởi chạy React UI (Vite - Port 5173)
Trước tiên cấu hình proxy cho GCP VM bằng cách mở `/root/Violence-Urban-Safety-UI/frontend/vite.config.js` và đảm bảo IP GCP VM được cập nhật chính xác (ví dụ: `34.124.131.144`).

```bash
cd /root/Violence-Urban-Safety-UI/frontend
npm install

# Khởi chạy Vite persistent (survive shell close)
nohup npx vite --host 0.0.0.0 --port 5173 </dev/null >/root/frontend.log 2>&1 &
```

### 4.4 — Khởi chạy MoViNet Visualizers (GPU)
Khởi chạy tuần tự 5 luồng xử lý AI cho 5 camera. Dùng `sleep 3` giữa các luồng để tránh nghẽn khởi tạo TensorFlow GPU:

```bash
TF_USE_LEGACY_KERAS=1 \
BACKBONE_WEIGHTS=/root/buildAPI/weights/a3_backbone/best_weights \
SV_WEIGHTS=/root/buildAPI/weights/sva_03/best_weights \
nohup /venv/main/bin/python /root/buildAPI/visualize_stream.py --input rtsp://localhost:8554/cam_01 --output rtsp://localhost:8554/cam_01_result > ~/visualize_cam_01.log 2>&1 & \
sleep 3 && \
TF_USE_LEGACY_KERAS=1 \
BACKBONE_WEIGHTS=/root/buildAPI/weights/a3_backbone/best_weights \
SV_WEIGHTS=/root/buildAPI/weights/sva_03/best_weights \
nohup /venv/main/bin/python /root/buildAPI/visualize_stream.py --input rtsp://localhost:8554/cam_02 --output rtsp://localhost:8554/cam_02_result > ~/visualize_cam_02.log 2>&1 & \
sleep 3 && \
TF_USE_LEGACY_KERAS=1 \
BACKBONE_WEIGHTS=/root/buildAPI/weights/a3_backbone/best_weights \
SV_WEIGHTS=/root/buildAPI/weights/sva_03/best_weights \
nohup /venv/main/bin/python /root/buildAPI/visualize_stream.py --input rtsp://localhost:8554/cam_03 --output rtsp://localhost:8554/cam_03_result > ~/visualize_cam_03.log 2>&1 & \
sleep 3 && \
TF_USE_LEGACY_KERAS=1 \
BACKBONE_WEIGHTS=/root/buildAPI/weights/a3_backbone/best_weights \
SV_WEIGHTS=/root/buildAPI/weights/sva_03/best_weights \
nohup /venv/main/bin/python /root/buildAPI/visualize_stream.py --input rtsp://localhost:8554/cam_04 --output rtsp://localhost:8554/cam_04_result > ~/visualize_cam_04.log 2>&1 & \
sleep 3 && \
TF_USE_LEGACY_KERAS=1 \
BACKBONE_WEIGHTS=/root/buildAPI/weights/a3_backbone/best_weights \
SV_WEIGHTS=/root/buildAPI/weights/sva_03/best_weights \
nohup /venv/main/bin/python /root/buildAPI/visualize_stream.py --input rtsp://localhost:8554/cam_05 --output rtsp://localhost:8554/cam_05_result > ~/visualize_cam_05.log 2>&1 &
```

### 4.5 — Khởi chạy Kafka Mock Event Publisher (Đẩy về GCP)
Tiến trình này nhận nhiệm vụ đẩy dữ liệu cảnh báo về GCP VM để lưu trữ database (Paimon/Iceberg):
```bash
cd /root/streamhouse
rm -f /tmp/STOP

PYTHONUNBUFFERED=1 \
KAFKA_BROKER=34.124.131.144:9093 \
KAFKA_TOPIC=urban-safety-alerts \
METADATA_FILE=/root/streamhouse/data/metadata/camera_registry.csv \
ACTIVE_CAMERAS=cam_01,cam_02,cam_03,cam_04,cam_05 \
STOP_FILE=/tmp/STOP \
nohup /venv/main/bin/python scripts/streaming/rtsp_inference_mock.py > ~/rtsp_inference.log 2>&1 &
```

---

## 5. Script kiểm tra nhanh trạng thái (Health Check)

Bạn có thể chạy lệnh sau để giám sát nhanh các tiến trình:
```bash
# Xem các tiến trình AI và Kafka có đang chạy không
ps aux | grep -E 'visualize_stream|rtsp_inference_mock|vite'

# Kiểm tra GPU Memory Usage
nvidia-smi

# Xem logs phát hiện bạo lực
tail -n 20 ~/rtsp_inference.log
```
