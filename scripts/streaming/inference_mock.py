import time
import json
import random
import csv
import sys
import os
import uuid
from kafka import KafkaProducer
from datetime import datetime, timezone

# ================= CONFIGURATION =================
# Ưu tiên lấy từ biến môi trường nếu có
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "urban-safety-alerts")
METADATA_FILE = os.getenv("METADATA_FILE", "/app/data/metadata/camera_registry.csv")

# Manual stop: tạo file này để dừng script
STOP_FILE = os.getenv("STOP_FILE", "/app/tmp/STOP")

# Tần suất gửi dữ liệu (giây)
HEARTBEAT_INTERVAL = 5.0  # Khi bình thường
ALERT_INTERVAL = 0.5      # Khi có bạo lực

# Xác suất chuyển đổi trạng thái (để tạo dữ liệu biến động)
PROB_START_VIOLENCE = 0.02  # 2% cơ hội bắt đầu bạo lực mỗi vòng lặp
PROB_STOP_VIOLENCE = 0.15   # 15% cơ hội kết thúc bạo lực

EVENT_TYPES = ["FIGHTING", "ASSAULT", "STABBING", "SHOOTING"]

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def load_camera_registry(csv_path):
    registry = {}
    try:
        if not os.path.exists(csv_path):
            print(f"Error: Metadata file {csv_path} not found.")
            return {}
            
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row['latitude'] = float(row['latitude'])
                    row['longitude'] = float(row['longitude'])
                except: pass
                registry[row['camera_id']] = row
        print(f"Loaded {len(registry)} cameras from CSV.")
        return registry
    except Exception as e:
        print(f"CSV Read Error: {e}")
        return {}

def main():
    print(f"Starting Mock Inference Producer...")
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Topic: {KAFKA_TOPIC}")

    # 1. Kết nối Kafka
    producer = None
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=json_serializer,
                acks=1
            )
            print("Successfully connected to Kafka.")
            break
        except Exception as e:
            print(f"Waiting for Kafka... ({retries} retries left): {e}")
            retries -= 1
            time.sleep(5)
    
    if not producer:
        print("Could not connect to Kafka. Exiting.")
        sys.exit(1)

    # 2. Load Camera Metadata
    registry = load_camera_registry(METADATA_FILE)
    if not registry:
        sys.exit(1)

    # 3. Xóa stop file cũ nếu còn tồn tại từ lần chạy trước
    if os.path.exists(STOP_FILE):
        os.remove(STOP_FILE)
        print(f"Cleared old stop file: {STOP_FILE}")

    # 4. Quản lý trạng thái camera (để giả lập logic thời gian thực)
    camera_states = {cam_id: {"is_violent": False, "last_sent": 0} for cam_id in registry}
    msg_count = 0

    try:
        while not os.path.exists(STOP_FILE):
            now = time.time()
            
            for cam_id, meta in registry.items():
                state = camera_states[cam_id]
                
                # Logic chuyển đổi trạng thái ngẫu nhiên
                if not state["is_violent"]:
                    if random.random() < PROB_START_VIOLENCE:
                        state["is_violent"] = True
                        print(f"!!! [ALERT] Violence detected on {cam_id}")
                else:
                    if random.random() < PROB_STOP_VIOLENCE:
                        state["is_violent"] = False
                        print(f"--- [NORMAL] Situation cleared on {cam_id}")

                # Kiểm tra tần suất gửi
                interval = ALERT_INTERVAL if state["is_violent"] else HEARTBEAT_INTERVAL
                if now - state["last_sent"] >= interval:
                    
                    # Tạo dữ liệu giả theo Data Contract
                    risk_score = random.uniform(0.75, 0.99) if state["is_violent"] else random.uniform(0.01, 0.15)
                    event_type = random.choice(EVENT_TYPES) if state["is_violent"] else None
                    confidence = round(random.uniform(0.85, 0.99), 4) if state["is_violent"] else round(random.uniform(0.3, 0.7), 4)

                    payload = {
                        "event_id": str(uuid.uuid4()),
                        "camera_id": cam_id,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "is_violent": state["is_violent"],
                        "risk_score": round(risk_score, 4),
                        "confidence": confidence,
                        "event_type": event_type,
                        "location": {
                            "city": meta.get("city", "Unknown"),
                            "district": meta.get("district", "Unknown"),
                            "ward": meta.get("ward", "Unknown"),
                            "street": meta.get("street", "Unknown"),
                            "lat": meta.get("latitude"),
                            "long": meta.get("longitude")
                        },
                        "metadata": {
                            "fps": random.randint(24, 30),
                            "latency_ms": random.randint(10, 50),
                            "mock": True
                        }
                    }

                    # Gửi tới Kafka
                    producer.send(KAFKA_TOPIC, value=payload)
                    state["last_sent"] = now
                    msg_count += 1

            # Sleep ngắn để giảm tải CPU
            time.sleep(0.1)

        print(f"Stop file detected: {STOP_FILE}. Shutting down gracefully...")
    except KeyboardInterrupt:
        print("Stopping Mock Inference Producer...")
    finally:
        print(f"Total messages sent: {msg_count}")
        if producer:
            producer.close()

if __name__ == "__main__":
    main()
