#!/bin/bash
# create-topics.sh: Script để tạo các Kafka topics

echo "Waiting for Kafka to be ready..."

# Kiểm tra kết nối với Kafka Broker (sử dụng localhost vì script chạy bên trong container Kafka)
while ! nc -z localhost 9092; do   
  sleep 1
done

echo "Kafka is ready. Creating topics..."

BOOTSTRAP_SERVER="localhost:9092"

# Raw inference input (RTSP pipeline → data contract validator reads from here)
/opt/kafka/bin/kafka-topics.sh --create \
    --topic urban-safety-alerts \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

# Data contract validator output: valid events (Flink streaming jobs read from here)
/opt/kafka/bin/kafka-topics.sh --create \
    --topic hot-violence-alerts-valid \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

# Data contract validator output: rejected events (quarantine)
/opt/kafka/bin/kafka-topics.sh --create \
    --topic urban-safety-quarantine \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

# Frame evidence events
/opt/kafka/bin/kafka-topics.sh --create \
    --topic hot-violence-frames-uploaded \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

# Legacy topics (kept for backwards compatibility)
/opt/kafka/bin/kafka-topics.sh --create \
    --topic ingest.media.events \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

/opt/kafka/bin/kafka-topics.sh --create \
    --topic model.inference.results \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

echo "Topics creation finished."

# THÊM: In ra danh sách các Topics đã tạo
echo "Created Topics:"
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP_SERVER
