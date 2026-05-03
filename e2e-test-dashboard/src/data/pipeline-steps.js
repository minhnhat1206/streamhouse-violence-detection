// Pipeline data — all phases, steps and commands defined here
// Edit this file to add/modify/remove pipeline steps

export const PIPELINE_PHASES = [
  {
    id: 0,
    title: "Docker Network Setup",
    icon: "🌐",
    description: "Tạo Docker network chung cho tất cả services",
    steps: [
      {
        id: "0.1",
        title: "Create Docker Network",
        command: "docker network create violence-detection-net",
        description:
          "Tạo Docker bridge network `violence-detection-net` dùng chung cho tất cả services. Mọi container có thể giao tiếp nội bộ qua network này mà không cần expose port ra host.",
        verification: [
          "Network 'violence-detection-net' được liệt kê trong `docker network ls`",
        ],
        estimatedTime: "~2s",
        dependencies: [],
        tags: ["network", "setup"],
        optional: false,
        skippable: true,
        skipReason: "Network đã tồn tại (đã tạo trước đó)",
      },
    ],
  },
  {
    id: 1,
    title: "Environment Setup",
    icon: "⚙️",
    description: "Cấu hình biến môi trường và API keys",
    steps: [
      {
        id: "1.1",
        title: "Copy Environment File",
        command: "cp docker/.env.example docker/.env",
        description:
          "Copy file `.env.example` thành `.env`. Sau khi copy, bạn cần mở file `docker/.env` và điền `GEMINI_API_KEY` của bạn vào. Key này dùng cho Chatbot AI (Phase 11).",
        verification: [
          "File `docker/.env` tồn tại",
          "Chỉnh sửa GEMINI_API_KEY trong docker/.env",
        ],
        estimatedTime: "~1s + manual edit",
        dependencies: [],
        tags: ["env", "config"],
        optional: false,
        skippable: true,
        skipReason: "Đã có file docker/.env (đã thực hiện trước đó)",
      },
    ],
  },
  {
    id: 2,
    title: "Core Infrastructure",
    icon: "🏗️",
    description: "Kafka message broker + MinIO object store",
    steps: [
      {
        id: "2.1",
        title: "Start Kafka + MinIO",
        command:
          "docker compose -f docker/docker-compose.yml up -d kafka minio minio_client",
        description:
          "Khởi động **Kafka** (message broker, port 19092) và **MinIO** (S3-compatible object store, port 9000/9001). MinIO client tự động tạo buckets: `inference-results`, `checkpoint`, `rtsp-frames`, `warehouse`.",
        verification: [
          "Kafka broker tại localhost:19092",
          "MinIO API tại localhost:9000",
          "MinIO Console tại http://localhost:9001 (admin/minioadmin)",
          "Buckets được tạo: inference-results, checkpoint, rtsp-frames, warehouse",
        ],
        estimatedTime: "~30s first run, ~10s restart",
        dependencies: ["0.1", "1.1"],
        tags: ["kafka", "minio", "storage"],
        optional: false,
      },
      {
        id: "2.2",
        title: "Start Kafka UI (Optional)",
        command:
          "docker compose -f docker/docker-compose.yml --profile ui up -d kafka-ui",
        description:
          "Khởi động **Kafka UI** (port 18085) — giao diện web để xem topics, messages, consumer groups theo thời gian thực. Hữu ích để debug dữ liệu trong Kafka.",
        verification: [
          "Kafka UI tại http://localhost:18085",
          "Hiển thị được topics trong cluster",
        ],
        estimatedTime: "~15s",
        dependencies: ["2.1"],
        tags: ["kafka", "ui", "monitoring"],
        optional: true,
      },
    ],
  },
  {
    id: 3,
    title: "Data Generation",
    icon: "🎲",
    description: "Sinh dữ liệu từ luồng RTSP thực tế + mock AI inference (ffmpeg backend)",
    steps: [
      {
        id: "3.1",
        title: "Start MediaMTX RTSP Server",
        command:
          "docker compose -f docker/docker-compose.yml --profile streaming up -d mediamtx",
        description:
          "Khởi động **MediaMTX** — RTSP media server (port 8554). Camera simulator sẽ push video streams đến đây; rtsp-inference-mock sẽ đọc lại qua ffmpeg. MediaMTX được cấu hình `source: publisher` để nhận push từ rtsp_pusher.",
        verification: [
          "Container mediamtx đang chạy",
          "Port 8554 (RTSP) accessible",
        ],
        estimatedTime: "~10s",
        dependencies: ["2.1"],
        tags: ["rtsp", "streaming", "mediamtx"],
        optional: false,
      },
      {
        id: "3.2",
        title: "Start RTSP Video Pusher",
        command:
          "docker compose -f docker/docker-compose.yml --profile streaming up -d rtsp_pusher",
        description:
          "Khởi động **RTSP Pusher** — tạo playlist video từ dataset RWF-2000, encode bằng FFmpeg và push 15 camera streams vào MediaMTX. Mỗi camera có mix bạo lực/bình thường khác nhau. **Yêu cầu:** Dataset trong `data/raw/RWF-2000/`.",
        verification: [
          "Container rtsp_pusher đang chạy",
          "RTSP streams live: rtsp://localhost:8554/cam_01 ... cam_15",
          "Kiểm tra: docker logs rtsp_pusher",
        ],
        estimatedTime: "~30s (FFmpeg start)",
        dependencies: ["3.1"],
        tags: ["rtsp", "streaming", "ffmpeg"],
        optional: false,
      },
      {
        id: "3.3",
        title: "Start RTSP Inference Mock",
        command:
          "docker compose -f docker/docker-compose.yml --profile streaming up -d rtsp-inference-mock",
        description:
          "Khởi động **RTSP Inference Mock** — capture frames thực từ MediaMTX qua **ffmpeg subprocess** (không cần OpenCV), chạy mock inference (random risk score, confidence, event_type), encode thumbnail JPEG base64, publish kết quả vào Kafka topic `urban-safety-alerts`. Sau này thay hàm `mock_inference()` bằng AI model thật (viomobilenet_api).",
        verification: [
          "Container rtsp-inference-mock đang chạy",
          "Log: '[cam_XX] RTSP connected: rtsp://mediamtx:8554/cam_XX'",
          "Messages trong Kafka topic 'urban-safety-alerts' có trường 'rtsp_connected: true'",
          "Kiểm tra: docker logs rtsp-inference-mock",
        ],
        estimatedTime: "~15s",
        dependencies: ["3.2"],
        tags: ["rtsp", "mock", "kafka", "inference", "ffmpeg"],
        optional: false,
      },
      {
        id: "3.4",
        title: "Verify RTSP Connection + Kafka Messages",
        command:
          "docker logs --tail=20 rtsp-inference-mock && docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic urban-safety-alerts --from-beginning --max-messages 3 --timeout-ms 8000 2>/dev/null",
        description:
          "Verify **RTSP frames đang được capture** từ MediaMTX và **messages đang chảy** vào Kafka. Output log phải có `[cam_XX] RTSP connected` — xác nhận ffmpeg đang kết nối RTSP thật. Mỗi message Kafka phải có `rtsp_connected: true` và `thumbnail_b64` non-empty (ảnh JPEG base64 thực từ stream).",
        verification: [
          "Log rtsp-inference-mock có '[cam_XX] RTSP connected: rtsp://mediamtx:8554/cam_XX'",
          "Kafka messages trong urban-safety-alerts có trường rtsp_connected=true",
          "Trường thumbnail_b64 trong message không rỗng (có JPEG data)",
          "Messages xuất hiện đều đặn ~1 msg/5s per camera",
        ],
        estimatedTime: "~10s",
        dependencies: ["3.3"],
        tags: ["rtsp", "verify", "kafka", "ffmpeg"],
        optional: false,
      },
    ],
  },
  {
    id: 4,
    title: "Compute Layer (Flink)",
    icon: "⚡",
    description: "Apache Flink cluster + Data Contract Validator job",
    steps: [
      {
        id: "4.1",
        title: "Start Flink Cluster",
        command:
          "docker compose -f docker/docker-compose.yml up -d --build jobmanager taskmanager",
        description:
          "Build và khởi động **Flink cluster** (JobManager + TaskManager). JobManager điều phối jobs, TaskManager xử lý dữ liệu. Flink Web UI tại port 8081. Lần đầu build sẽ tải các connector JARs (Kafka, Fluss, Paimon, Iceberg) — có thể mất 5-10 phút.",
        verification: [
          "Flink Web UI tại http://localhost:8081",
          "TaskManager registered (hiển thị trong UI)",
        ],
        estimatedTime: "~5-10 phút (first build), ~30s restart",
        dependencies: ["2.1", "3.3"],
        tags: ["flink", "compute"],
        optional: false,
      },
      {
        id: "4.2",
        title: "Copy Scripts + Submit Data Contract Validator",
        command:
          "docker exec jobmanager sh -c 'cp /opt/flink/scripts/*.py /tmp/ && flink run -d -py /tmp/data_contract_validator.py'",
        description:
          "Copy scripts sang `/tmp/` (workaround cho PyFlink mount issue trên Windows/WSL2), rồi submit **Data Contract Validator** job. Đọc dữ liệu thô từ `urban-safety-alerts`, validate schema (timestamp, camera_id, risk_score, confidence, event_type). Records hợp lệ → `hot-violence-alerts-valid`, records lỗi → `urban-safety-quarantine`. **Flink Job 1/4.**",
        verification: [
          "Job submitted: 'Job has been submitted with JobID ...'",
          "Flink UI http://localhost:8081 — job RUNNING",
          "Topic 'hot-violence-alerts-valid' nhận được messages",
        ],
        estimatedTime: "~20s",
        dependencies: ["4.1"],
        tags: ["flink", "validation", "kafka"],
        optional: false,
      },
    ],
  },
  {
    id: 5,
    title: "Hot Storage (Fluss)",
    icon: "🔥",
    description: "Apache Fluss — latency <100ms real-time query",
    steps: [
      {
        id: "5.1",
        title: "Start Fluss Cluster",
        command:
          "docker compose -f docker/docker-compose.yml up -d fluss-zookeeper fluss-coordinator fluss-tablet",
        description:
          "Khởi động **Fluss cluster** — hệ thống lưu trữ streaming với latency <100ms. Gồm: ZooKeeper (coordination service), Coordinator (master node), TabletServer (data storage node).",
        verification: [
          "fluss-zookeeper, fluss-coordinator, fluss-tablet đang chạy",
          "Coordinator log: 'Server started'",
        ],
        estimatedTime: "~20s",
        dependencies: ["2.1"],
        tags: ["fluss", "hot-storage"],
        optional: false,
      },
      {
        id: "5.2",
        title: "Init Fluss Tables",
        command:
          "docker exec jobmanager flink run -py /tmp/init_fluss_tables.py",
        description:
          "Tạo **Fluss catalog** và table `hot_violence_alerts` trong database `security`. Table có primary key `incident_id` để upsert dữ liệu real-time. Đây là hot layer cho queries <100ms.",
        verification: [
          "Script exit code 0",
          "Fluss catalog 'fluss_catalog' được tạo",
          "Table 'security.hot_violence_alerts' tồn tại",
        ],
        estimatedTime: "~15s",
        dependencies: ["5.1", "4.1"],
        tags: ["fluss", "schema"],
        optional: false,
      },
      {
        id: "5.3",
        title: "Submit Kafka → Fluss Sink (Job 2/4)",
        command:
          "docker exec jobmanager flink run -d -py /tmp/sink_to_fluss.py",
        description:
          "Submit **Kafka → Fluss Sink** job. Đọc events đã validate từ `hot-violence-alerts-valid`, chèn vào Fluss table `hot_violence_alerts`. Dữ liệu query được trong <100ms sau khi ingest. **Flink Job 2/4.**",
        verification: [
          "Job 'KafkaToFluss' RUNNING trong Flink UI",
          "Records xuất hiện trong Fluss table",
        ],
        estimatedTime: "~10s",
        dependencies: ["5.2", "4.2"],
        tags: ["flink", "fluss", "sink"],
        optional: false,
      },
    ],
  },
  {
    id: 6,
    title: "Warm Storage (Paimon)",
    icon: "🌊",
    description: "Apache Paimon — minutes latency, CDC enabled",
    steps: [
      {
        id: "6.1",
        title: "Init Paimon Tables",
        command:
          "docker exec jobmanager flink run -py /tmp/init_paimon_tables.py",
        description:
          "Tạo **Paimon catalog** và các tables: `violence_incidents` (deduplicate merge, CDC enabled), `daily_incident_stats` và `camera_stats` (aggregation tables). Dữ liệu lưu trên MinIO dạng ORC files.",
        verification: [
          "Script exit code 0",
          "Paimon catalog được tạo trên MinIO",
          "3 tables: violence_incidents, daily_incident_stats, camera_stats",
        ],
        estimatedTime: "~20s",
        dependencies: ["4.1", "2.1"],
        tags: ["paimon", "warm-storage", "schema"],
        optional: false,
      },
      {
        id: "6.2",
        title: "Submit Kafka → Paimon Sink (Job 3/4)",
        command:
          "docker exec jobmanager flink run -d -py /tmp/sink_to_paimon.py",
        description:
          "Submit **Kafka → Paimon Sink** job. Đọc events từ `hot-violence-alerts-valid`, chèn vào Paimon `violence_incidents`. Checkpointing mỗi 30s (bắt buộc để Paimon commit snapshot). **Flink Job 3/4.**",
        verification: [
          "Job 'KafkaToPaimon' RUNNING trong Flink UI",
          "Paimon snapshots được tạo (kiểm tra MinIO bucket 'warehouse')",
        ],
        estimatedTime: "~10s",
        dependencies: ["6.1", "4.2"],
        tags: ["flink", "paimon", "sink"],
        optional: false,
      },
      {
        id: "6.3",
        title: "Submit Paimon Aggregation (Job 4/4)",
        command:
          "docker exec jobmanager flink run -d -py /tmp/aggregate_paimon.py",
        description:
          "Submit **Paimon Aggregation** job. Đọc CDC changelog từ `violence_incidents`, tính thống kê theo ngày/location và ngày/camera, ghi vào `daily_incident_stats` và `camera_stats`. **Flink Job 4/4.**",
        verification: [
          "Job 'PaimonAggregation' RUNNING trong Flink UI",
          "Data trong daily_incident_stats và camera_stats",
          "Tất cả 4 Flink jobs đang RUNNING",
        ],
        estimatedTime: "~10s",
        dependencies: ["6.2"],
        tags: ["flink", "paimon", "aggregation"],
        optional: false,
      },
    ],
  },
  {
    id: 7,
    title: "Cold Storage (Iceberg)",
    icon: "🧊",
    description: "Apache Iceberg — years retention, time-travel",
    steps: [
      {
        id: "7.1",
        title: "Start MySQL + Hive Metastore",
        command:
          "docker compose -f docker/docker-compose.yml up -d mysql hive-metastore",
        description:
          "Khởi động **MySQL** (metastore database, port 3307) và **Hive Metastore** (Thrift service, port 9083). Hive Metastore quản lý metadata cho Iceberg tables — lưu schema, partition info, file locations.",
        verification: [
          "MySQL healthy tại port 3307",
          "Hive Metastore thrift service tại port 9083",
          "Kiểm tra: docker logs hive-metastore | grep 'Started'",
        ],
        estimatedTime: "~30s",
        dependencies: ["2.1"],
        tags: ["mysql", "hive", "metadata"],
        optional: false,
      },
      {
        id: "7.1b",
        title: "Wait for Hive Metastore Ready",
        command:
          "docker exec jobmanager python /opt/flink/scripts/wait_for_metastore.py",
        description:
          "Chờ **Hive Metastore** sẵn sàng nhận connection trên port 9083. Script thử tối đa 15 lần (mỗi lần cách 5s = tổng 75s). Hive Metastore cần khởi tạo schema DB trước khi accept connections.",
        verification: [
          "Output: 'Hive Metastore is READY'",
          "Exit code 0",
        ],
        estimatedTime: "~10-50s",
        dependencies: ["7.1"],
        tags: ["hive", "healthcheck", "wait"],
        optional: false,
      },
      {
        id: "7.2",
        title: "Init Iceberg Tables",
        command:
          "docker exec jobmanager python /opt/flink/scripts/init_iceberg_tables.py",
        description:
          "Tạo **Iceberg catalog** (HiveCatalog + S3FileIO) và table `historical_violence_incidents`. Partitioned by `incident_date` để tối ưu time-travel queries. Dữ liệu lưu trên MinIO dạng Parquet format.",
        verification: [
          "Script exit code 0",
          "Iceberg catalog visible trong Hive Metastore",
          "Parquet files trong MinIO 'warehouse' bucket",
        ],
        estimatedTime: "~15s",
        dependencies: ["7.1b", "4.1"],
        tags: ["iceberg", "cold-storage", "schema"],
        optional: false,
      },
      {
        id: "7.3",
        title: "Run Archive to Iceberg (Batch)",
        command:
          "docker exec jobmanager flink run -d -py /opt/flink/scripts/archive_to_iceberg.py",
        description:
          "Chạy **Paimon → Iceberg Archive** batch job. Copy records >7 ngày tuổi từ Paimon sang Iceberg. Dùng LEFT ANTI JOIN để tránh duplicate. Đây là batch job (không streaming), chạy theo schedule weekly.",
        verification: [
          "Job finished SUCCESSFULLY (không phải RUNNING)",
          "Records xuất hiện trong Iceberg table",
          "Parquet files mới trên MinIO",
        ],
        estimatedTime: "~30s (batch)",
        dependencies: ["7.2", "6.2"],
        tags: ["flink", "iceberg", "archive", "batch"],
        optional: false,
      },
    ],
  },
  {
    id: 8,
    title: "Query Federation (Trino)",
    icon: "🔍",
    description: "Trino federated query engine — SQL across all layers",
    steps: [
      {
        id: "8.1",
        title: "Start Trino Coordinator",
        command:
          "docker compose -f docker/docker-compose.yml up -d --build trino-coordinator",
        description:
          "Khởi động **Trino** (port 8082) — federated query engine. Có catalogs cho Iceberg, Paimon, Fluss → Query thống nhất across tất cả storage layers bằng SQL chuẩn ANSI.",
        verification: [
          "Trino UI tại http://localhost:8082",
          "Status: RUNNING (hiển thị worker count)",
          "Catalogs: fluss, paimon, iceberg",
        ],
        estimatedTime: "~30s first build, ~15s restart",
        dependencies: ["5.1", "6.1", "7.1"],
        tags: ["trino", "query", "federation"],
        optional: false,
      },
      {
        id: "8.2",
        title: "Scale Trino Workers (Optional)",
        command:
          "docker compose -f docker/docker-compose.yml --profile scaling up -d trino-worker-1 trino-worker-2",
        description:
          "Thêm **2 Trino workers** để scale query performance cho datasets lớn. Chỉ cần thiết khi query >1M records. Mỗi worker cần ~512MB RAM thêm.",
        verification: [
          "3 nodes total trong Trino UI (1 coordinator + 2 workers)",
          "Worker nodes đang ACTIVE",
        ],
        estimatedTime: "~20s",
        dependencies: ["8.1"],
        tags: ["trino", "scaling"],
        optional: true,
      },
    ],
  },
  {
    id: 9,
    title: "Time-Travel & Forensics",
    icon: "⏱️",
    description: "Verify historical data queries và audit logs",
    steps: [
      {
        id: "9.1",
        title: "Run Time-Travel Query Tests",
        command:
          "docker exec jobmanager python /opt/flink/scripts/time_travel_queries.py",
        description:
          "Chạy 5 **time-travel query tests**: Paimon snapshot listing, snapshot ID travel, timestamp travel, CDC audit log, Iceberg time-travel. Verify khả năng forensic analysis (\"Trạng thái dữ liệu 2 giờ trước?\"). **Kỳ vọng: 4/5 tests PASS.**",
        verification: [
          "Output: ≥ 4/5 tests PASSED",
          "Không có Python exception",
          "Paimon snapshots được liệt kê",
          "Iceberg time-travel trả về results",
        ],
        estimatedTime: "~60s",
        dependencies: ["6.2", "7.2", "8.1"],
        tags: ["time-travel", "forensics", "testing"],
        optional: false,
      },
    ],
  },
  {
    id: 10,
    title: "Monitoring (Optional)",
    icon: "📊",
    description: "Prometheus + Grafana monitoring stack",
    steps: [
      {
        id: "10.1",
        title: "Start Monitoring Stack",
        command:
          "docker compose -f docker/docker-compose.yml --profile monitoring up -d prometheus grafana node-exporter",
        description:
          "Khởi động **Prometheus** (port 9090, metrics collector) + **Grafana** (port 3001, dashboards, login admin/admin) + **Node Exporter** (host metrics). Dashboard tự động provision cho violence security monitoring.",
        verification: [
          "Prometheus tại http://localhost:9090",
          "Grafana tại http://localhost:3001 (admin/admin)",
          "Dashboard 'Violence Security Monitor' visible",
        ],
        estimatedTime: "~20s",
        dependencies: ["4.1"],
        tags: ["prometheus", "grafana", "monitoring"],
        optional: true,
      },
    ],
  },
  {
    id: 11,
    title: "Agentic RAG Chatbot",
    icon: "🤖",
    description: "LangGraph + Gemini AI chatbot với Text-to-SQL",
    steps: [
      {
        id: "11.1",
        title: "Start Chatbot API",
        command:
          "docker compose -f docker/docker-compose.yml up -d --build chatbot",
        description:
          "Build và khởi động **Chatbot API** (port 5002). Sử dụng LangGraph + Google Gemini 2.0 Flash + ChromaDB + PyTrino. Hỗ trợ Text-to-SQL, tự động chọn storage layer, self-correction (max 3 retries). **Yêu cầu:** `GEMINI_API_KEY` đã được đặt trong `docker/.env`.",
        verification: [
          "Chatbot API tại http://localhost:5002/health",
          "Response: {\"status\": \"healthy\"}",
          "Thử chat: curl -X POST localhost:5002/chat -d '{\"message\":\"Hello\"}'",
        ],
        estimatedTime: "~2-3 phút (first build)",
        dependencies: ["8.1", "1.1"],
        tags: ["chatbot", "ai", "langraph", "gemini"],
        optional: false,
      },
    ],
  },
  {
    id: 13,
    title: "E2E Pipeline Verification",
    icon: "🧪",
    description: "Chạy test suite tự động kiểm tra toàn bộ pipeline: Kafka flow, Flink jobs, HOT/WARM/COLD layers, Chatbot routing",
    steps: [
      {
        id: "13.0",
        title: "Deploy Latest Test Script to Flink Container",
        command:
          "docker cp scripts/tests/test_pipeline_e2e.py jobmanager:/opt/flink/scripts/tests/test_pipeline_e2e.py",
        description:
          "Copy phiên bản mới nhất của test script từ host vào Flink jobmanager container. Chạy lệnh này từ **thư mục gốc của project** (`realtime-violence-detection/`). Bước này đảm bảo container luôn dùng script mới nhất.",
        verification: [
          "Exit code 0",
          "Không có lỗi 'No such file or directory'",
        ],
        estimatedTime: "~2s",
        dependencies: ["4.1"],
        tags: ["test", "setup"],
        optional: false,
      },
      {
        id: "13.1",
        title: "Run Full E2E Test Suite (All Phases)",
        command:
          "docker exec jobmanager python -u /opt/flink/scripts/tests/test_pipeline_e2e.py",
        description:
          "Chạy **test suite E2E đầy đủ** — 7 phases kiểm tra toàn bộ pipeline:\n\n**Phase 0** Pre-flight: kiểm tra health của Flink, Trino, Chatbot, MinIO, Gateway.\n**Phase 1** Services: RTSP streaming + inference-mock đang chạy.\n**Phase 2** Kafka: broker connectivity + Flink job metrics làm bằng chứng message flow.\n**Phase 3** Flink jobs: 4/4 jobs RUNNING (validator, fluss, paimon, aggregation).\n**Phase 4** HOT (Fluss): verify KafkaToFluss job RUNNING (native <100ms).\n**Phase 5** WARM (Paimon): query violence_incidents + daily_stats + camera_stats.\n**Phase 6** COLD (Iceberg): Trino query + time-travel.\n**Phase 7** Chatbot: 4 test cases với Vietnamese NLP + layer routing.\n\nFlag `-u` đảm bảo output hiển thị real-time (không buffered).",
        verification: [
          "Phase 0: Flink, Trino, Chatbot, MinIO, Gateway đều healthy",
          "Phase 1: RTSP services running HOẶC inference-mock running",
          "Phase 2: Kafka broker reachable, Flink jobs consuming records",
          "Phase 3: 4/4 Flink jobs RUNNING",
          "Phase 4: KafkaToFluss RUNNING (Fluss native <100ms verified)",
          "Phase 5: Paimon violence_incidents COUNT > 0, aggregation tables populated",
          "Phase 6: Iceberg table accessible via Trino",
          "Phase 7: 4/4 chatbot test cases PASS, layer routing correct",
          "Final report: 0 FAIL phases",
        ],
        estimatedTime: "~15-25 phút (Paimon queries chiếm phần lớn thời gian)",
        dependencies: ["4.2", "5.3", "6.3", "8.1", "11.1"],
        tags: ["test", "e2e", "verify", "all-layers"],
        optional: false,
      },
      {
        id: "13.2",
        title: "Run Single Phase (Quick Check)",
        command:
          "docker exec jobmanager python -u /opt/flink/scripts/tests/test_pipeline_e2e.py --phase 3",
        description:
          "Chạy **một phase cụ thể** để debug nhanh. Thay `--phase 3` bằng số phase cần kiểm tra:\n- `--phase 0`: Pre-flight health checks (~5s)\n- `--phase 3`: Flink jobs status (~2s)\n- `--phase 4`: Fluss HOT queries (~30s)\n- `--phase 5`: Paimon WARM queries (~5 phút)\n- `--phase 6`: Iceberg COLD + Trino (~30s)\n- `--phase 7`: Chatbot routing (~5-20 phút)\n\nCó thể skip các phase chậm: `--skip 5 --skip 7`",
        verification: [
          "Chỉ phase được chỉ định chạy",
          "Output report hiển thị kết quả phase đó",
        ],
        estimatedTime: "Phụ thuộc vào phase",
        dependencies: ["13.0"],
        tags: ["test", "debug", "single-phase"],
        optional: true,
      },
      {
        id: "13.3",
        title: "Quick Smoke Test (Skip Slow Layers)",
        command:
          "docker exec jobmanager python -u /opt/flink/scripts/tests/test_pipeline_e2e.py --skip 5 --skip 6 --skip 7",
        description:
          "Chạy **smoke test nhanh** — bỏ qua các phase chậm (Paimon/Iceberg/Chatbot) để kiểm tra nhanh infrastructure.\n\nPhases chạy: Pre-flight (0) + Services (1) + Kafka (2) + Flink jobs (3) + Fluss HOT (4).\n\nHữu ích để kiểm tra sau khi restart services mà không muốn chờ Paimon queries.",
        verification: [
          "Phases 0-4 pass trong < 60s",
          "Kafka flow confirmed",
          "Flink 4/4 jobs RUNNING",
          "Fluss HOT data accessible",
        ],
        estimatedTime: "~1-2 phút",
        dependencies: ["13.0"],
        tags: ["test", "smoke", "quick"],
        optional: true,
      },
      {
        id: "13.4",
        title: "Verify E2E Dashboard Integration",
        command: "powershell -Command \"(Invoke-RestMethod http://localhost:3001/api/status).dataLayers | ConvertTo-Json -Depth 5\"",
        description:
          "Tự động kiểm tra **E2E Dashboard integration** — verify rằng:\n\n- Dashboard `/api/status` endpoint trả về dataLayers metrics (HOT/WARM/COLD)\n- LiveStreamsPanel có thể kết nối đến MediaMTX (15 RTSP streams)\n- DataLayerStatusPanel có sample data\n- DataMetricsPanel polling latency acceptable (<5s)\n- WebSocket connection ổn định (không có 1006 close codes)\n\nBước này chạy song song với Phase 14 manual verification.",
        verification: [
          "curl /api/status trả về dataLayers structure",
          "dataLayers.hot.technology = 'Apache Fluss'",
          "dataLayers.warm.technology = 'Apache Paimon'",
          "dataLayers.cold.technology = 'Apache Iceberg'",
          "Tất cả layers có status = 'HEALTHY'",
        ],
        estimatedTime: "~1 phút",
        dependencies: ["13.3"],
        tags: ["test", "dashboard", "verify"],
        optional: false,
      },
    ],
  },
  {
    id: 12,
    title: "Graceful Shutdown",
    icon: "🛑",
    description: "Dừng services an toàn — giữ lại dữ liệu",
    steps: [
      {
        id: "12.0",
        title: "Stop RTSP Inference Mock Gracefully",
        command: "docker exec rtsp-inference-mock touch /app/tmp/STOP",
        description:
          "Dừng **rtsp-inference-mock** gracefully qua STOP file. Service đóng tất cả RTSP connections, flush Kafka producer, chờ worker threads kết thúc rồi tắt. **Không dùng `docker stop` trực tiếp** để tránh mất messages.",
        verification: [
          "Container rtsp-inference-mock tự tắt sau vài giây",
          "Log: 'Stop file detected. Shutting down...'",
          "Log: '[cam_XX] Worker stopped.'",
        ],
        estimatedTime: "~5-10s",
        dependencies: ["3.3"],
        tags: ["shutdown", "graceful", "rtsp"],
        optional: false,
        skippable: true,
        skipReason: "rtsp-inference-mock không đang chạy",
      },
      {
        id: "12.2",
        title: "Stop All Services",
        command:
          "docker compose -f docker/docker-compose.yml down",
        description:
          "Dừng toàn bộ services. **Volumes được giữ lại** (Kafka data, MinIO buckets, Paimon/Iceberg data) — an toàn để restart sau. Dùng `--volumes` nếu muốn xóa hoàn toàn (cẩn thận!).",
        verification: [
          "Tất cả containers dừng",
          "Volumes vẫn còn (kiểm tra: docker volume ls)",
        ],
        estimatedTime: "~15s",
        dependencies: [],
        tags: ["shutdown", "cleanup"],
        optional: false,
      },
    ],
  },
  {
    id: 14,
    title: "Real-time Monitoring & Dashboards",
    icon: "🎬",
    description: "Khởi động E2E Dashboard với real-time RTSP streams, data layer visualization, và metrics monitoring",
    steps: [
      {
        id: "14.0",
        title: "Start E2E Dashboard Server",
        command: "cd e2e-test-dashboard && npm install && npm run dev",
        description:
          "Khởi động **E2E Dashboard** React development server trên port 5173. Nếu `npm install` đã chạy, có thể chỉ chạy `npm run dev`. Dashboard sẽ tự kết nối đến backend WebSocket tại `ws://localhost:3001/ws`.",
        verification: [
          "Dashboard accessible tại http://localhost:5173",
          "WebSocket connected (connection badge = green)",
          "Toggle buttons visible: 🎥 Streams, 📊 Data Layers, 📈 Metrics",
          "Sidebar + StepDetail + Right panel render không lỗi",
        ],
        estimatedTime: "~30s (nếu dependencies đã cài)",
        dependencies: ["13.3"],
        tags: ["dashboard", "monitoring", "ui"],
        optional: false,
      },
      {
        id: "14.1",
        title: "Verify Live Streams Panel",
        command: "echo Manual verification: Click Streams button in dashboard header to show RTSP panel",
        description:
          "**Manual verification** — Click button '🎥 Streams' để hiển thị RTSP streaming panel bên phải. Kiểm tra:\n\n- 15 camera cards render trong grid (2-3 columns)\n- Status badge: NORMAL/LOADING/OFFLINE\n- Real-time clock update every 1 second\n- Hover over card → maximize button appears\n- Click maximize → fullscreen modal opens\n- Video auto-plays (muted)\n- Click X hoặc ESC → close modal\n- Không có console errors",
        verification: [
          "15 camera cards visible in right panel",
          "Status badges show (NORMAL/LOADING/OFFLINE)",
          "Real-time clock updates each second",
          "Maximize button appears on hover",
          "Fullscreen modal opens with full video controls",
          "Video auto-plays (muted attribute set)",
          "Close button (X) works correctly",
          "ESC key closes modal",
          "Browser DevTools console: 0 errors",
        ],
        estimatedTime: "~2 phút",
        dependencies: ["14.0"],
        tags: ["dashboard", "rtsp", "streams", "verify"],
        optional: false,
      },
      {
        id: "14.2",
        title: "Verify Data Layers Status Panel",
        command: "echo Manual verification: Click Data Layers button in dashboard header to show HOT/WARM/COLD panel",
        description:
          "**Manual verification** — Click button '📊 Data Layers' để hiển thị 3-tier data layer visualization panel. Kiểm tra:\n\n- 3 columns visible: 🔥 HOT, 🌊 WARM, 🧊 COLD\n- Mỗi column hiển thị: technology, tables, record count, latency, retention\n- Status badges: HEALTHY/DEGRADED/OFFLINE\n- Sample data tables populate (5 rows per layer)\n- Refresh button working\n- Data auto-refresh mỗi 5 giây\n- Data flow diagram visible at bottom\n- Không có console errors",
        verification: [
          "3 columns visible (HOT/WARM/COLD)",
          "Each column shows: technology, table names, record counts",
          "Status badges show HEALTHY/DEGRADED/OFFLINE",
          "Latency displayed: HOT <100ms, WARM 1-10min, COLD 10+min",
          "Sample data tables populated (5 rows per layer)",
          "Refresh button works (spinner animation)",
          "Data auto-refreshes every 5 seconds",
          "Data flow diagram shows Kafka → Flink → layers",
          "Browser DevTools console: 0 errors",
        ],
        estimatedTime: "~2 phút",
        dependencies: ["14.0"],
        tags: ["dashboard", "data-layers", "verify"],
        optional: false,
      },
      {
        id: "14.3",
        title: "Verify Metrics Panel",
        command: "echo Manual verification: Click Metrics button in dashboard header to show pipeline metrics panel",
        description:
          "**Manual verification** — Click button '📈 Metrics' để hiển thị real-time metrics dashboard. Kiểm tra:\n\n- 4 metric cards visible: Kafka Lag, Flink Throughput, Data Completeness, Query Latency\n- Mỗi card hiển thị: current value + trend indicator\n- Sparkline charts render: line chart (30-min rolling) + bar chart (5-min buckets)\n- Auto/Paused toggle button working\n- Metrics auto-refresh mỗi 5 giây (nếu Auto mode)\n- Số liệu hợp lý: throughput > 0, latencies in range\n- Legend visible: Validator, Kafka→Fluss, Kafka→Paimon\n- Không có console errors",
        verification: [
          "4 metric cards visible with labels and values",
          "Each card displays current value + trend",
          "Sparkline charts render without errors",
          "Line chart: 30-min rolling window of record count",
          "Bar chart: Flink throughput by 5-min buckets",
          "Auto/Paused toggle button functional",
          "Metrics auto-refresh every 5 seconds (Auto mode)",
          "Values reasonable: throughput > 0, latencies > 0",
          "Legend shows: Validator (blue), Kafka→Fluss (purple), Kafka→Paimon (pink)",
          "Browser DevTools console: 0 errors",
        ],
        estimatedTime: "~2 phút",
        dependencies: ["14.0"],
        tags: ["dashboard", "metrics", "verify"],
        optional: false,
      },
    ],
  },
];

export const TOTAL_STEPS = PIPELINE_PHASES.reduce(
  (acc, phase) => acc + phase.steps.length,
  0
);

export const STATUS = {
  PENDING: "pending",
  RUNNING: "running",
  DONE: "done",
  ERROR: "error",
  SKIPPED: "skipped",
};
