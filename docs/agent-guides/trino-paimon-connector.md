# Trino-Paimon Connector — Hướng dẫn cấu hình

**Version**: 2.0  
**Ngày hoàn thành**: 2026-05-15  
**Trạng thái**: Production — đang chạy trên Trino 440

---

## Tóm tắt

Tài liệu này ghi lại toàn bộ quá trình kết nối Paimon (WARM layer) vào Trino để giảm latency WARM query từ **300–360 giây** (qua Flink SQL Gateway) xuống còn **~4 giây** (Trino native).

---

## Lý do chọn Trino 440 thay vì 476

Khi bắt đầu, project dùng Trino 476. Vấn đề:

| Phiên bản | Tình trạng paimon-trino JAR |
|-----------|---------------------------|
| Trino 476 | Không có JAR trên Maven Central; Apache snapshot repo không truy cập được từ Docker builder |
| Trino 440 | `paimon-trino-440` có thể build từ source tại `release-0.8` branch — stable, đã test |

**Quyết định**: Downgrade Trino 476 → 440. Đây là lựa chọn ít rủi ro nhất vì:
- Dữ liệu Paimon được ghi bởi Flink 1.18 với Paimon 0.8.x — tương thích hoàn toàn
- Iceberg connector trên Trino 440 vẫn hoạt động bình thường
- Không cần thay đổi bất kỳ pipeline nào

---

## Kiến trúc Docker multi-stage build

```
Stage 1: maven:3.9-eclipse-temurin-21
  └─ git clone paimon-trino (branch release-0.8)
  └─ Patch source: xóa HdfsModule dependency
  └─ mvn build → paimon-trino-440-plugin.tar.gz

Stage 2: trinodb/trino:440
  └─ Copy plugin từ Stage 1
  └─ Copy hdfs/ directory từ iceberg plugin
  └─ Kết quả: Trino 440 với paimon catalog hoạt động
```

---

## 4 lỗi cascading và cách sửa

Build paimon-trino-440 từ `release-0.8` gặp 4 lỗi liên tiếp. Mỗi lỗi chỉ xuất hiện sau khi sửa lỗi trước.

### Lỗi 1 — "HDFS should not be on the plugin classpath"

**Nguyên nhân**: `release-0.8` vẫn khởi tạo `HdfsModule` và `HdfsAuthenticationModule` trong `TrinoConnectorFactory.java`. Trino 440 cấm HDFS JAR trên plugin classloader.

**Cách sửa**: Rewrite toàn bộ `TrinoConnectorFactory.java` bằng heredoc trong Dockerfile — xóa hai import và hai dòng `new HdfsModule()`, `new HdfsAuthenticationModule()` khỏi Bootstrap constructor.

> **Tại sao không dùng `sed`?** Docker layer caching tái sử dụng bytecode Maven cũ ngay cả khi source đã được patch bằng sed. Heredoc buộc Maven phải compile lại file mới hoàn toàn.

### Lỗi 2 — Maven build thất bại vì Spotless

**Nguyên nhân**: Plugin `spotless-maven-plugin` kiểm tra code style và từ chối code heredoc do format dòng khác với project style.

**Cách sửa**: Thêm `-Dspotless.skip=true` vào lệnh Maven:
```bash
mvn clean install -DskipTests -Drat.skip=true -Dspotless.skip=true -pl paimon-trino-440 -am -q
```

### Lỗi 3 — "HDFS directory is missing: /tmp/trino-data/plugin/paimon/hdfs"

**Nguyên nhân**: `HdfsFileSystemLoader` trong `trino-filesystem-manager-440.jar` chạy tự động khi load bất kỳ plugin nào. Nó yêu cầu bắt buộc phải có thư mục con `hdfs/` bên trong plugin dir, dù plugin đó không dùng HDFS.

**Cách sửa**: Tạo thư mục `hdfs/` cho paimon plugin trong Stage 2 (xem Lỗi 4).

### Lỗi 4 — `NoClassDefFoundError: org/weakref/jmx/guice/MBeanModule`

**Nguyên nhân**: `HdfsFileSystemLoader` tạo ra `HdfsClassLoader` chỉ có thể nhìn thấy JAR trong thư mục `hdfs/`. Nếu chỉ có mỗi `trino-hdfs-440.jar` trong `hdfs/`, `HdfsClassLoader` không tìm thấy các dependency như `jmxutils`, `guava`, `airlift-bootstrap` (~95 JAR cần thiết).

**Cách sửa**: Copy toàn bộ thư mục `hdfs/` từ iceberg plugin (đã có sẵn trong `trinodb/trino:440` với đầy đủ ~95 JAR) sang paimon plugin:
```dockerfile
RUN cp -r /usr/lib/trino/plugin/iceberg/hdfs /usr/lib/trino/plugin/paimon/hdfs
```

> **Tại sao iceberg có sẵn?** Iceberg dùng HDFS thực sự nên trinodb/trino:440 đã đóng gói đầy đủ `hdfs/` cho iceberg. Paimon sau khi patch không dùng HDFS nhưng vẫn cần thư mục này để qua được `HdfsFileSystemLoader`.

---

## Dockerfile.trino — Nội dung chính

File đầy đủ tại [`docker/Dockerfile.trino`](../../docker/Dockerfile.trino).

### Stage 1: Build plugin

```dockerfile
FROM maven:3.9-eclipse-temurin-21 AS paimon-builder

RUN git clone --depth 1 --branch release-0.8 \
    https://github.com/apache/paimon-trino.git /paimon-trino
WORKDIR /paimon-trino

# Cung cấp toolchains.xml để maven-toolchains-plugin tìm JDK 21
RUN mkdir -p /root/.m2 && printf '...' > /root/.m2/toolchains.xml

# Patch 1: Rewrite TrinoConnectorFactory.java — xóa HdfsModule
RUN cat > paimon-trino-440/src/main/java/.../TrinoConnectorFactory.java << 'JAVA_EOF'
// (xem file đầy đủ trong Dockerfile)
JAVA_EOF

# Patch 2: Rewrite TrinoMetadataFactory.java — xóa HdfsConfig injection
RUN cat > paimon-trino-440/src/main/java/.../TrinoMetadataFactory.java << 'JAVA_EOF'
// (xem file đầy đủ trong Dockerfile)
JAVA_EOF

# Build với spotless bị tắt
RUN mvn clean install -DskipTests -Drat.skip=true -Dspotless.skip=true \
    -pl paimon-trino-440 -am -q

# Giải nén plugin archive
RUN mkdir -p /paimon-plugin && \
    tar -zxf /paimon-trino/paimon-trino-440/target/*-plugin.tar.gz -C /paimon-plugin/

# Xóa trino-hdfs-440.jar khỏi main classloader dir
# (iceberg/hdfs/ trong Stage 2 sẽ cung cấp toàn bộ HDFS deps)
RUN rm -f /paimon-plugin/paimon/trino-hdfs-440.jar
```

### Stage 2: Trino 440 + plugin

```dockerfile
FROM trinodb/trino:440

# Bắt buộc dùng USER root vì plugin dir thuộc root:root 755
USER root

# Copy paimon plugin (trino-hdfs-440.jar đã xóa ở Stage 1)
COPY --from=paimon-builder /paimon-plugin/ /usr/lib/trino/plugin/

# Copy hdfs/ từ iceberg — cung cấp ~95 JAR cho HdfsClassLoader
RUN cp -r /usr/lib/trino/plugin/iceberg/hdfs /usr/lib/trino/plugin/paimon/hdfs

USER trino
```

---

## Cấu hình paimon.properties

File áp dụng cho coordinator, worker1, worker2 tại:
- `config/trino/coordinator/etc/catalog/paimon.properties`
- `config/trino/worker1/etc/catalog/paimon.properties`
- `config/trino/worker2/etc/catalog/paimon.properties`

```properties
connector.name=paimon

# Phải trùng với warehouse path Flink dùng khi tạo bảng
warehouse=s3://warehouse/paimon

# Filesystem metastore — catalog lưu trực tiếp trên S3, không cần Hive Metastore
metastore=filesystem

# Trino native S3 filesystem (thay thế Hadoop S3A)
# Tên property theo Trino S3FileSystemConfig, KHÔNG phải paimon s3.*
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.aws-access-key=${ENV:MINIO_ROOT_USER}
s3.aws-secret-key=${ENV:MINIO_ROOT_PASSWORD}
s3.path-style-access=true
s3.ssl.enabled=false

# Scan optimization
scan.infer-parallelism=true
scan.split-target-size=134217728   # 128MB per split
```

**Lưu ý quan trọng về property names**:

| Key | Đúng | Sai |
|-----|------|-----|
| Access key | `s3.aws-access-key` | `s3.access-key` |
| Secret key | `s3.aws-secret-key` | `s3.secret-key` |
| Path style | `s3.path-style-access` | `s3.path.style.access` |

Paimon 0.8 dùng `s3.aws-access-key` (theo Trino S3FileSystemConfig), không phải `s3.access-key` (theo Hadoop S3A). Nhầm tên property gây lỗi authentication silently.

---

## Bảng trong Paimon

Sau khi catalog hoạt động, Trino thấy 3 bảng trong schema `security`:

| Bảng | Mô tả | Rows (2026-05-15) |
|------|-------|-------------------|
| `violence_incidents` | Sự cố bạo lực, merge engine: deduplicate | 290,474 |
| `daily_incident_stats` | Thống kê ngày, merge engine: aggregation | ~200 |
| `camera_stats` | Thống kê theo camera | ~60 |

---

## Kết quả đo latency

| Query type | Latency | Ghi chú |
|-----------|---------|---------|
| `SHOW CATALOGS` | 44s | Cold start (Trino vừa khởi động) |
| `COUNT(*)` toàn bảng | ~3 phút | Cold scan, 290k rows, coordinator-only |
| Filtered query (1 ngày) | ~90s | Cold scan với predicate pushdown |
| Chatbot WARM query (warm) | **~4 giây** | Connection đã warm, kết quả cache |
| Chatbot WARM query (cold) | ~180s | Trino vừa restart |

> **So sánh trước/sau**: Flink SQL Gateway WARM query mất 300–360s. Trino native WARM query warm connection mất 4s — **giảm 75–90 lần**.

---

## Cách rebuild Trino image

```bash
# Lần đầu hoặc khi thay đổi Dockerfile (--no-cache để tránh dùng bytecode cũ)
docker compose -f docker/docker-compose.yml build --no-cache trino-coordinator

# Sau khi build xong, restart
docker compose -f docker/docker-compose.yml up -d trino-coordinator

# Kiểm tra catalog load thành công
docker compose -f docker/docker-compose.yml logs trino-coordinator | grep "Added catalog"
# Expected: -- Added catalog paimon using connector paimon --
```

> **QUAN TRỌNG**: Luôn dùng `--no-cache` khi rebuild sau khi thay đổi source patch trong Dockerfile. Docker layer caching có thể tái sử dụng bytecode Maven cũ khiến patch không có tác dụng.

---

## Kiểm tra nhanh

```bash
# 1. Verify catalogs
docker exec trino-coordinator trino --execute "SHOW CATALOGS;"
# Expected: iceberg, paimon, system

# 2. Verify paimon tables
docker exec trino-coordinator trino --execute "SHOW TABLES IN paimon.security;"
# Expected: camera_stats, daily_incident_stats, violence_incidents

# 3. Count rows (warm query, ~4s nếu connection đã warm)
docker exec trino-coordinator trino \
  --execute "SELECT COUNT(*) FROM paimon.security.violence_incidents;"
```

---

## Troubleshooting

### "HDFS should not be on the plugin classpath"

Patch `TrinoConnectorFactory.java` chưa được compile. Rebuild với `--no-cache`:
```bash
docker compose -f docker/docker-compose.yml build --no-cache trino-coordinator
```

### "HDFS directory is missing: /tmp/trino-data/plugin/paimon/hdfs"

Stage 2 thiếu bước copy `hdfs/`. Kiểm tra Dockerfile có dòng:
```dockerfile
RUN cp -r /usr/lib/trino/plugin/iceberg/hdfs /usr/lib/trino/plugin/paimon/hdfs
```

### `NoClassDefFoundError` khi load paimon catalog

`HdfsClassLoader` không tìm thấy dependency. Nguyên nhân thường do chỉ copy `trino-hdfs-440.jar` vào `hdfs/` mà thiếu ~94 JAR còn lại. Dùng lệnh `cp -r` toàn bộ `iceberg/hdfs/` — không copy từng file riêng lẻ.

### Paimon catalog load nhưng query rất chậm (>10 phút)

Trino coordinator-only (không có worker). Bật scaling profile để thêm 2 workers:
```bash
docker compose -f docker/docker-compose.yml --profile scaling up -d
```

---

## Tham khảo

- [Dockerfile.trino](../../docker/Dockerfile.trino) — Source đầy đủ với heredoc patches
- [paimon-trino GitHub](https://github.com/apache/paimon-trino) — branch `release-0.8`
- [Paimon Trino docs](https://paimon.apache.org/docs/master/ecosystem/trino/)
- [trino-query-federation.md](trino-query-federation.md) — Kiến trúc tổng quan Trino trong project
