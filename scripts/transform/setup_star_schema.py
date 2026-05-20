"""
Setup Star Schema Tables — Streamhouse WARM Layer.

Tạo/cập nhật dimension và fact tables:
  - fluss.security.dim_camera   (versioned, temporal join capable)
  - paimon.security.dim_time    (date dimension, 2025-2026)
  - paimon.security.fact_violence_incidents  (star schema fact table)

Chạy một lần sau khi stack khởi động:
    flink run -py /opt/flink/scripts/setup_star_schema.py
"""
import os
import datetime
import glob
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.types import DataTypes


def main():
    s3_endpoint    = os.getenv("S3_ENDPOINT",        "http://minio:9000")
    s3_access_key  = os.getenv("MINIO_ROOT_USER",    "minio")
    s3_secret_key  = os.getenv("MINIO_ROOT_PASSWORD","mypassword")
    warehouse_path = os.getenv("PAIMON_WAREHOUSE",   "s3://warehouse/paimon")
    fluss_coord    = os.getenv("FLUSS_COORDINATOR",  "fluss-coordinator:9123")

    settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(settings)

    # ── Register catalogs ─────────────────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE CATALOG fluss WITH (
            'type'              = 'fluss',
            'bootstrap.servers' = '{fluss_coord}'
        )
    """)
    t_env.execute_sql(f"""
        CREATE CATALOG paimon WITH (
            'type'                 = 'paimon',
            'warehouse'            = '{warehouse_path}',
            's3.endpoint'          = '{s3_endpoint}',
            's3.access-key'        = '{s3_access_key}',
            's3.secret-key'        = '{s3_secret_key}',
            's3.path.style.access' = 'true'
        )
    """)

    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS `fluss`.`security`")
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS `paimon`.`security`")

    # ── dim_camera in Fluss (primary key table → versioned for temporal join) ──
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `fluss`.`security`.`dim_camera` (
            camera_id   STRING,
            location    STRING,
            ward_id     STRING,
            district    STRING,
            latitude    DOUBLE,
            longitude   DOUBLE,
            status      STRING,
            updated_at  TIMESTAMP(3),
            PRIMARY KEY (camera_id) NOT ENFORCED
        ) WITH (
            'bucket.num' = '3'
        )
    """)
    print("[INFO] Fluss dim_camera ready.")

    # ── dim_time in Paimon (pre-generated date dimension 2025-2026) ──────────
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`dim_time` (
            date_id      DATE,
            `year`       INT,
            `month`      INT,
            `day`        INT,
            day_of_week  STRING,
            week_of_year INT,
            is_weekend   BOOLEAN,
            PRIMARY KEY (date_id) NOT ENFORCED
        ) WITH (
            'merge-engine' = 'deduplicate',
            'bucket'       = '1'
        )
    """)
    print("[INFO] Paimon dim_time ready.")

    # ── fact_violence_incidents in Paimon (star schema fact table) ───────────
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`fact_violence_incidents` (
            incident_id STRING,
            camera_id   STRING,
            `timestamp` TIMESTAMP(3),
            date_id     DATE,
            risk_score  DOUBLE,
            confidence  DOUBLE,
            is_violent  BOOLEAN,
            event_type  STRING,
            location    STRING,
            ward_id     STRING,
            district    STRING,
            frame_url   STRING,
            PRIMARY KEY (incident_id) NOT ENFORCED
        ) WITH (
            'merge-engine'        = 'deduplicate',
            'changelog-producer'  = 'input',
            'bucket'              = '4'
        )
    """)
    print("[INFO] Paimon fact_violence_incidents ready.")

    # ── Seed dim_camera (15 cameras, Ho Chi Minh City Quận 1) ────────────────
    _seed_dim_camera(t_env)

    # ── Seed dim_time (2025-01-01 to 2026-12-31) ─────────────────────────────
    _seed_dim_time(t_env)

    print("[INFO] Star schema setup complete.")


def _seed_dim_camera(t_env: TableEnvironment) -> None:
    cameras = [
        ("cam_01", "Đường Nguyễn Huệ",          "Phường Bến Nghé",           "Quận 1", 10.77845, 106.70014),
        ("cam_02", "Đường Lê Lợi",               "Phường Nguyễn Thái Bình",   "Quận 1", 10.77322, 106.69453),
        ("cam_03", "Đường Nguyễn Thái Học",      "Phường Bến Thành",          "Quận 1", 10.77407, 106.70229),
        ("cam_04", "Đường Lê Thánh Tôn",         "Phường Cầu Ông Lãnh",       "Quận 1", 10.77613, 106.69705),
        ("cam_05", "Đường Pasteur",               "Phường Phạm Ngũ Lão",       "Quận 1", 10.77157, 106.70435),
        ("cam_06", "Đường Trần Hưng Đạo",        "Phường Tân Định",           "Quận 1", 10.77336, 106.70019),
        ("cam_07", "Đường Đồng Khởi",            "Phường Đa Kao",             "Quận 1", 10.77833, 106.69332),
        ("cam_08", "Đường Hai Bà Trưng",         "Phường Bến Thành",          "Quận 1", 10.78446, 106.70214),
        ("cam_09", "Đường Nguyễn Du",            "Phường Nguyễn Cư Trinh",    "Quận 1", 10.77002, 106.70027),
        ("cam_10", "Đường Võ Văn Kiệt",          "Phường Cầu Kho",            "Quận 1", 10.78266, 106.70826),
        ("cam_11", "Đường Nguyễn Công Trứ",      "Phường Tân Định",           "Quận 1", 10.77552, 106.70748),
        ("cam_12", "Đường Công Trường Mê Linh",  "Phường Nguyễn Thái Bình",   "Quận 1", 10.77956, 106.70549),
        ("cam_13", "Đường Hàm Nghi",             "Phường Phạm Ngũ Lão",       "Quận 1", 10.78320, 106.69630),
        ("cam_14", "Đường Nguyễn Bỉnh Khiêm",   "Phường Bến Nghé",           "Quận 1", 10.78074, 106.70235),
        ("cam_15", "Đường Trương Định",          "Phường Đa Kao",             "Quận 1", 10.77709, 106.69288),
    ]

    rows = ",\n    ".join(
        f"('{cid}', '{loc}', '{ward}', '{dist}', {lat}, {lon}, 'ACTIVE', "
        f"TIMESTAMP '2025-01-01 00:00:00')"
        for cid, loc, ward, dist, lat, lon in cameras
    )
    t_env.execute_sql(
        f"INSERT INTO `fluss`.`security`.`dim_camera` VALUES\n    {rows}"
    ).wait()
    print(f"[INFO] Seeded {len(cameras)} cameras into dim_camera.")


def _seed_dim_time(t_env: TableEnvironment) -> None:
    dow_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    start = datetime.date(2025, 1, 1)
    end   = datetime.date(2026, 12, 31)

    rows = []
    d = start
    while d <= end:
        iso = d.isocalendar()
        woy = iso[1]
        dow = dow_names[d.weekday()]
        is_weekend = "true" if d.weekday() >= 5 else "false"
        rows.append(
            f"(DATE '{d}', {d.year}, {d.month}, {d.day}, '{dow}', {woy}, {is_weekend})"
        )
        d += datetime.timedelta(days=1)

    # Insert in chunks of 100 to avoid SQL statement size limits
    chunk = 100
    total = 0
    for i in range(0, len(rows), chunk):
        batch = rows[i:i + chunk]
        t_env.execute_sql(
            "INSERT INTO `paimon`.`security`.`dim_time` VALUES\n    "
            + ",\n    ".join(batch)
        ).wait()
        total += len(batch)

    print(f"[INFO] Seeded {total} rows into dim_time (2025–2026).")


if __name__ == "__main__":
    main()
