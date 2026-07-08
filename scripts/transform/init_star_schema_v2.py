"""
Setup Star Schema v2 — Streamhouse WARM Layer (thay thế setup_star_schema.py).

Grain fact = 1 VỤ bạo lực (đã sessionize bằng incident_uid từ producer),
không còn đếm raw event. Khớp thiết kế thesis_report/figures/star_schema_v2.dbml.

Tạo:
  Paimon (WARM):
    - dim_camera            (SCD Type 2, seed từ camera_registry.csv — nguồn duy nhất)
    - dim_date              (2025–2026, grain ngày)
    - dim_time              (24 giờ, part_of_day, is_peak_hour)
    - dim_event_type        (FIGHTING/ASSAULT/STABBING/SHOOTING + severity)
    - fact_violence_incident (grain = 1 vụ, partial-update)
    - fact_incident_person  (bridge bbox per-person)
    - violence_incidents    (event grain, partial-update — fix bug frame_url bị NULL ghi đè)
  Fluss (HOT):
    - hot_violence_alerts   (event grain, +incident_uid/people_count)
    - hot_violence_incidents (grain = 1 vụ, upsert theo incident_uid) ← đếm vụ realtime
    - dim_camera            (copy phục vụ temporal join; seed qua SQL Gateway ở pipeline_manager)

Chạy một lần sau khi stack khởi động (batch mode):
    flink run -py /opt/flink/scripts/init_star_schema_v2.py
"""
import csv
import datetime
import os

from pyflink.table import EnvironmentSettings, TableEnvironment

CAMERA_REGISTRY_FILE = os.getenv(
    "CAMERA_REGISTRY_FILE", "/opt/flink/data/metadata/camera_registry.csv"
)

EVENT_TYPES = [
    # (event_type_id, event_code, event_name, severity)
    (0, "UNKNOWN",  "Không xác định", 0),
    (1, "FIGHTING", "Ẩu đả",          3),
    (2, "ASSAULT",  "Hành hung",      4),
    (3, "STABBING", "Đâm chém",       5),
    (4, "SHOOTING", "Nổ súng",        5),
]


def _register_catalogs(t_env: TableEnvironment) -> None:
    s3_endpoint    = os.getenv("S3_ENDPOINT",         "http://minio:9000")
    s3_access_key  = os.getenv("MINIO_ROOT_USER",     "minio")
    s3_secret_key  = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")
    warehouse_path = os.getenv("PAIMON_WAREHOUSE",    "s3://warehouse/paimon")
    fluss_coord    = os.getenv("FLUSS_COORDINATOR",   "fluss-coordinator:9123")

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


# ── Fluss HOT ─────────────────────────────────────────────────────────────────

def _create_fluss_tables(t_env: TableEnvironment) -> None:
    # Event grain — schema migration: Fluss PK table không ALTER được → DROP + CREATE.
    # Dữ liệu HOT ephemeral (tier mỗi 30'), mất khi upgrade là chấp nhận được.
    t_env.execute_sql("DROP TABLE IF EXISTS `fluss`.`security`.`hot_violence_alerts`")
    t_env.execute_sql("""
        CREATE TABLE `fluss`.`security`.`hot_violence_alerts` (
            incident_id  STRING,
            incident_uid STRING,
            camera_id    STRING,
            `timestamp`  TIMESTAMP(3),
            risk_score   DOUBLE,
            confidence   DOUBLE,
            is_violent   BOOLEAN,
            event_type   STRING,
            location     STRING,
            ward_id      STRING,
            district     STRING,
            people_count INT,
            PRIMARY KEY (incident_id) NOT ENFORCED
        ) WITH (
            'bucket.num' = '3'
        )
    """)
    print("[INFO] Fluss hot_violence_alerts ready (+incident_uid, +people_count).")

    # Incident grain — 1 dòng = 1 vụ, upsert liên tục khi vụ còn diễn ra.
    # Đếm số vụ realtime = COUNT trên bảng này (không phải trên events).
    t_env.execute_sql("DROP TABLE IF EXISTS `fluss`.`security`.`hot_violence_incidents`")
    t_env.execute_sql("""
        CREATE TABLE `fluss`.`security`.`hot_violence_incidents` (
            incident_uid   STRING,
            camera_id      STRING,
            start_ts       TIMESTAMP(3),
            last_ts        TIMESTAMP(3),
            event_count    BIGINT,
            max_risk_score DOUBLE,
            avg_confidence DOUBLE,
            event_type     STRING,
            location       STRING,
            ward_id        STRING,
            district       STRING,
            people_count   INT,
            PRIMARY KEY (incident_uid) NOT ENFORCED
        ) WITH (
            'bucket.num' = '3'
        )
    """)
    print("[INFO] Fluss hot_violence_incidents ready (grain = 1 vụ).")

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
    print("[INFO] Fluss dim_camera ready (temporal join copy).")


# ── Paimon WARM: dimensions ───────────────────────────────────────────────────

def _create_paimon_dims(t_env: TableEnvironment) -> None:
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`dim_camera` (
            camera_id  STRING,
            street     STRING,
            ward       STRING,
            district   STRING,
            city       STRING,
            latitude   DOUBLE,
            longitude  DOUBLE,
            status     STRING,
            valid_from TIMESTAMP(3),
            valid_to   TIMESTAMP(3),
            is_current BOOLEAN,
            PRIMARY KEY (camera_id, valid_from) NOT ENFORCED
        ) WITH (
            'merge-engine' = 'deduplicate',
            'bucket'       = '1'
        )
    """)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`dim_date` (
            date_id      DATE,
            `year`       INT,
            quarter      INT,
            `month`      INT,
            month_name   STRING,
            `day`        INT,
            day_of_week  INT,
            day_name     STRING,
            week_of_year INT,
            is_weekend   BOOLEAN,
            is_holiday   BOOLEAN,
            PRIMARY KEY (date_id) NOT ENFORCED
        ) WITH (
            'merge-engine' = 'deduplicate',
            'bucket'       = '1'
        )
    """)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`dim_time` (
            time_id      INT,
            `hour`       INT,
            part_of_day  STRING,
            is_peak_hour BOOLEAN,
            PRIMARY KEY (time_id) NOT ENFORCED
        ) WITH (
            'merge-engine' = 'deduplicate',
            'bucket'       = '1'
        )
    """)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`dim_event_type` (
            event_type_id INT,
            event_code    STRING,
            event_name    STRING,
            severity      INT,
            PRIMARY KEY (event_type_id) NOT ENFORCED
        ) WITH (
            'merge-engine' = 'deduplicate',
            'bucket'       = '1'
        )
    """)
    print("[INFO] Paimon dims ready: dim_camera(SCD2), dim_date, dim_time, dim_event_type.")


# ── Paimon WARM: facts ────────────────────────────────────────────────────────

def _create_paimon_facts(t_env: TableEnvironment) -> None:
    # Grain = 1 vụ. partial-update: build_incident_facts chạy lại không ghi đè
    # giá trị đã có bằng NULL (bảo vệ frame_url).
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`fact_violence_incident` (
            incident_id    STRING,
            camera_id      STRING,
            date_id        DATE,
            time_id        INT,
            event_type_id  INT,
            start_ts       TIMESTAMP(3),
            end_ts         TIMESTAMP(3),
            duration_sec   INT,
            event_count    BIGINT,
            max_risk_score DOUBLE,
            avg_confidence DOUBLE,
            is_violent     BOOLEAN,
            people_count   INT,
            frame_url      STRING,
            created_at     TIMESTAMP(3),
            PRIMARY KEY (incident_id) NOT ENFORCED
        ) WITH (
            'merge-engine'       = 'partial-update',
            'changelog-producer' = 'lookup',
            'bucket'             = '4'
        )
    """)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`fact_incident_person` (
            incident_id STRING,
            track_id    INT,
            person_role STRING,
            bbox        STRING,
            det_score   DOUBLE,
            PRIMARY KEY (incident_id, track_id) NOT ENFORCED
        ) WITH (
            'merge-engine' = 'deduplicate',
            'bucket'       = '2'
        )
    """)

    # Event grain (giữ cho drill-down/evidence). partial-update fix bug:
    # tiering ghi frame_url=NULL sau update_frame_url → NULL không còn ghi đè URL thật.
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS `paimon`.`security`.`violence_incidents` (
            incident_id      STRING,
            incident_uid     STRING,
            camera_id        STRING,
            `timestamp`      TIMESTAMP(3),
            risk_score       DOUBLE,
            confidence       DOUBLE,
            is_violent       BOOLEAN,
            event_type       STRING,
            location         STRING,
            is_deleted       BOOLEAN,
            frame_url        STRING,
            thumbnail_b64    STRING,
            frame_capture_ts BIGINT,
            people_json      STRING,
            people_count     INT,
            PRIMARY KEY (incident_id) NOT ENFORCED
        ) WITH (
            'merge-engine'       = 'partial-update',
            'changelog-producer' = 'lookup',
            'bucket'             = '4'
        )
    """)
    print("[INFO] Paimon facts ready: fact_violence_incident, fact_incident_person, "
          "violence_incidents (event grain, partial-update).")


# ── Seeds ─────────────────────────────────────────────────────────────────────

def _load_camera_registry(path: str) -> list:
    cameras = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cameras.append(row)
    return cameras


def _seed_dim_camera(t_env: TableEnvironment) -> None:
    """Seed dim_camera (Paimon, SCD2) từ camera_registry.csv — nguồn duy nhất.

    SCD2: nếu camera đã có bản ghi is_current với thuộc tính KHÁC → đóng bản cũ
    (valid_to = now, is_current = false) và chèn bản mới. Idempotent khi không đổi.
    """
    if not os.path.exists(CAMERA_REGISTRY_FILE):
        print(f"[WARN] Camera registry not found: {CAMERA_REGISTRY_FILE} — skip seed dim_camera.")
        return

    cameras = _load_camera_registry(CAMERA_REGISTRY_FILE)
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Đọc trạng thái hiện tại để so sánh SCD2
    current: dict = {}
    try:
        with t_env.execute_sql(
            "SELECT camera_id, street, ward, district, valid_from "
            "FROM `paimon`.`security`.`dim_camera` WHERE is_current = true"
        ).collect() as rs:
            for r in rs:
                current[r[0]] = {"street": r[1], "ward": r[2], "district": r[3],
                                 "valid_from": r[4]}
    except Exception as e:
        print(f"[WARN] Could not read existing dim_camera ({e}) — seeding fresh.")

    def esc(s: str) -> str:
        return str(s or "").replace("'", "''")

    new_rows, close_rows = [], []
    for cam in cameras:
        cid = cam["camera_id"]
        attrs = (cam.get("street", ""), cam.get("ward", ""), cam.get("district", ""))
        if cid in current:
            old = current[cid]
            if (old["street"], old["ward"], old["district"]) == attrs:
                continue  # không đổi → bỏ qua
            close_rows.append((cid, old, cam))
        new_rows.append(cam)

    # Đóng bản cũ: Paimon PK (camera_id, valid_from) → ghi đè dòng cũ với valid_to/is_current mới
    for cid, old, cam in close_rows:
        vf = old["valid_from"].strftime("%Y-%m-%d %H:%M:%S") if old["valid_from"] else "2025-01-01 00:00:00"
        t_env.execute_sql(f"""
            INSERT INTO `paimon`.`security`.`dim_camera` VALUES (
                '{esc(cid)}', '{esc(old["street"])}', '{esc(old["ward"])}',
                '{esc(old["district"])}', '', NULL, NULL, 'ACTIVE',
                TIMESTAMP '{vf}', TIMESTAMP '{now}', false)
        """).wait()

    if new_rows:
        values = ",\n    ".join(
            f"('{esc(c['camera_id'])}', '{esc(c.get('street'))}', '{esc(c.get('ward'))}', "
            f"'{esc(c.get('district'))}', '{esc(c.get('city'))}', "
            f"{float(c.get('latitude') or 0)}, {float(c.get('longitude') or 0)}, 'ACTIVE', "
            f"TIMESTAMP '{now}', CAST(NULL AS TIMESTAMP(3)), true)"
            for c in new_rows
        )
        t_env.execute_sql(
            f"INSERT INTO `paimon`.`security`.`dim_camera` VALUES\n    {values}"
        ).wait()
    print(f"[INFO] dim_camera seeded from CSV: {len(new_rows)} new/changed, "
          f"{len(close_rows)} closed (SCD2), source={CAMERA_REGISTRY_FILE}")


def _seed_dim_date(t_env: TableEnvironment) -> None:
    month_names = ["January", "February", "March", "April", "May", "June", "July",
                   "August", "September", "October", "November", "December"]
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    start = datetime.date(2025, 1, 1)
    end   = datetime.date(2026, 12, 31)

    rows, d = [], start
    while d <= end:
        woy = d.isocalendar()[1]
        rows.append(
            f"(DATE '{d}', {d.year}, {(d.month - 1) // 3 + 1}, {d.month}, "
            f"'{month_names[d.month - 1]}', {d.day}, {d.weekday() + 1}, "
            f"'{day_names[d.weekday()]}', {woy}, "
            f"{'true' if d.weekday() >= 5 else 'false'}, false)"
        )
        d += datetime.timedelta(days=1)

    chunk, total = 100, 0
    for i in range(0, len(rows), chunk):
        t_env.execute_sql(
            "INSERT INTO `paimon`.`security`.`dim_date` VALUES\n    "
            + ",\n    ".join(rows[i:i + chunk])
        ).wait()
        total += len(rows[i:i + chunk])
    print(f"[INFO] dim_date seeded: {total} rows (2025–2026).")


def _seed_dim_time(t_env: TableEnvironment) -> None:
    def part(h: int) -> str:
        if h < 6:  return "đêm"
        if h < 12: return "sáng"
        if h < 18: return "chiều"
        return "tối"

    rows = ",\n    ".join(
        f"({h}, {h}, '{part(h)}', {'true' if h in (7, 8, 9, 17, 18, 19) else 'false'})"
        for h in range(24)
    )
    t_env.execute_sql(
        f"INSERT INTO `paimon`.`security`.`dim_time` VALUES\n    {rows}"
    ).wait()
    print("[INFO] dim_time seeded: 24 rows.")


def _seed_dim_event_type(t_env: TableEnvironment) -> None:
    rows = ",\n    ".join(
        f"({eid}, '{code}', '{name}', {sev})" for eid, code, name, sev in EVENT_TYPES
    )
    t_env.execute_sql(
        f"INSERT INTO `paimon`.`security`.`dim_event_type` VALUES\n    {rows}"
    ).wait()
    print(f"[INFO] dim_event_type seeded: {len(EVENT_TYPES)} rows.")


def main():
    settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(settings)
    # TaskManager nhỏ (network buffers hạn chế) — parallelism mặc định cao làm
    # batch shuffle đòi >512 buffers → "Insufficient number of network buffers".
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("table.exec.resource.default-parallelism", "1")

    _register_catalogs(t_env)
    _create_fluss_tables(t_env)
    _create_paimon_dims(t_env)
    _create_paimon_facts(t_env)

    _seed_dim_camera(t_env)
    _seed_dim_date(t_env)
    _seed_dim_time(t_env)
    _seed_dim_event_type(t_env)

    print("[SUCCESS] Star schema v2 setup complete.")


if __name__ == "__main__":
    main()
