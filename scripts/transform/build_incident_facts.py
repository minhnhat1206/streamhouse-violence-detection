"""
Flink Batch Job: Paimon events → fact_violence_incident (grain = 1 VỤ) + bridge bbox.

Chạy định kỳ sau tiering (pipeline_manager, mỗi TIERING_INTERVAL_MINS):
  1. Đọc paimon.security.violence_incidents (event grain, 0.5s/event khi violent)
     trong cửa sổ BUILD_LOOKBACK_HOURS gần nhất, chỉ các event có incident_uid.
  2. GROUP BY incident_uid → 1 dòng / 1 vụ: start/end, duration, event_count,
     max_risk_score, avg_confidence, people_count, frame_url của event PEAK
     (risk_score cao nhất — ảnh này capture từ stream _bbox nên có sẵn bounding box).
  3. Join dims: date_id (ngày bắt đầu), time_id (giờ bắt đầu), event_type_id.
  4. Upsert vào fact_violence_incident (partial-update — chạy lại không mất frame_url).
  5. Parse people_json của event peak → fact_incident_person (bridge bbox per-person).

Idempotent: PK incident_id, chạy lại chỉ upsert giá trị mới.

    flink run -py /opt/flink/scripts/build_incident_facts.py
"""
import json
import os
from datetime import datetime, timedelta

from pyflink.table import EnvironmentSettings, TableEnvironment

BUILD_LOOKBACK_HOURS = int(os.getenv("BUILD_LOOKBACK_HOURS", "48"))
# TIMESTAMP literal thay vì INTERVAL 'N' HOUR — Flink giới hạn HOUR(2) ≤ 99
CUTOFF_TS = (datetime.utcnow() - timedelta(hours=BUILD_LOOKBACK_HOURS)).strftime(
    "%Y-%m-%d %H:%M:%S"
)


def _register_paimon(t_env: TableEnvironment) -> None:
    s3_endpoint   = os.getenv("S3_ENDPOINT",         "http://minio:9000")
    s3_access_key = os.getenv("MINIO_ROOT_USER",     "minio")
    s3_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "mypassword")
    warehouse     = os.getenv("PAIMON_WAREHOUSE",    "s3://warehouse/paimon")
    t_env.execute_sql(f"""
        CREATE CATALOG paimon WITH (
            'type'                 = 'paimon',
            'warehouse'            = '{warehouse}',
            's3.endpoint'          = '{s3_endpoint}',
            's3.access-key'        = '{s3_access_key}',
            's3.secret-key'        = '{s3_secret_key}',
            's3.path.style.access' = 'true'
        )
    """)


def build_facts(t_env: TableEnvironment) -> None:
    """GROUP BY incident_uid → upsert fact_violence_incident."""
    print(f"[INFO] Building incident facts (lookback {BUILD_LOOKBACK_HOURS}h)...")
    t_env.execute_sql(f"""
        INSERT INTO `paimon`.`security`.`fact_violence_incident`
        WITH events AS (
            SELECT *
            FROM `paimon`.`security`.`violence_incidents`
            WHERE incident_uid IS NOT NULL
              AND `timestamp` >= TIMESTAMP '{CUTOFF_TS}'
        ),
        peak AS (
            SELECT incident_uid, frame_url, people_count,
                   ROW_NUMBER() OVER (
                       PARTITION BY incident_uid
                       ORDER BY risk_score DESC, `timestamp` DESC
                   ) AS rn
            FROM events
            WHERE frame_url IS NOT NULL
        ),
        agg AS (
            SELECT
                incident_uid,
                MAX(camera_id)       AS camera_id,
                MIN(`timestamp`)     AS start_ts,
                MAX(`timestamp`)     AS end_ts,
                COUNT(*)             AS event_count,
                MAX(risk_score)      AS max_risk_score,
                AVG(confidence)      AS avg_confidence,
                MAX(event_type)      AS event_type_code,
                MAX(COALESCE(people_count, 0)) AS people_count
            FROM events
            GROUP BY incident_uid
        )
        SELECT
            a.incident_uid                                        AS incident_id,
            a.camera_id,
            CAST(a.start_ts AS DATE)                              AS date_id,
            EXTRACT(HOUR FROM a.start_ts)                         AS time_id,
            COALESCE(et.event_type_id, 0)                         AS event_type_id,
            a.start_ts,
            a.end_ts,
            CAST(TIMESTAMPDIFF(SECOND, a.start_ts, a.end_ts) AS INT) AS duration_sec,
            a.event_count,
            a.max_risk_score,
            a.avg_confidence,
            CAST(true AS BOOLEAN)                                 AS is_violent,
            GREATEST(a.people_count, COALESCE(p.people_count, 0)) AS people_count,
            p.frame_url,
            LOCALTIMESTAMP                                        AS created_at
        FROM agg a
        LEFT JOIN (SELECT incident_uid, frame_url, people_count FROM peak WHERE rn = 1) p
            ON a.incident_uid = p.incident_uid
        LEFT JOIN `paimon`.`security`.`dim_event_type` et
            ON et.event_code = a.event_type_code
    """).wait()
    print("[INFO] fact_violence_incident upserted.")


def build_bridge(t_env: TableEnvironment) -> None:
    """Parse people_json của event peak → fact_incident_person (bounded, Python-side)."""
    print("[INFO] Building fact_incident_person bridge from peak-event bbox...")
    rows = []
    try:
        with t_env.execute_sql(f"""
            SELECT incident_uid, people_json FROM (
                SELECT incident_uid, people_json,
                       ROW_NUMBER() OVER (
                           PARTITION BY incident_uid
                           ORDER BY risk_score DESC, `timestamp` DESC
                       ) AS rn
                FROM `paimon`.`security`.`violence_incidents`
                WHERE incident_uid IS NOT NULL
                  AND people_json IS NOT NULL
                  AND `timestamp` >= TIMESTAMP '{CUTOFF_TS}'
            ) WHERE rn = 1
        """).collect() as rs:
            for r in rs:
                rows.append((r[0], r[1]))
    except Exception as e:
        print(f"[WARN] Could not read peak people_json: {e}")
        return

    values = []
    for incident_uid, people_json in rows:
        try:
            people = json.loads(people_json) if people_json else []
        except (json.JSONDecodeError, TypeError):
            continue
        for idx, person in enumerate(people):
            if not isinstance(person, dict):
                continue
            track_id = person.get("track_id", person.get("id", idx))
            bbox = person.get("bbox", person.get("box", []))
            score = person.get("conf", person.get("score", person.get("confidence", 0.0)))
            uid = str(incident_uid).replace("'", "''")
            bbox_str = json.dumps(bbox).replace("'", "''")
            try:
                values.append(
                    f"('{uid}', {int(track_id)}, CAST(NULL AS STRING), "
                    f"'{bbox_str}', {float(score or 0.0)})"
                )
            except (TypeError, ValueError):
                continue

    if not values:
        print("[INFO] No bbox data to bridge.")
        return

    chunk = 200
    for i in range(0, len(values), chunk):
        t_env.execute_sql(
            "INSERT INTO `paimon`.`security`.`fact_incident_person` VALUES\n    "
            + ",\n    ".join(values[i:i + chunk])
        ).wait()
    print(f"[INFO] fact_incident_person upserted: {len(values)} person rows "
          f"from {len(rows)} incidents.")


def main() -> None:
    settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(settings)
    # Tránh "Insufficient number of network buffers" trên TaskManager nhỏ
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("table.exec.resource.default-parallelism", "1")
    # Paimon sink không hỗ trợ Adaptive Parallelism của batch scheduler
    t_env.get_config().set("execution.batch.adaptive.auto-parallelism.enabled", "false")
    _register_paimon(t_env)

    build_facts(t_env)
    build_bridge(t_env)
    print("[SUCCESS] Incident facts build complete.")


if __name__ == "__main__":
    main()
