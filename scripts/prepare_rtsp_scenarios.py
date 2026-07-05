#!/usr/bin/env python3
"""Prepare 5-stream SCVD RTSP scenario metadata for the project.

Input is the generated SCVD scenario folder:
  /home/dataguy/Documents/SCVD/rtsp_scenarios

Outputs are project-compatible files under data/metadata:
  camera_registry.csv
  camera_playlists.json
  camera_scenarios.json
  rtsp_annotations/*.csv
  rtsp_schedules/*.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path, PurePosixPath


CITY = "TP. Ho Chi Minh"
DISTRICT = "Quan 1"
GEO = [
    ("Duong Nguyen Hue", "Phuong Ben Nghe", 10.77845, 106.70014),
    ("Duong Le Loi", "Phuong Nguyen Thai Binh", 10.77322, 106.69453),
    ("Duong Nguyen Thai Hoc", "Phuong Ben Thanh", 10.77407, 106.70229),
    ("Duong Le Thanh Ton", "Phuong Cau Ong Lanh", 10.77613, 106.69705),
    ("Duong Pasteur", "Phuong Pham Ngu Lao", 10.77157, 106.70435),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build project metadata for deterministic SCVD RTSP scenarios."
    )
    parser.add_argument(
        "--scenario-root",
        type=Path,
        required=True,
        help="SCVD rtsp_scenarios directory.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/metadata"),
        help="Project metadata output directory.",
    )
    parser.add_argument(
        "--host-scvd-root",
        type=Path,
        required=True,
        help="Host SCVD workspace root, e.g. /home/dataguy/Documents/SCVD.",
    )
    parser.add_argument(
        "--container-scvd-root",
        default="/app/data/raw/SCVD",
        help="Runtime SCVD root corresponding to --host-scvd-root.",
    )
    parser.add_argument(
        "--metadata-runtime-root",
        default="/app/data/metadata",
        help="Runtime metadata root written into registry/scenario files.",
    )
    parser.add_argument(
        "--rtsp-base",
        default="rtsp://mediamtx:8554",
        help="RTSP base URL written into camera_registry.csv.",
    )
    return parser.parse_args()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def to_runtime_path(host_path: str, host_root: Path, runtime_root: str) -> str:
    path = Path(host_path).resolve()
    rel = path.relative_to(host_root.resolve())
    return str(PurePosixPath(runtime_root.rstrip("/")) / PurePosixPath(rel.as_posix()))


def runtime_metadata_path(runtime_root: str, *parts: str) -> str:
    return str(PurePosixPath(runtime_root.rstrip("/")).joinpath(*parts))


def camera_id_for_stream(stream_id: str) -> str:
    return f"cam_{int(stream_id.split('-')[-1]):02d}"


def scenario_folders(root: Path) -> list[Path]:
    return sorted([p for p in root.glob("rtsp-*") if p.is_dir()])


def load_summary(scenario_root: Path) -> dict[str, dict[str, str]]:
    path = scenario_root / "scenario_summary.csv"
    if not path.exists():
        return {}
    rows = read_csv(path)
    return {row["stream_id"]: row for row in rows}


def add_camera_fields(rows: list[dict[str, str]], camera_id: str, rtsp_url: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for row in rows:
        new_row = {"camera_id": camera_id, "rtsp_url": rtsp_url}
        new_row.update(row)
        out.append(new_row)
    return out


def main() -> None:
    args = parse_args()
    scenario_root = args.scenario_root.resolve()
    out_dir = args.out_dir.resolve()
    annotations_dir = out_dir / "rtsp_annotations"
    schedules_dir = out_dir / "rtsp_schedules"

    folders = scenario_folders(scenario_root)
    if not folders:
        raise SystemExit(f"No rtsp-* scenario folders found under {scenario_root}")

    summary_by_stream = load_summary(scenario_root)
    playlists: dict[str, list[str]] = {}
    host_playlists: dict[str, list[str]] = {}
    scenarios: dict[str, dict] = {}
    registry_rows: list[dict[str, str]] = []
    stream_links: list[dict[str, str]] = []
    ordered_links: list[dict[str, str]] = []

    for idx, folder in enumerate(folders):
        schedule_path = folder / "schedule.csv"
        annotation_path = folder / "annotations.csv"
        if not schedule_path.exists() or not annotation_path.exists():
            raise SystemExit(f"Scenario is missing schedule/annotations: {folder}")

        schedule_rows = read_csv(schedule_path)
        annotation_rows = read_csv(annotation_path)
        if not schedule_rows:
            raise SystemExit(f"Empty schedule: {schedule_path}")

        stream_id = schedule_rows[0]["stream_id"]
        stream_name = schedule_rows[0].get("stream_name") or folder.name
        camera_id = camera_id_for_stream(stream_id)
        rtsp_url = f"{args.rtsp_base.rstrip('/')}/{camera_id}"
        geo = GEO[idx % len(GEO)]
        summary = summary_by_stream.get(stream_id, {})

        host_playlist = []
        for row in schedule_rows:
            rel = row.get("original_relative_path")
            if rel:
                host_playlist.append(str((args.host_scvd_root / rel).resolve()))
            else:
                host_playlist.append(row["original_filepath"])
        runtime_playlist = [
            to_runtime_path(path, args.host_scvd_root, args.container_scvd_root)
            for path in host_playlist
        ]
        host_playlists[camera_id] = host_playlist
        playlists[camera_id] = runtime_playlist

        schedule_out = schedules_dir / f"{camera_id}_schedule.csv"
        annotation_out = annotations_dir / f"{camera_id}_annotations.csv"
        runtime_schedule = runtime_metadata_path(
            args.metadata_runtime_root, "rtsp_schedules", schedule_out.name
        )
        runtime_annotation = runtime_metadata_path(
            args.metadata_runtime_root, "rtsp_annotations", annotation_out.name
        )

        schedule_with_camera = add_camera_fields(schedule_rows, camera_id, rtsp_url)
        annotation_with_camera = add_camera_fields(annotation_rows, camera_id, rtsp_url)
        write_csv(schedule_out, schedule_with_camera, list(schedule_with_camera[0].keys()))
        write_csv(annotation_out, annotation_with_camera, list(annotation_with_camera[0].keys()))

        for row, runtime_path in zip(schedule_rows, runtime_playlist, strict=True):
            ordered_links.append(
                {
                    "camera_id": camera_id,
                    "stream_id": stream_id,
                    "stream_name": stream_name,
                    "rtsp_url_local": f"rtsp://127.0.0.1:8554/{camera_id}",
                    "rtsp_url_runtime": rtsp_url,
                    "segment_index": row.get("segment_index", ""),
                    "segment_type": row.get("segment_type", ""),
                    "event_id": row.get("event_id", ""),
                    "start_seconds": row.get("start_seconds", ""),
                    "end_seconds": row.get("end_seconds", ""),
                    "effective_end_seconds": row.get("effective_end_seconds", ""),
                    "clip_duration_seconds": row.get("clip_duration_seconds", ""),
                    "label": row.get("label", ""),
                    "class": row.get("class", ""),
                    "group": row.get("group", ""),
                    "context": row.get("context", ""),
                    "filename": row.get("filename", ""),
                    "host_path": row.get("original_filepath", ""),
                    "runtime_path": runtime_path,
                    "original_relative_path": row.get("original_relative_path", ""),
                    "split": row.get("split", ""),
                    "scvd_label": row.get("scvd_label", ""),
                    "sample_id": row.get("sample_id", ""),
                }
            )

        background_labels = sorted(
            {row["label"] for row in schedule_rows if row.get("segment_type") == "background"}
        )
        event_labels = sorted({row["label"] for row in annotation_rows})
        concat_duration = max(float(row["end_seconds"]) for row in schedule_rows)
        event_seconds = sum(float(row["duration_seconds"]) for row in annotation_rows)

        scenarios[camera_id] = {
            "camera_id": camera_id,
            "rtsp_url": rtsp_url,
            "stream_id": stream_id,
            "stream_name": stream_name,
            "difficulty": summary.get("difficulty", ""),
            "frequency": summary.get("frequency", ""),
            "purpose": summary.get("purpose", ""),
            "background_labels": background_labels,
            "event_labels": event_labels,
            "event_count": len(set(row["event_id"] for row in annotation_rows)),
            "render_duration_seconds": float(summary.get("render_duration_seconds") or 900.0),
            "concat_duration_seconds": round(concat_duration, 3),
            "event_seconds": round(event_seconds, 3),
            "schedule_file": runtime_schedule,
            "annotation_file": runtime_annotation,
            "source_schedule_file": str(schedule_path),
            "source_annotation_file": str(annotation_path),
            "playlist_file": runtime_metadata_path(args.metadata_runtime_root, "camera_playlists.json"),
        }

        registry_rows.append(
            {
                "camera_id": camera_id,
                "city": CITY,
                "district": DISTRICT,
                "ward": geo[1],
                "street": geo[0],
                "latitude": geo[2],
                "longitude": geo[3],
                "rtsp_url": rtsp_url,
                "has_violence": "true" if annotation_rows else "false",
                "scenario_id": stream_id,
                "scenario_name": stream_name,
                "difficulty": summary.get("difficulty", ""),
                "frequency": summary.get("frequency", ""),
                "purpose": summary.get("purpose", ""),
                "n_events": str(len(set(row["event_id"] for row in annotation_rows))),
                "duration_seconds": summary.get("render_duration_seconds", "900"),
                "n_playlist_clips": str(len(runtime_playlist)),
                "annotation_file": runtime_annotation,
                "schedule_file": runtime_schedule,
            }
        )
        stream_links.append(
            {
                "camera_id": camera_id,
                "stream_id": stream_id,
                "stream_name": stream_name,
                "rtsp_url_local": f"rtsp://127.0.0.1:8554/{camera_id}",
                "rtsp_url_runtime": rtsp_url,
                "playlist_clips": str(len(runtime_playlist)),
                "events": str(len(set(row["event_id"] for row in annotation_rows))),
                "annotation_file": runtime_annotation,
                "schedule_file": runtime_schedule,
            }
        )

    registry_fields = [
        "camera_id",
        "city",
        "district",
        "ward",
        "street",
        "latitude",
        "longitude",
        "rtsp_url",
        "has_violence",
        "scenario_id",
        "scenario_name",
        "difficulty",
        "frequency",
        "purpose",
        "n_events",
        "duration_seconds",
        "n_playlist_clips",
        "annotation_file",
        "schedule_file",
    ]
    stream_link_fields = [
        "camera_id",
        "stream_id",
        "stream_name",
        "rtsp_url_local",
        "rtsp_url_runtime",
        "playlist_clips",
        "events",
        "annotation_file",
        "schedule_file",
    ]
    ordered_link_fields = [
        "camera_id",
        "stream_id",
        "stream_name",
        "rtsp_url_local",
        "rtsp_url_runtime",
        "segment_index",
        "segment_type",
        "event_id",
        "start_seconds",
        "end_seconds",
        "effective_end_seconds",
        "clip_duration_seconds",
        "label",
        "class",
        "group",
        "context",
        "filename",
        "host_path",
        "runtime_path",
        "original_relative_path",
        "split",
        "scvd_label",
        "sample_id",
    ]

    write_csv(out_dir / "camera_registry.csv", registry_rows, registry_fields)
    write_csv(out_dir / "stream_links.csv", stream_links, stream_link_fields)
    write_csv(out_dir / "ordered_video_links.csv", ordered_links, ordered_link_fields)
    write_json(out_dir / "camera_playlists.json", playlists)
    write_json(out_dir / "camera_playlists.host.json", host_playlists)
    write_json(out_dir / "camera_scenarios.json", scenarios)

    readme = (
        "# SCVD RTSP Scenario Metadata\n\n"
        "Generated by `scripts/prepare_rtsp_scenarios.py`.\n\n"
        f"- scenario_root: `{scenario_root}`\n"
        f"- host_scvd_root: `{args.host_scvd_root.resolve()}`\n"
        f"- runtime_scvd_root: `{args.container_scvd_root}`\n"
        f"- metadata_runtime_root: `{args.metadata_runtime_root}`\n"
        "- streams: `cam_01` through `cam_05`\n"
    )
    (out_dir / "README.md").write_text(readme, encoding="utf-8")

    print(f"Metadata folder: {out_dir}")
    print(f"Cameras: {len(registry_rows)}")
    print(f"Runtime playlist JSON: {out_dir / 'camera_playlists.json'}")
    print(f"Registry CSV: {out_dir / 'camera_registry.csv'}")
    print(f"Scenario JSON: {out_dir / 'camera_scenarios.json'}")


if __name__ == "__main__":
    main()
