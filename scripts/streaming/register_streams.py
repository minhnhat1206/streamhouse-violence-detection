#!/usr/bin/env python3
"""
register_streams.py — Register RTSP cameras (with location) on the VioMoViNet AI server.

Design C (Hybrid): each simulated camera carries its geo location from the
camera registry, so VioMoViNet emits COMPLETE events (location populated at the
producer layer). Flink still enriches via dim_camera (COALESCE) downstream — so
location ends up populated at BOTH layers.

Reads data/metadata/camera_registry.csv and POSTs each active camera to the
VioMoViNet /api/stream/start endpoint with {camera_id, rtsp_url, location}.

Run from THIS machine (it has Tailscale). The POSTs go to the VioMoViNet GPU
server over Tailscale; the RTSP URLs point back at this machine's mediamtx,
which the GPU server pulls from over Tailscale.

Env (all optional — sensible defaults):
  METADATA_FILE       registry CSV (default <repo>/data/metadata/camera_registry.csv)
  ACTIVE_CAMERAS      comma-separated cam_01,cam_03,... (default: ALL rows)
  VIOMOVINET_API_URL  VioMoViNet API base (default http://100.94.25.122:8000)
  RTSP_HOST           host:port of this machine's mediamtx (default 100.94.25.122:8554)
                      — overrides the CSV's rtsp://mediamtx:8554 (compose hostname,
                        unreachable from the GPU server)
  STOP_EXISTING       "true"/"false" — POST /api/stream/stop first (default true, idempotent)

Usage:
  VIOMOVINET_API_URL=http://<gpu-tailscale-ip>:8000 \
  RTSP_HOST=100.94.25.122:8554 \
  ACTIVE_CAMERAS=cam_01,cam_02,cam_03,cam_04,cam_05 \
  python scripts/streaming/register_streams.py
"""
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[2]

DEFAULTS = {
    "METADATA_FILE": str(REPO_ROOT / "data" / "metadata" / "camera_registry.csv"),
    "ACTIVE_CAMERAS": "",  # empty → all rows
    "VIOMOVINET_API_URL": "http://100.94.25.122:8000",
    "RTSP_HOST": "100.94.25.122:8554",
    "STOP_EXISTING": "true",
}


def _env(key: str) -> str:
    return os.getenv(key, DEFAULTS[key]).strip()


def load_registry(csv_path: Path) -> list[dict]:
    """Read camera_registry.csv → list of rows with float lat/long."""
    rows: list[dict] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                row["lat"] = float(row["latitude"])
                row["long"] = float(row["longitude"])
            except (KeyError, ValueError, TypeError):
                row["lat"] = None
                row["long"] = None
            rows.append(row)
    return rows


def main() -> int:
    csv_path = Path(_env("METADATA_FILE"))
    api_url = _env("VIOMOVINET_API_URL").rstrip("/")
    rtsp_host = _env("RTSP_HOST")
    stop_existing = _env("STOP_EXISTING").lower() == "true"
    active = {c.strip() for c in _env("ACTIVE_CAMERAS").split(",") if c.strip()}

    if not csv_path.exists():
        print(f"[ERROR] Registry not found: {csv_path}", file=sys.stderr)
        return 1

    rows = load_registry(csv_path)
    if active:
        rows = [r for r in rows if r["camera_id"] in active]
        missing = active - {r["camera_id"] for r in rows}
        if missing:
            print(f"[WARN] Not in registry (skipped): {sorted(missing)}", file=sys.stderr)

    if not rows:
        print("[ERROR] No cameras to register.", file=sys.stderr)
        return 1

    print(f"[INFO] API={api_url}  RTSP_HOST={rtsp_host}  cameras={len(rows)}  stop_existing={stop_existing}")

    started: list[str] = []
    failed: list[tuple[str, str]] = []
    for row in rows:
        cam_id = row["camera_id"]
        rtsp_url = f"rtsp://{rtsp_host}/{cam_id}"
        location = {
            "city": row.get("city", ""),
            "district": row.get("district", ""),
            "ward": row.get("ward", ""),
            "street": row.get("street", ""),
            "lat": row.get("lat"),
            "long": row.get("long"),
        }

        # Idempotent: stop an existing stream first (ignore any error/404).
        if stop_existing:
            try:
                requests.post(
                    f"{api_url}/api/stream/stop",
                    json={"camera_id": cam_id},
                    timeout=10,
                )
            except requests.RequestException:
                pass  # server may be mid-startup; the start call below surfaces real errors.

        try:
            resp = requests.post(
                f"{api_url}/api/stream/start",
                json={"camera_id": cam_id, "rtsp_url": rtsp_url, "location": location},
                timeout=15,
            )
        except requests.RequestException as e:
            failed.append((cam_id, str(e)))
            print(f"  X {cam_id}: {e}", file=sys.stderr)
            continue

        if resp.status_code == 200:
            gpu = resp.json().get("gpu_id", "?")
            started.append(cam_id)
            print(f"  + {cam_id} -> {rtsp_url}  (gpu {gpu})  loc={location['street']}, {location['ward']}")
        else:
            failed.append((cam_id, f"HTTP {resp.status_code}"))
            print(f"  X {cam_id}: HTTP {resp.status_code} - {resp.text[:120]}", file=sys.stderr)

    print(f"\n[DONE] started={len(started)} failed={len(failed)}")
    if failed:
        print("Failed: " + ", ".join(c for c, _ in failed), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
