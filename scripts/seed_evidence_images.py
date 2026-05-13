"""
Generate proper placeholder JPEG evidence frames and upload to MinIO.
Replaces 218-byte stub files with renderable placeholder images.

Usage:
    python scripts/seed_evidence_images.py
"""

import io
import sys
import requests
import xml.etree.ElementTree as ET
from PIL import Image, ImageDraw, ImageFont

# ── Config ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS   = "minio"
MINIO_SECRET   = "mypassword"
BUCKET         = "evidence-frames"
TRINO_HOST     = "localhost"
TRINO_PORT     = 8082  # External port (container maps 8082->8080)

# Camera color palette for visual variety
CAM_COLORS = {
    "cam_01": "#1a3a5c",  # dark navy
    "cam_02": "#2d1b4e",  # dark purple
    "cam_03": "#1a4a2e",  # dark green
    "cam_04": "#4a1a1a",  # dark red
    "cam_05": "#1a3a4a",  # dark teal
    "cam_06": "#3a2a10",  # dark amber
    "cam_07": "#10243a",  # deep blue
    "cam_08": "#2a3a10",  # olive green
}
DEFAULT_COLOR = "#1e2a3a"

EVENT_BADGE_COLORS = {
    "Violence":   "#e53e3e",
    "Anomaly":    "#dd6b20",
    "Normal":     "#38a169",
    "Suspicious": "#d69e2e",
    "FIGHTING":   "#e53e3e",
    "ASSAULT":    "#c53030",
    "ROBBERY":    "#b7791f",
    "VANDALISM":  "#dd6b20",
    "STABBING":   "#9b2c2c",
    "UNKNOWN":    "#718096",
}


def hex_to_rgb(h: str) -> tuple:
    h = h.lstrip("#")
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))


def make_evidence_image(
    camera_id: str,
    incident_id: str,
    incident_date: str,
    event_type: str = "Anomaly",
    location: str = "",
    width: int = 640,
    height: int = 360,
) -> bytes:
    """Create a styled placeholder JPEG evidence frame (640×360)."""

    bg_color = hex_to_rgb(CAM_COLORS.get(camera_id, DEFAULT_COLOR))
    img = Image.new("RGB", (width, height), bg_color)
    draw = ImageDraw.Draw(img)

    # ── Scanline effect (subtle horizontal lines for CCTV look) ──────────────
    for y in range(0, height, 4):
        draw.line([(0, y), (width, y)], fill=tuple(max(0, c - 15) for c in bg_color), width=1)

    # ── Corner brackets ───────────────────────────────────────────────────────
    bracket_color = (100, 160, 200)
    blen = 25
    bw   = 2
    for cx, cy in [(10, 10), (width-10, 10), (10, height-10), (width-10, height-10)]:
        dx = 1 if cx < width // 2 else -1
        dy = 1 if cy < height // 2 else -1
        draw.line([(cx, cy), (cx + dx * blen, cy)], fill=bracket_color, width=bw)
        draw.line([(cx, cy), (cx, cy + dy * blen)], fill=bracket_color, width=bw)

    # ── Camera icon area (center) ─────────────────────────────────────────────
    cx, cy = width // 2, height // 2 - 20
    # Camera body
    draw.rounded_rectangle([cx-40, cy-20, cx+40, cy+20], radius=5, fill=(60, 80, 100), outline=(100, 140, 180), width=2)
    # Lens
    draw.ellipse([cx-18, cy-15, cx+18, cy+15], fill=(30, 40, 55), outline=(80, 120, 160), width=2)
    draw.ellipse([cx-10, cy-8, cx+10, cy+8], fill=(20, 30, 45), outline=(60, 100, 140), width=1)
    # Camera mount
    draw.rectangle([cx-5, cy+20, cx+5, cy+32], fill=(50, 70, 90))
    draw.rectangle([cx-20, cy+30, cx+20, cy+36], fill=(50, 70, 90))

    # ── Text labels ───────────────────────────────────────────────────────────
    try:
        # Try loading a basic monospace font
        font_large = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf", 18)
        font_med   = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", 13)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", 11)
    except Exception:
        try:
            font_large = ImageFont.truetype("arial.ttf", 18)
            font_med   = ImageFont.truetype("arial.ttf", 13)
            font_small = ImageFont.truetype("arial.ttf", 11)
        except Exception:
            font_large = font_med = font_small = ImageFont.load_default()

    # Top-left: Camera ID + REC indicator
    draw.text((18, 16), f"● REC  {camera_id.upper()}", font=font_large, fill=(220, 50, 50))

    # Top-right: Date/time
    draw.text((width - 160, 16), incident_date, font=font_med, fill=(180, 200, 220))

    # Bottom-left: Incident ID
    draw.text((18, height - 36), f"ID: {incident_id}", font=font_small, fill=(150, 170, 190))

    # Bottom-right: Location
    if location:
        loc_text = location[:28]
        loc_w = draw.textlength(loc_text, font=font_small)
        draw.text((width - loc_w - 18, height - 36), loc_text, font=font_small, fill=(150, 170, 190))

    # Center bottom: Event type badge
    badge_text = f"  {event_type.upper()}  "
    badge_color = hex_to_rgb(EVENT_BADGE_COLORS.get(event_type, "#718096"))
    badge_w = int(draw.textlength(badge_text, font=font_med)) + 4
    badge_x = (width - badge_w) // 2
    draw.rounded_rectangle([badge_x, cy + 50, badge_x + badge_w, cy + 72], radius=4, fill=badge_color)
    draw.text((badge_x + 2, cy + 53), badge_text, font=font_med, fill=(255, 255, 255))

    # ── Overlay: "EVIDENCE FRAME" watermark ───────────────────────────────────
    wm_text = "EVIDENCE FRAME — VIGILANCE AI"
    wm_w = int(draw.textlength(wm_text, font=font_small))
    draw.text(((width - wm_w) // 2, height // 2 + 85), wm_text, font=font_small, fill=(80, 100, 120))

    # Save as JPEG
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


# ── MinIO upload via presigned PUT ───────────────────────────────────────────

def minio_put(key: str, data: bytes) -> bool:
    """Upload bytes to MinIO via S3-compatible PUT (using requests + AWS4 signing)."""
    try:
        from minio import Minio
        client = Minio(
            "localhost:9000",
            access_key=MINIO_ACCESS,
            secret_key=MINIO_SECRET,
            secure=False,
        )
        client.put_object(
            BUCKET,
            key,
            io.BytesIO(data),
            length=len(data),
            content_type="image/jpeg",
        )
        return True
    except Exception as e:
        print(f"  [FAIL] Upload failed: {e}")
        return False


# ── Trino query ───────────────────────────────────────────────────────────────

def trino_query(sql: str) -> list:
    """Run SQL against Trino and return rows."""
    headers = {
        "X-Trino-User": "admin",
        "X-Trino-Catalog": "iceberg",
        "X-Trino-Schema": "security",
        "Content-Type": "text/plain",
    }
    base = f"http://{TRINO_HOST}:{TRINO_PORT}"
    r = requests.post(f"{base}/v1/statement", data=sql.encode(), headers=headers, timeout=30)
    r.raise_for_status()
    body = r.json()
    rows = list(body.get("data") or [])
    next_uri = body.get("nextUri")
    while next_uri:
        r = requests.get(next_uri, headers=headers, timeout=30)
        r.raise_for_status()
        body = r.json()
        rows.extend(body.get("data") or [])
        next_uri = body.get("nextUri")
        if body.get("stats", {}).get("state") in ("FINISHED", "FAILED", "CANCELED"):
            if not next_uri:
                break
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=== Seeding evidence-frames with proper JPEG images ===\n")

    # 1. Fetch incidents from Iceberg
    print("Fetching incidents from Iceberg via Trino...")
    sql = """
    SELECT incident_id, camera_id,
           CAST(incident_date AS VARCHAR) AS incident_date,
           COALESCE(event_type, 'Anomaly') AS event_type,
           COALESCE(location, '') AS location
    FROM iceberg.security.historical_violence_incidents
    ORDER BY timestamp DESC
    """
    try:
        rows = trino_query(sql)
        print(f"  Found {len(rows)} incidents\n")
    except Exception as e:
        print(f"  [FAIL] Trino query failed: {e}")
        print("  Falling back to synthetic incidents...")
        rows = [
            ("inc_001", "cam_01", "2026-04-01", "Violence",   "Khu vực A"),
            ("inc_002", "cam_02", "2026-04-02", "Anomaly",    "Khu vực B"),
            ("inc_003", "cam_01", "2026-04-05", "Suspicious", "Khu vực A"),
            ("inc_004", "cam_03", "2026-04-10", "Violence",   "Khu vực C"),
            ("inc_005", "cam_04", "2026-04-12", "Anomaly",    "Khu vực D"),
            ("inc_006", "cam_02", "2026-04-15", "Normal",     "Khu vực B"),
            ("inc_007", "cam_05", "2026-04-20", "Violence",   "Khu vực E"),
            ("inc_008", "cam_01", "2026-04-22", "Anomaly",    "Khu vực A"),
            ("inc_009", "cam_03", "2026-04-25", "Suspicious", "Khu vực C"),
            ("inc_010", "cam_06", "2026-04-26", "Violence",   "Khu vực F"),
        ]

    # 2. Generate and upload each image
    ok_count = 0
    skip_count = 0
    fail_count = 0

    for row in rows:
        incident_id, camera_id, incident_date, event_type, location = (
            str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4])
        )
        key = f"{camera_id}/{incident_date}/{incident_id}.jpg"

        print(f"  [{camera_id}] {incident_id} ({incident_date}) - {event_type}")
        sys.stdout.flush()

        img_bytes = make_evidence_image(
            camera_id=camera_id,
            incident_id=incident_id,
            incident_date=incident_date,
            event_type=event_type,
            location=location,
        )
        print(f"    Generated: {len(img_bytes):,} bytes", end="")

        if minio_put(key, img_bytes):
            print(f"  -> uploaded to {BUCKET}/{key}")
            ok_count += 1
        else:
            fail_count += 1

    print(f"\n=== Done: {ok_count} uploaded, {skip_count} skipped, {fail_count} failed ===")
    print(f"\nTest URL: {MINIO_ENDPOINT}/{BUCKET}/cam_01/2026-04-01/inc_001.jpg")


if __name__ == "__main__":
    main()
