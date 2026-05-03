#!/usr/bin/env python3
"""
Script kiểm tra Frame Evidence Storage
Chạy từ máy host: python scripts/check_frames.py

Yêu cầu: pip install boto3
"""
import boto3
import os
import webbrowser
from datetime import date

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "mypassword"
S3_BUCKET = "evidence-frames"
TODAY = date.today().strftime("%Y-%m-%d")
DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "Desktop", "evidence_frames")

def get_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )

def list_frames(camera_id=None, date_str=None, min_size_bytes=500):
    """Liệt kê frames theo camera/ngày, lọc real frames (>500B)."""
    s3 = get_client()
    prefix = ""
    if camera_id:
        prefix = f"{camera_id}/"
        if date_str:
            prefix += f"{date_str}/"

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    frames = []
    for obj in response.get("Contents", []):
        if obj["Size"] >= min_size_bytes:
            frames.append({
                "key": obj["Key"],
                "size_kb": round(obj["Size"] / 1024, 1),
                "last_modified": obj["LastModified"].strftime("%H:%M:%S"),
            })
    return frames

def download_frame(s3_key, output_dir=DOWNLOAD_DIR):
    """Download một frame về Desktop."""
    s3 = get_client()
    os.makedirs(output_dir, exist_ok=True)
    filename = s3_key.replace("/", "_")
    output_path = os.path.join(output_dir, filename)
    s3.download_file(S3_BUCKET, s3_key, output_path)
    return output_path

def summary():
    """Tổng kết số frames theo camera."""
    s3 = get_client()
    response = s3.list_objects_v2(Bucket=S3_BUCKET)
    stats = {}
    total_real = 0
    total_size = 0
    for obj in response.get("Contents", []):
        cam = obj["Key"].split("/")[0]
        stats[cam] = stats.get(cam, {"total": 0, "real": 0, "size_kb": 0})
        stats[cam]["total"] += 1
        if obj["Size"] > 500:
            stats[cam]["real"] += 1
            total_real += 1
        stats[cam]["size_kb"] += obj["Size"] / 1024
        total_size += obj["Size"] / 1024

    print(f"\n{'='*55}")
    print(f"  FRAME EVIDENCE STORAGE — SUMMARY")
    print(f"{'='*55}")
    print(f"  Bucket: s3://{S3_BUCKET}   Date: {TODAY}")
    print(f"  Total real frames (>500B): {total_real}")
    print(f"  Total size: {total_size:.1f} KB")
    print(f"{'='*55}")
    print(f"  {'Camera':<10} {'Total':>6} {'Real':>6} {'Size(KB)':>10}")
    print(f"  {'-'*35}")
    for cam in sorted(stats.keys()):
        s = stats[cam]
        print(f"  {cam:<10} {s['total']:>6} {s['real']:>6} {s['size_kb']:>10.1f}")
    print(f"{'='*55}\n")

def main():
    print("\n[1] Tổng kết frames trong S3...")
    summary()

    print("[2] Liệt kê real frames từ cam_01 hôm nay...")
    frames = list_frames(camera_id="cam_01", date_str=TODAY)
    if frames:
        for f in frames[:5]:
            print(f"  {f['last_modified']} | {f['size_kb']:>5} KB | {f['key']}")
        print(f"  ... và {len(frames)} frames thêm\n")
    else:
        # Try any camera
        frames = list_frames()
        print(f"  (Không tìm thấy cam_01, có {len(frames)} real frames tổng cộng)\n")

    if not frames:
        print("  Chưa có frames. Chờ inference-mock detect violence...")
        return

    # Download 3 frames về Desktop
    print(f"[3] Downloading 3 real frames về {DOWNLOAD_DIR} ...")
    for f in frames[:3]:
        path = download_frame(f["key"])
        print(f"  ✓ {f['key'].split('/')[-1][:30]}... ({f['size_kb']} KB) → {path}")

    print(f"\n  Mở thư mục:")
    print(f"  {DOWNLOAD_DIR}")
    if os.name == 'nt':
        os.startfile(DOWNLOAD_DIR)

    print("\n  Hoặc xem trực tiếp trên MinIO Console:")
    print("  http://localhost:9001  (minio / mypassword)")
    print(f"  → Bucket: evidence-frames\n")

if __name__ == "__main__":
    try:
        import boto3
        main()
    except ImportError:
        print("Cài boto3 trước: pip install boto3")
