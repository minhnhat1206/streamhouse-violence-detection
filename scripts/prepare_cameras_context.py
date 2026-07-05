#!/usr/bin/env python3
"""
prepare_cameras_context.py — Build context-continuous camera playlists from SCVD.

The old prepare_cameras_dataset.py sampled clips at RANDOM per camera, so one
RTSP stream jumped between unrelated scenes (mall -> alley -> airport). This
script instead groups SCVD clips by visual similarity into N "scene" clusters
(one cluster == one simulated camera location), orders each cluster for
temporally-smooth playback, and injects a realistic ~10% violence event rhythm.

Outputs (under --out-dir, default ./data/metadata):
  - camera_registry.csv     clean schema; geo FIXED to mirror dim_camera seed
  - camera_playlists.json   {cam_id: [ordered in-container clip paths]}
  - clip_features.npz       feature cache (N x 130 float32)
  - clip_features_index.json  relpath -> row mapping (cache sidecar)
  - clip_features_skipped.log  corrupt/unreadable clips, if any

Run on HOST (needs numpy / scikit-learn / opencv-python / ffmpeg — all
preinstalled on this machine). The pusher container has none of these, so
clustering must happen here, once, offline.

Geo tuples mirror scripts/transform/setup_star_schema.py:_seed_dim_camera so the
CSV and the Fluss dim_camera table stay identical (the old prep script
randomized lat/lon, drifting away from dim_camera).
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import cv2
import numpy as np
from sklearn.cluster import KMeans

# ─── Fixed geo (mirror setup_star_schema.py:121-135) ──────────────────────────
# Order is cam_01..cam_15. (street, ward, lat, lon). District/City constant.
CITY = "TP. Hồ Chí Minh"
DISTRICT = "Quận 1"
GEO = [
    ("Đường Nguyễn Huệ",         "Phường Bến Nghé",         10.77845, 106.70014),
    ("Đường Lê Lợi",             "Phường Nguyễn Thái Bình", 10.77322, 106.69453),
    ("Đường Nguyễn Thái Học",    "Phường Bến Thành",        10.77407, 106.70229),
    ("Đường Lê Thánh Tôn",       "Phường Cầu Ông Lãnh",     10.77613, 106.69705),
    ("Đường Pasteur",            "Phường Phạm Ngũ Lão",     10.77157, 106.70435),
    ("Đường Trần Hưng Đạo",      "Phường Tân Định",         10.77336, 106.70019),
    ("Đường Đồng Khởi",          "Phường Đa Kao",           10.77833, 106.69332),
    ("Đường Hai Bà Trưng",       "Phường Bến Thành",        10.78446, 106.70214),
    ("Đường Nguyễn Du",          "Phường Nguyễn Cư Trinh",  10.77002, 106.70027),
    ("Đường Võ Văn Kiệt",        "Phường Cầu Kho",          10.78266, 106.70826),
    ("Đường Nguyễn Công Trứ",    "Phường Tân Định",         10.77552, 106.70748),
    ("Đường Công Trường Mê Linh","Phường Nguyễn Thái Bình", 10.77956, 106.70549),
    ("Đường Hàm Nghi",           "Phường Phạm Ngũ Lão",     10.78320, 106.69630),
    ("Đường Nguyễn Bỉnh Khiêm",  "Phường Bến Nghé",         10.78074, 106.70235),
    ("Đường Trương Định",        "Phường Đa Kao",           10.77709, 106.69288),
]

# Class-folder aliases (same logic as prepare_cameras_dataset.py).
_VIOLENCE_NAMES = {"classa", "violence", "fight", "violent", "weaponized"}
_NON_VIOLENCE_NAMES = {"classb", "nonviolence", "nonviolent", "normal", "safe", "nonfight"}
VIDEO_EXTS = (".avi", ".mp4")

RTSP_BASE = "rtsp://mediamtx:8554"


@dataclass
class Clip:
    path: Path          # absolute host path
    rel: str            # posix path relative to scvd_root (e.g. Train/Normal/n001_converted.avi)
    label: str          # "normal" | "violence"  (Weaponized -> violence)


# ─── Discovery & classification ──────────────────────────────────────────────

def _norm(name: str) -> str:
    return "".join(c for c in name.lower() if c.isalnum())


def classify_label(path: Path) -> str:
    """Label by any ancestor (directory) folder name; filename ignored."""
    for part in path.parts[:-1]:
        n = _norm(part)
        if n in _VIOLENCE_NAMES or "weapon" in n or "violent" in n:
            return "violence"
        if n in _NON_VIOLENCE_NAMES or "normal" in n:
            return "normal"
    return "normal"  # default when no class folder matched


def discover_clips(root: Path) -> list[Clip]:
    if not root.is_dir():
        sys.exit(f"[ERROR] SCVD root not found: {root}")
    clips: list[Clip] = []
    for ext in VIDEO_EXTS:
        for p in sorted(root.rglob(f"*{ext}")):
            if p.name.startswith("."):
                continue
            clips.append(Clip(path=p, rel=p.relative_to(root).as_posix(),
                              label=classify_label(p)))
    return clips


# ─── Feature extraction ──────────────────────────────────────────────────────

def extract_feature(path: Path) -> np.ndarray | None:
    """130-d feature: 128-d HSV hue/sat histogram (L1) + 2-d grayscale mean/std,
    then L2-normalized. Returns None if the clip can't be read."""
    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        return None
    n = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if n > 1:
        cap.set(cv2.CAP_PROP_POS_FRAMES, n // 2)
    ok, frame = cap.read()
    cap.release()
    if not ok or frame is None:
        return None
    frame = cv2.resize(frame, (160, 90))
    hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
    hist = cv2.calcHist([hsv], [0, 1], None, [16, 8], [0, 180, 0, 256])
    hist = cv2.normalize(hist, None, norm_type=cv2.NORM_L1).flatten()
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    luma = np.array([gray.mean(), gray.std()], dtype=np.float32)
    feat = np.concatenate([hist.astype(np.float32), luma])
    norm = np.linalg.norm(feat)
    if norm > 0:
        feat /= norm
    return feat.astype(np.float32)


def extract_all_features(clips: list[Clip], cache_npz: Path, cache_index: Path,
                         force: bool, skipped_log: Path) -> tuple[np.ndarray, list[Clip]]:
    """Return (features[N,130], valid_clips[N]). Incremental cache keyed by rel path."""
    cached_feats = None
    cached_index: dict[str, int] = {}
    if not force and cache_npz.exists() and cache_index.exists():
        cached_feats = np.load(cache_npz)["feats"]
        cached_index = {r: i for i, r in enumerate(json.loads(cache_index.read_text()))}
        print(f"[INFO] Loaded feature cache: {len(cached_index)} clips")

    feats_rows: list[np.ndarray] = []
    valid: list[Clip] = []
    skipped: list[str] = []

    for i, clip in enumerate(clips, 1):
        if cached_feats is not None and clip.rel in cached_index:
            feats_rows.append(cached_feats[cached_index[clip.rel]])
            valid.append(clip)
            continue
        feat = extract_feature(clip.path)
        if feat is None:
            skipped.append(clip.rel)
            continue
        feats_rows.append(feat)
        valid.append(clip)
        if i % 50 == 0:
            print(f"  ...extracted {i}/{len(clips)}", flush=True)

    if skipped:
        skipped_log.write_text("\n".join(skipped) + "\n")
        print(f"[WARN] Skipped {len(skipped)} unreadable clips -> {skipped_log}")

    feats = np.vstack(feats_rows).astype(np.float32)

    # Persist cache (full rebuild for simplicity/robustness).
    np.savez(cache_npz, feats=feats)
    cache_index.write_text(json.dumps([c.rel for c in valid], ensure_ascii=False))
    print(f"[INFO] Cached features for {len(valid)} clips -> {cache_npz.name}")
    return feats, valid


# ─── Clustering ──────────────────────────────────────────────────────────────

def cluster_features(feats: np.ndarray, n_clusters: int, seed: int,
                     min_size: int) -> tuple[np.ndarray, int, np.ndarray]:
    """KMeans with multi-seed balancing. Returns (labels, chosen_seed, sizes).
    Prefers a seed where every cluster >= min_size; otherwise picks the most
    balanced (smallest max/min ratio)."""
    candidates = []
    for s in range(seed, seed + 9):
        km = KMeans(n_clusters=n_clusters, n_init=10, random_state=s).fit(feats)
        sizes = np.bincount(km.labels_, minlength=n_clusters)
        ratio = sizes.max() / max(sizes.min(), 1)
        candidates.append((sizes.min(), ratio, s, km.labels_, sizes))
    ok = [c for c in candidates if c[0] >= min_size]
    pool = sorted(ok or candidates, key=lambda c: c[1])
    chosen = pool[0]
    if not ok:
        print(f"[WARN] No seed produced all clusters >= {min_size}; "
              f"using seed {chosen[2]} (min={chosen[0]}, max/min={chosen[1]:.2f})")
    else:
        print(f"[INFO] Chosen seed {chosen[2]} "
              f"(min={chosen[0]}, max/min={chosen[1]:.2f})")
    return chosen[3], chosen[2], chosen[4]


# ─── Ordering & event injection ──────────────────────────────────────────────

def order_nn(feats_subset: np.ndarray) -> list[int]:
    """Greedy nearest-neighbor chain (cosine dist on L2-normalized features),
    starting from the clip nearest the subset centroid -> temporally smooth."""
    m = len(feats_subset)
    if m <= 1:
        return list(range(m))
    sim = feats_subset @ feats_subset.T
    dist = 1.0 - sim
    np.fill_diagonal(dist, np.inf)
    centroid = feats_subset.mean(axis=0)
    cn = np.linalg.norm(centroid)
    if cn > 0:
        centroid /= cn
    start = int(np.argmax(feats_subset @ centroid))
    order = [start]
    visited = {start}
    cur = start
    for _ in range(m - 1):
        row = dist[cur].copy()
        row[list(visited)] = np.inf
        nxt = int(np.argmin(row))
        order.append(nxt)
        visited.add(nxt)
        cur = nxt
    return order


def build_playlist(ordered: list[Clip], target_density: float) -> tuple[list[Clip], bool]:
    """Build a long, diverse, realistic timeline using ALL clips in the cluster.

    Strategy (mimics a real CCTV feed: mostly calm, rare diverse incidents):
      - ALL normal clips form the calm "background", repeated `r` times so the
        peaceful timeline is long (a camera is quiet most of the time).
      - ALL violent clips are used (full coverage / diverse violence), each at
        least once, evenly interleaved as events across the background.
      - `r` is chosen so the resulting violence density approaches `target_density`
        while still covering every clip — so we use 100% of the dataset AND keep a
        realistic (~10-12%) event rate, with a long non-repeating timeline.

    Returns (playlist, has_violence)."""
    normals = [c for c in ordered if c.label == "normal"]
    violents = [c for c in ordered if c.label == "violence"]
    if not normals:
        return list(violents), len(violents) > 0
    if not violents:
        return list(normals), False

    n_n, n_v = len(normals), len(violents)
    d = min(max(target_density, 0.02), 0.9)
    # repeats of the normal pool needed to approach target density while using
    # ALL violence clips once:  density = n_v / (n_n*r + n_v)
    r = max(1, math.ceil(n_v * (1.0 / d - 1.0) / n_n))
    r = min(r, 15)  # cap playlist length (still long: ~15 repeats max)

    bg: list[Clip] = []
    for _ in range(r):
        bg.extend(normals)

    # Evenly interleave every violent clip across the background timeline.
    n_v_eff = len(violents)
    asc_pos = sorted(int(round((i + 1) * len(bg) / (n_v_eff + 1)))
                     for i in range(n_v_eff))
    playlist = list(bg)
    for pos, vclip in sorted(zip(asc_pos, violents), key=lambda x: -x[0]):
        playlist.insert(min(pos, len(playlist)), vclip)
    return playlist, True


# ─── Output ──────────────────────────────────────────────────────────────────

def write_outputs(out_dir: Path, container_root: str,
                  per_camera: list[dict]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    registry_path = out_dir / "camera_registry.csv"
    playlists_path = out_dir / "camera_playlists.json"

    fieldnames = ["camera_id", "city", "district", "ward", "street",
                  "latitude", "longitude", "rtsp_url", "has_violence",
                  "scene_cluster", "n_clips"]
    with open(registry_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in per_camera:
            w.writerow({k: row[k] for k in fieldnames})

    playlists = {c["camera_id"]: c["playlist_paths"] for c in per_camera}
    playlists_path.write_text(json.dumps(playlists, ensure_ascii=False, indent=2),
                              encoding="utf-8")
    print(f"[OK] Wrote {registry_path}")
    print(f"[OK] Wrote {playlists_path}")


# ─── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scvd-root", default=os.getenv(
        "SCVD_ROOT", "/home/dataguy/Documents/01 - Projects/KLTN/"
                     "MSA-MoViNet/data/SCVD/SCVD_converted"))
    ap.add_argument("--container-root", default=os.getenv(
        "CONTAINER_SCVD_ROOT", "/app/data/raw/SCVD"))
    ap.add_argument("--out-dir", default=os.getenv("OUT_DIR", "./data/metadata"))
    ap.add_argument("--n-clusters", type=int,
                    default=int(os.getenv("N_CAMERAS", "15")))
    ap.add_argument("--target-density", type=float,
                    default=float(os.getenv("TARGET_DENSITY", "0.12")),
                    help="target per-camera violence density; full coverage of all "
                         "clips is always kept (normals repeated to hit this rate)")
    ap.add_argument("--random-state", type=int, default=42)
    ap.add_argument("--min-cluster-size", type=int, default=5)
    ap.add_argument("--force-reextract", action="store_true")
    args = ap.parse_args()

    scvd_root = Path(args.scvd_root).resolve()
    out_dir = Path(args.out_dir)
    container_root = args.container_root.rstrip("/")
    n_clusters = args.n_clusters

    print("=" * 60)
    print("  prepare_cameras_context — scene clustering")
    print("=" * 60)
    print(f"  SCVD root     : {scvd_root}")
    print(f"  Container root: {container_root}")
    print(f"  Out dir       : {out_dir}")
    print(f"  Cameras       : {n_clusters}")
    print(f"  Target density : {args.target_density}")
    print("=" * 60)

    # Phase A — discover
    clips = discover_clips(scvd_root)
    if not clips:
        sys.exit("[ERROR] No clips found.")
    n_viol = sum(1 for c in clips if c.label == "violence")
    print(f"[INFO] Discovered {len(clips)} clips "
          f"({len(clips) - n_viol} normal + {n_viol} violence)")

    # Phase B — features
    cache_npz = out_dir / "clip_features.npz"
    cache_index = out_dir / "clip_features_index.json"
    skipped_log = out_dir / "clip_features_skipped.log"
    feats, valid = extract_all_features(
        clips, cache_npz, cache_index, args.force_reextract, skipped_log)
    labels_all = np.array([c.label for c in valid])

    # Phase C — cluster
    cluster_labels, chosen_seed, sizes = cluster_features(
        feats, n_clusters, args.random_state, args.min_cluster_size)
    print(f"[INFO] Cluster sizes: {dict(enumerate(sizes.tolist()))}")

    # Phase D — order + inject + assign geo (cam_01..cam_N by ascending cluster id)
    per_camera: list[dict] = []
    for cluster_id in range(n_clusters):
        idx = np.where(cluster_labels == cluster_id)[0]
        if len(idx) == 0:
            continue
        sub_feats = feats[idx]
        order = order_nn(sub_feats)
        ordered_clips = [valid[idx[j]] for j in order]
        playlist, has_violence = build_playlist(ordered_clips, args.target_density)

        cam_idx = len(per_camera)  # 0-based position
        geo = GEO[cam_idx % len(GEO)]
        cam_id = f"cam_{cam_idx + 1:02d}"
        playlist_paths = [
            str(PurePosixPath(container_root) / PurePosixPath(c.rel))
            for c in playlist
        ]
        n_viol_in = sum(1 for c in playlist if c.label == "violence")
        per_camera.append({
            "camera_id": cam_id,
            "city": CITY,
            "district": DISTRICT,
            "ward": geo[1],
            "street": geo[0],
            "latitude": geo[2],
            "longitude": geo[3],
            "rtsp_url": f"{RTSP_BASE}/{cam_id}",
            "has_violence": str(has_violence),
            "scene_cluster": cluster_id,
            "n_clips": len(playlist),
            "playlist_paths": playlist_paths,
        })
        print(f"  {cam_id}: cluster={cluster_id} size={len(idx)} "
              f"playlist={len(playlist)} (violence={n_viol_in}) "
              f"{'VIOLENT' if has_violence else 'CALM'}")

    # Phase E — write
    write_outputs(out_dir, container_root, per_camera)

    n_calm = sum(1 for c in per_camera if c["has_violence"] == "False")
    total_slots = sum(c["n_clips"] for c in per_camera)
    total_viol_slots = sum(1 for c in per_camera for p in c["playlist_paths"]
                           if "/Violence/" in p or "/Weaponized/" in p)
    print(f"\n[SUMMARY] {len(per_camera)} cameras "
          f"({n_calm} calm, {len(per_camera) - n_calm} violent)")
    print(f"[SUMMARY] {total_slots} total clip slots "
          f"({total_viol_slots} violence = "
          f"{100*total_viol_slots/max(total_slots,1):.1f}%)")


if __name__ == "__main__":
    main()
