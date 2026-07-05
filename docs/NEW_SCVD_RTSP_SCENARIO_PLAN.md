# New SCVD RTSP Scenario Migration Plan

## Goal

Replace the old local RTSP simulation flow with a deterministic 5-stream benchmark built from the newly labeled SCVD behavior dataset.

## Implementation Status

Status on 2026-07-06: implemented locally, deployed on Vast.ai, and committed on `dev.VastAI`.

Git commit:

```text
797775f Add deterministic SCVD RTSP scenario pipeline
```

Implemented files:

```text
scripts/prepare_rtsp_scenarios.py
scripts/streaming/rtsp_pusher.py
docker/docker-compose.scvd-scenarios.yml
```

Project-local dataset copy:

```text
data/raw/SCVD/
├── SCVD_converted/
├── labels/
├── rtsp_scenarios/
├── scripts/
├── requirements.txt
└── README_rtsp_fiftyone.md
```

`data/raw/SCVD` is now a real project-local directory, not the old symlink to the MSA-MoViNet dataset. The `data/` tree remains gitignored; it must be copied or regenerated on each runtime machine.

Vast.ai runtime state:

```text
/root/streamhouse/data/raw/SCVD          # new labeled SCVD data
/root/streamhouse/data/metadata          # regenerated runtime metadata
rtsp://localhost:8554/cam_01..cam_05     # scenario source streams
rtsp://localhost:8554/cam_01_result..cam_05_result
```

The old random/context-cluster RTSP data source is no longer the active Vast.ai runtime source.

The new RTSP setup must support:

- 5 fixed streams instead of 15 random/context-cluster cameras
- scenario-specific background/event pools
- exact event annotations in seconds
- repeatable 15-minute loops
- ordered clip selection by natural filename order inside each behavior label
- stable camera IDs and RTSP URLs for StreamViD-A evaluation
- later metric comparison against `annotations.csv`

## Current Artifacts

New labeled dataset artifacts are currently generated under:

```text
/home/dataguy/Documents/SCVD/
```

Important files:

```text
labels/behavior_manifest.csv
labels/behavior_manifest_with_duration.csv
labels/video_durations.csv
rtsp_scenarios/scenario_summary.csv
rtsp_scenarios/rtsp-01_baseline/schedule.csv
rtsp_scenarios/rtsp-01_baseline/annotations.csv
rtsp_scenarios/rtsp-01_baseline/concat.txt
rtsp_scenarios/rtsp-02_crowd/schedule.csv
rtsp_scenarios/rtsp-02_crowd/annotations.csv
rtsp_scenarios/rtsp-02_crowd/concat.txt
rtsp_scenarios/rtsp-03_difficult_conditions/schedule.csv
rtsp_scenarios/rtsp-03_difficult_conditions/annotations.csv
rtsp_scenarios/rtsp-03_difficult_conditions/concat.txt
rtsp_scenarios/rtsp-04_hard_negative/schedule.csv
rtsp_scenarios/rtsp-04_hard_negative/annotations.csv
rtsp_scenarios/rtsp-04_hard_negative/concat.txt
rtsp_scenarios/rtsp-05_peak_frequency/schedule.csv
rtsp_scenarios/rtsp-05_peak_frequency/annotations.csv
rtsp_scenarios/rtsp-05_peak_frequency/concat.txt
```

Current exported label counts:

```text
NV-BASE   84
NV-CROWD  118
NV-DIFF   62
NV-NEG    37
V-BASE    63
V-CROWD   55
V-DIFF    49
```

CSV duration summary:

```text
399 unique labeled videos
468 label rows
total duration: ~38.15 minutes across unique labeled videos
```

## Source Of Truth And Regeneration Flow

The source of truth for the new benchmark is the manually labeled CSV exported from FiftyOne:

```text
/home/dataguy/Documents/SCVD/labels/behavior_manifest.csv
```

Everything else is derived from this file:

```text
behavior_manifest.csv
  -> behavior_manifest_with_duration.csv
  -> rtsp_scenarios/*/{schedule.csv,annotations.csv,concat.txt}
  -> project data/metadata/{camera_registry.csv,camera_playlists.json,camera_scenarios.json}
  -> docker RTSP streams
```

Do not edit generated scenario files by hand unless debugging. If labels change, regenerate downstream artifacts from the CSV.

Current CSV columns used by the scenario builder:

```text
filename
label
class
group
context
original_filepath
original_relative_path
split
scvd_label
sample_id
duration_seconds
```

`behavior_manifest.csv` does not naturally contain `duration_seconds`. The duration-enhanced file is derived from ffprobe:

```text
labels/behavior_manifest_with_duration.csv
```

Required source update sequence after relabeling in FiftyOne:

```bash
cd /home/dataguy/Documents/SCVD
source .venv/bin/activate

# 1) Export labels from FiftyOne
python scripts/export_behavior_manifest.py --sync-fields

# 2) Recompute duration-enhanced manifest
python scripts/add_video_durations.py \
  --manifest labels/behavior_manifest.csv \
  --out labels/behavior_manifest_with_duration.csv \
  --video-out labels/video_durations.csv

# 3) Rebuild 5 stream schedules and annotations
python scripts/build_rtsp_scenarios.py \
  --manifest labels/behavior_manifest_with_duration.csv \
  --out rtsp_scenarios
```

Required project update sequence after scenario rebuild:

```bash
cd "/home/dataguy/Documents/01 - Projects/KLTN/streamhouse-violence-detection"

python scripts/prepare_rtsp_scenarios.py \
  --scenario-root data/raw/SCVD/rtsp_scenarios \
  --out-dir data/metadata \
  --host-scvd-root data/raw/SCVD \
  --container-scvd-root /app/data/raw/SCVD \
  --metadata-runtime-root /app/data/metadata
```

This separation matters:

- `behavior_manifest.csv` answers what each source clip means.
- `behavior_manifest_with_duration.csv` answers how long each source clip is.
- `rtsp_scenarios/*/schedule.csv` answers what each RTSP camera plays and in what order.
- `rtsp_scenarios/*/annotations.csv` answers when violence events occur.
- project `data/metadata/*.json/csv` answers what Docker containers should mount and stream.

Ordering policy:

```text
No random shuffle.
Each behavior-label pool is sorted by natural filename order.
Streams with multiple background/event labels use round-robin selection across
labels, while preserving filename order inside each label.
```

This preserves continuity when source clips are consecutive cuts from the same
long video.

## Target Stream Design

| Stream | Camera ID | RTSP Path | Background Pool | Event Pool | Frequency | Purpose |
|---|---|---|---|---|---|---|
| RTSP-01 Baseline | `cam_01` | `rtsp://mediamtx:8554/cam_01` | `NV-BASE` | `V-BASE` | 1 event / 5 min | baseline accuracy |
| RTSP-02 Crowd | `cam_02` | `rtsp://mediamtx:8554/cam_02` | `NV-CROWD` | `V-CROWD` | 1 event / 5 min | occlusion / crowded scenes |
| RTSP-03 Difficult Conditions | `cam_03` | `rtsp://mediamtx:8554/cam_03` | `NV-DIFF` | `V-DIFF` | 1 event / 5 min | lighting / distance / shake |
| RTSP-04 Hard Negative | `cam_04` | `rtsp://mediamtx:8554/cam_04` | `NV-NEG` | `V-BASE` | 1 event / 15 min | false alarm control |
| RTSP-05 Peak Frequency | `cam_05` | `rtsp://mediamtx:8554/cam_05` | `NV-BASE`, `NV-CROWD` | `V-BASE`, `V-CROWD` | 1 event / 2 min | dense alert stress test |

Generated scenario summary today:

```text
RTSP-01 baseline:              3 events, event_seconds=26.400
RTSP-02 crowd:                 3 events, event_seconds=28.833
RTSP-03 difficult_conditions:  3 events, event_seconds=25.700
RTSP-04 hard_negative:         1 event,  event_seconds=7.833
RTSP-05 peak_frequency:        7 events, event_seconds=49.566
```

## Existing Project Flow

Current files involved:

```text
docker/docker-compose.local-stream.yml
scripts/streaming/rtsp_pusher.py
scripts/prepare_cameras_dataset.py
scripts/prepare_cameras_context.py
```

Current behavior:

- `prepare_cameras_dataset.py` randomly copies SCVD clips into per-camera playlists.
- `prepare_cameras_context.py` clusters clips visually into 15 camera-like playlists and writes:
  - `data/metadata/camera_registry.csv`
  - `data/metadata/camera_playlists.json`
- `rtsp_pusher.py` reads `camera_registry.csv` and `camera_playlists.json`.
- Each camera gets one ffmpeg concat playlist and pushes to MediaMTX.
- `docker-compose.local-stream.yml` starts:
  - `mediamtx`
  - `rtsp_pusher`
  - `rtsp-inference-mock`

Main incompatibility:

- Old flow is camera/scene-cluster based.
- New flow is deterministic scenario/annotation based.
- Old metadata has no event timeline.
- New benchmark needs `schedule.csv` and `annotations.csv` as first-class outputs.

## Implemented Migration

### Phase 1. Add Scenario Dataset Preparation Script

Create a new script in the project:

```text
scripts/prepare_rtsp_scenarios.py
```

Do not replace `prepare_cameras_context.py` immediately. Keep both flows available.

Responsibilities:

- Read the new SCVD scenario outputs from either:
  - external SCVD workspace: `/home/dataguy/Documents/SCVD/rtsp_scenarios`
  - or project-local copy: `data/metadata/rtsp_scenarios`
- Generate project-compatible metadata:
  - `data/metadata/camera_registry.csv`
  - `data/metadata/camera_playlists.json`
  - `data/metadata/camera_scenarios.json`
  - `data/metadata/rtsp_annotations/*.csv`
  - `data/metadata/rtsp_schedules/*.csv`
- Map scenario stream IDs to camera IDs:
  - `RTSP-01 -> cam_01`
  - `RTSP-02 -> cam_02`
  - `RTSP-03 -> cam_03`
  - `RTSP-04 -> cam_04`
  - `RTSP-05 -> cam_05`

Docker/local CLI:

```bash
python scripts/prepare_rtsp_scenarios.py \
  --scenario-root data/raw/SCVD/rtsp_scenarios \
  --out-dir data/metadata \
  --host-scvd-root data/raw/SCVD \
  --container-scvd-root /app/data/raw/SCVD \
  --metadata-runtime-root /app/data/metadata
```

Vast.ai bare-metal CLI:

```bash
cd ~/streamhouse

python3 scripts/prepare_rtsp_scenarios.py \
  --scenario-root data/raw/SCVD/rtsp_scenarios \
  --out-dir data/metadata \
  --host-scvd-root data/raw/SCVD \
  --container-scvd-root /root/streamhouse/data/raw/SCVD \
  --metadata-runtime-root /root/streamhouse/data/metadata \
  --rtsp-base rtsp://localhost:8554
```

Key implementation detail:

- The generated `camera_playlists.json` must contain container paths, not host paths.
- Current `rtsp_pusher.py` filters playlist paths with `os.path.exists()` inside the container.
- Therefore paths should look like:

```text
/app/data/raw/SCVD/SCVD_converted/Train/Normal/n001_converted.avi
```

or, if mounting only the inner dataset root:

```text
/app/data/raw/SCVD/Train/Normal/n001_converted.avi
```

Pick one mount convention and keep it consistent.

Implemented project-local mount convention:

```text
host: data/raw/SCVD/SCVD_converted
container: /app/data/raw/SCVD/SCVD_converted
```

For Vast.ai bare-metal, generated playlists use:

```text
/root/streamhouse/data/raw/SCVD/SCVD_converted/Train/Normal/n001_converted.avi
```

### Phase 2. Metadata Schema

`camera_registry.csv` should keep the fields expected by the current inference stack:

```text
camera_id
city
district
ward
street
latitude
longitude
rtsp_url
has_violence
scenario_id
scenario_name
difficulty
frequency
purpose
n_events
duration_seconds
annotation_file
schedule_file
```

Keep the original geo values from `prepare_cameras_context.py` for `cam_01..cam_05` so dashboard joins remain stable.

`camera_playlists.json` should remain compatible with `rtsp_pusher.py`:

```json
{
  "cam_01": ["/app/data/raw/SCVD/SCVD_converted/Train/Normal/n001_converted.avi"],
  "cam_02": [],
  "cam_03": [],
  "cam_04": [],
  "cam_05": []
}
```

`camera_scenarios.json` should be new and explicit:

```json
{
  "cam_01": {
    "stream_id": "RTSP-01",
    "stream_name": "baseline",
    "background_labels": ["NV-BASE"],
    "event_labels": ["V-BASE"],
    "annotation_file": "/app/data/metadata/rtsp_annotations/cam_01_annotations.csv",
    "schedule_file": "/app/data/metadata/rtsp_schedules/cam_01_schedule.csv"
  }
}
```

### Phase 3. Update `rtsp_pusher.py`

Keep the existing context-playlist path, but add scenario-aware logging and optional exact loop mode.

Minimal required changes:

- Add env var:

```text
SCENARIO_METADATA_FILE=/app/data/metadata/camera_scenarios.json
```

- Load `camera_scenarios.json` if present.
- When a camera has scenario metadata, log:

```text
cam_01: SCENARIO RTSP-01 baseline, clips=..., events=3
annotation=/app/data/metadata/rtsp_annotations/cam_01_annotations.csv
```

- Keep using `camera_playlists.json` for the actual ordered clip list.

Recommended `write_playlist()` change:

- Current function repeats the entire playlist 200 times.
- For the new 15-minute scenario, use the schedule once and let ffmpeg loop/restart the 15-minute script.
- Add env var:

```text
PLAYLIST_REPEAT=1
```

- Default can remain `200` for legacy mode.
- For new scenario mode, compose sets `PLAYLIST_REPEAT=1`.

Optional stronger approach:

- Render each scenario into a single 15-minute MP4.
- Then `rtsp_pusher.py` streams one MP4 per camera with `-stream_loop -1`.
- This gives exact loop boundaries and lower pusher complexity, but costs disk space and render time.

Recommended first implementation:

- Use scenario concat playlists directly.
- Do not render MP4 unless ffmpeg concat causes instability.

### Phase 4. Add Docker Compose Override for New Dataset

Create a new compose file instead of changing the old one in place:

```text
docker/docker-compose.scvd-scenarios.yml
```

Base it on `docker/docker-compose.local-stream.yml`.

Key differences:

- `MAX_CAMERAS=5`
- `ACTIVE_CAMERAS=cam_01,cam_02,cam_03,cam_04,cam_05`
- Use scenario metadata files.
- Mount the new SCVD dataset path.
- Mount scenario metadata read-only.

Implemented `rtsp_pusher` environment:

```yaml
environment:
  METADATA_FILE: /app/data/metadata/camera_registry.csv
  CAMERA_PLAYLISTS_FILE: /app/data/metadata/camera_playlists.json
  SCENARIO_METADATA_FILE: /app/data/metadata/camera_scenarios.json
  MEDIAMTX_HOST: mediamtx
  MAX_CAMERAS: "5"
  ACTIVE_CAMERAS: "cam_01,cam_02,cam_03,cam_04,cam_05"
  PLAYLIST_REPEAT: "1"
  STOP_FILE: /app/tmp/STOP
```

Implemented Docker volumes:

```yaml
volumes:
  - ../scripts/streaming/rtsp_pusher.py:/app/rtsp_pusher.py:ro
  - ../data/raw/SCVD/SCVD_converted:/app/data/raw/SCVD/SCVD_converted:ro
  - ../data/metadata:/app/data/metadata:ro
  - scvd-pusher-tmp:/app/tmp
```

Start locally:

```bash
docker compose -f docker/docker-compose.scvd-scenarios.yml up -d --build
```

The compose file also starts `rtsp-inference-mock` for the 5 scenario cameras.

### Phase 5. Annotation Contract

The new benchmark requires an annotation contract for evaluation.

Per-camera annotation path:

```text
data/metadata/rtsp_annotations/cam_01_annotations.csv
```

Required columns:

```text
camera_id
stream_id
stream_name
event_id
target_start_seconds
start_seconds
end_seconds
duration_seconds
start_offset_seconds
label
class
group
context
filename
original_filepath
sample_id
```

Important semantics:

- `start_seconds` and `end_seconds` are relative to the beginning of the 15-minute loop.
- The RTSP stream loops forever.
- Evaluation must compare model alert timestamps modulo `900` seconds:

```text
loop_time = elapsed_stream_seconds % 900
```

- Event boundaries are not exactly at 270/570/870 because clips are not cut mid-clip.
- Use actual `annotations.csv`, not target times.

### Phase 6. Evaluation Plan

Add a future evaluator script:

```text
scripts/evaluate_rtsp_alerts.py
```

Inputs:

- model alerts from Kafka or exported detection log
- `camera_scenarios.json`
- `rtsp_annotations/*.csv`

Metrics:

- true positive event hit rate
- missed events
- false alarms outside event intervals
- detection latency:

```text
first_alert_time - event_start_seconds
```

- per-stream summary:
  - RTSP-01 baseline accuracy
  - RTSP-02 crowd robustness
  - RTSP-03 hard-condition robustness
  - RTSP-04 false alarm rate
  - RTSP-05 dense-alert stability

## Implementation Checklist

### Data Preparation

- [x] Keep source data under `/home/dataguy/Documents/SCVD`.
- [x] Copy required runtime data into project-local `data/raw/SCVD`.
- [x] Treat this file as the source of truth after manual labeling:

```text
/home/dataguy/Documents/SCVD/labels/behavior_manifest.csv
```

- [ ] Regenerate labels if labels change:

```bash
cd /home/dataguy/Documents/SCVD
source .venv/bin/activate
python scripts/export_behavior_manifest.py --sync-fields
```

- [ ] Regenerate duration-enhanced manifest if labels change:

```bash
python scripts/add_video_durations.py \
  --manifest labels/behavior_manifest.csv \
  --out labels/behavior_manifest_with_duration.csv \
  --video-out labels/video_durations.csv
```

- [ ] Regenerate scenario files after label changes:

```bash
cd /home/dataguy/Documents/SCVD
source .venv/bin/activate
python scripts/build_rtsp_scenarios.py
```

### Project Integration

- [x] Add `scripts/prepare_rtsp_scenarios.py`.
- [x] Generate project metadata:

```bash
cd "/home/dataguy/Documents/01 - Projects/KLTN/streamhouse-violence-detection"
python scripts/prepare_rtsp_scenarios.py \
  --scenario-root data/raw/SCVD/rtsp_scenarios \
  --out-dir data/metadata \
  --host-scvd-root data/raw/SCVD \
  --container-scvd-root /app/data/raw/SCVD \
  --metadata-runtime-root /app/data/metadata
```

- [x] Confirm outputs:

```text
data/metadata/camera_registry.csv
data/metadata/camera_playlists.json
data/metadata/camera_scenarios.json
data/metadata/rtsp_annotations/cam_01_annotations.csv
data/metadata/rtsp_schedules/cam_01_schedule.csv
```

### Pusher Integration

- [x] Add `SCENARIO_METADATA_FILE` support in `rtsp_pusher.py`.
- [x] Add `PLAYLIST_REPEAT` env var in `rtsp_pusher.py`.
- [x] Keep old random/context fallback working.
- [x] Log each scenario stream with annotation file path.

### Docker Integration

- [x] Add `docker/docker-compose.scvd-scenarios.yml`.
- [x] Set `MAX_CAMERAS=5`.
- [x] Mount `data/raw/SCVD/SCVD_converted` into the pusher.
- [x] Set `ACTIVE_CAMERAS=cam_01,cam_02,cam_03,cam_04,cam_05`.
- [x] Keep `rtsp-inference-mock` using the same `camera_registry.csv`.

### Smoke Test

- [x] Start stack:

```bash
docker compose -f docker/docker-compose.scvd-scenarios.yml up -d
```

- [x] Check pusher logs:

```bash
docker logs rtsp_pusher --tail 80
```

- [x] Check streams:

```bash
ffprobe rtsp://localhost:8554/cam_01
ffprobe rtsp://localhost:8554/cam_02
ffprobe rtsp://localhost:8554/cam_03
ffprobe rtsp://localhost:8554/cam_04
ffprobe rtsp://localhost:8554/cam_05
```

- [x] Check MediaMTX API:

```bash
curl http://localhost:9997/v3/paths/list
```

- [x] Confirm inference mock sees 5 cameras only.

### Evaluation Readiness

- [ ] Confirm each `annotations.csv` has event windows.
- [ ] Record stream start wall-clock timestamp for each test run.
- [ ] Convert model alert timestamps to loop-relative seconds.
- [ ] Compare against annotations modulo 900 seconds.

## Risks And Decisions

### Risk: container path mismatch

The current `rtsp_pusher.py` drops playlist paths that do not exist inside the container. The migration script must convert host paths to container paths exactly.

Decision:

- Standardize source mount:

```text
/home/dataguy/Documents/SCVD/SCVD_converted
  -> /app/data/raw/SCVD/SCVD_converted
```

### Risk: concat stream boundary drift

The scenario schedules are built from whole clips. The concat duration is slightly above 900 seconds, and render mode trims to 900 seconds.

Decision:

- Annotation uses actual schedule seconds.
- If exact 900-second loop boundary matters, render MP4 with:

```bash
cd /home/dataguy/Documents/SCVD
source .venv/bin/activate
python scripts/build_rtsp_scenarios.py --render
```

Then stream rendered MP4 files with `-stream_loop -1`.

### Risk: RTSP-04 has low event count by design

This is expected. RTSP-04 is mainly a false-alarm stream.

Decision:

- Keep `NV-NEG` as background and one `V-BASE` event.
- Evaluate false alarms outside the one event interval.

### Risk: current labels have no `IN/OUT` context

`context` is blank in current CSV because labels are group-only, e.g. `NV-CROWD`.

Decision:

- Do not block RTSP scenario build on context.
- Add `IN/OUT` later if needed, but keep current 5-stream benchmark based on behavior groups.

## Acceptance Criteria

The migration is complete when:

- [x] `docker compose -f docker/docker-compose.scvd-scenarios.yml up -d` starts 5 RTSP streams.
- [x] `rtsp://localhost:8554/cam_01` through `cam_05` are playable.
- [x] `camera_registry.csv` contains exactly 5 scenario cameras.
- [x] `camera_playlists.json` contains exactly 5 ordered playlists.
- [x] `camera_scenarios.json` maps each camera to its scenario and annotation file.
- [x] `rtsp_annotations/*.csv` contain ground-truth event windows.
- [x] Inference mock can consume the 5 streams without code changes outside metadata/compose/pusher.
- [x] The old `docker-compose.local-stream.yml` flow remains usable for legacy demos.
