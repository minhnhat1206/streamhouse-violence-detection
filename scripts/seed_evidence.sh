#!/bin/bash
# Copy existing real demo images to recent dates
# Source images: cam_01/2026-04-01/inc_001.jpg, etc.
# Target: recent dates 2026-05-05 through 2026-05-12 for all cameras

CAMERAS="cam_01 cam_02 cam_03 cam_04 cam_05 cam_06 cam_07 cam_08 cam_09 cam_10 cam_11 cam_12 cam_13 cam_14 cam_15"
DATES="2026-05-05 2026-05-06 2026-05-07 2026-05-08 2026-05-09 2026-05-10 2026-05-11 2026-05-12"

# Source images pool (the 40KiB real ones)
SOURCES=(
  "cam_01/2026-04-01/inc_001.jpg"
  "cam_02/2026-04-02/inc_002.jpg"
  "cam_03/2026-04-10/inc_004.jpg"
  "cam_04/2026-04-12/inc_005.jpg"
  "cam_05/2026-04-20/inc_007.jpg"
  "cam_06/2026-04-26/inc_010.jpg"
)

# Download source images first
echo "Downloading source images..."
for src in "${SOURCES[@]}"; do
    fname=$(basename "$src")
    mc cp "local/evidence-frames/$src" "/tmp/src_$fname" 2>/dev/null && echo "  Got $fname" || echo "  SKIP $src"
done

# Check we got at least one
ls /tmp/src_*.jpg 2>/dev/null | head -1 | grep -q . || { echo "ERROR: No source images found"; exit 1; }

# Now copy to recent dates - 3 images per camera per day
COUNT=0
NUM_SRCS=${#SOURCES[@]}
SRC_IDX=0

for cam in $CAMERAS; do
    cam_num="${cam#cam_}"
    for date in $DATES; do
        for i in 1 2 3; do
            # Rotate through available source files
            src_file="/tmp/src_$(basename ${SOURCES[$SRC_IDX]})"
            [ -f "$src_file" ] || src_file=$(ls /tmp/src_*.jpg 2>/dev/null | head -1)
            SRC_IDX=$(( (SRC_IDX + 1) % NUM_SRCS ))

            # Generate a UUID-like name
            uuid="$(cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "$(od -An -N16 -tx1 /dev/urandom 2>/dev/null | tr -d ' \n')")"
            [ -z "$uuid" ] && uuid="${cam_num}${date//[-]//}${i}"

            target="local/evidence-frames/$cam/$date/$uuid.jpg"
            mc cp "$src_file" "$target" >/dev/null 2>&1 && COUNT=$((COUNT+1))
        done
    done
done
echo "Seeded $COUNT evidence images for recent dates"
