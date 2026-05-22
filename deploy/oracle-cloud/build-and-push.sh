#!/bin/bash
# =============================================================================
# Build ARM64 Docker Images and Push to DockerHub
# Run from the repo root on an ARM64 machine (Oracle VM) or with buildx
# Usage: bash deploy/oracle-cloud/build-and-push.sh <dockerhub-user> [--push]
# =============================================================================
set -euo pipefail

DOCKER_USER="${1:?Usage: $0 <dockerhub-user> [--push]}"
PUSH="${2:-}"
TAG="${IMAGE_TAG:-latest}"
PLATFORM="linux/arm64"

echo "════════════════════════════════════════════════"
echo "  Build ARM64 Images → ${DOCKER_USER}/*:${TAG}"
echo "════════════════════════════════════════════════"

build_image() {
  local name=$1 dockerfile=$2 context=${3:-.}
  local full_name="${DOCKER_USER}/${name}:${TAG}"

  echo ""
  echo "▶ Building: $full_name"
  docker buildx build \
    --platform "$PLATFORM" \
    --file "$dockerfile" \
    --build-arg TARGETPLATFORM="$PLATFORM" \
    --tag "$full_name" \
    ${PUSH:+--push} \
    "$context"

  echo "✅ Done: $full_name"
}

# ── Images ────────────────────────────────────────────────────────────────
build_image "streamhouse-flink"   "docker/Dockerfile.flink"   "."
build_image "streamhouse-hive"    "docker/Dockerfile.hive"    "."
build_image "streamhouse-chatbot" "docker/Dockerfile.chatbot" "."

echo ""
echo "════════════════════════════════════════════════"
if [ -n "$PUSH" ]; then
  echo "  ✅ All images built and pushed to DockerHub"
  echo "  Images:"
  echo "    ${DOCKER_USER}/streamhouse-flink:${TAG}"
  echo "    ${DOCKER_USER}/streamhouse-hive:${TAG}"
  echo "    ${DOCKER_USER}/streamhouse-chatbot:${TAG}"
  echo ""
  echo "  Update docker-compose.cloud.yml to use these images"
  echo "  instead of build: directives for faster cloud deploys."
else
  echo "  ✅ All images built locally (not pushed)"
  echo "  Re-run with --push to upload to DockerHub"
fi
echo "════════════════════════════════════════════════"
