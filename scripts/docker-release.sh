#!/usr/bin/env bash
#
# Builds and pushes the multi-arch Wiggle server image (linux/amd64 + linux/arm64, so it runs on
# Intel and Apple Silicon alike) to Docker Hub. The Dockerfile is self-contained, so this compiles
# everything inside the build.
#
#   docker login                       # once, needs push access to the image repo
#   scripts/docker-release.sh          # tag = the project version (build.gradle.kts)
#   scripts/docker-release.sh 2.1.5    # explicit tag
#   WIGGLE_IMAGE=myrepo/wiggle scripts/docker-release.sh   # override the image name
#
# Set PUSH=false to build without pushing (single-arch, loaded into the local daemon) for a smoke test.
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE="${WIGGLE_IMAGE:-hadielmougy/wiggle}"
TAG="${1:-$(grep -oE 'version = "[^"]+"' build.gradle.kts | head -1 | sed -E 's/.*"([^"]+)".*/\1/')}"
: "${TAG:?could not determine version from build.gradle.kts; pass a tag explicitly}"
PUSH="${PUSH:-true}"

if [ "$PUSH" != "true" ]; then
    echo "Building ${IMAGE}:${TAG} for the local platform (no push)..."
    docker build -t "${IMAGE}:${TAG}" .
    echo "Built ${IMAGE}:${TAG} locally. Run: docker run --rm -p 8080:8080 -p 8090:8090 ${IMAGE}:${TAG}"
    exit 0
fi

echo "Building and pushing ${IMAGE}:${TAG} (+ latest) for linux/amd64,linux/arm64 ..."
# Reuse the buildx builder if it already exists; otherwise create it.
docker buildx inspect wiggle-builder >/dev/null 2>&1 || docker buildx create --name wiggle-builder >/dev/null
docker buildx use wiggle-builder
docker buildx build --platform linux/amd64,linux/arm64 \
    -t "${IMAGE}:${TAG}" \
    -t "${IMAGE}:latest" \
    --push .
echo "Pushed ${IMAGE}:${TAG} and ${IMAGE}:latest"
echo "Verify: docker buildx imagetools inspect ${IMAGE}:${TAG}"
