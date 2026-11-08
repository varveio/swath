#!/usr/bin/env bash
# MinIO for the durable spill campaign (cmd/swath's TestDurableMinIO).
#
# usage: minio.sh up KEYFILE [BUCKET]   start MinIO in Docker (if not running) and fill BUCKET
#                                       (default swath-durable) from KEYFILE, one empty object per key
#        minio.sh down                  stop it (its data goes with it)
# Then:  SWATH_MINIO_ENDPOINT=http://127.0.0.1:$PORT go test ./cmd/swath -run TestDurableMinIO -v
#
# The data lives on a tmpfs inside the container (an object per key costs a directory and a block
# on disk; TMPFS, default 1536m, bounds it), so `down` discards it and the next `up` refills. The
# fill is idempotent (bench/s3fill records the key file's SHA-256), about a minute per 100,000 keys.
# Environment: PORT (default 9000), IMAGE (a pinned MinIO image), MINIO_ACCESS_KEY / MINIO_SECRET_KEY
# (default swath / swath-secret, the test's defaults), TMPFS, MEMORY (the container's cap, tmpfs
# included, default 3g), MINIO_GOMEMLIMIT (MinIO's heap target, default 1GiB: without it MinIO's heap
# grew to the cap during the campaign and the container thrashed, timing out requests).
set -euo pipefail
cd "$(dirname "$0")/.."
NAME=swath-durable-minio
PORT=${PORT:-9000}
IMAGE=${IMAGE:-cgr.dev/chainguard/minio@sha256:869ca3178831d9f521ea02258a0f0d4fbc1e9f84302e707514e1f861b860865d}
AK=${MINIO_ACCESS_KEY:-swath} SK=${MINIO_SECRET_KEY:-swath-secret}
case ${1:-} in
up)
  KEYS=${2:?usage: minio.sh up KEYFILE [BUCKET]} BUCKET=${3:-swath-durable}
  if [ -z "$(docker ps -q -f "name=^${NAME}$")" ]; then
    docker rm -f "$NAME" > /dev/null 2>&1 || true
    docker run -d --name "$NAME" -p "127.0.0.1:$PORT:9000" --memory "${MEMORY:-3g}" \
      --tmpfs "/data:rw,size=${TMPFS:-1536m},mode=1777" \
      -e GOMEMLIMIT="${MINIO_GOMEMLIMIT:-1GiB}" \
      -e MINIO_ROOT_USER="$AK" -e MINIO_ROOT_PASSWORD="$SK" "$IMAGE" server /data > /dev/null
  fi
  for _ in $(seq 1 60); do
    curl -sf "http://127.0.0.1:$PORT/minio/health/ready" > /dev/null && break
    sleep 1
  done
  curl -sf "http://127.0.0.1:$PORT/minio/health/ready" > /dev/null || { docker logs "$NAME" >&2; exit 1; }
  AWS_ACCESS_KEY_ID=$AK AWS_SECRET_ACCESS_KEY=$SK go run ./bench/s3fill \
    -endpoint "http://127.0.0.1:$PORT" -bucket "$BUCKET" -keys "$KEYS"
  echo "SWATH_MINIO_ENDPOINT=http://127.0.0.1:$PORT SWATH_MINIO_BUCKET=$BUCKET"
  ;;
down)
  docker rm -f "$NAME" > /dev/null
  ;;
*)
  echo "usage: minio.sh up KEYFILE [BUCKET] | down" >&2
  exit 2
  ;;
esac
