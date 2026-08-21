#!/usr/bin/env bash
# Runtime smoke for the replay image using a real Parquet capture.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <replay-image-ref> <fixture-directory>" >&2
  exit 2
fi

image=$1
fixture=$2

command -v curl >/dev/null || { echo "replay smoke requires curl on PATH" >&2; exit 2; }
test -d "$fixture" || { echo "replay fixture directory does not exist: $fixture" >&2; exit 2; }
fixture=$(cd "$fixture" && pwd)
find "$fixture" -maxdepth 1 -type f -name '*.parquet' -print -quit | grep -q . || {
  echo "replay fixture directory contains no Parquet files: $fixture" >&2
  exit 2
}

container_id=$(docker run --detach \
  -p 127.0.0.1::19090 \
  -v "$fixture:/fixtures:ro" \
  "$image" serve \
  --fixture /fixtures --bucket smoke --host 0.0.0.0 --port 19090 \
  --serving-mode duckdb)

cleanup() {
  docker rm -f "$container_id" >/dev/null 2>&1 || true
}
trap cleanup EXIT

host_port=$(docker inspect --format \
  '{{(index (index .NetworkSettings.Ports "19090/tcp") 0).HostPort}}' "$container_id")
body=
for _attempt in {1..80}; do
  if body=$(curl -fsS "http://127.0.0.1:${host_port}/smoke?list-type=2&max-keys=1" 2>/dev/null); then
    break
  fi
  sleep 0.25
done

if [[ -z "$body" ]]; then
  echo "replay smoke failed to receive a listing response" >&2
  docker logs "$container_id" >&2
  exit 1
fi
grep -q '<ListBucketResult' <<< "$body"
grep -q '<Name>smoke</Name>' <<< "$body"
grep -q '<Key>' <<< "$body"

test "$(docker inspect --format '{{.Config.User}}' "$container_id")" = "10001:10001"
echo "replay container smoke passed: $image"
