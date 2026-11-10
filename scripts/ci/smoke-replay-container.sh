#!/usr/bin/env bash
# Runtime smoke for a replay image using a real Parquet capture.
#
#   smoke-replay-container.sh <replay-image-ref> <fixture-directory> [duckdb|sorted]
#
# The serving mode selects the image: `duckdb` (the default) for the Java image, which
# queries any capture through DuckDB; `sorted` for the Go image, which serves sorted
# fixtures only. Both modes check a listing page (a ListBucketResult naming the bucket and
# holding a key) and the container's user: 10001:10001 for the Java image, 65532:65532 for
# Go. In `sorted` mode the server also gets a metrics port, and the smoke polls `/healthz`
# before listing and requires `/metrics` to report schema_version 2; its fixture may also be
# a dataset directory (the parts under data/, as `swath list` writes it).
#
# A `sorted`-mode fixture may be an unsorted capture (a `swath list` dataset written without
# --sort, for one): when the server refuses it as not servable sorted, the smoke sorts it
# with the image's own `sort-fixture` into a temporary directory and serves that. Any other
# startup failure fails the smoke.
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 <replay-image-ref> <fixture-directory> [duckdb|sorted]" >&2
  exit 2
fi

image=$1
fixture=$2
mode=${3:-duckdb}
case "$mode" in
  duckdb|sorted) ;;
  *) echo "unknown serving mode: $mode (duckdb or sorted)" >&2; exit 2 ;;
esac

command -v curl >/dev/null || { echo "replay smoke requires curl on PATH" >&2; exit 2; }
test -d "$fixture" || { echo "replay fixture directory does not exist: $fixture" >&2; exit 2; }
fixture=$(cd "$fixture" && pwd)
# The sorted server also opens a dataset directory, whose parts are in data/.
depth=1
if [[ "$mode" = sorted ]]; then
  depth=2
fi
find "$fixture" -maxdepth "$depth" -type f -name '*.parquet' -print -quit | grep -q . || {
  echo "replay fixture directory contains no Parquet files: $fixture" >&2
  exit 2
}

container_id=
sorted_dir=
cleanup() {
  if [[ -n "$container_id" ]]; then
    docker rm -f "$container_id" >/dev/null 2>&1 || true
  fi
  if [[ -n "$sorted_dir" ]]; then
    rm -rf "$sorted_dir"
  fi
}
trap cleanup EXIT

# start FIXTURE: runs the server on FIXTURE in the background and sets container_id.
start() {
  local publish=() serve=()
  if [[ "$mode" = sorted ]]; then
    publish=(-p 127.0.0.1::19192)
    serve=(--metrics-port 19192)
  fi
  container_id=$(docker run --detach \
    -p 127.0.0.1::19090 "${publish[@]}" \
    -v "$1:/fixtures:ro" \
    "$image" serve \
    --fixture /fixtures --bucket smoke --host 0.0.0.0 --port 19090 "${serve[@]}" \
    --serving-mode "$mode")
}

# host_port PORT: the host port the container's PORT is published on. An inspection that
# fails or yields anything but a port number fails, so no request is ever sent to an empty
# or wrong port (curl would read an empty port as 80, where another service may answer).
host_port() {
  local port
  port=$(docker inspect --format \
    "{{(index (index .NetworkSettings.Ports \"$1/tcp\") 0).HostPort}}" "$container_id") || return 1
  [[ "$port" =~ ^[0-9]+$ ]] || return 1
  printf '%s' "$port"
}

running() {
  test "$(docker inspect --format '{{.State.Running}}' "$container_id")" = true
}

# get URL MAX_SECONDS: one bounded request (connect and whole transfer), so a server that
# accepts a connection and never answers cannot hold the smoke.
get() {
  curl -fsS --connect-timeout 2 --max-time "$2" "$1"
}

# ready PORT PATH: polls the container's PORT at PATH until it answers, the container
# exits, or the 20 s wall-clock deadline passes; prints the body. The deadline is checked
# between attempts and caps each request's time, so a hung request cannot outlast it. A
# stopped container has no port mapping, so the host port is looked up only while it runs.
readonly ready_seconds=20
ready() {
  local body port left deadline=$((SECONDS + ready_seconds))
  while (( (left = deadline - SECONDS) > 0 )); do
    running || return 1
    port=$(host_port "$1") || { echo "replay smoke: no host port for $1" >&2; return 1; }
    if body=$(get "http://127.0.0.1:${port}$2" $((left < 5 ? left : 5)) 2>/dev/null); then
      printf '%s' "$body"
      return 0
    fi
    sleep 0.25
  done
  echo "replay smoke: no answer on $1$2 within ${ready_seconds}s" >&2
  return 1
}

fail() {
  echo "replay smoke: $1" >&2
  docker logs "$container_id" >&2 || true
  exit 1
}

start "$fixture"
if [[ "$mode" = sorted ]] && ! health=$(ready 19192 /healthz); then
  if running || ! docker logs "$container_id" 2>&1 | grep -q 'not servable sorted'; then
    fail "/healthz did not answer"
  fi
  docker logs "$container_id" >&2
  echo "replay smoke: the fixture is not sorted; sorting it with the image's sort-fixture" >&2
  docker rm -f "$container_id" >/dev/null
  container_id=
  # The sort runs as the invoking user so the temporary output stays removable here; the
  # server below still runs as the image's own user and reads it read-only.
  sorted_dir=$(mktemp -d)
  chmod 0755 "$sorted_dir"
  docker run --rm --user "$(id -u):$(id -g)" \
    -v "$fixture:/capture:ro" -v "$sorted_dir:/work" \
    "$image" sort-fixture --capture /capture --output /work/sorted
  start "$sorted_dir/sorted"
  health=$(ready 19192 /healthz) || fail "/healthz did not answer after sorting"
fi
if [[ "$mode" = sorted && "$health" != ok ]]; then
  fail "/healthz answered '$health', not ok"
fi

body=$(ready 19090 '/smoke?list-type=2&max-keys=1') || fail "no listing response"
grep -q '<ListBucketResult' <<< "$body"
grep -q '<Name>smoke</Name>' <<< "$body"
grep -q '<Key>' <<< "$body"

if [[ "$mode" = sorted ]]; then
  metrics_port=$(host_port 19192) || fail "no host port for 19192"
  metrics=$(get "http://127.0.0.1:${metrics_port}/metrics" 10) || fail "no /metrics response"
  grep -Eq '"schema_version"[[:space:]]*:[[:space:]]*2[,}[:space:]]' <<< "$metrics" ||
    fail "/metrics does not report schema_version 2: $(head -c 400 <<< "$metrics")"
fi

if [[ "$mode" == sorted ]]; then expected_user=65532:65532; else expected_user=10001:10001; fi
test "$(docker inspect --format '{{.Config.User}}' "$container_id")" = "$expected_user"
echo "replay container smoke passed ($mode): $image"
