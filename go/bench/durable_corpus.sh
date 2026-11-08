#!/usr/bin/env bash
# The durable spill campaign over production key lists (cmd/swath's TestDurableCorpus): each list's
# first MAX_KEYS keys (default 3,000,000) are served by a static listing fake at a 5 ms delay, and
# the campaign's kill schedule runs against it, one list at a time.
#
# usage: durable_corpus.sh KEYFILE...
# Environment: KEYFAKE (the static key-list fake: -keys FILE -addr HOST:PORT -delay D), PORT
#   (default 19620), MAX_KEYS, DELAY (default 5ms), RESULTS (the results file each arm appends a
#   line to, default durable-corpus.tsv; each list's test log goes beside it), GOTEST_FLAGS (extra go test flags).
# The results' columns: list, mode, arm, status, live certificates before the final resume,
# accepted, replayed rows, resume requests, resume/cold requests, boundary overfetch keys,
# requests inside a replayed certificate (must be 0).
set -euo pipefail
KF=${KEYFAKE:?path to the static key-list fake}
PORT=${PORT:-19620} MAX_KEYS=${MAX_KEYS:-3000000} DELAY=${DELAY:-5ms}
RESULTS=$(realpath -m "${RESULTS:-durable-corpus.tsv}")
[ $# -gt 0 ] || { echo "usage: durable_corpus.sh KEYFILE..." >&2; exit 2; }
LISTS=()
for f in "$@"; do
  [ -r "$f" ] || { echo "cannot read $f" >&2; exit 2; }
  LISTS+=("$(realpath "$f")")
done
cd "$(dirname "$0")/.."
failed=0
for f in "${LISTS[@]}"; do
  name=$(basename "$f" .keys)
  "$KF" -keys <(head -n "$MAX_KEYS" "$f") -addr "127.0.0.1:$PORT" -delay "$DELAY" > "/dev/null" 2>&1 &
  kf=$!
  trap 'kill $kf 2>/dev/null' EXIT
  until curl -s -o /dev/null "http://127.0.0.1:$PORT/bkt?list-type=2&max-keys=1"; do
    kill -0 "$kf" || { echo "the fake did not start for $f" >&2; exit 1; }
    sleep 1
  done
  rc=0
  # shellcheck disable=SC2086
  SWATH_CORPUS_ENDPOINT=http://127.0.0.1:$PORT SWATH_CORPUS_NAME=$name SWATH_CAMPAIGN_RESULTS=$RESULTS \
    SWATH_CAMPAIGN_KEYS=$f SWATH_CAMPAIGN_MAX_KEYS=$MAX_KEYS \
    go test -p 1 -count=1 -timeout 3h ${GOTEST_FLAGS:-} ./cmd/swath -run '^TestDurableCorpus$' -v > "${RESULTS%.*}-$name.log" 2>&1 || rc=$?
  grep -E '^(--- FAIL|ok|FAIL)|durable_campaign_test' "${RESULTS%.*}-$name.log" || true
  [ "$rc" = 0 ] || failed=1
  kill "$kf" 2>/dev/null
  wait "$kf" 2>/dev/null || true
  trap - EXIT
done
exit "$failed"
