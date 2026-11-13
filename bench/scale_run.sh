#!/usr/bin/env bash
# Scale tier measured run: the Go binary lists a lazy synthetic keyspace (bench/scalefake: no dataset on
# disk) through the S3 route at W workers with the shipped defaults, sampled every INTERVAL
# seconds. Synthetic: label every number from it so, with the shape line from OUTDIR/meta.txt.
#
# usage: scale_run.sh OUTDIR KEYS WORKERS PORT
# env:   SWATH       the binary (default: built from this tree into OUTDIR/bin)
#        SHAPE       scalefake shape flags (default "-fanout 32,32,32,8 -skew 1": deep, skewed)
#        LATENCY     injected per-page latency (default 30ms; JITTER default 20ms; FAIL503 default 0)
#        SWATH_FLAGS extra swath list flags (default none: the shipped defaults)
#        SWATH_WRAP  a command prefix for swath only, not the fake (e.g. "prlimit --nofile=256:256" or
#                    "systemd-run --user --scope -p MemoryMax=1536M"); RSS is sampled from swath's own pid
#        INTERVAL    sampling period in seconds (default 10)
#        DATA        the output dataset directory (default OUTDIR/out; removed at the end unless KEEP=1)
# Writes OUTDIR/meta.txt (the fake's shape line, argv, host), samples.jsonl (one line per INTERVAL: RSS,
# progress.json, the fake's accounting), report.json (the run report), fake-final.json, swath.log,
# fake.log. Tabulate with scale_table.py OUTDIR... . Run timing campaigns under the host's measurement lock.
set -u
OUT=$1 KEYS=$2 W=$3 PORT=$4
HERE=$(cd "$(dirname "$0")" && pwd)
SHAPE=${SHAPE:--fanout 32,32,32,8 -skew 1}
LATENCY=${LATENCY:-30ms} JITTER=${JITTER:-20ms} FAIL503=${FAIL503:-0}
INTERVAL=${INTERVAL:-10}
DATA=${DATA:-$OUT/out}
mkdir -p "$OUT/bin"
if [ -z "${SWATH:-}" ]; then
  (cd "$HERE/.." && go build -o "$OUT/bin/swath" ./cmd/swath) || exit 2
  SWATH=$OUT/bin/swath
fi
(cd "$HERE/.." && go build -o "$OUT/bin/scalefake" ./bench/scalefake) || exit 2
EP=http://127.0.0.1:$PORT
# shellcheck disable=SC2086
"$OUT/bin/scalefake" -addr "127.0.0.1:$PORT" -keys "$KEYS" $SHAPE -latency "$LATENCY" -jitter "$JITTER" \
  -fail503 "$FAIL503" > "$OUT/fake.log" 2>&1 &
FAKE=$!
trap 'kill $FAKE 2>/dev/null' EXIT
until curl -sf "$EP/__scalefake/stats" > /dev/null; do
  kill -0 $FAKE 2>/dev/null || { cat "$OUT/fake.log" >&2; exit 2; }
  sleep 0.2
done
rm -rf "$DATA"
ARGV=(list s3://bkt --endpoint-url "$EP" --s3-path-style --anonymous --s3-region us-east-1 --workers "$W"
  -o "$DATA" --report "$OUT/report.json")
# shellcheck disable=SC2206
ARGV+=(${SWATH_FLAGS:-})
{
  echo "synthetic: $(head -1 "$OUT/fake.log")"
  echo "latency=$LATENCY jitter=$JITTER fail503=$FAIL503 interval=${INTERVAL}s"
  echo "argv: ${SWATH_WRAP:-}${SWATH_WRAP:+ }$SWATH ${ARGV[*]}"
  echo "host: $(nproc) cores, $(free -g | awk '/Mem:/{print $2}') GiB, $(uname -r), $("$SWATH" --version 2>&1 | head -1)"
  echo "revision: $(git -C "$HERE" rev-parse --short HEAD)$(git -C "$HERE" diff --quiet HEAD -- . || echo -dirty)"
  echo "started: $(date -u +%FT%TZ)"
} > "$OUT/meta.txt"
curl -sf "$EP/__scalefake/stats?reset=1" > /dev/null
# shellcheck disable=SC2086
${SWATH_WRAP:-} "$SWATH" "${ARGV[@]}" > "$OUT/stdout.json" 2> "$OUT/swath.log" < /dev/null &
PID=$!
if [ -n "${SWATH_WRAP:-}" ]; then # sample swath itself, not the wrapper
  for _ in $(seq 50); do
    P=$(pgrep -n -f -- "$SWATH list s3://bkt --endpoint-url $EP") && { [ "$P" = "$PID" ] || PID_RSS=$P; break; } # an exec keeps the pid
    sleep 0.1
  done
fi
python3 "$HERE/scale_sample.py" "${PID_RSS:-$PID}" "$DATA/.swath/progress.json" "$EP/__scalefake/stats" "$INTERVAL" > "$OUT/samples.jsonl" &
SAMPLER=$!
wait $PID
RC=$?
wait $SAMPLER
curl -sf "$EP/__scalefake/stats" > "$OUT/fake-final.json"
{
  echo "finished: $(date -u +%FT%TZ) rc=$RC"
  du -sb "$DATA" 2>/dev/null | awk '{print "output_bytes: " $1}'
} >> "$OUT/meta.txt"
[ "${KEEP:-0}" = 1 ] || rm -rf "$DATA"
echo "rc=$RC $OUT"
exit $RC
