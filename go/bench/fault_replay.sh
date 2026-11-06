#!/usr/bin/env bash
# Fault-injection measurement on a replayed recording: Go arms listed through bench/faultproxy in front of
# swath-replay serving a Java recording at a fixed injected page latency (the recording's p50).
#
# usage: fault_replay.sh ARMS BUCKET LATENCY_MS PORT REPS OUTDIR
#   ARMS is a file, one arm per line: NAME<TAB>BINARY [swath diag list flags]<TAB>[faultproxy flags]
#   e.g.  storm-on<TAB>/tmp/swath<TAB>-window throttle:5s-15s:0.5
#   Every arm lists s3://BUCKET through a fresh proxy (so its windows start at the run's first request),
#   anonymous, path-style, 64 workers with the seed, sorted Parquet. Arms run in file order, REPS times.
# Needs REPLAY_ENV (the replay oracle's env.sh) and the recording under RECORDINGS (default
# /home/user/bench-data/recordings); FAULTPROXY is the built bench/faultproxy binary. Writes
# OUTDIR/arms.tsv (exit code, wall, the proxy's totals), OUTDIR/NAME-REP.json (run report) and
# OUTDIR/NAME-REP.tsv (the proxy's timeline).
set -u
source "${REPLAY_ENV:?path to the replay env.sh}"
ARMS=$1 B=$2 LAT=$3 PORT=$4 REPS=$5 W=$6
FP=${FAULTPROXY:?path to the built bench/faultproxy}
REC=${RECORDINGS:-/home/user/bench-data/recordings}
MP=$((PORT + 100)) PP=$((PORT + 1))
mkdir -p "$W"
swath-replay serve --fixture "$REC/$B/dataset/data" --bucket "$B" --host 127.0.0.1 --port "$PORT" \
  --serving-mode sorted --metrics-port "$MP" --parquet-connections 8 --max-concurrent-requests 1024 \
  --inject-latency "worker_page=${LAT}ms,pivot_probe=${LAT}ms,structure_probe=${LAT}ms" > "$W/replay.log" 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null' EXIT
until curl -sf "http://127.0.0.1:$MP/healthz" > /dev/null; do sleep 1; done
for rep in $(seq 1 "$REPS"); do
  while IFS=$'\t' read -r name args proxy; do
    [ -z "$name" ] || [ "${name:0:1}" = "#" ] && continue
    # shellcheck disable=SC2086
    "$FP" -target "http://127.0.0.1:$PORT" -listen "127.0.0.1:$PP" -timeline "$W/$name-$rep.tsv" $proxy \
      > "$W/$name-$rep.proxy" 2>&1 &
    FPID=$!
    until (: < "/dev/tcp/127.0.0.1/$PP") 2> /dev/null; do sleep 0.2; done   # no request: windows start at the run's first
    out=$W/out
    rm -rf "$out"
    read -r bin flags <<< "$args"
    # shellcheck disable=SC2086
    /usr/bin/time -f '%e' -o "$W/t" "$bin" diag list "s3://$B" --endpoint "http://127.0.0.1:$PP" --path-style \
      --anonymous --region us-east-1 --workers 64 --seed -o "$out" --report-format json $flags \
      > "$W/$name-$rep.json" 2> "$W/$name-$rep.log" < /dev/null
    rc=$?
    kill "$FPID"; wait "$FPID" 2>/dev/null
    printf '%s\t%s\t%s\trc=%s\twall=%s\t%s\n' "$B" "$name" "$rep" "$rc" "$(tail -1 "$W/t")" \
      "$(tail -1 "$W/$name-$rep.proxy")" | tee -a "$W/arms.tsv"
  done < "$ARMS"
done
rm -rf "$W/out"
