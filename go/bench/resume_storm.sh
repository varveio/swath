#!/usr/bin/env bash
# "Resume into a storm": a Go run listing a replayed recording through bench/faultproxy is SIGKILLed during
# a sustained throttle phase, then `swath resume` continues it while the phase goes on; each arm's
# limiter is compared on the resume.
#
# usage: resume_storm.sh BUCKET LATENCY_MS PORT REPS OUTDIR WORKERS...
#   Per rep, per worker count, the arms run interleaved (ARMS, default "shipped slowstart off"):
#     shipped    the limiter as built (no limiter flag)
#     slowstart  --limiter-slow-start (recorded by the list, replayed by the resume)
#     off        --limiter=false
#   Each arm gets a fresh proxy whose window throttle:0s-${PHASE}s:${FRACTION} starts at the list's first
#   request (503 SlowDown to that fraction of requests); the list is SIGKILLed KILL_AT seconds after it
#   starts and resumed at once, through the same proxy, so the phase continues into the resume.
# Environment: REPLAY_ENV (the replay oracle's env.sh), SWATH (Go binary), FAULTPROXY (built
#   bench/faultproxy), RECORDINGS (default $BENCH_DATA/recordings), KILL_AT (s, default 30),
#   PHASE (s from the first request, default KILL_AT+120), FRACTION (default 0.2), LIST_FLAGS (extra
#   `swath list` flags, default "--seed --part-interval 10s"), PIN (a command prefix such as
#   "taskset -c 0", default none), TARGET (an S3 listing endpoint URL such as a bench scale fake: no replay
#   server is started, LATENCY_MS is then unused and BUCKET names the fake's bucket).
# Writes OUTDIR/arms.tsv (one line per run), OUTDIR/BUCKET-W-ARM-REP.{tsv,json,log,proxy,times};
# tabulate with resume_storm_table.py OUTDIR.
set -u
TARGET=${TARGET:-}
[ -n "$TARGET" ] || source "${REPLAY_ENV:?path to the replay env.sh}"
B=$1 LAT=$2 PORT=$3 REPS=$4 W=$5
shift 5
WORKERS=("$@")
[ ${#WORKERS[@]} -gt 0 ] || { echo "no worker counts" >&2; exit 2; }
SW=${SWATH:?path to the Go swath binary}
FP=${FAULTPROXY:?path to the built bench/faultproxy}
REC=${RECORDINGS:-${BENCH_DATA:?set BENCH_DATA to the bench-data directory}/recordings}
KILL_AT=${KILL_AT:-30} FRACTION=${FRACTION:-0.2}
PHASE=${PHASE:-$((KILL_AT + 120))}
LIST_FLAGS=${LIST_FLAGS:---seed --part-interval 10s}
ARMS=${ARMS:-shipped slowstart off}
PIN=${PIN:-}
MP=$((PORT + 100)) PP=$((PORT + 1))
mkdir -p "$W"
if [ -n "$TARGET" ]; then
  UP=$TARGET
else
  UP=http://127.0.0.1:$PORT
  swath-replay serve --fixture "$REC/$B/dataset/data" --bucket "$B" --host 127.0.0.1 --port "$PORT" \
    --serving-mode sorted --metrics-port "$MP" --parquet-connections 8 --max-concurrent-requests 1024 \
    --inject-latency "worker_page=${LAT}ms,pivot_probe=${LAT}ms,structure_probe=${LAT}ms" > "$W/replay.log" 2>&1 &
  SRV=$!
  trap 'kill $SRV 2>/dev/null' EXIT
  until curl -sf "http://127.0.0.1:$MP/healthz" > /dev/null; do sleep 1; done
fi
ms() { date +%s%3N; }
for rep in $(seq 1 "$REPS"); do
  for w in "${WORKERS[@]}"; do
    for arm in $ARMS; do
      case $arm in
        shipped) af="" ;;
        slowstart) af="--limiter-slow-start" ;;
        off) af="--limiter=false" ;;
        *) echo "unknown arm $arm" >&2; exit 2 ;;
      esac
      id=$B-$w-$arm-$rep out=$W/out
      rm -rf "$out"
      "$FP" -target "$UP" -listen "127.0.0.1:$PP" -timeline "$W/$id.tsv" \
        -window "throttle:0s-${PHASE}s:$FRACTION" > "$W/$id.proxy" 2>&1 &
      FPID=$!
      until (: < "/dev/tcp/127.0.0.1/$PP") 2> /dev/null; do sleep 0.2; done
      t0=$(ms)
      # shellcheck disable=SC2086
      $PIN "$SW" list "s3://$B" --endpoint "http://127.0.0.1:$PP" --path-style --anonymous --region us-east-1 \
        --workers "$w" -o "$out" --report-format json $LIST_FLAGS $af > "$W/$id.list.json" 2> "$W/$id.list.log" < /dev/null &
      LPID=$!
      killed=0
      for _ in $(seq 1 $((KILL_AT * 10))); do
        kill -0 "$LPID" 2> /dev/null || break
        sleep 0.1
      done
      if kill -9 "$LPID" 2> /dev/null; then killed=1; fi
      wait "$LPID" 2> /dev/null
      t1=$(ms)
      rc=-
      if [ "$killed" = 1 ]; then
        # shellcheck disable=SC2086
        $PIN "$SW" resume "$out" --report-format json > "$W/$id.json" 2> "$W/$id.log" < /dev/null
        rc=$?
      fi
      t2=$(ms)
      kill "$FPID"; wait "$FPID" 2> /dev/null
      printf 't0=%s t1=%s t2=%s killed=%s\n' "$t0" "$t1" "$t2" "$killed" > "$W/$id.times"
      printf '%s\t%s\t%s\t%s\tkilled=%s\trc=%s\tlist_s=%.1f\tresume_s=%.1f\t%s\n' "$B" "$w" "$arm" "$rep" "$killed" "$rc" \
        "$(awk "BEGIN{print ($t1-$t0)/1000}")" "$(awk "BEGIN{print ($t2-$t1)/1000}")" "$(tail -1 "$W/$id.proxy")" | tee -a "$W/arms.tsv"
    done
  done
done
rm -rf "$W/out"
