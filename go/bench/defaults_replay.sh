#!/usr/bin/env bash
# Defaults measured: the default `swath list` argv against the measured argv and Java swath's defaults,
# interleaved, on a replayed recording at a fixed injected page latency (the recording's p50), at the
# host's CPU count and emulating smaller hosts.
#
# usage: defaults_replay.sh BUCKET LATENCY_MS PORT REPS OUTDIR
#   Per rep, per CPU setting (CPUS, default "all 1"), the arms (ARMS, default "default measured java")
#   run in order:
#     default   swath list with no policy flags: no --seed, the CPU-derived workers, the limiter on
#     measured  swath list --seed --workers 64 (every headline arm's argv)
#     java      Java swath list with no tuning flags (--concurrency defaults to 64)
#   A CPU setting N other than "all" runs each arm under `taskset -c 0-(N-1)` and, for Go, GOMAXPROCS=N
#   (the default arm's workers then follow min(64, max(4, 3*N))). Every arm lists s3://BUCKET anonymous,
#   path-style, into a sorted Parquet dataset; extra Go flags in GO_FLAGS, extra Java flags in JAVA_FLAGS.
# Environment: REPLAY_ENV (the replay oracle's env.sh), SWATH (Go binary), JAVA_SWATH (the Java CLI's
#   bin/swath), RECORDINGS (default /home/user/bench-data/recordings).
# Writes OUTDIR/arms.tsv (wall and the replay server's HTTP request count per run) and
# OUTDIR/BUCKET-CPUS-ARM-REP.{json,log}; tabulate with defaults_table.py OUTDIR.
set -u
source "${REPLAY_ENV:?path to the replay env.sh}"
B=$1 LAT=$2 PORT=$3 REPS=$4 W=$5
SW=${SWATH:?path to the Go swath binary}
JSW=${JAVA_SWATH:-}
REC=${RECORDINGS:-/home/user/bench-data/recordings}
CPUS=${CPUS:-all} ARMS=${ARMS:-default measured java}
GO_FLAGS=${GO_FLAGS:-} JAVA_FLAGS=${JAVA_FLAGS:-}
MP=$((PORT + 100)) EP=http://127.0.0.1:$PORT
mkdir -p "$W"
swath-replay serve --fixture "$REC/$B/dataset/data" --bucket "$B" --host 127.0.0.1 --port "$PORT" \
  --serving-mode sorted --metrics-port "$MP" --parquet-connections 8 --max-concurrent-requests 1024 \
  --inject-latency "worker_page=${LAT}ms,pivot_probe=${LAT}ms,structure_probe=${LAT}ms" > "$W/replay.log" 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null' EXIT
until curl -sf "http://127.0.0.1:$MP/healthz" > /dev/null; do sleep 1; done
reqs() {
  curl -s "http://127.0.0.1:$MP/metrics" | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(int(sum(m.get("count", 0) or 0 for m in d["meters"] if m["name"] == "swath.replay.http.requests")))'
}
for rep in $(seq 1 "$REPS"); do
  for cpus in $CPUS; do
    pin=() gmp=()
    if [ "$cpus" != all ]; then
      pin=(taskset -c "0-$((cpus - 1))") gmp=(env "GOMAXPROCS=$cpus")
    fi
    for arm in $ARMS; do
      id=$B-$cpus-$arm-$rep out=$W/out
      rm -rf "$out"
      r0=$(reqs)
      case $arm in
        default|measured)
          af=""
          [ "$arm" = measured ] && af="--seed --workers 64"
          # shellcheck disable=SC2086
          "${gmp[@]}" "${pin[@]}" /usr/bin/time -f '%e %U %S' -o "$W/t" "$SW" list "s3://$B" --endpoint "$EP" --path-style \
            --anonymous --region us-east-1 -o "$out" --report-format json $af $GO_FLAGS \
            > "$W/$id.json" 2> "$W/$id.log" < /dev/null ;;
        java)
          [ -n "$JSW" ] || { echo "JAVA_SWATH unset: skipping the java arm" >&2; continue; }
          # shellcheck disable=SC2086
          "${pin[@]}" /usr/bin/time -f '%e %U %S' -o "$W/t" "$JSW" -v --color never list "s3://$B" --endpoint-url "$EP" \
            --force-path-style --no-sign-request --region us-east-1 --format parquet --sort -o "$out" \
            --report "$W/$id.json" $JAVA_FLAGS > "$W/$id.log" 2>&1 < /dev/null ;;
        *) echo "unknown arm $arm" >&2; exit 2 ;;
      esac
      rc=$?
      n=$(($(reqs) - r0))
      read -r wall usr sys < <(tail -1 "$W/t")
      printf '%s\t%s\t%s\t%s\trc=%s\twall=%s\tcpu=%s\treplay_http=%s\n' "$B" "$cpus" "$arm" "$rep" "$rc" "$wall" \
        "$(awk "BEGIN{print $usr+$sys}")" "$n" | tee -a "$W/arms.tsv"
    done
  done
done
rm -rf "$W/out"
