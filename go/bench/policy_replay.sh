#!/usr/bin/env bash
# Policy measurement on a replayed recording: Java swath and Go swath arms, interleaved, against
# swath-replay serving a Java recording with a fixed injected page latency (the recording's p50).
#
# usage: policy_replay.sh ARMS BUCKET LATENCY_MS PORT REPS OUTDIR
#   ARMS is a file, one arm per line: NAME<TAB>KIND<TAB>ARGS, where KIND is
#     java   ARGS are extra Java `swath list` flags (e.g. the --engine-toggle set; empty for defaults)
#     go     ARGS are "BINARY extra flags" for `BINARY diag list` (e.g. "/path/swath --probe")
#   Every arm lists s3://BUCKET through the replay endpoint, anonymous, path-style, writes a sorted
#   Parquet dataset, and Go arms run 64 workers with the seed. Arms run in file order, REPS times.
# Needs REPLAY_ENV (the replay oracle's env.sh, notes runbook) and the recording under RECORDINGS
# (default $BENCH_DATA/recordings). Writes OUTDIR/arms.tsv (wall and the replay server's HTTP
# request count per arm and rep) and OUTDIR/NAME-REP.json (the run report); tabulate with
# policy_table.py OUTDIR.
set -u
source "${REPLAY_ENV:?path to the replay env.sh}"
ARMS=$1 B=$2 LAT=$3 PORT=$4 REPS=$5 W=$6
REC=${RECORDINGS:-${BENCH_DATA:?set BENCH_DATA to the bench-data directory}/recordings}
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
  while IFS=$'\t' read -r name kind args; do
    [ -z "$name" ] || [ "${name:0:1}" = "#" ] && continue
    r0=$(reqs) out=$W/out
    rm -rf "$out"
    case $kind in
      java)
        # shellcheck disable=SC2086
        /usr/bin/time -f '%e' -o "$W/t" swath -v --color never list "s3://$B" --endpoint-url "$EP" --force-path-style \
          --no-sign-request --region us-east-1 --format parquet --sort -o "$out" --report "$W/$name-$rep.json" $args \
          > "$W/$name-$rep.log" 2>&1 < /dev/null ;;
      go)
        read -r bin flags <<< "$args"
        # shellcheck disable=SC2086
        /usr/bin/time -f '%e' -o "$W/t" "$bin" diag list "s3://$B" --endpoint "$EP" --path-style --anonymous \
          --region us-east-1 --workers 64 --seed -o "$out" --report-format json $flags \
          > "$W/$name-$rep.json" 2> "$W/$name-$rep.log" < /dev/null ;;
      *) echo "unknown arm kind $kind" >&2; exit 2 ;;
    esac
    rc=$?
    n=$(($(reqs) - r0))
    printf '%s\t%s\t%s\trc=%s\twall=%s\treplay_http=%s\n' "$B" "$name" "$rep" "$rc" "$(tail -1 "$W/t")" "$n" | tee -a "$W/arms.tsv"
  done < "$ARMS"
done
rm -rf "$W/out"
