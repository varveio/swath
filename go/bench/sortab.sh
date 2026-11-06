#!/usr/bin/env bash
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
#
# The ordered-emission vs external-sort A/B (go/bench/sortab; the decision
# record's experiment). Builds the harness (a test binary, never part of
# the command), then runs every cell: reps outermost, arms A and B
# interleaved innermost, each cell under $MEASURE (a lock or a wrapper) if
# set. Results are JSON lines in $OUT; tabulate with sortab_table.py.
#
# Synthetic: every number comes from the trace generator configured here
# (go/bench/sortab/trace_test.go, Shape); label it so.
#
#   SHAPES="slowfirst slowlast flat skewed" WORKERS="64 256 512" REPS=3 \
#   KEYS=200000000 KILL="0 peak" WORK=/data/sortab OUT=ab.jsonl bench/sortab.sh
#
# KILL lists the kill modes: 0 (uninterrupted) and/or peak (SIGKILL at peak
# speculation, then resume): slowfirst/slowlast at 1 - SLOW_SHARE/2 of the
# keys listed (the fast ranges done, the slow one half), flat and skewed
# at 0.5; or a fraction.
set -euo pipefail
cd "$(dirname "$0")/.."

SHAPES=${SHAPES:-slowfirst}
WORKERS=${WORKERS:-8}
REPS=${REPS:-1}
KILL=${KILL:-"0 peak"}
ARMS=${ARMS:-"A B"}
KEYS=${KEYS:-3000000}
RANGES=${RANGES:-300}
PAGE_KEYS=${PAGE_KEYS:-1000}
LATENCY_MS=${LATENCY_MS:-50}
SLOW_LATENCY_MS=${SLOW_LATENCY_MS:-500}
SLOW_SHARE=${SLOW_SHARE:-0.02}   # slowfirst/slowlast: the slow range's share of the keys
SKEW=${SKEW:-1.1}
JITTER=${JITTER:-0.3}
SEED=${SEED:-7}
MEMORY=${MEMORY:-268435456}      # --memory (the product default, 256 MiB)
ENCODERS=${ENCODERS:-4}
PART_SIZE=${PART_SIZE:-8388608}
SPILL_MAX=${SPILL_MAX:-67108864} # A's live spill cap; B's runs cannot be capped
SPILL_RESERVE=${SPILL_RESERVE:-0}
PART_INTERVAL_MS=${PART_INTERVAL_MS:-0} # 0: the default (60 s)
MIN_FREE_GB=${MIN_FREE_GB:-2}    # refuse a cell below this much free disk in WORK
WORK=${WORK:-$(mktemp -d)}
OUT=${OUT:-$WORK/sortab.jsonl}
MEASURE=${MEASURE:-}
KEEP_LOGS=${KEEP_LOGS:-0}

mkdir -p "$WORK"
BIN=$WORK/sortab.test
go test -c -o "$BIN" ./bench/sortab
echo "harness $BIN, results $OUT" >&2

killfrac() { # shape mode
  case $2 in
    0) echo 0 ;;
    peak) case $1 in
            slowfirst|slowlast) python3 -c "print(1 - $SLOW_SHARE/2)" ;;
            *) echo 0.5 ;;
          esac ;;
    *) echo "$2" ;;
  esac
}

cell=0
for rep in $(seq 1 "$REPS"); do
  for shape in $SHAPES; do
    for w in $WORKERS; do
      for kill in $KILL; do
        for arm in $ARMS; do
          cell=$((cell + 1))
          free=$(df -Pk "$WORK" | awk 'NR==2 {print int($4/1048576)}')
          if [ "$free" -lt "$MIN_FREE_GB" ]; then
            echo "only ${free} GiB free in $WORK (MIN_FREE_GB=$MIN_FREE_GB): stopping" >&2
            exit 1
          fi
          label="r${rep}-${shape}-w${w}-k${kill}-${arm}"
          dir=$WORK/cells/$label
          mkdir -p "$WORK/cells"
          slow=$(python3 -c "print(int($KEYS * $SLOW_SHARE))")
          frac=$(killfrac "$shape" "$kill")
          cat > "$WORK/cells/$label.json" <<JSON
{"label":"$label","arm":"$arm","rep":$rep,"kill_frac":$frac,"dir":"$dir",
 "shape":{"name":"$shape","keys":$KEYS,"ranges":$RANGES,"page_keys":$PAGE_KEYS,"latency_ms":$LATENCY_MS,
          "slow_latency_ms":$SLOW_LATENCY_MS,"slow_keys":$slow,"skew":$SKEW,"jitter":$JITTER,"seed":$SEED},
 "limits":{"workers":$w,"memory":$MEMORY,"encoders":$ENCODERS,"part_size":$PART_SIZE,"spill_max":$SPILL_MAX,
           "spill_reserve":$SPILL_RESERVE,"part_interval_ms":$PART_INTERVAL_MS}}
JSON
          echo "[$cell] $label" >&2
          # shellcheck disable=SC2086
          SORTAB_DRIVE=$WORK/cells/$label.json SORTAB_OUT=$OUT $MEASURE "$BIN" || echo "  cell $label failed (see $OUT)" >&2
          [ "$KEEP_LOGS" = 1 ] || rm -rf "$dir"
        done
      done
    done
  done
done
echo "done: $OUT" >&2
