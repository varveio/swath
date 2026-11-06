#!/usr/bin/env bash
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
# Before/after measurement for the ordered-walk optimization (REPORT.md
# "Ordered walk optimization"). Warm cache, REPS interleaved repetitions.
# usage: BEFORE=bin AFTER=bin OUT=dir bench/order_opt.sh [root]
set -euo pipefail
R=${1:-${BENCH_DATA:?set BENCH_DATA to the bench-data directory}/realistic}
REPS=${REPS:-3}
OUT=${OUT:?}
SCR=$OUT/scratch
mkdir -p "$OUT" "$SCR"
RAW=$OUT/raw.tsv
printf 'config\trep\twall_s\tuser_s\tsys_s\tmaxrss_kb\n' > "$RAW"
find "$R" -printf '%s\n' > /dev/null
configs() {
  local o="--out $SCR/out --overwrite"
  for b in before after; do
    local B; [ $b = before ] && B=$BEFORE || B=$AFTER
    echo "$b unordered-count-only|$B list $R --json-stats --count-only"
    echo "$b dfs-parallel-count-only-r100k|$B list $R --json-stats --count-only --order dfs-parallel --reorder-rows 100000"
    echo "$b dfs-parallel-full-r100k|$B list $R --json-stats --order dfs-parallel --reorder-rows 100000 $o"
    echo "$b dfs-sequential-count-only|$B list $R --json-stats --count-only --order dfs-sequential"
    echo "$b dfs-sequential-full|$B list $R --json-stats --order dfs-sequential $o"
    echo "$b none-full|$B list $R --json-stats --order none $o"
  done
  echo "after none-full-enc1|$AFTER list $R --json-stats --order none --encoders 1 $o"
}
for rep in $(seq 1 "$REPS"); do
  configs | while IFS='|' read -r name cmd; do
    /usr/bin/time -f '%e %U %S %M' -o "$SCR/t" bash -c "$cmd" > "$SCR/o"
    read -r wall user sys rss < "$SCR/t"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$rep" "$wall" "$user" "$sys" "$rss" >> "$RAW"
    cp "$SCR/o" "$OUT/json-${name// /_}-$rep.json"
    echo "$name #$rep wall=$wall rss=${rss}KB"
  done
done
rm -rf "$SCR"
