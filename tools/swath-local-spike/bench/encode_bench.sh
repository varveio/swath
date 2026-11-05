#!/usr/bin/env bash
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
# Pass B: per-part vs per-column parallel encoding (REPORT.md "Per-part parallel encoding").
# usage: BIN=swath-local OUT=dir bench/encode_bench.sh [root]   (warm, REPS interleaved)
set -euo pipefail
R=${1:-/home/user/bench-data/realistic}
REPS=${REPS:-3}; OUT=${OUT:?}; BIN=${BIN:?}
SCR=$OUT/scratch; mkdir -p "$SCR"
RAW=$OUT/raw.tsv
printf 'config\trep\twall_s\tuser_s\tsys_s\tmaxrss_kb\n' > "$RAW"
find "$R" -printf '%s\n' > /dev/null
configs() {
  local o="--out $SCR/out --overwrite --json-stats"
  for sch in part column; do
    for e in 1 2 4; do echo "none-4MB-$sch-enc$e|$BIN list $R $o --order none --part-size 4000000 --encode-scheme $sch --encoders $e"; done
    echo "dfs-parallel-4MB-$sch-enc4|$BIN list $R $o --order dfs-parallel --reorder-rows 100000 --part-size 4000000 --encode-scheme $sch --encoders 4"
    echo "none-64MiB-$sch-enc4|$BIN list $R $o --order none --encode-scheme $sch --encoders 4"
    echo "dfs-parallel-64MiB-$sch-enc4|$BIN list $R $o --order dfs-parallel --reorder-rows 100000 --encode-scheme $sch --encoders 4"
  done
  echo "none-4MB-part-enc4-fastest|$BIN list $R $o --order none --part-size 4000000 --encode-scheme part --encoders 4 --codec go-entropy-fastest"
  echo "dfs-parallel-count-only|$BIN list $R --json-stats --count-only --order dfs-parallel --reorder-rows 100000"
}
for rep in $(seq 1 "$REPS"); do
  configs | while IFS='|' read -r name cmd; do
    /usr/bin/time -f '%e %U %S %M' -o "$SCR/t" bash -c "$cmd" > "$SCR/o"
    read -r wall user sys rss < "$SCR/t"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$rep" "$wall" "$user" "$sys" "$rss" >> "$RAW"
    cp "$SCR/o" "$OUT/json-$name-$rep.json"
    echo "$name #$rep wall=$wall"
  done
done
rm -rf "$SCR"
