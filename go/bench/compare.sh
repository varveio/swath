#!/usr/bin/env bash
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
# Pass C: comparison with other listers (REPORT.md "Comparison with other listers").
# usage: BIN=swath OUT=dir bench/compare.sh [tree...]   warm, REPS interleaved
# needs gdu (go install github.com/dundee/gdu/v5/cmd/gdu@latest), cargo-built diskus, dust (du-dust),
# pdu (parallel-disk-usage), dua (dua-cli), and fdfind.
set -euo pipefail
TREES=${*:-realistic wide}
BASE=${BASE:-/home/user/bench-data}; REPS=${REPS:-3}; OUT=${OUT:?}; BIN=${BIN:?}
export PATH=$PATH:$HOME/go/bin:$HOME/.cargo/bin
SCR=$OUT/scratch; mkdir -p "$SCR"
RAW=$OUT/raw.tsv
printf 'tree\tconfig\trep\twall_s\tuser_s\tsys_s\tmaxrss_kb\n' > "$RAW"
configs() {
  local D=$1
  echo "gdu|gdu -n -p -s $D"
  echo "dust|dust -d 0 $D"
  echo "diskus|diskus $D"
  echo "pdu|pdu --max-depth=1 $D"
  echo "dua|dua aggregate $D"
  echo "fd-stat|fdfind -uu --changed-before 2100-01-01 . $D"
  echo "fd-nostat|fdfind -uu . $D"
  echo "find-printf|find $D -type f -printf '%s %T@ %p\n'"
  echo "swath-local-names|$BIN list $D --count-only --fields names"
  echo "swath-local-stat|$BIN list $D --count-only"
  echo "swath-local-full-dfs-parallel|$BIN list $D --order dfs-parallel --reorder-rows 100000 --out $SCR/out --overwrite"
  echo "swath-local-full-none|$BIN list $D --out $SCR/out --overwrite"
}
for t in $TREES; do find "$BASE/$t" -printf '%s\n' > /dev/null; done
for rep in $(seq 1 "$REPS"); do
  for t in $TREES; do
    configs "$BASE/$t" | while IFS='|' read -r name cmd; do
      /usr/bin/time -f '%e %U %S %M' -o "$SCR/t" bash -c "$cmd > /dev/null"
      read -r wall user sys rss < "$SCR/t"
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$t" "$name" "$rep" "$wall" "$user" "$sys" "$rss" >> "$RAW"
      echo "$t $name #$rep wall=$wall"
    done
  done
done
rm -rf "$SCR"
