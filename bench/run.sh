#!/usr/bin/env bash
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
# Benchmark swath-local against find, fd and Python on the trees from gen_tree.py.
# Usage: bench/run.sh [warm|cold] [tree...]
#   env: BIN (swath binary built from cmd/swath), BASE ($BENCH_DATA), REPS (3), OUT (results dir), SCRATCH,
#        FILTER (regex on config names, default all)
# Warm: one untimed warm-up of the tree, then every configuration REPS times.
# Cold: `sync; echo 3 > /proc/sys/vm/drop_caches` before each single run.
# Every run appends: tree mode config rep wall user sys maxrss_kb count  (TSV) to $OUT/raw.tsv.
set -euo pipefail
MODE=${1:-warm}; shift || true
TREES=${*:-wide deep realistic}
BIN=${BIN:?set BIN to the swath binary (go build ./cmd/swath)}
BASE=${BASE:-${BENCH_DATA:?set BENCH_DATA to the bench-data directory}}
REPS=${REPS:-3}
OUT=${OUT:?set OUT to a results directory outside the tree}
SCRATCH=${SCRATCH:-/tmp/swath-local-bench}
FILTER=${FILTER:-.*}
mkdir -p "$OUT" "$SCRATCH"
RAW=$OUT/raw.tsv
[ -f "$RAW" ] || printf 'tree\tmode\tconfig\trep\twall_s\tuser_s\tsys_s\tmaxrss_kb\tcount\n' > "$RAW"

PYWALK=$(cd "$(dirname "$0")" && pwd)/oswalk_lstat.py

measure() { # tree config rep cmd...
  local tree=$1 cfg=$2 rep=$3; shift 3
  if [ "$MODE" = cold ]; then sync; echo 3 > /proc/sys/vm/drop_caches; fi
  local tf=$SCRATCH/time.$$ of=$SCRATCH/out.$$
  if ! /usr/bin/time -f '%e %U %S %M' -o "$tf" bash -c "$*" > "$of" 2>&1; then
    echo "FAILED: $tree $cfg: $(tail -3 "$of")" >&2
  fi
  read -r wall user sys rss < <(tail -1 "$tf")
  local count; count=$(grep -oE '^[0-9]+$|"rows": [0-9]+' "$of" | head -1 | grep -oE '[0-9]+' || true)
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$tree" "$MODE" "$cfg" "$rep" "$wall" "$user" "$sys" "$rss" "${count:-}" >> "$RAW"
  # keep swath-local JSON stats for the walk/sort split
  if grep -q '"tool": "swath-local"' "$of"; then cp "$of" "$OUT/json-$tree-$MODE-${cfg//[^A-Za-z0-9=._-]/_}-$rep.json"; fi
  echo "$tree $MODE $cfg #$rep: wall=$wall user=$user sys=$sys rss=${rss}KB count=${count:-}"
}

configs() { # tree -> "name|command" lines
  local r=$1
  echo "find|find $r -type f | wc -l"
  echo "find-printf|find $r -type f -printf '%s %T@ %p\n' | wc -l"
  echo "python-oswalk-lstat|python3 $PYWALK $r"
  if command -v fdfind >/dev/null; then echo "fd|fdfind --type f --hidden --no-ignore . $r | wc -l"; fi
  local sl="$BIN list $r --json-stats"
  echo "sl-walkdir-w1|$sl --count-only --engine walkdir"
  for e in parallel-lstat portable fstatat getdents; do
    for w in 1 2 4 8 12; do echo "sl-$e-w$w|$sl --count-only --engine $e --workers $w"; done
  done
  # metadata cost tiers (fstatat and getdents engines, default workers)
  for e in fstatat getdents; do
    echo "sl-$e-names|$sl --count-only --engine $e --fields names"
    echo "sl-$e-statx-basic|$sl --count-only --engine $e --fields statx --statx-mask basic"
    echo "sl-$e-statx-all|$sl --count-only --engine $e --fields statx --statx-mask all"
  done
  echo "sl-getdents-inosort|$sl --count-only --engine getdents --ino-sort"
  echo "sl-fstatat-nolink|$sl --count-only --engine fstatat --no-link-target"
  # full walk + sort + Parquet (default --encoders = GOMAXPROCS)
  local o="--out $SCRATCH/out-$(basename $r) --overwrite"
  echo "sl-full-fstatat|$sl --engine fstatat $o"
  echo "sl-full-getdents|$sl --engine getdents $o"
  echo "sl-full-fstatat-enc1|$sl --engine fstatat --encoders 1 $o"
  echo "sl-full-getdents-names|$sl --engine getdents --fields names $o"
  echo "sl-full-getdents-statx-all|$sl --engine getdents --fields statx $o"
  # --order experiment (full pipeline; default engine getdents)
  echo "order-none|$sl --order none $o"
  echo "order-none-spill300k|$sl --order none --sort-buffer-rows 300000 $o"
  echo "order-dfs-sequential|$sl --order dfs-sequential $o"
  echo "order-dfs-parallel|$sl --order dfs-parallel $o"
  echo "order-dfs-parallel-w4|$sl --order dfs-parallel --workers 4 $o"
  echo "order-dfs-parallel-r100k|$sl --order dfs-parallel --reorder-rows 100000 $o"
  echo "order-co-dfs-sequential|$sl --order dfs-sequential --count-only"
  echo "order-co-dfs-parallel|$sl --order dfs-parallel --count-only"
}

cold_configs() {
  local r=$1
  configs "$r" | grep -E '^(find|find-printf|fd|python-oswalk-lstat|sl-walkdir-w1|sl-fstatat-w(4|12)|sl-getdents-w(4|12)|sl-getdents-inosort|sl-parallel-lstat-w12|sl-full-getdents)\|'
  echo "sl-getdents-w32|$BIN list $r --json-stats --count-only --engine getdents --workers 32"
  echo "sl-getdents-inosort-w32|$BIN list $r --json-stats --count-only --engine getdents --workers 32 --ino-sort"
}

for t in $TREES; do
  root=$BASE/$t
  if [ "$MODE" = warm ]; then
    find "$root" -printf '%s\n' > /dev/null   # warm the dentry/inode caches
    for rep in $(seq 1 "$REPS"); do
      configs "$root" | grep -E "^($FILTER)\|" | while IFS='|' read -r name cmd; do measure "$t" "$name" "$rep" "$cmd"; done
    done
  else
    cold_configs "$root" | grep -E "^($FILTER)\|" | while IFS='|' read -r name cmd; do measure "$t" "$name" 1 "$cmd"; done
  fi
  rm -rf "$SCRATCH/out-$t"
done
