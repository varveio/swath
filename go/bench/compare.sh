#!/usr/bin/env bash
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
# Compare the local walker with other listers on the trees from gen_tree.py
# (docs/measurements.md, "Local filesystem walker"). Warm cache, REPS rounds, configurations interleaved.
# usage: BIN=swath OUT=dir bench/compare.sh [tree...]
#   env: BASE ($BENCH_DATA), REPS (3), FILTER (regex on config names, default all)
# Optional tools are skipped when missing: fdfind (fd-find), bfs, gdu, dust, diskus, pdu, dua.
# Every run appends: tree config rep wall user sys maxrss_kb count to $OUT/raw.tsv. count is the
# non-directory entries the tool reported (a cross-check: every lister on a tree should agree).
set -euo pipefail
TREES=${*:-realistic wide}
BASE=${BASE:-${BENCH_DATA:?set BENCH_DATA to the bench-data directory}}
REPS=${REPS:-3}; OUT=${OUT:?set OUT to a results directory outside the tree}; BIN=${BIN:?set BIN to the swath binary}
FILTER=${FILTER:-.*}
export PATH=$PATH:$HOME/go/bin:$HOME/.cargo/bin
PYWALK=$(cd "$(dirname "$0")" && pwd)/oswalk_lstat.py
SCR=$OUT/scratch; mkdir -p "$SCR"
RAW=$OUT/raw.tsv
[ -f "$RAW" ] || printf 'tree\tconfig\trep\twall_s\tuser_s\tsys_s\tmaxrss_kb\tcount\n' > "$RAW"
have() { command -v "$1" > /dev/null; }

# name|command: a command that prints the non-directory count last (or nothing, for the disk-usage tools)
configs() {
  local D; D=$(printf '%q' "$1") # a path with spaces stays one argument of bash -c
  local W S; W=$(printf '%q' "$PYWALK"); S=$(printf '%q' "$SCR/out")
  # names only: the entry type from the directory entry, no stat
  echo "find-names|find $D -mindepth 1 ! -type d | wc -l"
  if have bfs; then echo "bfs-names|bfs $D -mindepth 1 ! -type d | wc -l"; fi
  if have fdfind; then echo "fd-names|fdfind -uu -t f -t l -t p -t s . $D | wc -l"; fi
  echo "swath-names|$BIN count $D --metadata names --no-stats"
  # one stat per entry
  echo "find-printf|find $D -mindepth 1 ! -type d -printf '%s %T@ %p\n' | wc -l"
  if have bfs; then echo "bfs-printf|bfs $D -mindepth 1 ! -type d -printf '%s %T@ %p\n' | wc -l"; fi
  if have fdfind; then echo "fd-stat|fdfind -uu -t f -t l -t p -t s --changed-before 2100-01-01 . $D | wc -l"; fi
  echo "python-oswalk-lstat|python3 $W $D"
  echo "swath-stat|$BIN count $D --no-stats"
  echo "swath-stat-w4|$BIN count $D --workers 4 --no-stats"
  echo "swath-stat-portable|SWATH_LOCALFS_ENGINE=portable $BIN count $D --no-stats"
  echo "swath-statx|$BIN count $D --metadata statx --no-stats"
  echo "swath-parquet|$BIN list $D -o $S --overwrite --no-stats > /dev/null && echo"
  # disk-usage tools: stat everything, sum sizes, print no rows
  if have gdu; then echo "gdu|gdu -n -p -s $D > /dev/null"; fi
  if have dust; then echo "dust|dust -d 0 $D > /dev/null"; fi
  if have diskus; then echo "diskus|diskus $D > /dev/null"; fi
  if have pdu; then echo "pdu|pdu --max-depth=1 $D > /dev/null"; fi
  if have dua; then echo "dua|dua aggregate $D > /dev/null"; fi
}

count_of() { # last line of output -> a count, from wc -l, python's print, or swath count's files=N
  local last; last=$(tail -1 "$1")
  if [[ $last =~ files=([0-9]+) ]]; then echo "${BASH_REMATCH[1]}"; elif [[ $last =~ ^[[:space:]]*([0-9]+)[[:space:]]*$ ]]; then echo "${BASH_REMATCH[1]}"; fi
}

for t in $TREES; do find "$BASE/$t" -printf '%s\n' > /dev/null; done   # warm the dentry and inode caches
for rep in $(seq 1 "$REPS"); do
  for t in $TREES; do
    configs "$BASE/$t" | grep -E "^($FILTER)\|" | while IFS='|' read -r name cmd; do
      if ! /usr/bin/time -f '%e %U %S %M' -o "$SCR/t" bash -c "$cmd" > "$SCR/o" 2>&1; then
        echo "FAILED: $t $name: $(tail -3 "$SCR/o")" >&2
        continue # a failed run is not a measurement: not in raw.tsv
      fi
      read -r wall user sys rss < <(tail -1 "$SCR/t")
      count=$(count_of "$SCR/o")
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$t" "$name" "$rep" "$wall" "$user" "$sys" "$rss" "$count" >> "$RAW"
      printf '%-10s %-22s #%s wall=%-6s user=%-6s sys=%-6s rss=%sKB count=%s\n' "$t" "$name" "$rep" "$wall" "$user" "$sys" "$rss" "$count"
    done
  done
done
rm -rf "$SCR"
