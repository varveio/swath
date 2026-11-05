#!/usr/bin/env bash
# Before/after measurement for the restructure (REPORT.md "Restructure 2026-11-05").
# usage: BEFORE=old-binary AFTER=new-binary OUT=dir bench/restructure.sh [root]
# Warm cache, REPS interleaved repetitions; prints medians.
set -euo pipefail
R=${1:-/home/user/bench-data/realistic}
REPS=${REPS:-3}; OUT=${OUT:?}; BEFORE=${BEFORE:?}; AFTER=${AFTER:?}
SCR=$OUT/scratch; mkdir -p "$SCR"
RAW=$OUT/raw.tsv
printf 'config\trep\twall_s\tuser_s\tsys_s\tmaxrss_kb\n' > "$RAW"
find "$R" -printf '%s\n' > /dev/null
o="$SCR/out"
configs() {
  echo "before count-only stat (unordered)|$BEFORE list $R --count-only"
  echo "before count-only stat (dfs-parallel 100k)|$BEFORE list $R --count-only --order dfs-parallel --reorder-rows 100000"
  echo "after count-only stat|$AFTER list $R -count-only"
  echo "before count-only names (unordered)|$BEFORE list $R --count-only --fields names"
  echo "before count-only names (dfs-parallel 100k)|$BEFORE list $R --count-only --fields names --order dfs-parallel --reorder-rows 100000"
  echo "after count-only names|$AFTER list $R -count-only -fields names"
  echo "before full 64MiB (dfs-parallel 100k, column)|$BEFORE list $R --order dfs-parallel --reorder-rows 100000 --out $o --overwrite"
  echo "after full 64MiB|$AFTER list $R -out $o -overwrite"
  for e in 1 2 4; do
    echo "before full 4MB enc$e (dfs-parallel 100k, part)|$BEFORE list $R --order dfs-parallel --reorder-rows 100000 --encode-scheme part --part-size 4000000 --encoders $e --out $o --overwrite"
    echo "after full 4MB enc$e|$AFTER list $R -part-size 4000000 -encoders $e -out $o -overwrite"
  done
}
for rep in $(seq 1 "$REPS"); do
  configs | while IFS='|' read -r name cmd; do
    /usr/bin/time -f '%e %U %S %M' -o "$SCR/t" bash -c "$cmd" > /dev/null
    read -r wall user sys rss < "$SCR/t"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$rep" "$wall" "$user" "$sys" "$rss" >> "$RAW"
    echo "$name #$rep wall=$wall"
  done
done
rm -rf "$SCR"
python3 - "$RAW" <<'PY'
import csv, statistics, sys, collections
g = collections.defaultdict(list)
for r in csv.DictReader(open(sys.argv[1]), delimiter="\t"):
    g[r["config"]].append(r)
for c, rs in g.items():
    m = lambda k: statistics.median(float(r[k]) for r in rs)
    print(f"{c:52s} wall={m('wall_s'):.2f} {[r['wall_s'] for r in rs]} user={m('user_s'):.2f} sys={m('sys_s'):.2f} rss={m('maxrss_kb')/1024:.0f}MiB")
PY
