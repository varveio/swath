#!/usr/bin/env bash
# Linux count A/B (localfs): SWATH_A against SWATH_B, paired rounds (paired_rounds.py), on an ext4 image
# loop-attached with direct I/O, cold (drop_caches before each arm) and warm.
# Trees: huge (HUGE_FILES files in one directory), realistic (~1M nested), manybig (32 directories x 60,000).
# Two machines: the whole runner, at the default workers and at 64; and a small one (systemd scope: 1 CPU,
# 512 MiB, no swap) at the default workers and at 1. find runs beside them as a reference.
# Env: SWATH_A, SWATH_B, GEN, OUT, ROUNDS (8), SMALL_ROUNDS (3), TREES ("huge realistic manybig").
set -euo pipefail
: "${SWATH_A:?} ${SWATH_B:?} ${GEN:?} ${OUT:?}"
mkdir -p "$OUT"
PAIRED=$(dirname "$0")/paired_rounds.py
ROOT=/mnt/sb IMG=${IMG:-/mnt/sb.img}
export HUGE_FILES=${HUGE_FILES:-1000000}
TREES=${TREES:-huge realistic manybig}

sudo mkdir -p $ROOT
sudo fallocate -l 16G "$IMG"
LOOP=$(sudo losetup --find --show --direct-io=on "$IMG")
sudo mkfs.ext4 -q -F -N 6000000 "$LOOP"
sudo mount "$LOOP" $ROOT; sudo chown "$(id -u):$(id -g)" $ROOT
t0=$(date +%s)
python3 "$GEN" --base $ROOT realistic huge
python3 - $ROOT <<'PY'
import os, sys
for d in range(32):
    p = os.path.join(sys.argv[1], "manybig", f"d{d:02d}")
    os.makedirs(p)
    for f in range(60000):
        open(os.path.join(p, f"entry-{f:05d}"), "w").close()
PY
sync
echo "generated in $(( $(date +%s) - t0 )) s; $(nproc) CPUs, $(free -g | awk '/Mem/{print $2}') GiB" | tee "$OUT/machine.txt"
uname -r >> "$OUT/machine.txt"

DROP='["bash", "-c", "sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null"]'
SMALL='"sudo", "systemd-run", "--scope", "-q", "--uid", "'$(id -u)'", "-p", "MemoryMax=512M", "-p", "MemorySwapMax=0", "-p", "AllowedCPUs=0"'
arm() { # name, prefix (JSON argv items or empty), binary, tree, extra args (JSON items or empty)
  printf '{"name": "%s", "argv": [%s"%s", "diag", "count", "%s", "--no-stats"%s]}' "$1" "${2:+$2, }" "$3" "$ROOT/$4" "${5:+, $5}"
}
findarm() { printf '{"name": "find", "argv": [%s"find", "%s", "-size", "+100000000M"]}' "${2:+$2, }" "$ROOT/$1"; }

summary() {
  python3 - "$1" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print(f"### {sys.argv[1].rsplit('/', 1)[-1][:-5]}\n\n| arm | median s | n | ratio to A | 95 % CI |\n|---|---|---|---|---|")
for k, v in d["arms"].items():
    lo, hi = v["ratio_ci"]
    print(f"| {k} | {v['median']:.2f} | {len(v['times'])} | {v['ratio']:.3f} | {lo:.3f}–{hi:.3f} |")
print()
PY
}

fail=0
for tree in $TREES; do
  for mode in cold warm; do
    d=null wu=1; [ $mode = cold ] && d=$DROP wu=0
    f=$OUT/full-$tree-$mode
    echo "{\"rounds\": ${ROUNDS:-8}, \"warmup\": $wu, \"ref\": \"A\", \"drop\": $d, \"arms\": [
      $(arm B "" "$SWATH_B" $tree), $(arm A "" "$SWATH_A" $tree),
      $(arm "B w64" "" "$SWATH_B" $tree '"--workers", "64"'), $(arm "A w64" "" "$SWATH_A" $tree '"--workers", "64"'),
      $(findarm $tree)]}" > $f.spec
    if python3 "$PAIRED" $f.spec $f.json; then summary $f.json | tee -a "$OUT/summary.md"; else echo "FAILED $f"; fail=1; fi
  done
  f=$OUT/small-$tree-cold
  echo "{\"rounds\": ${SMALL_ROUNDS:-3}, \"warmup\": 0, \"ref\": \"A\", \"drop\": $DROP, \"arms\": [
    $(arm B "$SMALL" "$SWATH_B" $tree), $(arm A "$SMALL" "$SWATH_A" $tree),
    $(arm "B w1" "$SMALL" "$SWATH_B" $tree '"--workers", "1"'), $(arm "A w1" "$SMALL" "$SWATH_A" $tree '"--workers", "1"'),
    $(findarm $tree "$SMALL")]}" > $f.spec
  if python3 "$PAIRED" $f.spec $f.json; then summary $f.json | tee -a "$OUT/summary.md"; else echo "FAILED $f"; fail=1; fi
done
exit $fail
