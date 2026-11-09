#!/usr/bin/env bash
# Local walker benchmark on Windows (NTFS) and macOS (APFS) GitHub runners; driven by
# .github/workflows/localfs-bench-xos.yml. The trees live on a disk image (a VHDX on Windows, an APFS sparse
# image on macOS) that is detached and re-attached before every cold run, which drops that volume's cache.
# Env: SWATH_A, SWATH_B (binaries), GEN (gen_tree.py), OUT, TREES, WARM_REPS, COLD_REPS.
set -uo pipefail
: "${SWATH_A:?} ${SWATH_B:?} ${GEN:?} ${OUT:?}"
TREES=${TREES:-realistic huge}
mkdir -p "$OUT"
T=${RUNNER_TEMP:?}/xos; mkdir -p "$T"

case "$(uname -s)" in
MINGW*|MSYS*|CYGWIN*)
  OS=windows VHD=$(cygpath -w "$T/bench.vhdx")
  export MSYS2_ARG_CONV_EXCL='*' MSYS_NO_PATHCONV=1 # Git Bash would rewrite /s, /c and /L as paths
  dp() { printf '%s\n' "$@" > "$T/dp.txt"; diskpart /s "$(cygpath -w "$T/dp.txt")" > "$T/dp.log" || { cat "$T/dp.log"; return 1; }; }
  dp "create vdisk file=\"$VHD\" maximum=16000 type=expandable" "select vdisk file=\"$VHD\"" "attach vdisk" \
     "create partition primary" "format fs=ntfs quick label=bench" "assign letter=V" || exit 1
  ROOT=V:
  # remount: detach and re-attach the VHDX, then wait for V: (the mount manager keeps the letter)
  WT=$(cygpath -m "$T")
  cat > "$T/remount.ps1" <<EOF
"select vdisk file=\`"$VHD\`"","detach vdisk" | Set-Content $WT/d.txt
"select vdisk file=\`"$VHD\`"","attach vdisk" | Set-Content $WT/a.txt
diskpart /s $WT/d.txt | Out-Null
diskpart /s $WT/a.txt | Out-Null
for (\$i = 0; \$i -lt 100 -and -not (Test-Path V:\\); \$i++) { Start-Sleep -Milliseconds 100 }
EOF
  DROP="pwsh -NoProfile -File $WT/remount.ps1"
  # gen_tree's sparse sizes would be allocated on NTFS (943 GB): files stay empty here
  sed 's/^    if size:$/    if size and os.name != "nt":/' "$GEN" > "$T/gen_tree.py"
  GEN=$T/gen_tree.py
  ;;
Darwin)
  OS=macos IMG=$T/bench.sparseimage
  hdiutil create -quiet -size 16g -fs APFS -volname bench -type SPARSE "$IMG" || exit 1
  hdiutil attach -quiet "$IMG" || exit 1
  ROOT=/Volumes/bench
  printf '#!/bin/sh\nhdiutil detach -quiet /Volumes/bench && sudo purge && hdiutil attach -quiet %s\n' "$IMG" > "$T/remount.sh"
  chmod +x "$T/remount.sh"
  DROP=$T/remount.sh
  ;;
*) echo "unsupported: $(uname -s)" >&2; exit 1 ;;
esac

t0=$(date +%s)
python3 "$GEN" --base "$ROOT/" $TREES > "$OUT/$OS-gen.log" 2>&1 || { cat "$OUT/$OS-gen.log"; exit 1; }
echo "$OS: trees generated in $(( $(date +%s) - t0 )) s"; cat "$OUT/$OS-gen.log"
have() { command -v "$1" >/dev/null 2>&1; }

for tree in $TREES; do
  P=$ROOT/$tree
  names=("$SWATH_A count $P --metadata names --no-stats" "$SWATH_B count $P --metadata names --no-stats"
         "fd -j64 -u ^\$ $P")
  stat=("$SWATH_A count $P --no-stats" "$SWATH_B count $P --no-stats" "fd -j64 -u --search-path $P --size +100Ti")
  if [ $OS = windows ]; then
    # hyperfine -N splits words like a POSIX shell: backslashes are doubled
    W='V:\\'$tree N=$(cygpath -w "$T/none" | sed 's/\\/\\\\/g')
    names+=("cmd /c dir /s /b /a $W > nul")
    stat+=("robocopy $W $N /L /E /NFL /NDL /NJH /NJS /NP /R:0 /MT:64")
  else
    names+=("find $P -false")
    stat+=("find $P -size +100000000M")
    have bfs && { names+=("bfs $P -false"); stat+=("bfs $P -size +100000000M"); }
    have fdu && stat+=("fdu --view=summary --no-gitignore --cache off $P")
  fi
  for mode in warm cold; do
    if [ $mode = warm ]; then H=(-w 1 -r ${WARM_REPS:-6}); else H=(-r ${COLD_REPS:-3} --prepare "$DROP"); fi
    for tier in names stat; do
      if [ $tier = names ]; then arms=("${names[@]}"); else arms=("${stat[@]}"); fi # bash 3.2 on macOS: no declare -n
      # -i: robocopy exits 1 when it would copy files
      hyperfine -N -i "${H[@]}" --output=null --export-json "$OUT/$OS-$tree-$mode-$tier.json" "${arms[@]}" \
        > "$OUT/$OS-$tree-$mode-$tier.log" 2>&1 || { echo "FAILED $OS $tree $mode $tier"; tail -5 "$OUT/$OS-$tree-$mode-$tier.log"; }
    done
  done
  echo "$tree: $($SWATH_B count "$P" 2>&1 | grep -o 'files=[0-9]* dirs=[0-9]*')" | tee -a "$OUT/$OS-counts.txt"
done

python3 - "$OUT" "$OS" <<'EOF' | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
import glob, json, os, sys
out, osn = sys.argv[1:]
print(f"## {osn}\n")
for p in sorted(glob.glob(f"{out}/{osn}-*.json")):
    res = json.load(open(p))["results"]
    best = min(r["mean"] for r in res)
    print(f"### {os.path.basename(p)[:-5]}\n\n| mean (s) | ± | command |\n|---:|---:|---|")
    for r in res:
        m = f"{r['mean']:.3f}"
        print(f"| {'**'+m+'**' if r['mean'] == best else m} | {r['stddev'] or 0:.3f} | `{r['command']}` |")
    print()
EOF
