#!/usr/bin/env bash
# Local walker benchmark on Windows (NTFS) and macOS (APFS) GitHub runners; driven by
# .github/workflows/localfs-bench-xos.yml. The trees live on a disk image (a VHDX on Windows, an APFS sparse
# image on macOS) that is detached and re-attached before every cold run, which drops that volume's cache.
# Env: SWATH_A, SWATH_B (binaries), GEN (gen_tree.py), OUT, TREES (all huge), HUGE_FILES, PEER_ROUNDS.
set -uo pipefail
: "${SWATH_A:?} ${SWATH_B:?} ${GEN:?} ${OUT:?}"
TREES=${TREES:-all huge}
mkdir -p "$OUT"
T=${RUNNER_TEMP:?}/xos; mkdir -p "$T"

case "$(uname -s)" in
MINGW*|MSYS*|CYGWIN*)
  OS=windows VHD=$(cygpath -w "$T/bench.vhdx")
  export MSYS2_ARG_CONV_EXCL='*' MSYS_NO_PATHCONV=1 # Git Bash would rewrite /s, /c and /L as paths
  dp() { printf '%s\n' "$@" > "$T/dp.txt"; diskpart /s "$(cygpath -w "$T/dp.txt")" > "$T/dp.log" || { cat "$T/dp.log"; return 1; }; }
  dp "create vdisk file=\"$VHD\" maximum=48000 type=expandable" "select vdisk file=\"$VHD\"" "attach vdisk" \
     "create partition primary" "format fs=ntfs quick label=bench" "assign letter=V" || exit 1
  ROOT=V:
  # remount: detach and re-attach the VHDX, then wait for V: (the mount manager keeps the letter)
  WT=$(cygpath -m "$T")
  cat > "$T/remount.ps1" <<EOF
\$ErrorActionPreference = "Stop"
"select vdisk file=\`"$VHD\`"","detach vdisk" | Set-Content $WT/d.txt
"select vdisk file=\`"$VHD\`"","attach vdisk" | Set-Content $WT/a.txt
diskpart /s $WT/d.txt | Out-Null; if (\$LASTEXITCODE) { throw "detach failed" }
if (Test-Path V:\\) { throw "V: still present after detach" }
diskpart /s $WT/a.txt | Out-Null; if (\$LASTEXITCODE) { throw "attach failed" }
for (\$i = 0; \$i -lt 100 -and -not (Test-Path V:\\huge); \$i++) { Start-Sleep -Milliseconds 100 }
if (-not (Test-Path V:\\huge)) { throw "V: not back after attach" }
EOF
  DROP_SCRIPT=$WT/remount.ps1
  # gen_tree's sparse sizes would be allocated on NTFS (943 GB): files stay empty here
  sed 's/^    if size:$/    if size and os.name != "nt":/' "$GEN" > "$T/gen_tree.py"
  GEN=$T/gen_tree.py
  ;;
Darwin)
  OS=macos IMG=$T/bench.sparseimage
  hdiutil create -quiet -size 48g -fs APFS -volname bench -type SPARSE "$IMG" || exit 1
  # Spotlight indexes a new volume and keeps it busy (detach fails with EBUSY): indexing off, forced detach
  hdiutil attach -quiet -nobrowse "$IMG" || exit 1
  ROOT=/Volumes/bench
  sudo mdutil -i off $ROOT >/dev/null
  cat > "$T/remount.sh" <<EOF
#!/bin/sh
# a detach can still find the volume busy for a moment: retry
set -e
for i in 1 2 3 4 5 6 7 8 9 10; do hdiutil detach -quiet $ROOT && break; sleep 1; done
[ -d $ROOT ] && { echo "forced detach" >&2; hdiutil detach -quiet -force $ROOT; }
[ ! -d $ROOT ]
sudo purge
hdiutil attach -quiet -nobrowse "$IMG"
sudo mdutil -i off $ROOT >/dev/null
[ -d $ROOT/huge ]
EOF
  chmod +x "$T/remount.sh"
  DROP_SCRIPT=$T/remount.sh
  ;;
*) echo "unsupported: $(uname -s)" >&2; exit 1 ;;
esac

t0=$(date +%s)
# Runs under a second are mostly noise: "all" is wide + deep + realistic (~2.5M entries) walked as one tree,
# "huge" is HUGE_FILES (1M) files in one directory.
export HUGE_FILES=${HUGE_FILES:-1000000}
{ python3 "$GEN" --base "$ROOT/all" wide deep realistic && python3 "$GEN" --base "$ROOT/" huge; } > "$OUT/$OS-gen.log" 2>&1 \
  || { cat "$OUT/$OS-gen.log"; exit 1; }
echo "$OS: trees generated in $(( $(date +%s) - t0 )) s"; cat "$OUT/$OS-gen.log"
# Paired, interleaved rounds: xos_arms.py builds the arms, paired_rounds.py times them.
export SWATH_A SWATH_B TREES
python3 "$(dirname "$0")/xos_arms.py" "$OUT" "$OS" "$ROOT" "$(cygpath -m "$T" 2>/dev/null || echo "$T")" "$DROP_SCRIPT" \
  | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
