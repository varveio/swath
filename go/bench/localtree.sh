#!/usr/bin/env bash
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
#
# Sorted local listings of the spill-shape trees (localtree_gen.py): every
# binary at every --sort-buffer, warm cache, reps outermost and the arms
# shuffled within each rep (interleaved), one run report per cell. Tabulate
# with localtree_table.py. Local host timings: label them so.
#
#   BINS="base=/path/swath-old new=/path/swath" TREES="/data/t/smalldirs /dev/shm/t/bigdirs" \
#   BUFFERS="128MiB 1GiB" REPS=5 OUT=/data/lt bench/localtree.sh
#
# Every run: swath list TREE -o OUT/cell/out --sort-buffer B --encoders
# $ENCODERS --report R -q (the output, and so the spill directory under it,
# is under OUT). Each tree is listed once before the reps (warm-up, not
# kept). A line per cell goes to OUT/runs.tsv: rep, tree, binary, buffer,
# exit code, 1/5/15-minute load (uptime) before the cell, report path.
set -euo pipefail

BINS=${BINS:?BINS=\"name=path ...\"}
TREES=${TREES:?TREES=\"dir ...\"}
BUFFERS=${BUFFERS:-"128MiB 1GiB"}
REPS=${REPS:-5}
ENCODERS=${ENCODERS:-4}
EXTRA=${EXTRA:-}
OUT=${OUT:?OUT=dir}
SEED=${SEED:-1}

mkdir -p "$OUT/reports"
[ -f "$OUT/runs.tsv" ] || printf 'rep\ttree\tbin\tbuffer\texit\tload1\tload5\tload15\treport\n' >"$OUT/runs.tsv"

firstbin=${BINS%% *}
firstbin=${firstbin#*=}
for t in $TREES; do
	rm -rf "$OUT/warm"
	"$firstbin" list "$t" -o "$OUT/warm" -q --encoders "$ENCODERS" >/dev/null 2>&1 || true
done
rm -rf "$OUT/warm"

cells=()
for t in $TREES; do
	for b in $BINS; do
		for sb in $BUFFERS; do
			cells+=("$t|$b|$sb")
		done
	done
done

for rep in $(seq 1 "$REPS"); do
	mapfile -t order < <(printf '%s\n' "${cells[@]}" | shuf --random-source=<(yes "$SEED$rep"))
	for c in "${order[@]}"; do
		IFS='|' read -r t b sb <<<"$c"
		name=${b%%=*}
		bin=${b#*=}
		tn=$(basename "$t")
		fs=$(findmnt -no FSTYPE -T "$t")
		label="$rep.$tn.$fs.$name.$sb"
		out="$OUT/cell"
		rm -rf "$out"
		read -r l1 l5 l15 _ </proc/loadavg
		rc=0
		# shellcheck disable=SC2086
		"$bin" list "$t" -o "$out" -q --sort-buffer "$sb" --encoders "$ENCODERS" \
			--report "$OUT/reports/$label.json" $EXTRA >/dev/null 2>"$OUT/reports/$label.err" || rc=$?
		printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$rep" "$tn.$fs" "$name" "$sb" "$rc" "$l1" "$l5" "$l15" \
			"reports/$label.json" >>"$OUT/runs.tsv"
		echo "$label exit $rc load $l1" >&2
	done
done
rm -rf "$OUT/cell"
