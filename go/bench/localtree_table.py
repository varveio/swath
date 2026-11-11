#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Tabulate localtree.sh: medians over reps per (tree, binary, buffer).

usage: localtree_table.py [--runs] OUT [OUT...]

Columns: runs (exit 0 / all), wall, user and system CPU, peak RSS, spilled
rows, frames, ranges, rows per frame, opens, opens per frame and per range, the spill
read handles' hits and misses (reports that have them), the drain tail
(resources.drain.tail_ns) and the median 1-minute load before the cells.
--runs prints every run instead of the medians: how much a listing spills
varies with scheduling (on a loaded host a run may spill nothing), so
compare per-file and per-frame counters run by run.
Wall and CPU are medians; the counters are medians too (a listing's spill
varies from run to run with scheduling).
"""
import csv
import json
import os
import statistics
import sys
from collections import defaultdict


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def get(d, path):
    for k in path.split("."):
        if not isinstance(d, dict) or k not in d:
            return None
        d = d[k]
    return d


def main():
    args = sys.argv[1:]
    runs = "--runs" in args
    args = [a for a in args if a != "--runs"]
    cells = defaultdict(list)
    for out in args:
        with open(os.path.join(out, "runs.tsv")) as f:
            for r in csv.DictReader(f, delimiter="\t"):
                rep = None
                p = os.path.join(out, r["report"])
                if os.path.exists(p):
                    with open(p) as g:
                        rep = json.load(g)
                cells[(r["tree"], r["buffer"], r["bin"])].append((r, rep))
    cols = ["tree", "buffer", "bin", "ok", "wall_s", "user_s", "sys_s", "rss_mib", "spill_rows", "frames",
            "ranges", "rows/frame", "opens", "opens/frame", "opens/range", "hits", "misses", "tail_s", "load1"]
    print("| " + " | ".join(cols) + " |")
    print("|" + "---|" * len(cols))
    rows = sorted(cells.items())
    if runs:
        rows = [((t, buf, f"{b} r{r['rep']}"), [(r, rep)]) for (t, buf, b), rs in rows for r, rep in rs]
    for (tree, buf, b), runs in rows:
        ok = [rep for r, rep in runs if r["exit"] == "0" and rep]
        sp = lambda k: med([get(x, "resources.spill." + k) for x in ok])
        srows, frames, opens = sp("rows"), sp("frames"), sp("opens")
        hits = med([get(x, "resources.spill.read_handles.hits") for x in ok])
        misses = med([get(x, "resources.spill.read_handles.misses") for x in ok])
        tail = med([get(x, "resources.drain.tail_ns") for x in ok])
        v = [tree, buf, b, f"{len(ok)}/{len(runs)}",
             med([get(x, "timings.wall_ms") / 1000 for x in ok]),
             med([get(x, "timings.user_cpu_ms") / 1000 for x in ok]),
             med([get(x, "timings.sys_cpu_ms") / 1000 for x in ok]),
             med([get(x, "resources.peak_rss_bytes") / (1 << 20) for x in ok]),
             srows, frames, sp("ranges"),
             srows / frames if srows and frames else None,
             opens, opens / frames if opens and frames else None,
             opens / sp("ranges") if opens and sp("ranges") else None,
             hits, misses,
             tail / 1e9 if tail is not None else None,
             med([float(r["load1"]) for r, _ in runs])]
        print("| " + " | ".join(fmt(x) for x in v) + " |")


def fmt(x):
    if x is None:
        return "-"
    if isinstance(x, float):
        return f"{x:,.2f}" if abs(x) < 1000 else f"{x:,.0f}"
    if isinstance(x, int):
        return f"{x:,}"
    return str(x)


if __name__ == "__main__":
    main()
