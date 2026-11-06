#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Summarize bench/run.sh raw.tsv into Markdown tables (median of reps).

usage: summarize.py RESULTS_DIR [entries.json]
entries.json maps tree -> {"files": non-dir entries, "dirs": dirs} (from swath-local runs).
"""
import csv
import glob
import json
import os
import statistics
import sys
from collections import defaultdict

res = sys.argv[1]
rows = list(csv.DictReader(open(os.path.join(res, "raw.tsv")), delimiter="\t"))
groups = defaultdict(list)
for r in rows:
    groups[(r["tree"], r["mode"], r["config"])].append(r)

# tree sizes from any swath-local stat-tier JSON
sizes = {}
for f in glob.glob(os.path.join(res, "json-*.json")):
    d = json.load(open(f))
    tree = os.path.basename(d["root"])
    sizes[tree] = (d["walk"]["files"], d["walk"]["dirs"])


def med(rs, k):
    return statistics.median(float(r[k]) for r in rs)


def json_med(tree, mode, cfg, path):
    vals = []
    for f in glob.glob(os.path.join(res, f"json-{tree}-{mode}-{cfg}-[0-9]*.json")):
        d = json.load(open(f))
        for p in path:
            d = d[p]
        vals.append(d)
    return statistics.median(vals) if vals else None


for mode in ("warm", "cold"):
    trees = sorted({t for (t, m, _) in groups if m == mode}, key=["wide", "deep", "realistic"].index)
    for tree in trees:
        files, dirs = sizes.get(tree, (None, None))
        print(f"\n#### {tree} ({mode}{', median of %d' % len(next(v for (t,m,_),v in groups.items() if t==tree and m==mode)) if mode=='warm' else ', single run'})")
        print(f"\nTree: {files} non-directory entries, {dirs} directories.\n")
        print("| config | wall s | user s | sys s | peak RSS MiB | non-dir entries/s | count | walk s | sort+write s |")
        print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        for (t, m, cfg), rs in groups.items():
            if t != tree or m != mode:
                continue
            wall = med(rs, "wall_s")
            rate = f"{files / wall:,.0f}" if files and wall > 0 else ""
            ws = json_med(tree, mode, cfg, ["walk_seconds"])
            ss = json_med(tree, mode, cfg, ["sort_write_seconds"])
            print(f"| {cfg} | {wall:.2f} | {med(rs,'user_s'):.2f} | {med(rs,'sys_s'):.2f} | {med(rs,'maxrss_kb')/1024:.0f} | {rate} | {rs[0]['count']} | "
                  f"{'' if ws is None else f'{ws:.2f}'} | {'' if not ss else f'{ss:.2f}'} |")
