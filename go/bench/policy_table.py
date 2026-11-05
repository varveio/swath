#!/usr/bin/env python3
"""Tabulate a policy_replay.sh output directory: one Markdown row per arm, median (min-max) over reps.

usage: policy_table.py OUTDIR [ARM...]   (arms in the given order; default: the order in arms.tsv)

Wall and attempts come from arms.tsv (process wall, the replay server's HTTP request count); the rest
from each run report through summaries.row, which maps Java's meters onto the Go columns: splits are
Java's splits plus steals, empty children Java's CHILD_MASS/empty, overfetched keys Java's
swath.page.raw_keys less the objects listed (keys fetched past a range's bound and dropped).
"""
import collections, json, os, statistics, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from summaries import row  # noqa: E402


def rng(xs, f="{:,.0f}"):
    xs = [x for x in xs if x is not None]
    if not xs:
        return ""
    s = f.format(statistics.median(xs))
    return s if len(xs) < 2 else f"{s} ({f.format(min(xs))}–{f.format(max(xs))})"


def main():
    d = sys.argv[1]
    order, tsv = [], collections.defaultdict(list)
    for line in open(os.path.join(d, "arms.tsv")):
        b, arm, rep, rc, wall, http = line.rstrip("\n").split("\t")
        if arm not in order:
            order.append(arm)
        tsv[arm].append((rep, rc.split("=")[1], float(wall.split("=")[1]), int(http.split("=")[1])))
    arms = sys.argv[2:] or order
    print("| Arm | n | Wall s | Requests | Attempts | Splits | Empty children | Overfetched keys | Probes | Mean active |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for arm in arms:
        runs = [(t, row(os.path.join(d, f"{arm}-{t[0]}.json"))) for t in tsv[arm] if t[1] == "0"]
        bad = len(tsv[arm]) - len(runs)
        col = lambda k: [float(r[k]) if r.get(k) not in (None, "") else None for _, r in runs]
        splits = [float(r["splits"] or 0) + float(r["steals"] or 0) for _, r in runs]
        print(f"| {arm}{f' ({bad} failed)' if bad else ''} | {len(runs)} | {rng([t[2] for t, _ in runs], '{:,.1f}')} "
              f"| {rng(col('api_calls'))} | {rng([t[3] for t, _ in runs])} | {rng(splits)} | {rng(col('empty_children'))} "
              f"| {rng(col('overfetch_keys'))} | {rng(col('probe_requests'))} | {rng(col('avg_in_flight'), '{:.1f}')} |")


if __name__ == "__main__":
    main()
