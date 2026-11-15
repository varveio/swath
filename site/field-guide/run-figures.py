#!/usr/bin/env python3
"""Reduces the home page's run trace to the figures the field guide draws (run-figures.json).

The trace is kept outside git (site/README.md, "The run report"). Usage:

    site/field-guide/run-figures.py noaa-gestofs-pds.trace.jsonl > site/field-guide/run-figures.json

Stdlib only. Times are seconds from the trace's run_start; a range's starting range is found by
following its splits back to the seeded range it came from.
"""
import json
import math
import sys

BIN_S = 0.25


def main(path: str) -> None:
    events = [json.loads(line) for line in open(path, encoding="utf-8")]
    t0 = next(e["ts_ns"] for e in events if e["event"] == "run_start")
    seeded = [e for e in events if e["event"] == "seeded"]
    root = {e["id"]: e["id"] for e in seeded}
    for e in events:
        if e["event"] == "split":
            root[e["lower"]] = root[e["upper"]] = root[e["id"]]

    keys = {e["id"]: 0 for e in seeded}
    splits = {e["id"]: 0 for e in seeded}
    for e in events:
        if e["event"] == "page":
            keys[root[e["id"]]] += e["rows"]
        elif e["event"] == "split":
            splits[root[e["id"]]] += 1
    heavy = max(keys, key=keys.get)

    end = next(e["ts_ns"] for e in events if e["event"] == "run_end")
    bins = math.ceil((end - t0) / 1e9 / BIN_S)
    on_heavy = [0.0] * bins
    elsewhere = [0.0] * bins
    heavy_splits = [0] * bins
    for e in events:
        if e["event"] == "page":
            start, stop = (e["ts_ns"] - e["dur_ns"] - t0) / 1e9, (e["ts_ns"] - t0) / 1e9
            series = on_heavy if root[e["id"]] == heavy else elsewhere
            for b in range(int(start / BIN_S), min(bins - 1, int(stop / BIN_S)) + 1):
                overlap = min(stop, (b + 1) * BIN_S) - max(start, b * BIN_S)
                if overlap > 0:
                    series[b] += overlap / BIN_S
        elif e["event"] == "split" and root[e["id"]] == heavy:
            heavy_splits[int((e["ts_ns"] - t0) / 1e9 / BIN_S)] += 1

    seconds = lambda ns: round((ns - t0) / 1e9, 3)
    figures = {
        "source": "noaa-gestofs-pds.trace.jsonl, run " + next(e["time"] for e in events if e["event"] == "run_start"),
        "workers": next(e["workers"] for e in events if e["event"] == "run_start"),
        "objects": sum(keys.values()),
        "heavy": {"index": [e["id"] for e in seeded].index(heavy), "keys": keys[heavy], "splits": splits[heavy]},
        "seed_keys": [keys[e["id"]] for e in seeded],
        "first_split_s": seconds(min(e["ts_ns"] for e in events if e["event"] == "split")),
        "last_range_s": seconds(max(e["ts_ns"] for e in events if e["event"] == "completed")),
        "end_s": seconds(end),
        "bin_s": BIN_S,
        "in_flight_heavy": [round(v, 2) for v in on_heavy],
        "in_flight_other": [round(v, 2) for v in elsewhere],
        "heavy_splits": heavy_splits,
    }
    json.dump(figures, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main(sys.argv[1])
