#!/usr/bin/env python3
"""Tabulate scale_run.sh runs: per run, the totals, requests per 1k keys and the plateau check; with
--series, the 10 s time series. Synthetic numbers: quote them with each run's shape line (meta.txt).

usage: scale_table.py [--series] [--rss-slack MiB] [--nodes-per-worker N] [--age-slack S] OUTDIR...

Plateau gates (each PASS, FAIL or n/a when the run has too few samples):
  rss     peak RSS (the report's peak_rss_bytes, else the sampled VmHWM) <= --memory + RSS_SLACK (default
          1024 MiB), and the last third's sampled max <= 1.10 x the middle third's + 64 MiB (no late growth)
  nodes   peak outstanding nodes <= NODES_PER_WORKER (default 64) x workers, and the last third's max <=
          1.5 x the middle third's + workers
  readers peak spill readers <= 8 (order.MaxSpillReaders)
  age     the largest sampled recorded-part age, once a part was published, <= --part-interval + AGE_SLACK
          (default 60 s)
A run that did not commit fails every gate. Budget wait is goroutine-seconds blocked on the row budget (a
listing that waits instead of spilling: high with a small spill cap); spill refused counts refused spills.
"""
import argparse
import json
import os
import re
import sys

MIB = 1 << 20


def load(d):
    r = {"dir": d, "name": os.path.basename(os.path.normpath(d))}
    with open(os.path.join(d, "report.json")) as f:
        r["report"] = json.load(f)
    r["samples"] = []
    with open(os.path.join(d, "samples.jsonl")) as f:
        for line in f:
            if line.strip():
                r["samples"].append(json.loads(line))
    try:
        with open(os.path.join(d, "fake-final.json")) as f:
            r["fake"] = json.load(f)
    except OSError:
        r["fake"] = r["samples"][-1].get("fake", {}) if r["samples"] else {}
    with open(os.path.join(d, "meta.txt")) as f:
        r["meta"] = f.read()
    return r


def thirds(xs):
    n = len(xs)
    if n < 6:
        return None, None
    return max(xs[n // 3: 2 * n // 3]), max(xs[2 * n // 3:])


def interval_s(argv):
    for i, a in enumerate(argv):
        m = re.match(r"-?-part-interval(?:=(.*))?$", a)
        if m:
            v = m.group(1) or argv[i + 1]
            m2 = re.match(r"^(\d+(?:\.\d+)?)(ms|s|m|h)$", v)
            if m2:
                return float(m2.group(1)) * {"ms": 1e-3, "s": 1, "m": 60, "h": 3600}[m2.group(2)]
    return 60.0  # the shipped default


def verdict(ok):
    return "n/a" if ok is None else ("PASS" if ok else "FAIL")


def check(r, a):
    rep, ss = r["report"], r["samples"]
    inv, res, out = rep["invocation"], rep["resources"], rep["output"]
    workers, mem = inv["workers"], inv["memory_bytes"]
    committed = rep["outcome"]["status"] == "committed"
    rows = out["rows_committed"] or out["rows_listed"]
    wall = rep["timings"]["wall_ms"] / 1000
    hwm = max([s.get("vmhwm_bytes", 0) for s in ss] + [0])
    peak_rss = res.get("peak_rss_bytes") or hwm
    rss = [s["vmrss_bytes"] for s in ss if "vmrss_bytes" in s]
    mid, late = thirds(rss)
    g = {}
    g["rss"] = peak_rss <= mem + a.rss_slack * MIB and (mid is None or late <= 1.10 * mid + 64 * MIB)
    nodes = [s["progress"]["order"]["nodes"] for s in ss if s.get("progress", {}).get("order")]
    peak_nodes = max([res.get("peak_nodes", 0)] + nodes + [0])
    mid, late = thirds(nodes)
    g["nodes"] = peak_nodes <= a.nodes_per_worker * workers and (mid is None or late <= 1.5 * mid + workers)
    spill = res.get("spill") or {}
    readers = max([spill.get("peak_readers", 0)] +
                  [s["progress"]["order"]["peak_spill_readers"] for s in ss if s.get("progress", {}).get("order")] + [0])
    g["readers"] = readers <= 8
    ages = [s["progress"]["part_age_ms"] / 1000 for s in ss if s.get("progress", {}).get("parts", 0) > 0]
    max_age = max(ages) if ages else None
    g["age"] = None if max_age is None else max_age <= interval_s(inv["argv"]) + a.age_slack
    if not committed:
        g = dict.fromkeys(g, False)
    f = r["fake"]
    reqs = f.get("requests", 0)
    busy = f.get("busy_ns", 0) / 1e9
    return {
        "run": r["name"], "status": rep["outcome"]["status"], "rows": rows, "workers": workers,
        "wall_s": wall, "rows_per_s": rows / wall if wall else 0,
        "memory_mib": mem / MIB, "peak_rss_mib": peak_rss / MIB, "peak_nodes": peak_nodes,
        "peak_markers": max([res.get("peak_markers", 0)] + [0]), "peak_readers": readers,
        "peak_spill_mib": spill.get("peak_bytes", 0) / MIB, "spill_cap_mib": spill.get("max_bytes", 0) / MIB,
        "spill_refused": spill.get("refused", 0), "budget_wait_s": res.get("budget_wait_ns", 0) / 1e9, "max_part_age_s": max_age,
        "requests": reqs, "req_per_1k": 1000 * reqs / rows if rows else 0,
        "served_per_listed": f.get("keys", 0) / rows if rows else 0, "throttled": f.get("throttled", 0),
        "mean_inflight": busy / wall if wall else 0, "peak_inflight": f.get("peak_inflight", 0),
        "gates": g,
    }


def series(r):
    print(f"\n{r['name']}: {r['meta'].splitlines()[0]}\n")
    print("| t s | RSS MiB | nodes | markers | spill readers | spill MiB | part age s | req/s | mean inflight | peak inflight | listed/s | encoded/s |")
    print("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    prev = None
    for s in r["samples"]:
        p, f = s.get("progress", {}), s.get("fake", {})
        o = p.get("order") or {}
        if prev:
            dt = (f.get("as_of_unix_ms", 0) - prev[1].get("as_of_unix_ms", 0)) / 1000 or 1
            rq = (f.get("requests", 0) - prev[1].get("requests", 0)) / dt
            inf = (f.get("busy_ns", 0) - prev[1].get("busy_ns", 0)) / 1e9 / dt
            ls = (p.get("objects", 0) - prev[0].get("objects", 0)) / dt
            es = (p.get("encoded", 0) - prev[0].get("encoded", 0)) / dt
        else:
            rq = inf = ls = es = 0
        print(f"| {s['t']:.0f} | {s.get('vmrss_bytes', 0) / MIB:.0f} | {o.get('nodes', 0)} | {o.get('markers', 0)} | "
              f"{o.get('spill_readers', 0)} | {o.get('spill_live_bytes', 0) / MIB:.0f} | {p.get('part_age_ms', 0) / 1000:.0f} | "
              f"{rq:.0f} | {inf:.1f} | {f.get('window_peak_inflight', 0)} | {ls:.0f} | {es:.0f} |")
        prev = (p, f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="+")
    ap.add_argument("--series", action="store_true")
    ap.add_argument("--rss-slack", type=float, default=1024)
    ap.add_argument("--nodes-per-worker", type=float, default=64)
    ap.add_argument("--age-slack", type=float, default=60)
    a = ap.parse_args()
    runs = [load(d) for d in a.dirs]
    print("| run | status | rows | workers | wall s | rows/s | --memory MiB | peak RSS MiB | peak nodes | peak markers |"
          " spill readers | peak spill MiB | spill cap MiB | spill refused | budget wait s | max part age s | requests | req per 1k keys | served per listed |"
          " throttled | mean inflight | peak inflight | rss | nodes | readers | age |")
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|")
    bad = False
    for r in runs:
        c = check(r, a)
        age = "-" if c["max_part_age_s"] is None else f"{c['max_part_age_s']:.0f}"
        print(f"| {c['run']} | {c['status']} | {c['rows']:,} | {c['workers']} | {c['wall_s']:.1f} | {c['rows_per_s']:,.0f} | "
              f"{c['memory_mib']:.0f} | {c['peak_rss_mib']:.0f} | {c['peak_nodes']} | {c['peak_markers']} | {c['peak_readers']} | "
              f"{c['peak_spill_mib']:.0f} | {c['spill_cap_mib']:.0f} | {c['spill_refused']:,} | {c['budget_wait_s']:.0f} | {age} | {c['requests']:,} | {c['req_per_1k']:.3f} | {c['served_per_listed']:.3f} | "
              f"{c['throttled']} | {c['mean_inflight']:.1f} | {c['peak_inflight']} | "
              + " | ".join(verdict(c["gates"][k]) for k in ("rss", "nodes", "readers", "age")) + " |")
        bad = bad or any(v is False for v in c["gates"].values())
    for r in runs:
        print(f"\n{r['name']}: " + "; ".join(r["meta"].splitlines()[:2]))
    if a.series:
        for r in runs:
            series(r)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
