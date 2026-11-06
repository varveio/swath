#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Tabulate sortab.sh results (JSON lines) and evaluate the decision record's gate.

usage: sortab_table.py RESULTS.jsonl [--margin 0.10] [--restart-slack-s 10]

Medians over reps per (shape, workers, kill mode, arm). The gate, per shape
and worker count, on the stated envelope:

  relist   killed cells: relisted keys <= one part (the run's largest part,
           rows) + 5 % of the keyspace
  restart  killed cells: resume wall <= 1.5 x the arm's uninterrupted wall x
           the fraction of keys not durable at the kill, + slack (default 10 s)
  rss      peak RSS (VmHWM) <= 2 x --memory + encoders x 24 MiB (the row
           budget and its equal headroom, the garbage collector's doubling,
           and the encoders' writers, parquetout.EncoderBytes)
  disk     peak spill (A) or live sorted runs (B) <= the spill cap
  wall     A's median wall <= B's x (1 + margin), uninterrupted and
           completion-plus-recovery
"""
import json
import statistics
import sys
from collections import defaultdict

MiB = 1 << 20


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def cell_metrics(r):
    """One result line -> flat metrics."""
    m = {}
    at = r["attempts"]
    m["wall_s"] = r["wall_ms"] / 1000
    m["first_wall_s"] = at[0]["wall_ms"] / 1000 if at else None
    m["cpu_us_row"] = r["cpu_ns_per_row"] / 1000
    m["rss_mib"] = r["peak_rss_kb"] / 1024
    m["requests"] = r["requests"]
    m["resume_requests"] = r.get("resume_requests", 0)
    m["relisted"] = r.get("relisted_keys", 0)
    m["restart_s"] = r.get("restart_ms", 0) / 1000
    m["durable_at_kill"] = r.get("durable_rows_at_kill", 0)
    m["listed_at_kill"] = r.get("listed_at_kill", 0)
    m["max_part_rows"] = r.get("max_part_rows", 0)
    m["parts"] = r.get("parts", 0)
    m["verified"] = r.get("verified", False)
    m["error"] = r.get("error", "")
    spill_peak = writes = 0
    nodes = readers = 0
    waits = defaultdict(float)
    enc_active = enc_samples = 0
    first_durable = first_part = None
    rchar = wchar = 0
    t0 = 0.0
    for a in at:
        last = a.get("last") or {}
        rchar += last.get("rchar", 0)
        wchar += last.get("wchar", 0)
        enc_active += last.get("encode_active", 0)
        enc_samples += last.get("samples", 0)
        if first_durable is None and last.get("first_durable_ms", -1) >= 0:
            first_durable = t0 + last["first_durable_ms"] / 1000
        if first_part is None and last.get("first_part_ms", -1) >= 0:
            first_part = t0 + last["first_part_ms"] / 1000
        t0 += a["wall_ms"] / 1000
        spill_peak = max(spill_peak, last.get("peak_spill_bytes", 0))
        nodes = max(nodes, last.get("peak_nodes", 0))
        readers = max(readers, last.get("peak_readers", 0))
        res = a.get("result") or {}
        if res.get("a"):
            rs = res["a"]["resources"]
            sp = rs.get("spill") or {}
            spill_peak = max(spill_peak, sp.get("peak_bytes", 0))
            writes += sp.get("bytes", 0)
            nodes = max(nodes, rs.get("peak_nodes", 0))
            readers = max(readers, sp.get("peak_readers", 0))
            waits["frontier"] += rs.get("frontier_wait_ns", 0) / 1e9
            waits["budget"] += rs.get("budget_wait_ns", 0) / 1e9
            waits["encoder"] += (rs.get("encoder_wait_ns") or 0) / 1e9
        if res.get("b"):
            b = res["b"]
            spill_peak = max(spill_peak, b.get("peak_staging_bytes", 0))
            writes += b.get("run_bytes_written", 0)
            nodes = max(nodes, b.get("peak_nodes", 0))
            readers = max(readers, b.get("peak_readers", 0))
            waits["seal"] += b.get("seal_wait_ns", 0) / 1e9
            waits["encoder"] += b.get("encoder_wait_ns", 0) / 1e9
            waits["merge_phase"] += b.get("merge_ms", 0) / 1000
    m["spill_peak_mib"] = spill_peak / MiB
    m["spill_write_mib"] = writes / MiB
    m["rchar_mib"] = rchar / MiB
    m["wchar_mib"] = wchar / MiB
    m["nodes"] = nodes
    m["readers"] = readers
    m["enc_util"] = 100 * enc_active / enc_samples if enc_samples else None
    m["first_durable_s"] = first_durable
    m["first_part_s"] = first_part
    m["waits"] = dict(waits)
    return m


def fmt(v, d=1):
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:,.{d}f}"
    return f"{v:,}"


def main():
    args = sys.argv[1:]
    margin, slack = 0.10, 10.0
    if "--margin" in args:
        i = args.index("--margin")
        margin = float(args[i + 1])
        del args[i:i + 2]
    if "--restart-slack-s" in args:
        i = args.index("--restart-slack-s")
        slack = float(args[i + 1])
        del args[i:i + 2]
    rows = [json.loads(line) for line in open(args[0]) if line.strip()]
    groups = defaultdict(list)
    info = {}
    for r in rows:
        k = (r["shape"]["name"], r["limits"]["workers"], r["kill_frac"] > 0, r["arm"])
        groups[k].append(r)
        info[k[:2]] = r
    r0 = rows[0]
    print(f"Synthetic trace: {r0['shape']['keys']:,} keys, {r0['shape']['ranges']} ranges, {r0['shape']['page_keys']} keys/page, "
          f"latency {r0['shape']['latency_ms']} ms (slow range {r0['shape']['slow_latency_ms']} ms, "
          f"{r0['shape']['slow_keys']:,} keys), jitter {r0['shape']['jitter']}, seed {r0['shape']['seed']}; "
          f"--memory {r0['limits']['memory'] / MiB:g} MiB, {r0['limits']['encoders']} encoders, part {r0['limits']['part_size'] / MiB:g} MiB, "
          f"A spill cap {(str(round(r0['limits']['spill_max'] / MiB, 2)) + ' MiB') if r0['limits']['spill_max'] > 0 else 'none'}; "
          f"host {r0.get('host', '?')}. Spill/runs written: completed attempts only.\n")
    cols = ["shape", "workers", "kill", "arm", "reps", "ok", "wall s", "1st attempt s", "CPU µs/row", "requests", "resume req",
            "listed at kill", "durable at kill", "relisted keys", "restart s", "peak RSS MiB", "peak spill/runs MiB",
            "spill/runs written MiB", "rchar MiB", "wchar MiB", "peak nodes", "readers", "encoder active %",
            "first durable s", "first part s", "waits s"]
    print("| " + " | ".join(cols) + " |")
    print("|" + "---|" * len(cols))
    agg = {}
    for k in sorted(groups):
        ms = [cell_metrics(r) for r in groups[k]]
        a = {f: med([m[f] for m in ms]) for f in ms[0] if f not in ("waits", "verified", "error")}
        a["verified"] = all(m["verified"] for m in ms)
        a["errors"] = [m["error"] for m in ms if m["error"]]
        wk = sorted({w for m in ms for w in m["waits"]})
        a["waits"] = {w: med([m["waits"].get(w, 0) for m in ms]) for w in wk}
        agg[k] = a
        waits = ", ".join(f"{w} {v:.1f}" for w, v in a["waits"].items())
        print("| " + " | ".join([k[0], str(k[1]), "peak" if k[2] else "none", k[3], str(len(ms)),
                                 "yes" if a["verified"] else "NO", fmt(a["wall_s"]), fmt(a["first_wall_s"]), fmt(a["cpu_us_row"], 2),
                                 fmt(a["requests"]), fmt(a["resume_requests"]) if k[2] else "-",
                                 fmt(a["listed_at_kill"]) if k[2] else "-", fmt(a["durable_at_kill"]) if k[2] else "-",
                                 fmt(a["relisted"]) if k[2] else "-", fmt(a["restart_s"]) if k[2] else "-",
                                 fmt(a["rss_mib"], 0), fmt(a["spill_peak_mib"], 0), fmt(a["spill_write_mib"], 0),
                                 fmt(a["rchar_mib"], 0), fmt(a["wchar_mib"], 0), fmt(a["nodes"]), fmt(a["readers"]),
                                 fmt(a["enc_util"], 0), fmt(a["first_durable_s"]), fmt(a["first_part_s"]), waits]) + " |")
    for k, a in agg.items():
        for e in a["errors"]:
            print(f"\nerror in {k}: {e}")
    print("\nGate (identity and adjacency are required in every cell: 'ok' above).\n")
    print("| shape | workers | arm | relist ≤ part + 5 % | restart bounded | RSS in envelope | disk in envelope | wall vs B (none / with kill) |")
    print("|---|---|---|---|---|---|---|---|")
    for (shape, w), r in sorted(info.items()):
        lim, keys = r["limits"], r["shape"]["keys"]
        rss_env = (2 * lim["memory"] + lim["encoders"] * 24 * MiB) / MiB
        cap = lim["spill_max"] / MiB if lim["spill_max"] > 0 else None
        for arm in ("A", "B"):
            clean, killed = agg.get((shape, w, False, arm)), agg.get((shape, w, True, arm))
            cells = [c for c in (clean, killed) if c]
            if not cells:
                continue

            def yn(ok, detail):
                return ("pass" if ok else "FAIL") + f" ({detail})"
            relist = restart = "-"
            if killed:
                bound = killed["max_part_rows"] + 0.05 * keys
                relist = yn(killed["relisted"] <= bound, f"{killed['relisted']:,.0f} vs {bound:,.0f}")
                if clean:
                    undone = 1 - (killed["durable_at_kill"] or 0) / keys
                    rb = 1.5 * clean["wall_s"] * undone + slack
                    restart = yn(killed["restart_s"] <= rb, f"{killed['restart_s']:.1f} s vs {rb:.1f} s")
            rss = max(c["rss_mib"] for c in cells)
            rssv = yn(rss <= rss_env, f"{rss:.0f} vs {rss_env:.0f} MiB")
            sp = max(c["spill_peak_mib"] for c in cells)
            disk = yn(cap is None or sp <= cap, f"{sp:.0f} vs {fmt(cap, 0)} MiB")
            wall = "-"
            if arm == "A":
                parts = []
                for kill in (False, True):
                    a_, b_ = agg.get((shape, w, kill, "A")), agg.get((shape, w, kill, "B"))
                    if a_ and b_:
                        parts.append(yn(a_["wall_s"] <= b_["wall_s"] * (1 + margin), f"{a_['wall_s']:.1f} vs {b_['wall_s']:.1f} s"))
                wall = " / ".join(parts) or "-"
            print(f"| {shape} | {w} | {arm} | {relist} | {restart} | {rssv} | {disk} | {wall} |")
    print(f"\nWall margin {margin:.0%}; restart slack {slack:.0f} s.")


if __name__ == "__main__":
    main()
