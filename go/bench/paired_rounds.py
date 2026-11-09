#!/usr/bin/env python3
"""Paired, interleaved timing rounds for noisy runners.

Each round runs the pair (the first two arms) back to back, AB and BA alternately, and the other
arms (peers, only in the first peer_rounds rounds) in a shuffled order around it (fixed seed). A cold
round runs the drop command before each arm and fails if it fails. Comparisons use per-round ratios
against the reference arm, so drift between rounds (a busy neighbour on a shared runner) hits both
sides of a ratio alike; a slow peer never sits between A and B.

  paired_rounds.py SPEC.json OUT.json

SPEC: {"rounds": N, "peer_rounds": M, "warmup": 0|1, "ref": "arm name", "drop": [argv] or null,
       "arms": [{"name": ..., "argv": [...], "ok": [exit codes]}]}
OUT: per-arm times and exit codes, and per arm the median ratio to ref with a bootstrap 95 % CI.
"""
import json
import random
import statistics
import subprocess
import sys
import time


def run(argv, timeout=1800):
    t = time.perf_counter()
    rc = subprocess.run(argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout).returncode
    return time.perf_counter() - t, rc


def ci(xs, rng, n=2000):
    meds = sorted(statistics.median(rng.choices(xs, k=len(xs))) for _ in range(n))
    return meds[int(0.025 * n)], meds[int(0.975 * n)]


def main():
    spec = json.load(open(sys.argv[1]))
    arms, drop = spec["arms"], spec.get("drop")
    rng = random.Random(1)
    times = {a["name"]: [] for a in arms}
    rcs = {a["name"]: [] for a in arms}
    log = []
    warm, peer_rounds = spec.get("warmup", 0), spec.get("peer_rounds", spec["rounds"])
    for r in range(warm + spec["rounds"]):
        pair = arms[:2] if (r - warm) % 2 == 0 else arms[1::-1]
        order = arms[2:] if r < warm + peer_rounds else []
        rng.shuffle(order)
        at = rng.randrange(len(order) + 1)
        for a in order[:at] + pair + order[at:]:
            reset = 0.0
            if drop:
                reset, rc = run(drop, timeout=600)
                if rc:
                    sys.exit(f"drop failed: exit {rc}: {drop}")
            dt, rc = run(a["argv"])
            if rc not in a.get("ok", [0]):
                sys.exit(f"{a['name']}: exit {rc}: {a['argv']}")
            log.append({"round": r - warm, "arm": a["name"], "s": dt, "reset_s": reset, "at": time.time()})
            if r >= warm:
                times[a["name"]].append((r - warm, dt))
                rcs[a["name"]].append(rc)
    ref = dict(times[spec["ref"]])
    out = {"spec": spec, "log": log, "arms": {}}
    for a in arms:
        rt = times[a["name"]]
        t = [dt for _, dt in rt]
        ratios = [dt / ref[r] for r, dt in rt]
        lo, hi = ci(ratios, rng)
        out["arms"][a["name"]] = {"median": statistics.median(t), "times": t, "exit": rcs[a["name"]],
                                  "rounds": len(t), "ratio": statistics.median(ratios), "ratio_ci": [lo, hi]}
    json.dump(out, open(sys.argv[2], "w"), indent=1)


if __name__ == "__main__":
    main()
