#!/usr/bin/env python3
"""Sampler for scale_run.sh: every INTERVAL s while PID lives, one JSON line with the process's RSS (VmRSS,
VmHWM), the run's progress.json (as written, every 2 s) and the fake's accounting (reset: the window peak
of requests in flight is per sample).

usage: scale_sample.py PID PROGRESS_JSON STATS_URL INTERVAL > samples.jsonl
"""
import json
import os
import sys
import time
import urllib.request


def rss(pid):
    out = {}
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                k, _, v = line.partition(":")
                if k in ("VmRSS", "VmHWM"):
                    out[k.lower() + "_bytes"] = int(v.split()[0]) * 1024
    except OSError:
        return None
    return out


def main():
    pid, progress, stats, interval = int(sys.argv[1]), sys.argv[2], sys.argv[3], float(sys.argv[4])
    t0 = time.time()
    while True:
        time.sleep(interval - (time.time() - t0) % interval)
        mem = rss(pid)
        if mem is None:
            return
        s = {"t": round(time.time() - t0, 1), **mem}
        try:
            with open(progress) as f:
                s["progress"] = json.load(f)
        except (OSError, ValueError):
            pass
        try:
            with urllib.request.urlopen(stats + "?reset=1", timeout=5) as r:
                s["fake"] = json.load(r)
        except OSError:
            pass
        print(json.dumps(s), flush=True)
        if not os.path.exists(f"/proc/{pid}"):
            return


if __name__ == "__main__":
    main()
