#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Opt-in bounded-resource observer for an already-running replay benchmark arm.

The parent runner supplies the server PID and stops this process before server shutdown.
No request driver or performance verdict lives here; this observer records Linux/JVM peaks
and fails only a predeclared resource cap or an incomplete observation.
"""

import argparse
import json
import math
import os
from pathlib import Path
import re
import signal
import threading
import time

from resource_fault import http_json, jcmd, rss, sha256
from run_pair import clean_java_env


def nmt_committed_bytes(summary):
    match = re.search(r"Total:\s+reserved=\d+(?:KB|MB|GB)?,\s+committed=(\d+)(KB|MB|GB)?",
                      summary)
    if not match:
        raise ValueError("NMT summary omitted committed total")
    return int(match.group(1)) * {None: 1, "KB": 1024, "MB": 1024 ** 2,
                                  "GB": 1024 ** 3}[match.group(2)]


def count_entries(path):
    return sum(1 for _ in path.iterdir())


def required_gauge(snapshot, name, tags=None):
    tags = tags or {}
    matches = [meter for meter in snapshot.get("meters", [])
               if meter.get("name") == name
               and all(meter.get("tags", {}).get(key) == value for key, value in tags.items())]
    if not matches:
        raise RuntimeError(f"required resource gauge is missing: {name}")
    values = [meter.get("value") for meter in matches]
    if any(not isinstance(value, (int, float)) or not math.isfinite(value) for value in values):
        raise RuntimeError(f"required resource gauge is null or nonfinite: {name}")
    return sum(values)


def read_sample(pid, metrics_url):
    snapshot = http_json(metrics_url)
    if snapshot.get("schema_version") != 2:
        raise RuntimeError("resource observer needs metrics schema 2")
    serving = snapshot["serving"]
    return {"at_ns": time.monotonic_ns(),
            "rss_bytes": rss(pid),
            "fd_count": count_entries(Path(f"/proc/{pid}/fd")),
            "thread_count": count_entries(Path(f"/proc/{pid}/task")),
            "heap_bytes": required_gauge(snapshot, "jvm.memory.used", {"area": "heap"}),
            "direct_bytes": required_gauge(snapshot, "jvm.buffer.memory.used", {"id": "direct"}),
            "charged_response_bytes": serving["charged_response_bytes"],
            "server_peak_charged_response_bytes": serving["peak_charged_response_bytes"],
            "active_responses": serving["active_responses"],
            "cached_rows": required_gauge(snapshot, "swath.replay.prefetch.rows.live"),
            "cached_windows": required_gauge(snapshot, "swath.replay.prefetch.windows.live")}


def check_bounds(peaks, caps):
    for field, cap in caps.items():
        if peaks[field] > cap:
            raise RuntimeError(f"observed {field}={peaks[field]} exceeded declared cap={cap}")


def charged_peak_for_cap(samples):
    return max(max(sample["charged_response_bytes"],
                   sample["server_peak_charged_response_bytes"]) for sample in samples)


def inside_git_checkout(path):
    # A worktree has a .git file; an ordinary checkout has a .git directory.
    return any((parent / ".git").is_file() or (parent / ".git").is_dir()
               for parent in (path, *path.parents))


def observe(args, stop_event=None):
    output = Path(args.output).resolve()
    if inside_git_checkout(output):
        raise ValueError("resource observer output must be outside the repository")
    output.parent.mkdir(parents=True, exist_ok=True)
    caps = {"rss_bytes": args.rss_cap, "fd_count": args.fd_cap,
            "thread_count": args.thread_cap, "heap_bytes": args.heap_cap,
            "direct_bytes": args.direct_cap,
            "charged_response_bytes": args.charged_cap,
            "active_responses": args.active_cap,
            "cached_rows": args.cache_rows_cap,
            "cached_windows": args.cache_windows_cap,
            "nmt_committed_bytes": args.nmt_committed_cap}
    if any(value <= 0 for value in caps.values()) or args.sample_ms < 10 or args.nmt_interval_s < 1:
        raise ValueError("resource caps and sampling intervals must be positive")
    environment, removed = clean_java_env()
    environment["JAVA_HOME"] = args.java_home
    stopped = stop_event or threading.Event()
    if stop_event is None:
        signal.signal(signal.SIGTERM, lambda _signum, _frame: stopped.set())
        signal.signal(signal.SIGINT, lambda _signum, _frame: stopped.set())
    receipt = {"status": "failed", "pid": args.pid, "metrics_url": args.metrics_url,
               "output": str(output), "caps": caps, "sample_ms": args.sample_ms,
               "nmt_interval_s": args.nmt_interval_s,
               "removed_inherited_java_option_keys": sorted(removed),
               "observer_source_sha256": sha256(Path(__file__)),
               "java_home": args.java_home, "samples": [], "nmt": [],
               "charged_cap_basis": "server exact peak_charged_response_bytes plus sampled live charge"}
    started = time.monotonic()
    try:
        def capture_nmt():
            result = jcmd(args.java_home, args.pid, "VM.native_memory", "summary",
                          environment=environment)
            if result.get("exit") != 0:
                raise RuntimeError("NMT summary failed during resource observation")
            committed = nmt_committed_bytes(result.get("stdout", ""))
            receipt["nmt"].append({"at_ns": time.monotonic_ns(),
                                   "committed_bytes": committed,
                                   "command": result["command"],
                                   "exit": result["exit"],
                                   "stdout": result["stdout"]})
        capture_nmt()
        receipt["samples"].append(read_sample(args.pid, args.metrics_url))
        print(json.dumps({"event": "READY", "pid": args.pid}), flush=True)
        next_nmt = time.monotonic() + args.nmt_interval_s
        while not stopped.is_set():
            receipt["samples"].append(read_sample(args.pid, args.metrics_url))
            if time.monotonic() >= next_nmt:
                capture_nmt()
                next_nmt = time.monotonic() + args.nmt_interval_s
            stopped.wait(args.sample_ms / 1000)
        receipt["samples"].append(read_sample(args.pid, args.metrics_url))
        capture_nmt()
        if len(receipt["samples"]) < 2:
            raise RuntimeError("resource observer did not span a benchmark arm")
        if any(sample["rss_bytes"] is None for sample in receipt["samples"]):
            raise RuntimeError("resource observer lost the server /proc RSS reading")
        fields = (field for field in caps if field != "nmt_committed_bytes")
        peaks = {field: max(sample[field] for sample in receipt["samples"])
                 for field in fields}
        peaks["charged_response_bytes"] = charged_peak_for_cap(receipt["samples"])
        receipt["peaks"] = peaks
        receipt["nmt_committed_peak_bytes"] = max(item["committed_bytes"] for item in receipt["nmt"])
        peaks["nmt_committed_bytes"] = receipt["nmt_committed_peak_bytes"]
        check_bounds(peaks, caps)
        receipt["status"] = "passed"
    except Exception as error:
        receipt["error"] = repr(error)
        raise
    finally:
        receipt["duration_seconds"] = time.monotonic() - started
        output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--metrics-url", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--java-home", required=True)
    parser.add_argument("--sample-ms", type=int, default=100)
    parser.add_argument("--nmt-interval-s", type=float, default=5)
    parser.add_argument("--rss-cap", type=int, required=True)
    parser.add_argument("--fd-cap", type=int, required=True)
    parser.add_argument("--thread-cap", type=int, required=True)
    parser.add_argument("--heap-cap", type=int, required=True)
    parser.add_argument("--direct-cap", type=int, required=True)
    parser.add_argument("--charged-cap", type=int, required=True)
    parser.add_argument("--active-cap", type=int, required=True)
    parser.add_argument("--cache-rows-cap", type=int, required=True)
    parser.add_argument("--cache-windows-cap", type=int, required=True)
    parser.add_argument("--nmt-committed-cap", type=int, required=True)
    args = parser.parse_args()
    observe(args)


if __name__ == "__main__":
    main()
