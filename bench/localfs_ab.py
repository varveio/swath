#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Opt-in localfs A/B: exact output plus interleaved warm-cache measurements.

Run under flock /tmp/swath-gates.lock. Requires two built swath binaries;
creates fixtures only in a new --out directory, which must be outside the repo.
Uses localtree_gen.py's shapes, with deterministic nonzero sparse file sizes.
No timing threshold: shared-host measurements are evidence, not a CI gate.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys
import time

sys.dont_write_bytecode = True
import localtree_gen


STAMP = 1577934245
STAMP_TEXT = "2020-01-02T03:04:05.000000Z"


def fixture(root, shape, files):
    root.mkdir()
    made, _ = localtree_gen.SHAPES[shape](str(root), files, random.Random(20261127))
    if made != files:
        raise AssertionError(f"generator made {made} files, wanted {files}")
    paths = sorted(root.rglob("*"))
    for i, p in enumerate(paths):
        if p.is_file():
            with p.open("r+b") as f:
                f.truncate(1 + (i * 7919) % 65536)
        os.utime(p, (STAMP, STAMP))
    expected = []
    for p in paths:
        st = p.stat()
        directory = p.is_dir()
        expected.append(dict(key=p.relative_to(root).as_posix() + ("/" if directory else ""), key_base64=None,
                             kind="dir" if directory else "file", size=st.st_size,
                             mode=st.st_mode & 0o7777, last_modified=STAMP_TEXT))
    return sorted(expected, key=lambda r: r["key"].encode())


def verify_rows(actual, expected):
    if len(actual) != len(expected):
        raise AssertionError(f"row count {len(actual)} != {len(expected)}")
    for i, (got, want) in enumerate(zip(actual, expected)):
        if got != want:
            raise AssertionError(f"row {i}: {got!r} != {want!r}")


def verify_totals(actual, expected):
    files = [r for r in expected if r["kind"] == "file"]
    want = dict(row_type="total", depth=0, prefix="", objects=len(files),
                dirs=len(expected) - len(files), size=sum(r["size"] for r in files),
                size_missing=0, max_last_modified=STAMP_TEXT)
    if len(actual) != 1 or any(actual[0].get(k) != v for k, v in want.items()):
        raise AssertionError(f"totals {actual!r} do not match {want!r}")


def self_test():
    row = dict(key="a", kind="file", size=7, mode=420, last_modified=STAMP_TEXT)
    expected = [row, dict(row, key="b")]
    verify_rows(expected, expected)
    for broken in [expected[:1], expected + expected[:1], expected[::-1],
                   [dict(row, size=8), expected[1]], [dict(row, key="x"), expected[1]]]:
        try:
            verify_rows(broken, expected)
        except AssertionError:
            continue
        raise AssertionError("verifier accepted corrupt rows")
    totals = [dict(row_type="total", depth=0, prefix="", objects=2, dirs=0,
                   size=14, size_missing=0, max_last_modified=STAMP_TEXT)]
    verify_totals(totals, expected)
    try:
        verify_totals([dict(totals[0], objects=1)], expected)
    except AssertionError:
        return
    raise AssertionError("verifier accepted corrupt totals")


def run(args):
    if sys.platform != "linux":
        raise ValueError("native/portable A/B requires Linux")
    out = Path(args.out).resolve()
    repo = Path(__file__).resolve().parent.parent
    if out == repo or repo in out.parents:
        raise ValueError("--out must be outside the repository")
    bins = {k: str(Path(getattr(args, k)).resolve()) for k in ("base", "candidate")}
    out.mkdir(parents=True, exist_ok=False)
    (out / "reports").mkdir()
    env = dict(os.environ, GOMAXPROCS=str(args.workers), GOMEMLIMIT="2GiB", GOGC="100")
    for name in ("SWATH_LOCALFS_ENGINE", "SWATH_CPUPROFILE", "SWATH_MEMPROFILE",
                 "SWATH_MEMPROFILERATE", "GODEBUG"):
        env.pop(name, None)
    manifest = dict(platform=platform.platform(), cpu_count=os.cpu_count(), workers=args.workers,
                    files_per_shape=args.files, repetitions=args.reps, cache="warm",
                    runtime=dict(GOMAXPROCS=args.workers, GOMEMLIMIT="2GiB", GOGC=100),
                    binaries={k: dict(path=v, sha256=hashlib.sha256(Path(v).read_bytes()).hexdigest())
                              for k, v in bins.items()})
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    records = []
    for shape in ("bigdirs", "smalldirs"):
        root = out / shape
        expected = fixture(root, shape, args.files)
        (out / f"{shape}-oracle.json").write_text(json.dumps(expected) + "\n")
        for engine in ("native", "portable"):
            cell_env = dict(env)
            if engine == "portable":
                cell_env["SWATH_LOCALFS_ENGINE"] = "portable"
            for command in ("list", "du"):
                digests = set()
                for rep in range(args.reps + 1):  # rep 0 warms both arms; still validated
                    arms = ("base", "candidate") if rep % 2 == 0 else ("candidate", "base")
                    for arm in arms:
                        label = f"{shape}-{engine}-{command}-{rep}-{arm}"
                        report = out / "reports" / f"{label}.json"
                        cmd = [bins[arm], command, str(root), "--metadata", "stat", "--format", "jsonl",
                               "--workers", str(args.workers), "--report", str(report), "--no-stats"]
                        if command == "list":
                            cmd += ["--columns", "key,kind,size,mode,last_modified"]
                        else:
                            cmd += ["--apparent-size", "-d", "0"]
                        load = os.getloadavg()
                        start = time.perf_counter()
                        result = subprocess.run(cmd, env=cell_env, capture_output=True, timeout=180)
                        elapsed = time.perf_counter() - start
                        (out / "reports" / f"{label}.stderr").write_bytes(result.stderr)
                        if result.returncode:
                            raise RuntimeError(f"{label}: exit {result.returncode}: {result.stderr.decode()}")
                        actual = [json.loads(line) for line in result.stdout.splitlines()]
                        (verify_rows if command == "list" else verify_totals)(actual, expected)
                        digest = hashlib.sha256(result.stdout).hexdigest()
                        digests.add(digest)
                        if len(digests) != 1:
                            raise AssertionError(f"{label}: base/candidate output differs")
                        data = json.loads(report.read_text())
                        record = dict(shape=shape, engine=engine, command=command, rep=rep, arm=arm,
                                      seconds=elapsed, rss_bytes=data["process"]["peak_rss_bytes"],
                                      session_ms=data["timings"]["session_ms"],
                                      load=load, output_sha256=digest)
                        records.append(record)
                        with (out / "runs.jsonl").open("a") as f:
                            f.write(json.dumps(record) + "\n")
                        print(f"{label}: {elapsed:.3f}s {record['rss_bytes'] / 2**20:.1f} MiB; exact", flush=True)
    summary = []
    for shape in ("bigdirs", "smalldirs"):
        for engine in ("native", "portable"):
            for command in ("list", "du"):
                cell = dict(shape=shape, engine=engine, command=command)
                for arm in bins:
                    rs = [r for r in records if r["shape"] == shape and r["engine"] == engine
                          and r["command"] == command and r["arm"] == arm and r["rep"]]
                    cell[arm] = {"seconds": statistics.median(r["seconds"] for r in rs),
                                 "min_seconds": min(r["seconds"] for r in rs),
                                 "max_seconds": max(r["seconds"] for r in rs),
                                 "rss_mib": statistics.median(r["rss_bytes"] for r in rs) / 2**20}
                summary.append(cell)
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--self-test", action="store_true")
    for name in ("base", "candidate", "out"):
        p.add_argument("--" + name)
    p.add_argument("--files", type=int, default=120000)
    p.add_argument("--reps", type=int, default=4, help="measured runs per arm; even counts balance run order")
    p.add_argument("--workers", type=int, default=4)
    a = p.parse_args()
    self_test()
    if a.self_test:
        print("verifier rejects missing, duplicate, reordered, wrong-key/size rows and wrong totals")
    else:
        if not all((a.base, a.candidate, a.out)) or min(a.files, a.reps, a.workers) < 1:
            p.error("positive --files/--reps/--workers and --base/--candidate/--out are required")
        run(a)
