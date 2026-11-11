#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Generate the spill-shape local trees for localtree.sh: equal file counts,
different directory shapes (what a sorted local listing spills depends on).

usage: localtree_gen.py SHAPE DIR [--files N] [--seed S]

Shapes (every file empty, created with mknod; deterministic per seed):
  smalldirs  many small directories: each holds 3-9 files and up to 4
             subdirectories, breadth first, so the tree is deep (about
             log4(N/6) levels) with about N/6 directories. Close to the
             owner's macOS tree (1.35M files in 251k directories).
  bigdirs    few large directories: N files in 24 directories of N/24.
  skewed     deep and skewed, long names: directories made by a random walk
             biased to the newest (chains tens of levels deep), each with a
             heavy-tailed file count (most 1-3, a few tens of thousands), names
             of 60-200 bytes.

A tree that exists is refused (delete it first).
"""
import argparse
import os
import random
import sys
import time

WORDS = ("alpha beta gamma delta core util http json model view render parse codec "
         "stream buffer index cache store queue event user auth token config server "
         "client router handler schema table query plan merge sort").split()


def mk(path):
    os.mknod(path, 0o644)


def smalldirs(root, n, rng):
    made, dirs, i = 0, [root], 0
    while made < n:
        d = dirs[i]
        i += 1
        for f in range(min(rng.randint(3, 9), n - made)):
            mk(os.path.join(d, f"f{f}.dat"))
            made += 1
        for s in range(4):
            if (len(dirs) - i) * 6 >= n - made:  # enough directories queued for the rest
                break
            p = os.path.join(d, f"d{s}")
            os.mkdir(p)
            dirs.append(p)
    return made, len(dirs)


def bigdirs(root, n, rng):
    per = (n + 23) // 24
    made = 0
    for d in range(24):
        p = os.path.join(root, f"dir-{d:02d}")
        os.mkdir(p)
        for f in range(min(per, n - made)):
            mk(os.path.join(p, f"file-{f:07d}.dat"))
            made += 1
    return made, 24


def longname(rng, lo, hi):
    want = rng.randint(lo, hi)
    parts = []
    while sum(len(x) + 1 for x in parts) < want:
        parts.append(rng.choice(WORDS) + str(rng.randrange(1000)))
    return "-".join(parts)[:want]


def skewed(root, n, rng):
    made, dirs = 0, [root]
    while made < n:
        # biased to the newest directories: long chains
        parent = dirs[-1 - min(int(rng.expovariate(0.5)), len(dirs) - 1)] if rng.random() < 0.85 else rng.choice(dirs)
        if parent.count(os.sep) - root.count(os.sep) > 40 or len(parent) > 3000:
            parent = rng.choice(dirs[: max(1, len(dirs) // 2)])
        p = os.path.join(parent, longname(rng, 40, 120))
        if os.path.exists(p):
            continue
        os.mkdir(p)
        dirs.append(p)
        k = min(int(rng.paretovariate(0.9)), 40000, n - made)
        seen = set()
        for _ in range(k):
            name = longname(rng, 60, 200) + ".bin"
            if name in seen:
                continue
            seen.add(name)
            mk(os.path.join(p, name))
            made += 1
    return made, len(dirs)


SHAPES = {"smalldirs": smalldirs, "bigdirs": bigdirs, "skewed": skewed}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("shape", choices=sorted(SHAPES))
    ap.add_argument("dir")
    ap.add_argument("--files", type=int, default=1_200_000)
    ap.add_argument("--seed", type=int, default=20261111)
    a = ap.parse_args()
    if os.path.exists(a.dir):
        sys.exit(f"{a.dir} exists; delete it first")
    os.makedirs(a.dir)
    t0 = time.time()
    files, dirs = SHAPES[a.shape](a.dir, a.files, random.Random(a.seed))
    print(f"{a.shape}: {files} files in {dirs} directories, {time.time() - t0:.1f}s", flush=True)


if __name__ == "__main__":
    main()
