#!/usr/bin/env python3
"""Arms for xos-bench.sh: swath A and B, fd and the platform's own tools, in paired rounds.

  xos_arms.py OUT OS ROOT TMP DROP
Env: SWATH_A, SWATH_B, TREES. Rounds (A/B pairs) per tier and mode below; peers run in the first
PEER_ROUNDS (6) rounds only. Exits non-zero if any cell failed. Writes OUT/OS-TREE-MODE-TIER.json
(paired_rounds.py) and prints a Markdown summary.
"""
import glob
import json
import os
import shutil
import subprocess
import sys

out, osn, root, tmp, drop = sys.argv[1:]
A, B = os.environ["SWATH_A"], os.environ["SWATH_B"]
here = os.path.dirname(os.path.abspath(__file__))
win = osn == "windows"
drop = ["pwsh", "-NoProfile", "-File", drop] if win else [drop]
# the stat tier carries the #368 A/B question: more pairs there
rounds = {("warm", "stat"): 20, ("cold", "stat"): 12, ("warm", "names"): 10, ("cold", "names"): 6}
peer_rounds = int(os.environ.get("PEER_ROUNDS", "6"))
failed = []

for tree in os.environ["TREES"].split():
    P = f"{root}/{tree}"
    for b in (A, B):  # a swath that fails at once would time as fast: check both run
        subprocess.run([b, "diag", "count", P, "--no-stats"], check=True, stdout=subprocess.DEVNULL)
    names = [("swath-a", [A, "diag", "count", P, "--metadata", "names", "--no-stats"], [0]),
             ("swath-b", [B, "diag", "count", P, "--metadata", "names", "--no-stats"], [0]),
             ("fd", ["fd", "-j64", "-u", "^$", P], [0, 1])]
    stat = [("swath-a", [A, "diag", "count", P, "--no-stats"], [0]),
            ("swath-b", [B, "diag", "count", P, "--no-stats"], [0]),
            ("fd", ["fd", "-j64", "-u", "--search-path", P, "--size", "+100Ti"], [0, 1])]
    if win:
        W = root + "\\" + tree
        names.append(("dir /s", ["cmd", "/c", "dir", "/s", "/b", "/a", W], [0]))
        none = tmp.replace("/", "\\") + "\\none"
        # robocopy exits 0-7 on success (1: files would be copied)
        stat.append(("robocopy /MT:64", ["robocopy", W, none, "/L", "/E", "/NFL", "/NDL", "/NJH", "/NJS", "/NP",
                                         "/R:0", "/SL", "/MT:64"], list(range(8))))
    else:
        # find is the slowest arm on macOS by far (earlier macOS runs: 3 s per 200k entries): left out to fit the job's time
        if shutil.which("bfs"):
            names.append(("bfs", ["bfs", P, "-false"], [0]))
            stat.append(("bfs", ["bfs", P, "-size", "+100000000M"], [0]))
        if shutil.which("fdu"):
            stat.append(("fdu", ["fdu", "--view=summary", "--no-gitignore", "--cache", "off", P], [0]))
    for mode in ("warm", "cold"):
        for tier, arms in (("names", names), ("stat", stat)):
            spec = {"rounds": rounds[mode, tier], "peer_rounds": peer_rounds, "warmup": 1 if mode == "warm" else 0, "ref": "swath-b",
                    "drop": drop if mode == "cold" else None,
                    "arms": [{"name": n, "argv": a, "ok": ok} for n, a, ok in arms]}
            name = f"{osn}-{tree}-{mode}-{tier}"
            with open(f"{out}/{name}.spec", "w") as f:
                json.dump(spec, f)
            r = subprocess.run([sys.executable, f"{here}/paired_rounds.py", f"{out}/{name}.spec", f"{out}/{name}.json"])
            print(name, "ok" if r.returncode == 0 else f"FAILED {r.returncode}", file=sys.stderr, flush=True)
            if r.returncode:
                failed.append(name)
    for b in (A, B):  # same entries, no read errors
        c = subprocess.run([b, "diag", "count", P], capture_output=True, text=True)
        print(tree, b, " ".join(w for w in (c.stdout + c.stderr).split()
                                if w.startswith(("files=", "dirs=", "read_errors="))), file=sys.stderr)

print(f"## {osn}: median of paired rounds; ratio = per-round time / swath-b, bootstrap 95 % CI\n")
for p in sorted(glob.glob(f"{out}/{osn}-*.json")):
    arms = json.load(open(p))["arms"]
    print(f"### {os.path.basename(p)[:-5]}\n\n| arm | rounds | median (s) | ratio to swath-b | 95 % CI |\n"
          "|---|---:|---:|---:|---|")
    for k, v in arms.items():
        print(f"| {k} | {v['rounds']} | {v['median']:.3f} | {v['ratio']:.3f} | "
              f"{v['ratio_ci'][0]:.3f} to {v['ratio_ci'][1]:.3f} |")
    print()
sys.exit(f"failed: {failed}" if failed else 0)
