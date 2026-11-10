#!/usr/bin/env bash
# Runs "$@" inside the current cgroup (a systemd scope) and appends the scope's memory counters to $MEMLOG as
# one JSON line, read before the scope goes away: refaults and reclaim show whether the run was short of memory.
"$@"; rc=$?
cg=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)
python3 - "$cg" "$rc" "$*" >> "$MEMLOG" <<'PY'
import json, sys
cg, rc, cmd = sys.argv[1:]
st = dict(l.split() for l in open(f"{cg}/memory.stat"))
keep = ("workingset_refault_file", "workingset_refault_anon", "pgscan", "pgsteal", "pgmajfault", "file", "anon", "slab")
peak = open(f"{cg}/memory.peak").read().strip()
print(json.dumps({"cmd": cmd, "rc": int(rc), "peak": int(peak), **{k: int(st[k]) for k in keep}}))
PY
exit $rc
