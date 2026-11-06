#!/usr/bin/env python3
"""Tabulate defaults_replay.sh output: per bucket, CPU setting and arm, the median (min-max) over reps of
the wall, the CPU time, the replay server's HTTP requests and requests per 1k keys (keys from the Go
runs' reports, one number per bucket since every arm lists the same keys), plus the default arm's
workers. usage: defaults_table.py OUTDIR
"""
import json, os, statistics as st, sys

W = sys.argv[1]
groups, keys = {}, {}
for line in open(os.path.join(W, 'arms.tsv')):
    f = line.rstrip('\n').split('\t')
    b, cpus, arm, rep = f[:4]
    kv = dict(x.split('=', 1) for x in f[4:])
    groups.setdefault((b, cpus, arm), []).append((rep, kv))
    if arm != 'java':
        try:
            d = json.load(open(os.path.join(W, f'{b}-{cpus}-{arm}-{rep}.json')))
            keys[b] = d['output']['rows_listed']
        except Exception:
            pass


def med(xs, fmt='{:.1f}'):
    xs = [x for x in xs if x is not None]
    if not xs:
        return '-'
    m = st.median(xs)
    return f'{fmt.format(m)} ({fmt.format(min(xs))}-{fmt.format(max(xs))})' if len(xs) > 1 else fmt.format(m)


print('| bucket | CPUs | arm | runs (rc 0) | wall s | CPU s | requests | requests per 1k keys | workers |')
print('|---|---|---|---|---|---|---|---|---|')
for (b, cpus, arm), runs in groups.items():
    wall = [float(kv['wall']) for _, kv in runs]
    cpu = [float(kv['cpu']) for _, kv in runs]
    req = [int(kv['replay_http']) for _, kv in runs]
    k = keys.get(b)
    per = [r * 1000 / k for r in req] if k else []
    wk = []
    for rep, _ in runs:
        try:
            wk.append(json.load(open(os.path.join(W, f'{b}-{cpus}-{arm}-{rep}.json')))['invocation']['workers'])
        except Exception:
            pass
    ok = sum(kv['rc'] == '0' for _, kv in runs)
    print(f'| {b} | {cpus} | {arm} | {len(runs)} ({ok}) | {med(wall)} | {med(cpu)} | {med(req, "{:,.0f}")} | '
          f'{med(per, "{:.2f}")} | {med(wk, "{:.0f}") if arm != "java" else "64 (Java default)"} |')
