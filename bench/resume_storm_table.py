#!/usr/bin/env python3
"""Tabulate resume_storm.sh output: per bucket, worker count and arm, the median (min-max) over reps.

Readings, all on the resume (its start is t1 - t0 into the proxy timeline, whose zero is the list's first
request): throttle answers in the first 60 s; requests at the proxy; time to a steady limit (the first
1 s, ten 100 ms buckets, with no throttle answer and at least 0.9 x workers in flight in every bucket; '-'
when the resume ends first); the resume's wall; list plus resume wall; the resume report's attempts and
limiter counters. usage: resume_storm_table.py OUTDIR
"""
import json, os, statistics as st, sys

W = sys.argv[1]
groups = {}
for line in open(os.path.join(W, 'arms.tsv')):
    f = line.rstrip('\n').split('\t')
    b, w, arm, rep = f[:4]
    kv = dict(x.split('=', 1) for x in f[4:7])
    groups.setdefault((b, int(w), arm), []).append((rep, kv))


def med(xs, fmt='{:.1f}'):
    xs = [x for x in xs if x is not None]
    if not xs:
        return '-'
    m = st.median(xs)
    return f'{fmt.format(m)} ({fmt.format(min(xs))}-{fmt.format(max(xs))})' if len(xs) > 1 else fmt.format(m)


print('| bucket | workers | arm | runs (killed, rc 0) | 503s first 60 s | requests on resume | steady limit s | resume wall s | list+resume wall s | resume attempts | decreases | increases | slow-start doubles |')
print('|---|---|---|---|---|---|---|---|---|---|---|---|---|')
for (b, w, arm), runs in groups.items():
    thr, req, steady, rw, tw, att, dec, inc, dbl = ([] for _ in range(9))
    ok = 0
    for rep, kv in runs:
        if kv['killed'] != '1':
            continue
        ok += kv['rc'] == '0'
        stem = os.path.join(W, f'{b}-{w}-{arm}-{rep}')
        t = dict(x.split('=') for x in open(stem + '.times').read().split())
        t0, t1, t2 = int(t['t0']), int(t['t1']), int(t['t2'])
        off = (t1 - t0) / 1000
        tl = []
        for l in open(stem + '.tsv').read().splitlines()[1:]:
            a = l.split('\t')
            tl.append((int(a[0]) / 1000, int(a[1]), int(a[2]), int(a[5])))
        after = [x for x in tl if x[0] >= off]
        thr.append(sum(x[2] for x in after if x[0] < off + 60))
        req.append(sum(x[1] for x in after))
        s = None
        for i in range(len(after) - 9):
            win = after[i:i + 10]
            if all(x[2] == 0 and x[3] >= 0.9 * w for x in win):
                s = win[0][0] - off
                break
        steady.append(s)
        rw.append((t2 - t1) / 1000)
        tw.append((t2 - t0) / 1000)
        try:
            v = json.load(open(stem + '.json'))['source']['stats']['counters']['values']
            att.append(v.get('attempts')); dec.append(v.get('limiter.decreases')); inc.append(v.get('limiter.increases'))
            dbl.append(v.get('limiter.slow_start.doubles'))
        except Exception:
            pass
    n = sum(1 for _, kv in runs if kv['killed'] == '1')
    print(f'| {b} | {w} | {arm} | {len(runs)} ({n}, {ok}) | {med(thr, "{:,.0f}")} | {med(req, "{:,.0f}")} | {med(steady)} | '
          f'{med(rw)} | {med(tw)} | {med(att, "{:,.0f}")} | {med(dec, "{:,.0f}")} | {med(inc, "{:,.0f}")} | {med(dbl, "{:,.0f}")} |')
