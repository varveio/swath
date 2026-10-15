#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Summarize measured distributions, preserving individual raw run records."""
import argparse, collections, json, pathlib, statistics
p=argparse.ArgumentParser();p.add_argument('evidence',type=pathlib.Path);args=p.parse_args()
groups=collections.defaultdict(list)
for line in (args.evidence/'performance.jsonl').read_text().splitlines():
    r=json.loads(line)
    writer=r['engine'] if r['mode']=='write' else r['label'].split('-')[0]
    groups[(r['rows'],writer,r['engine'],r['mode'])].append(r)
summary=[]
for (rows,writer,engine,mode),runs in sorted(groups.items()):
    out=dict(rows=rows,writer=writer,engine=engine,mode=mode,runs=len(runs),exits=[r['exit'] for r in runs])
    for k in ['operation_seconds','process_seconds','peak_rss_kib','sampled_peak_heap_bytes','file_bytes']:
        values=[r[k] for r in runs if k in r]
        if values:out[k]=dict(median=statistics.median(values),min=min(values),max=max(values))
    summary.append(out)
(args.evidence/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for r in summary:
    print(r['rows'],r['writer'],r['engine'],r['mode'],r.get('operation_seconds'),r.get('peak_rss_kib'))
