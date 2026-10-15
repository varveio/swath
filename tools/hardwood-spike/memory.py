#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Sequential, same-file Hardwood memory admission probes; OOM is an observed envelope result."""
import argparse
import hashlib
import json
import os
import pathlib
import platform
import signal
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument('--evidence', required=True, type=pathlib.Path)
p.add_argument('--classpath-file', required=True, type=pathlib.Path)
p.add_argument('--small', required=True, type=pathlib.Path)
p.add_argument('--stress', required=True, type=pathlib.Path)
p.add_argument('--rows', type=int, default=1_000_000)
p.add_argument('--corpus', default='legacy')
p.add_argument('--phase', choices=['smoke', 'matrix', 'batch', 'warm', 'bracket', 'qualify', 'internal'], required=True)
p.add_argument('--java', default=os.environ.get('JAVA_HOME', '') + '/bin/java')
p.add_argument('--timeout', type=int, default=45)
p.add_argument('--warm-heap', type=int, default=128, help='Heap MiB for warm phase (default128; use64 for selected qualification)')
p.add_argument('--warm-api', choices=['both', 'row', 'column'], default='both')
a = p.parse_args()
a.evidence.mkdir(parents=True, exist_ok=True)
if (a.evidence / (a.phase + ".jsonl")).exists():
    raise SystemExit("Use a fresh evidence directory; existing phase receipts will not be overwritten")
cp = a.classpath_file.read_text().strip()
files = {'small': a.small.resolve(), 'stress': a.stress.resolve()}
for source in files.values():
    if not source.is_file():
        raise SystemExit('Missing immutable fixture: ' + str(source))
results = []

def run(which, heap, threads, projection='full', api='row', batch=0, warmups=0, passes=1, properties=None):
    properties = properties or {}
    stem = f'{a.phase}-{which}-{heap}m-t{threads}-{projection}-{api}-b{batch}-w{warmups}'
    if properties:
        stem += '-diagnostic-internals'
    command = ['/usr/bin/time', '-f', '%e\t%M', '-o', str(a.evidence / (stem + '.time')),
               a.java, '-Xms32m', f'-Xmx{heap}m', '-XX:+ExitOnOutOfMemoryError',
               '--enable-native-access=ALL-UNNAMED', '-Dspike.corpus=' + a.corpus,
               '-Xlog:gc:file=' + str(a.evidence / (stem + '.gc.log'))]
    command += ['-D' + k + '=' + str(v) for k, v in properties.items()]
    command += ['-cp', cp, 'io.varve.swath.spike.bench.MemoryProbe', str(files[which]), str(a.rows),
                str(threads), projection, api, str(batch), str(warmups), str(passes)]
    started = time.monotonic()
    with (a.evidence / (stem + '.stdout')).open('w') as out, (a.evidence / (stem + '.stderr')).open('w') as err:
        child = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
        try:
            code = child.wait(timeout=a.timeout)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
            code = 'TIMEOUT'
    stdout = (a.evidence / (stem + '.stdout')).read_text()
    stderr = (a.evidence / (stem + '.stderr')).read_text()
    events = [json.loads(line) for line in stdout.splitlines() if line.startswith('{')]
    status = 'PASS' if code == 0 else 'MEMORY_LIMIT' if 'OutOfMemoryError' in stdout + stderr else 'TIMEOUT' if code == 'TIMEOUT' else 'FAIL'
    result = dict(status=status, exit=code, corpus=a.corpus, geometry=which, path=str(files[which]), heap_mib=heap,
                  requested_threads=threads, projection=projection, api=api, batch_rows=batch,
                  warmups=warmups, passes=passes, internal_diagnostic_properties=properties,
                  events=events, command=command, elapsed_driver_seconds=time.monotonic() - started)
    timing = (a.evidence / (stem + '.time')).read_text().splitlines()
    if timing and '\t' in timing[-1]:
        wall, rss = timing[-1].split('\t')
        result.update(process_seconds=float(wall), process_peak_rss_kib=int(rss))
    results.append(result)
    with (a.evidence / (a.phase + '.jsonl')).open('a') as out:
        out.write(json.dumps(result, sort_keys=True) + '\n')
    report = {k: result[k] for k in ['status', 'geometry', 'heap_mib', 'requested_threads', 'projection', 'api', 'batch_rows']}
    report['process_peak_rss_kib'] = result.get('process_peak_rss_kib')
    print(json.dumps(report), flush=True)

if a.phase == 'smoke':
    run('small', 128, 1)
    run('small', 128, 1, api='column', batch=1024)
    run('stress', 64, 1, api='column', batch=1024)
elif a.phase == 'matrix':
    for which in files:
        for heap in [64, 128, 256]:
            for projection in ['full', 'key']:
                for threads in [1, 2, 4, 0]:
                    run(which, heap, threads, projection)
elif a.phase == 'batch':
    for which in files:
        for heap in [64, 128]:
            for batch in [256, 1024, 4096, 16384]:
                run(which, heap, 1, api='column', batch=batch)
        run(which, 64, 4, api='column', batch=1024)
        run(which, 64, 1, projection='key', api='column', batch=1024)
    run('small', 64, 1, api='validation-only')
elif a.phase == 'bracket':
    for which in files:
        for heap in [32, 48, 64, 96]:
            run(which, heap, 1)
            run(which, heap, 1, api='column', batch=1024)
elif a.phase == 'qualify':
    for which in files:
        for heap in [48, 64, 128]:
            run(which, heap, 1, api='column', batch=1024)
        run(which, 64, 1)
        run(which, 64, 1, projection='key', api='column', batch=1024)
elif a.phase == 'warm':
    for which in files:
        if a.warm_api in ['both', 'row']:
            run(which, a.warm_heap, 1, warmups=1, passes=2)
        if a.warm_api in ['both', 'column']:
            run(which, a.warm_heap, 1, api='column', batch=1024, warmups=1, passes=2)
else:
    # Released but explicitly INTERNAL controls: diagnosis, not a supported production recommendation.
    for which in files:
        for threads in [1, 0]:
            run(which, 64, threads, properties={'hardwood.internal.maxOutstanding': 1})

def sha(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()
source_dir = pathlib.Path(__file__).parent
meta = dict(platform=platform.platform(), uname=list(platform.uname()), cpu_count=os.cpu_count(), java=a.java,
            java_version=subprocess.run([a.java, '-version'], capture_output=True, text=True).stderr,
            phase=a.phase, corpus=a.corpus, fixtures={k: dict(path=str(v), bytes=v.stat().st_size, sha256=sha(v)) for k, v in files.items()},
            source_sha256={str(f): sha(f) for f in [pathlib.Path(__file__).resolve(), source_dir / 'src/bench/hardwood/io/varve/swath/spike/bench/MemoryProbe.java']},
            scope=['Fresh JVM unless warmups explicitly positive; warm OS cache, no cache eviction.',
                   'Every selected value/null and total row count validated. Expected generator/mapping included equally across API modes.',
                   'Peak heap sampled every5ms; peak post-GC heap is the sum of sampled heap-pool collection usage; neither is a strict bound.',
                   'RSS from /usr/bin/time is whole-process peak including native/JIT/mapping; supplemental VmRSS samples are per-pass.',
                   'Retained heap after close uses explicit full GC outside operation timing; this is not concurrent live-set profiling.',
                   'ExitOnOutOfMemoryError makes worker OOM bounded and explicit. No memory bound inferred for other files or simultaneous readers.'])
(a.evidence / (a.phase + '-environment.json')).write_text(json.dumps(meta, indent=2) + '\n')
if any(r['status'] not in ['PASS', 'MEMORY_LIMIT'] for r in results):
    raise SystemExit(1)
