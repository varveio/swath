#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Fresh-JVM, sequential comparison. Run only after other intensive work has stopped."""
import argparse, json, os, pathlib, platform, signal, subprocess, time

parser = argparse.ArgumentParser()
parser.add_argument('--evidence', type=pathlib.Path, required=True)
parser.add_argument('--classpath-dir', type=pathlib.Path, required=True)
parser.add_argument('--phase', choices=['performance', 'memory', 'smoke', 'calibration', 'memory-reader'], required=True)
parser.add_argument('--apache-target', type=int, default=8 << 20)
parser.add_argument('--hardwood-row-group-rows', type=int, default=(1 << 63)-1)
parser.add_argument('--java', default=os.environ.get('JAVA_HOME', '') + '/bin/java')
args = parser.parse_args()
args.evidence.mkdir(parents=True, exist_ok=True)
cp = {name: (args.classpath_dir / (name + 'Bench.classpath')).read_text().strip() for name in ['hardwood', 'apache']}
results = []

def invoke(engine, mode, path, rows, heap=256, target=8 << 20, label=''):
    stem = f'{args.phase}-{label}-{engine}-{mode}-{rows}-{heap}-{target}'
    command = ['/usr/bin/time', '-f', '%e\t%M', '-o', str(args.evidence / (stem + '.time')),
               args.java, f'-Xmx{heap}m', '-Xms32m', '--enable-native-access=ALL-UNNAMED',
               '-cp', cp[engine], f'io.varve.swath.spike.bench.{engine.capitalize()}Bench',
               mode, str(path), str(rows), str(target),str(args.hardwood_row_group_rows)]
    with (args.evidence / (stem + '.stdout')).open('w') as out, (args.evidence / (stem + '.stderr')).open('w') as err:
        started = time.monotonic()
        try:
            process = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
            code = process.wait(timeout=180)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            code = 'TIMEOUT'
    result = dict(engine=engine, mode=mode, rows=rows, heap_mib=heap, target_bytes=target,
                  hardwood_row_group_rows=args.hardwood_row_group_rows,
                  path=str(path), label=label, exit=code, command=command, elapsed_driver=time.monotonic()-started)
    for line in (args.evidence / (stem + '.stdout')).read_text().splitlines():
        if line.startswith('{'):
            result.update(json.loads(line))
    timing = (args.evidence / (stem + '.time')).read_text().splitlines()
    if timing and '\t' in timing[-1]:
        wall, rss = timing[-1].split('\t')
        result.update(process_seconds=float(wall), peak_rss_kib=int(rss))
    results.append(result)
    with (args.evidence / (args.phase + '.jsonl')).open('a') as out:
        out.write(json.dumps(result, sort_keys=True) + '\n')
    print(json.dumps({k: result.get(k) for k in ['engine','mode','rows','heap_mib','label','exit','operation_seconds','peak_rss_kib']}), flush=True)
    return code == 0

if args.phase == 'smoke':
    for writer in ['hardwood', 'apache']:
        p=args.evidence / f'smoke-{writer}.parquet'
        if invoke(writer, 'write', p, 1000):
            for reader in ['hardwood', 'apache']:
                invoke(reader, 'read', p, 1000, label=writer)
elif args.phase == 'performance':
    for rows in [100_000, 1_000_000]:
        for rep in range(3):
            for writer in (['hardwood','apache'] if rep % 2 == 0 else ['apache','hardwood']):
                p=args.evidence / f'{writer}-{rows}-{rep}.parquet'
                target=args.apache_target if writer=='apache' else 8 << 20
                if invoke(writer,'write',p,rows,target=target,label=f'r{rep}'):
                    for reader in (['hardwood','apache'] if rep % 2 == 0 else ['apache','hardwood']):
                        invoke(reader,'read',p,rows,target=target,label=f'{writer}-r{rep}')
elif args.phase=='calibration':
    for target in ([args.apache_target] if args.apache_target!=8<<20 else [512 << 10, 1 << 20, 4 << 20, 6 << 20, 7 << 20]):
        invoke('apache','write',args.evidence/f'calibration-apache-{target}.parquet',1_000_000,target=target,label='geometry')
elif args.phase=='memory-reader':
    # Reuse the parent's immutable million-row inputs; change only reader parallelism.
    for writer in ['hardwood','apache']:
        invoke('hardwood','read-single',args.evidence.parent/f'{writer}-1000000-0.parquet',1_000_000,64,label=f'{writer}-single-thread')
else:
    for target in [8 << 20, 32 << 20]:
        for heap in [64, 128]:
            for engine in ['hardwood', 'apache']:
                p=args.evidence / f'memory-{engine}-{heap}-{target}.parquet'
                invoke(engine,'write',p,1_000_000,heap,target,label='bounded')
                if p.exists():
                    invoke(engine,'read',p,1_000_000,heap,target,label='bounded')
(args.evidence / 'environment.json').write_text(json.dumps(dict(platform=platform.platform(),
        cpu_count=os.cpu_count(), java=args.java, uname=list(platform.uname()),
        java_version=subprocess.run([args.java,'-version'],capture_output=True,text=True,check=True).stderr),indent=2)+'\n')
if args.phase in ['performance','smoke','calibration']:
    files=sorted(args.evidence.glob('*.parquet'))
    if files:
        with (args.evidence/'geometry.jsonl').open('w') as out, (args.evidence/'geometry.stderr').open('w') as err:
            subprocess.run([args.java,'-Xmx256m','--enable-native-access=ALL-UNNAMED','-cp',cp['apache'],
                    'io.varve.swath.spike.bench.Geometry']+[str(f) for f in files],stdout=out,stderr=err,check=True,timeout=30)
if any(r['exit'] != 0 for r in results) and args.phase not in ['memory','memory-reader']:
    raise SystemExit(1)
