#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Fresh-JVM, sequential comparison. Run only after other intensive work has stopped."""
import argparse, hashlib, json, os, pathlib, platform, signal, subprocess, sys, time

parser = argparse.ArgumentParser()
parser.add_argument('--evidence', type=pathlib.Path, required=True)
parser.add_argument('--classpath-dir', type=pathlib.Path, required=True)
parser.add_argument('--phase', choices=['performance', 'memory', 'smoke', 'calibration', 'memory-reader','production-reader','stress-fixture'], required=True)
parser.add_argument('--apache-target', type=int, default=8 << 20)
parser.add_argument('--hardwood-row-group-rows', type=int, default=(1 << 63)-1)
parser.add_argument('--corpus', choices=['legacy','irregular'], default='legacy')
parser.add_argument('--encoding-profile', choices=['auto','delta'], default='auto')
parser.add_argument('--apache-reader', choices=['production','generic'], default='generic')
parser.add_argument('--java', default=os.environ.get('JAVA_HOME', '') + '/bin/java')
args = parser.parse_args()
args.evidence.mkdir(parents=True, exist_ok=True)
if (args.evidence/(args.phase+'.jsonl')).exists():parser.error('This phase already has evidence; choose a fresh directory to preserve independent trials')
cp = {name: (args.classpath_dir / (name + 'Bench.classpath')).read_text().strip() for name in ['hardwood', 'apache']}
toolkit=pathlib.Path(__file__).resolve().parent
sources=[p for p in sorted(toolkit.glob('src/**/*.java'))+sorted(toolkit.glob('*.py'))+sorted(toolkit.glob('*.gradle')) if p.is_file()]
artifact_paths=set()
for classpath in cp.values():
    for value in classpath.split(os.pathsep):
        path=pathlib.Path(value)
        if path.is_file():artifact_paths.add(path)
        elif path.is_dir():artifact_paths.update(path.rglob('*.class'))
(args.evidence/'run-provenance.json').write_text(json.dumps(dict(command=sys.argv,
    git_head=subprocess.run(['git','rev-parse','HEAD'],cwd=toolkit,capture_output=True,text=True,check=True).stdout.strip(),
    source_sha256={str(p.relative_to(toolkit)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
    runtime_sha256={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(artifact_paths)}),indent=2)+'\n')
results = []
expected={}
for rows in ([1000] if args.phase=='smoke' else [100_000,1_000_000] if args.phase=='performance' else [1_000_000]):
    command=[args.java,'-Xmx256m','-cp',cp['apache'],'io.varve.swath.spike.bench.CorpusManifest',args.corpus,str(rows)]
    manifest=json.loads(subprocess.run(command,capture_output=True,text=True,check=True,timeout=60).stdout)
    expected[rows]=manifest
(args.evidence/'corpus-manifest.json').write_text(json.dumps(list(expected.values()),indent=2)+'\n')

def invoke(engine, mode, path, rows, heap=256, target=8 << 20, label=''):
    if engine=='apache' and mode=='read' and args.apache_reader=='production':mode='read-production'
    stem = f'{args.phase}-{label}-{engine}-{mode}-{rows}-{heap}-{target}'
    command = ['/usr/bin/time', '-f', '%e\t%M', '-o', str(args.evidence / (stem + '.time')),
               args.java, f'-Xmx{heap}m', '-Xms32m', '--enable-native-access=ALL-UNNAMED',
               '-cp', cp[engine], f'io.varve.swath.spike.bench.{engine.capitalize()}Bench',
               mode, str(path), str(rows), str(target),str(args.hardwood_row_group_rows),args.corpus,args.encoding_profile]
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
                  corpus=args.corpus,encoding_profile=args.encoding_profile,
                  path=str(path), label=label, exit=code, command=command, elapsed_driver=time.monotonic()-started)
    for line in (args.evidence / (stem + '.stdout')).read_text().splitlines():
        if line.startswith('{'):
            result.update(json.loads(line))
    if code==0 and result.get('logical_sha256')!=expected[rows]['logical_sha256']:
        code='HASH_MISMATCH';result.update(exit=code,expected_logical_sha256=expected[rows]['logical_sha256'])
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
elif args.phase=='production-reader':
    for writer in ['hardwood','apache']:
        for rep in range(3):
            invoke('apache','read-production',args.evidence.parent/f'{writer}-1000000-{rep}.parquet',1_000_000,label=f'{writer}-r{rep}')
elif args.phase=='stress-fixture':
    if args.hardwood_row_group_rows!=(1<<63)-1:parser.error('Stress fixture must have no row-count cap')
    path=args.evidence/'hardwood-onegroup.parquet'
    if invoke('hardwood','write',path,1_000_000,512,1<<30,label='fixture-only'):
        invoke('apache','read',path,1_000_000,512,1<<30,label='independent-validation')
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
if args.phase in ['performance','smoke','calibration','stress-fixture']:
    files=sorted(args.evidence.glob('*.parquet'))
    if files:
        with (args.evidence/'geometry.jsonl').open('w') as out, (args.evidence/'geometry.stderr').open('w') as err:
            subprocess.run([args.java,'-Xmx256m','--enable-native-access=ALL-UNNAMED','-cp',cp['apache'],
                    'io.varve.swath.spike.bench.Geometry']+[str(f) for f in files],stdout=out,stderr=err,check=True,timeout=30)
if any(r['exit'] != 0 for r in results) and args.phase not in ['memory','memory-reader']:
    raise SystemExit(1)
