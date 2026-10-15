#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run candidate and independent oracle in disjoint JVM classpaths; retain all evidence.

Compile and emit classpaths first with the standalone spike Gradle harness. Supply a fresh
run name because the ownership adapter deliberately uses CREATE_NEW. Exit nonzero if any
probe fails; expected semantic exclusions are explicit UNSUPPORTED records in JSONL.
"""
import argparse
import json
import os
import pathlib
import subprocess
import sys
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--evidence-root', type=pathlib.Path, required=True)
parser.add_argument('--run-name', default='core-final')
parser.add_argument('--profile', choices=['auto', 'delta'], default='auto')
parser.add_argument('--classpath-root', type=pathlib.Path, help='Directory holding core.classpath/oracle.classpath, or a legacy evidence root with harness/; defaults to evidence-root')
parser.add_argument('--native-root', type=pathlib.Path, help='Directory holding native-resource-PROFILE-CODEC.parquet; defaults to the resolved classpath directory/native when present, otherwise the classpath directory')
parser.add_argument('--native', action='store_true', help='Also decode native-resource codec outputs')
args = parser.parse_args()
root = args.evidence_root.resolve()
classpath_root = args.classpath_root.resolve() if args.classpath_root else root
classpath_dir = classpath_root if (classpath_root / 'core.classpath').is_file() else classpath_root / 'harness'
classpath_files = {which: classpath_dir / (which + '.classpath') for which in ['core', 'oracle']}
for path in classpath_files.values():
    if not path.is_file() or not path.read_text().strip():
        parser.error('Missing or empty classpath file: ' + str(path))
if args.native_root:
    native_root = args.native_root.resolve()
else:
    native_root = classpath_dir / 'native' if (classpath_dir / 'native').is_dir() else classpath_dir
if args.native:
    for codec in ['UNCOMPRESSED', 'ZSTD']:
        path = native_root / ('native-resource-' + args.profile + '-' + codec + '.parquet')
        if not path.is_file():
            parser.error('Missing native fixture: ' + str(path))
if 'JAVA_HOME' not in os.environ:
    parser.error('JAVA_HOME must name the JDK 25 installation')
java_path = pathlib.Path(os.environ['JAVA_HOME']) / 'bin' / 'java'
if not java_path.is_file():
    parser.error('Missing Java executable: ' + str(java_path))
out = root / 'core' / args.run_name
if out.exists():
    parser.error('Run directory already exists; choose a fresh --run-name: ' + str(out))
# Validate all inputs before creating a durable run directory.
out.mkdir(parents=True, exist_ok=False)
java = str(java_path)
results = []


def run(which, arguments, label):
    cp = classpath_files[which].read_text().strip()
    arguments = list(arguments)
    if which == 'core':
        arguments.append(args.profile)
    elif arguments[0] in ['read', 'read-unstamped'] and (pathlib.Path(arguments[1]).name.startswith('hardwood-') or pathlib.Path(arguments[1]).name == 'swath-owned.parquet'):
        arguments.append(args.profile)
    main = 'HardwoodCore' if which == 'core' else 'ApacheOracle'
    command = [java, '--enable-native-access=ALL-UNNAMED', '-cp', cp,
               'io.varve.swath.spike.core.' + main, *map(str, arguments)]
    start = time.monotonic()
    with (out / (label + '.log')).open('w') as stream:
        proc = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT,
                              timeout=120, check=False)
    result = {'label': label, 'exit': proc.returncode,
              'status': 'PASS' if proc.returncode == 0 else 'FAIL',
              'seconds': round(time.monotonic() - start, 3), 'command': command}
    results.append(result)
    print(label, result['status'], flush=True)
    (out / 'runs.json').write_text(json.dumps(results, indent=2) + '\n')
    return proc.returncode == 0


for kind in ['canonical', 'semantic', 'dense', 'single', 'equal', 'boundaries', 'boundaries-truncated', 'delta-adversarial', 'dictionary-transitions']:
    path = out / ('hardwood-' + kind + '.parquet')
    if run('core', ['write', path, kind], 'write-hardwood-' + kind):
        run('core', ['read', path, kind], 'read-hardwood-hardwood-' + kind)
        run('oracle', ['read', path, kind], 'read-apache-hardwood-' + kind)
    path = out / ('apache-' + kind + '.parquet')
    if run('oracle', ['write', path, kind], 'write-apache-' + kind):
        run('core', ['read', path, kind], 'read-hardwood-apache-' + kind)
production = out / 'swath-production.parquet'
if run('oracle', ['write-production', production], 'write-swath-production'):
    run('core', ['read', production, 'production'], 'read-hardwood-swath-production')
    run('oracle', ['read-unstamped', production, 'production'], 'read-apache-swath-production')
run('core', ['geometry', out], 'geometry')
if run('core', ['lifecycle', out], 'lifecycle'):
    run('oracle', ['read', out / 'swath-owned.parquet', 'semantic'],
        'read-apache-owned-semantic')
if args.native:
    for codec in ['UNCOMPRESSED', 'ZSTD']:
        run('oracle', ['read-unstamped', native_root /
                      ('native-resource-' + args.profile + '-' + codec + '.parquet'), 'canonical', args.profile],
            'native-oracle-' + codec)
with (out / 'results.jsonl').open('w') as stream:
    for result in results:
        for line in (out / (result['label'] + '.log')).read_text().splitlines():
            if line.startswith('{'):
                json.loads(line)
                stream.write(line + '\n')
sys.exit(0 if all(r['exit'] == 0 for r in results) else 1)
