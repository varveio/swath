#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Bounded core+codec native feasibility, with exact reproducible command evidence."""
import argparse, hashlib, json, pathlib, subprocess
p=argparse.ArgumentParser()
p.add_argument('--graal-home', type=pathlib.Path, required=True)
p.add_argument('--classpath-dir', type=pathlib.Path, required=True)
p.add_argument('--evidence', type=pathlib.Path, required=True)
p.add_argument('--profiles', choices=['auto','delta'], nargs='+', default=['auto'])
args=p.parse_args();args.evidence.mkdir(parents=True,exist_ok=True)
if (args.evidence/'native-command.json').exists():p.error('Native evidence already exists; choose a fresh directory')
toolkit=pathlib.Path(__file__).resolve().parent
sources=[toolkit/'src/bench/native/io/varve/swath/spike/bench/NativeProbe.java']+sorted((toolkit/'src/core/java').rglob('*.java'))
(args.evidence/'native-source-sha256.json').write_text(json.dumps({str(path.relative_to(toolkit)):hashlib.sha256(path.read_bytes()).hexdigest() for path in sources},indent=2)+'\n')
exe=args.evidence/'native-probe'
cmd=['timeout','180',str(args.graal_home/'bin/native-image'),'-J-Xmx4g','--parallelism=2',
     '--no-fallback','--enable-native-access=ALL-UNNAMED','-H:IncludeResources=linux/aarch64/libzstd-jni.*',
     '-cp',(args.classpath_dir/'nativeBench.classpath').read_text().strip(),
     'io.varve.swath.spike.bench.NativeProbe',str(exe)]
(args.evidence/'native-command.json').write_text(json.dumps(cmd,indent=2)+'\n')
with (args.evidence/'native-build.log').open('w') as log:
    subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,check=True)
files=[]
for profile in args.profiles:
    for codec in ['UNCOMPRESSED','ZSTD']:
        suffix=codec if args.profiles==['auto'] else f'{profile}-{codec}'
        path=args.evidence/f'native-resource-{suffix}.parquet';files.append(path)
        with (args.evidence/f'native-resource-{suffix}.log').open('w') as log:
            subprocess.run([str(exe),str(path),codec,profile],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=30)
        with (args.evidence/f'native-oracle-{suffix}.log').open('w') as log:
            subprocess.run([str(args.graal_home/'bin/java'),'--enable-native-access=ALL-UNNAMED','-cp',
                    (args.classpath_dir/'oracle.classpath').read_text().strip(),'io.varve.swath.spike.core.ApacheOracle',
                    'read-unstamped',str(path),'canonical',profile],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=30)
(args.evidence/'native-toolchain.txt').write_text(subprocess.run([str(args.graal_home/'bin/native-image'),'--version'],capture_output=True,text=True,check=True).stdout)
with (args.evidence/'native-geometry.jsonl').open('w') as log:
    subprocess.run([str(args.graal_home/'bin/java'),'--enable-native-access=ALL-UNNAMED','-cp',
            (args.classpath_dir/'apacheBench.classpath').read_text().strip(),'io.varve.swath.spike.bench.Geometry']+
            [str(path) for path in files],stdout=log,check=True,timeout=30)
(args.evidence/'native-binary.sha256').write_text(hashlib.sha256(exe.read_bytes()).hexdigest()+'  '+exe.name+'\n')
