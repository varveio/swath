#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Bounded core+codec native feasibility, with exact reproducible command evidence."""
import argparse, hashlib, json, pathlib, subprocess
p=argparse.ArgumentParser()
p.add_argument('--graal-home', type=pathlib.Path, required=True)
p.add_argument('--classpath-dir', type=pathlib.Path, required=True)
p.add_argument('--evidence', type=pathlib.Path, required=True)
args=p.parse_args();args.evidence.mkdir(parents=True,exist_ok=True)
exe=args.evidence/'native-probe'
cmd=['timeout','180',str(args.graal_home/'bin/native-image'),'-J-Xmx4g','--parallelism=2',
     '--no-fallback','--enable-native-access=ALL-UNNAMED','-H:IncludeResources=linux/aarch64/libzstd-jni.*',
     '-cp',(args.classpath_dir/'nativeBench.classpath').read_text().strip(),
     'io.varve.swath.spike.bench.NativeProbe',str(exe)]
(args.evidence/'native-command.json').write_text(json.dumps(cmd,indent=2)+'\n')
with (args.evidence/'native-build.log').open('w') as log:
    subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,check=True)
for codec in ['UNCOMPRESSED','ZSTD']:
    path=args.evidence/f'native-resource-{codec}.parquet'
    with (args.evidence/f'native-resource-{codec}.log').open('w') as log:
        subprocess.run([str(exe),str(path),codec],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=30)
    with (args.evidence/f'native-oracle-{codec}.log').open('w') as log:
        subprocess.run([str(args.graal_home/'bin/java'),'--enable-native-access=ALL-UNNAMED','-cp',
                (args.classpath_dir/'oracle.classpath').read_text().strip(),'io.varve.swath.spike.core.ApacheOracle',
                'read-unstamped',str(path),'canonical'],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=30)
(args.evidence/'native-toolchain.txt').write_text(subprocess.run([str(args.graal_home/'bin/native-image'),'--version'],capture_output=True,text=True,check=True).stdout)
with (args.evidence/'native-geometry.jsonl').open('w') as log:
    subprocess.run([str(args.graal_home/'bin/java'),'--enable-native-access=ALL-UNNAMED','-cp',
            (args.classpath_dir/'apacheBench.classpath').read_text().strip(),'io.varve.swath.spike.bench.Geometry']+
            [str(args.evidence/f'native-resource-{codec}.parquet') for codec in ['UNCOMPRESSED','ZSTD']],stdout=log,check=True,timeout=30)
(args.evidence/'native-binary.sha256').write_text(hashlib.sha256(exe.read_bytes()).hexdigest()+'  '+exe.name+'\n')
