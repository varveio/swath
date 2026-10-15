#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Inspect resolved artifacts, rather than assuming a dependency-free POM means no linkage."""
import argparse, csv, hashlib, json, pathlib, zipfile
p=argparse.ArgumentParser();p.add_argument('evidence',type=pathlib.Path);args=p.parse_args()
result={}
for table in sorted(args.evidence.glob('*.tsv')):
    artifacts=[]; total=0
    for row in csv.DictReader(table.open(),delimiter='\t'):
        path=pathlib.Path(row['path'])
        if not zipfile.is_zipfile(path): continue
        with zipfile.ZipFile(path) as z:
            hadoop=[]; references=[]; parquet=[]; parquet_refs=[]; native=[]; metadata=[]; classes=0
            for name in z.namelist():
                if name.endswith('.class'):
                    classes+=1
                    if name.startswith('org/apache/hadoop/'): hadoop.append(name)
                    if name.startswith('org/apache/parquet/'): parquet.append(name)
                    data=z.read(name)
                    if b'org/apache/hadoop/' in data or b'org.apache.hadoop.' in data: references.append(name)
                    if b'org/apache/parquet/' in data or b'org.apache.parquet.' in data: parquet_refs.append(name)
                if name.startswith('META-INF/native-image/'):metadata.append(name)
                if name.endswith(('.so','.dll','.dylib','.jnilib')):native.append(name)
            artifacts.append(dict(coordinate=row['coordinate'],bytes=int(row['bytes']),sha256=row['sha256'],classes=classes,
                hadoop_classes=len(hadoop),hadoop_reference_classes=references,native_image_metadata=metadata,native_libraries=native))
            artifacts[-1].update(apache_parquet_classes=len(parquet),apache_parquet_reference_classes=parquet_refs)
            total+=int(row['bytes'])
    result[table.stem]=dict(jars=len(artifacts),bytes=total,artifacts=artifacts)
sources={}
for path in sorted((args.evidence/'sources').glob('*.jar')):
    matches={}
    with zipfile.ZipFile(path) as z:
        for name in z.namelist():
            if name.endswith('.java'):
                hits=[f'{i}: {line.strip()}' for i,line in enumerate(z.read(name).decode().splitlines(),1)
                      if any(word in line for word in ['org.apache.hadoop','org/apache/hadoop','Class.forName','Zstd.defaultCompressionLevel','Zstd.decompress','force(','fsync'])]
                if hits:matches[name]=hits
    sources[path.name]=dict(sha256=hashlib.sha256(path.read_bytes()).hexdigest(),matches=matches)
(args.evidence/'dependency-audit.json').write_text(json.dumps(dict(closures=result,source_evidence=sources),indent=2)+'\n')
print(json.dumps({name:dict(jars=v['jars'],bytes=v['bytes'],hadoop_classes=sum(a['hadoop_classes'] for a in v['artifacts']),
    hadoop_referencing_classes=sum(len(a['hadoop_reference_classes']) for a in v['artifacts']),
    apache_parquet_classes=sum(a['apache_parquet_classes'] for a in v['artifacts']),
    apache_parquet_referencing_classes=sum(len(a['apache_parquet_reference_classes']) for a in v['artifacts'])) for name,v in result.items()},indent=2))
