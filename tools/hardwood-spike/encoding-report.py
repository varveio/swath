#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Audit and summarize matched AUTO/delta experiments; raw JVM records remain authoritative."""
import argparse, hashlib, json, pathlib, statistics
p=argparse.ArgumentParser();p.add_argument('iteration',type=pathlib.Path);args=p.parse_args()
report={'release':'1.1.0.Beta2','zstd_jni':'1.5.7-9','jvm_processes':0,'corpora':{},
        'method':{'fresh_jvm_forks':3,'heap_mib':256,
                  'warmup':'none; cold JVM, warm filesystem cache','logical_sha256_in_operation':True,
                  'durability':'footer close, no file/directory fsync',
                  'timing_comparable_to_iteration_1':False}}
report['environment']=json.loads((args.iteration/'bench/legacy/delta/environment.json').read_text())
def distribution(rows,key):
    values=[r[key] for r in rows]
    return dict(median=statistics.median(values),min=min(values),max=max(values))
for corpus in ['legacy','irregular']:
    c={'row_counts':{}}
    for count in [100_000,1_000_000]:
        item={'profiles':{}};logical=None;geometry=None;page_count=None
        for profile in ['auto','delta']:
            root=args.iteration/'bench'/corpus/profile
            rows=[json.loads(line) for line in (root/'performance.jsonl').read_text().splitlines()]
            assert len(rows)==36 and all(r['exit']==0 for r in rows),(root,'complete successful phase required')
            manifest=next(r for r in json.loads((root/'corpus-manifest.json').read_text()) if r['rows']==count)
            selected=[r for r in rows if r['rows']==count]
            assert all(r['logical_sha256']==manifest['logical_sha256'] for r in selected),'decoded/input hash mismatch'
            assert logical is None or logical==manifest['logical_sha256'],'profiles changed logical values'
            logical=manifest['logical_sha256']
            files=[json.loads(line) for line in (root/'geometry.jsonl').read_text().splitlines()]
            results={}
            for engine in ['hardwood','apache']:
                groups={}
                for mode in ['write','read']:
                    for writer in ([engine] if mode=='write' else ['hardwood','apache']):
                        relevant=[r for r in selected if r['engine']==engine and r['mode']==mode and (mode=='write' or r['label'].startswith(writer+'-'))]
                        assert len(relevant)==3,'three independent JVM forks required'
                        groups[mode+('-'+writer if mode=='read' else '')]={k:distribution(relevant,k) for k in ['operation_seconds','process_seconds','peak_rss_kib','sampled_peak_heap_bytes']}
                file=root/f'{engine}-{count}-0.parquet'
                meta=next(r for r in files if pathlib.Path(r['path'])==file)
                assert meta['logical_rows']==count and len(meta['columns'])==13
                replicated=[r for r in files if pathlib.Path(r['path']).name.startswith(f'{engine}-{count}-')]
                assert len(replicated)==3
                if engine=='hardwood':assert all(r['row_group_rows']==meta['row_group_rows'] and r['all_column_pages']==meta['all_column_pages'] for r in replicated),'candidate geometry varied between forks'
                fork_content={}
                for replica in replicated:
                    payload=pathlib.Path(replica['path']).read_bytes()
                    assert payload[-4:]==b'PAR1'
                    footer_offset=len(payload)-8-int.from_bytes(payload[-8:-4],'little')
                    assert footer_offset>=4
                    fork_content[replica['path']]=dict(bytes=len(payload),sha256=hashlib.sha256(payload).hexdigest(),
                        footer_offset=footer_offset,pre_footer_sha256=hashlib.sha256(payload[:footer_offset]).hexdigest())
                fork_hashes={path:content['sha256'] for path,content in fork_content.items()}
                hashes_identical=len(set(fork_hashes.values()))==1
                if engine=='hardwood':assert hashes_identical,(root,engine,count,'candidate file bytes varied between forks')
                sizes=[c['bytes'] for c in fork_content.values()]
                results[engine]=dict(file_bytes=file.stat().st_size,file_sha256=fork_hashes[str(file)],replicate_file_sha256=fork_hashes,geometry=meta,measurements=groups,
                    replicate_content=fork_content,fork_file_sha256_identical=hashes_identical,
                    fork_file_bytes=dict(median=statistics.median(sizes),min=min(sizes),max=max(sizes)),
                    fork_pre_footer_sha256_identical=len({c['pre_footer_sha256'] for c in fork_content.values()})==1,
                    replicate_geometry=[dict(path=r['path'],row_groups=r['row_groups'],row_group_rows=r['row_group_rows'],all_column_pages=r['all_column_pages']) for r in replicated])
            current=results['hardwood']['geometry']['row_group_rows']
            assert geometry is None or geometry==current,'AUTO/delta row-group boundaries changed'
            current_pages=results['hardwood']['geometry']['all_column_pages']
            assert page_count is None or page_count==current_pages,'AUTO/delta column-page counts changed'
            geometry=current
            page_count=current_pages
            item['profiles'][profile]=results
            report['jvm_processes']+=len(selected)
        auto=item['profiles']['auto']['hardwood'];delta=item['profiles']['delta']['hardwood']
        item['logical_sha256']=logical
        item['corpus_manifest']=manifest
        item['candidate_file_change_bytes']=delta['file_bytes']-auto['file_bytes']
        item['candidate_file_change_percent']=100*(delta['file_bytes']/auto['file_bytes']-1)
        item['column_compressed_byte_changes']={}
        for name,a in auto['geometry']['columns'].items():
            d=delta['geometry']['columns'][name]
            item['column_compressed_byte_changes'][name]={'auto':a['compressed_bytes'],'delta':d['compressed_bytes'],'change':d['compressed_bytes']-a['compressed_bytes']}
        c['row_counts'][str(count)]=item
        print(corpus,count,'AUTO',auto['file_bytes'],'delta',delta['file_bytes'],'Apache',item['profiles']['delta']['apache']['file_bytes'],f"change {item['candidate_file_change_percent']:.2f}%")
    report['corpora'][corpus]=c
assert report['jvm_processes']==144
secondary=args.iteration/'bench/irregular/delta/production-reader/production-reader.jsonl'
if secondary.exists():
    records=[json.loads(line) for line in secondary.read_text().splitlines()]
    assert len(records)==6 and all(r['exit']==0 and r['logical_sha256']==report['corpora']['irregular']['row_counts']['1000000']['logical_sha256'] for r in records)
    report['production_reader_comparison']={'reader_engine':'apache-production','corpus':'irregular',
        'encoding_profile':'delta','rows':1_000_000,'by_file_writer':{}}
    for writer in ['hardwood','apache']:
        rows=[r for r in records if r['label'].startswith(writer+'-')]
        report['production_reader_comparison']['by_file_writer'][writer]={k:distribution(rows,k) for k in ['operation_seconds','process_seconds','peak_rss_kib','sampled_peak_heap_bytes']}
output=args.iteration/'encoding'/'comparison-summary.json'
output.write_text(json.dumps(report,indent=2)+'\n')
print('Wrote',output)
