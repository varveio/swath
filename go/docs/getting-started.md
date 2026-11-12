# Getting started

This walk-through goes from a source checkout to a queried inventory: a local directory first, which needs no
cloud account, then a public S3 prefix, then a run that is interrupted and resumed.

swath reads listing metadata only. It never downloads or changes object contents. It is not released yet, so
you build it from source; the latest published release, v0.3.2, has another command line and output
([migrating from 0.3](upgrade.md)). The words used below are defined in [concepts.md](concepts.md).

## 1. Build and check the binary

```bash
git clone https://github.com/varveio/swath.git
cd swath/go
./build.sh -o swath
./swath --version
```

```text
swath devel (11ae90e42babcd159d52e8f492460802b9e0816c)
```

`devel` marks a build from a checkout; the hash is the commit. The examples below call `swath`: put the binary
on your `PATH`, or write `./swath`. [install.md](install.md) has the requirements and other platforms.

## 2. Total a local directory

The local steps use a directory called `photos`: replace it with a directory of yours. The output shown is from
a small tree of four files.

```bash
swath du -d 1 photos
```

```text
    size  files  dirs  last_modified (local)  path
 410 KiB      2     1  2026-03-01 10:00:00    2025/
48.8 KiB      1     0  2026-11-02 09:30:00    2026/
 459 KiB      4     3  2026-11-02 09:30:00    (total)
```

Each row totals everything below it, as `du` does: the files, the directories, their bytes and the newest
time. `-d 1` stops one level down; without it you get the total alone. In a pipe the same rows are TSV, for
scripts ([output.md](output.md#du-rows)).

## 3. List a public prefix

```bash
swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous | head -5
```

`--anonymous` sends no credentials, which a public bucket needs: without it swath looks for your AWS
credentials and stops if there are none. Closing the pipe early ends the run with exit 0.

In a pipe the rows are TSV: a header line, then one row per object, `key` first, in ascending byte order of the
key. From a test bucket, the lines look like this:

```text
key	size	last_modified	etag	storage_class	owner_id	owner_display_name	checksum_algorithm	checksum_type
logs/2026/00/app-00000.log.gz	1000	2026-01-01T00:00:00.000000Z	00000000000000000000000000000000	GLACIER	\N	\N	\N	\N
logs/2026/00/app-00001.log.gz	1007	2026-01-01T00:00:00.000000Z	00000000000000000000000000000001	STANDARD	\N	\N	\N	\N
logs/2026/00/app-00002.log.gz	1014	2026-01-01T00:00:00.000000Z	00000000000000000000000000000002	STANDARD	\N	\N	\N	\N
logs/2026/00/app-00003.log.gz	1021	2026-01-01T00:00:00.000000Z	00000000000000000000000000000003	STANDARD	\N	\N	\N	\N
```

`\N` is null. On a terminal, without `| head`, the rows are a table instead; `--format jsonl` gives JSON
lines, and `-o rows.tsv.zst` a compressed file ([output.md](output.md)).

## 4. Write a dataset

```bash
swath list photos -o photos.parquet
swath show photos.parquet
```

```text
  7 entries in 0.0s · 438 entries/s
  1 part · 3.74 KiB written · peak RSS 31.3 MiB
dataset photos.parquet: SeekParquet format 0 draft 1, swath schema swath/local-entries/v1, swath metadata version 1
key key unique=true, published 2026-11-10T12:50:07Z
source /tmp/swath-docs-check/photos, scope whole ""
coverage complete=true stop=completed skipped=0 filters=0 exclusions=0
1 part, 7 rows, 3832 bytes, keys "2025/" .. "notes.txt"
```

A swath dataset is a directory of sorted Parquet parts and a `manifest.json` that commits them, in the
SeekParquet format, with one of swath's registered schema ids and valid swath metadata
([the swath dataset profile](dataset.md)). The manifest is written last: until it exists, the directory is not
a dataset. The `coverage` line says how much of the tree it holds; the next step reads it.

The output directory must be new or empty. It may lie inside the tree you list: the walk then skips it and the
dataset records it in `exclusions`. A local tree's rows include its directories (`kind` `DIR`), so the dataset
holds 7 rows for 4 files.

## 5. Check that it is complete

A program that needs the whole source reads the manifest's coverage first:

```bash
duckdb -c "SELECT c.complete, c.stop_reason, c.skipped_entries AS skipped,
                  len(c.filters) AS filters, len(c.exclusions) AS exclusions
           FROM (SELECT extensions.swath.coverage AS c FROM read_json('photos.parquet/manifest.json'))"
```

```text
┌──────────┬─────────────┬─────────┬─────────┬────────────┐
│ complete │ stop_reason │ skipped │ filters │ exclusions │
│ boolean  │   varchar   │  int64  │  int64  │   int64    │
├──────────┼─────────────┼─────────┼─────────┼────────────┤
│ true     │ completed   │       0 │       0 │          0 │
└──────────┴─────────────┴─────────┴─────────┴────────────┘
```

`complete` alone says only that the listing reached the end of its scope. Errors that kept entries out are
counted in `skipped`, and a filtered run is complete over what its filters kept. When `complete` is true,
`stop_reason` is `completed`, `skipped` is 0 and `filters` is empty, the dataset covers the recorded scope (the
manifest's `source` and `scope`) minus its `exclusions`, which name what was left out by design, such as an
output directory inside the tree. A program that needs a whole-source inventory also checks that the source and
scope are the ones it expects, accepts any exclusions explicitly, and treats a missing coverage field as unknown
([dataset.md](dataset.md#the-swath-extension)).

## 6. Query it with DuckDB

Read the parts the manifest lists, not a glob of the directory: a directory can hold parts of a run that has
not committed.

```bash
duckdb -c "
  SET VARIABLE parts = (SELECT list('photos.parquet/' || p.path)
                        FROM (SELECT unnest(parts) AS p FROM read_json('photos.parquet/manifest.json')));
  SELECT kind, count(*) AS entries, sum(size) AS bytes
  FROM read_parquet(getvariable('parts'), union_by_name = true)
  GROUP BY kind ORDER BY kind;"
```

```text
┌─────────┬─────────┬────────┐
│  kind   │ entries │ bytes  │
│ varchar │  int64  │ int128 │
├─────────┼─────────┼────────┤
│ DIR     │       3 │  12288 │
│ FILE    │       4 │ 470003 │
└─────────┴─────────┴────────┘
```

`union_by_name = true` reads optional columns by name across parts. A local tree's `key` is binary, since file
names need not be UTF-8: `decode(key)` reads it as text. An object store's `key` is text. The columns of each
source are in the [schema registry](dataset.md#schema-registry).

## 7. Interrupt and resume a run

A dataset run of an object store keeps a journal in the output directory and publishes parts as it goes, so it
can be continued after a stop. Start one, and press Ctrl-C after a few seconds (`--request-rate 5` slows it down
so that there is time to):

```bash
swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous -o stofs.parquet --request-rate 5
```

swath stops gracefully, keeps what it published and the journal, exits with code 130, and says how to
continue. From a run on a test bucket:

```text
^C  INCOMPLETE (interrupted) — resume: swath resume stofs.parquet
  0 objects in 8.0s · 0 keys/s
  40 API calls · in flight avg 0.01 · peak 1
  peak RSS 38.2 MiB
swath: stopped by interrupt
```

`swath show` prints the recorded command and where a resume will continue; `swath resume` continues with the
recorded command. A resume may change the resource and request options, here dropping the rate cap:

```bash
swath show stofs.parquet
swath resume stofs.parquet --request-rate 0
```

```text
...
no part committed; swath resume lists from the start
  60,000 objects in 0.2s · 306,122 keys/s
  223 API calls · in flight avg 31.39 · peak 62
  1 part · 105 KiB written · peak RSS 77.8 MiB
```

This run was stopped before its first part was published, so the resume listed from the start. A longer run
closes a part that holds rows every 30 seconds (`--part-interval`) and publishes the parts in key order, and a
resume keeps every published part and lists only after the last one: [swath-resume](commands/swath-resume.md) shows such a run. Resuming a complete
dataset does nothing and exits 0. Local trees and text output keep no journal: an interrupted one is run again.

## 8. List a private bucket

Leave out `--anonymous`. swath takes credentials from the AWS SDK's usual chain: environment variables, the
shared config and credentials files, SSO, web identity, container and instance roles. Name a profile and the
bucket's region if they are not your defaults:

```bash
swath list s3://my-bucket/prefix/ -o my-inventory --s3-profile analytics --s3-region eu-west-1
```

The listing needs `s3:ListBucket` on the bucket, nothing on the objects. A request to the wrong region fails
with an error that names the right one. GCS (`gs://`) and Azure Blob (`az://`) work the same way with their own
credentials: [sources.md](sources.md).

## Cost and consistency

An S3 or GCS listing request returns up to 1,000 keys, an Azure Blob one up to 5,000, and providers charge per
request. A run sends about one request per page of keys, plus the requests that divide the work between workers
and any retries. The run report counts
them (`requests.http_attempts`) and, on AWS S3 and GCS, estimates their cost at the provider's reference list
rate (`cost.cost_usd`):

```bash
swath list s3://my-bucket -o my-inventory-2 --report attempt-1.json
jq '.requests.http_attempts, .cost.cost_usd' attempt-1.json
```

If a recent S3 Inventory report already exists, querying it costs less than any live listing.

swath publishes the complete result of the live listing it performed. It is not a point-in-time snapshot of a
bucket that changes during the run: an object added or removed while swath lists may or may not appear.

## Next

- [concepts.md](concepts.md): what a run, a part, the manifest, coverage and resume promise.
- [running.md](running.md): size, watch, schedule and resume large runs.
- [output.md](output.md) and [filters.md](filters.md): text formats, totals, and selecting rows.
- [commands/swath.md](commands/swath.md): every command and option.
- [troubleshooting.md](troubleshooting.md) when a command fails.
