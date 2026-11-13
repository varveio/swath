# swath show

swath show prints a committed dataset's summary, or an interrupted run's recorded command and progress.

## Synopsis

<!-- synopsis:begin -->
```text
swath show DIR [OPTIONS]
```
<!-- synopsis:end -->

## Description

For a committed dataset, `swath show` prints the manifest's summary: the format and schema, the source and
scope, the coverage, and the parts, rows and key range.

```text
$ swath show inv
dataset inv: SeekParquet format 0 draft 1, swath schema swath/local-entries/v1, swath metadata version 1
key key unique=true, published 2026-11-10T12:40:24Z
source /tmp/swath-docs-check/photos, scope whole ""
coverage complete=true stop=completed unreadable=0 filters=0 exclusions=0
1 part, 7 rows, 3832 bytes, keys "2025/" .. "notes.txt"
```

`complete=true` says the listing reached the end of its scope, not that it holds every entry: `unreadable` counts
entries that could not be read and are missing, `filters` the rules that left rows out, `exclusions` what was left out by design
(an output directory inside the tree). With `complete=true stop=completed unreadable=0 filters=0` the dataset covers
the `source` and `scope` on the line above it, minus its exclusions
([getting-started.md](../getting-started.md#5-check-that-it-is-complete),
[dataset.md](../dataset.md#the-swath-extension)).

For an interrupted run, it prints the command the journal recorded, the frozen identity, the parts committed
so far and where `swath resume` will continue:

```text
$ swath show logs-inventory
interrupted run in logs-inventory (journal version 9, started 2026-11-10T12:40:50Z)
command: swath list --endpoint-url=http://127.0.0.1:19777 --anonymous --output=logs-inventory --request-rate=10 --part-interval=1s s3://bkt/logs/
identity: provider s3, schema swath/s3-objects/v1, writer swath/parquet-r4, part size 67108864, source s3://bkt/logs/, endpoint http://127.0.0.1:19777, region us-east-1, path style true, fips false, dual-stack false, fetch owner false
1 part committed, 1000 rows; swath resume lists after "logs/2026/00/app-00999.log.gz"
```

The command is recorded in its canonical spelling, each option `--name=value`, then SOURCE.

`swath show` reads no rows; to query a dataset, read the parts its manifest lists
([getting-started.md](../getting-started.md#6-query-it-with-duckdb)).

## Options

<!-- flags:begin -->
```text
      --journal  dump the journal's header and records as JSON lines
```
<!-- flags:end -->

`swath show` also takes `-v`, `-q`, `--color` and `--log-format` ([swath(1)](swath.md#options)).

## Examples

Summarize a dataset:

```bash
swath show logs-inventory
```

Dump an interrupted run's journal:

```bash
swath show logs-inventory --journal
```

## Exit status

0 when it printed the summary; 1 when DIR holds neither a dataset nor an interrupted run, or with `--journal`
on a committed dataset (which keeps no journal); 2 for a usage error.

## Files

`DIR/manifest.json`, `DIR/.swath/journal`.

## See also

[swath-list](swath-list.md), [swath-resume](swath-resume.md), [dataset.md](../dataset.md)
