# Using swath

This page covers the choices that change a listing's output or durability. For every
visible option and its current default, use the installed CLI:

```bash
swath list --help
swath resume --help
```

For a first run, start with [Getting started](getting-started.md). The stable flag contract
is listed in [Supported CLI surface](cli.md). Credentials, IAM, endpoint compatibility,
and request cost are in [Operating swath](operating.md).

## Commands and targets

```text
swath list <s3-uri> [options]
swath resume <output-directory> [options]
```

Verbs are explicit: `swath s3://bucket` is an error and suggests `swath list`.

Targets are `s3://bucket` or `s3://bucket/prefix`. The path is literal:
`s3://bucket/a%20b` means the bytes `a%20b`, not `a b`.

swath currently lists current objects from general-purpose S3 buckets. It rejects S3
directory buckets before the first request because their listing contract does not
provide the global ordering and `StartAfter` primitive required by the parallel range
scan. Version history and delete-marker listing are planned but not implemented.

A completed run is the complete result of the live listing swath performed. It is not a
point-in-time snapshot of a bucket that changed while the run was in progress.

## Choose an output

| Goal | Command shape | Durable resume? |
| --- | --- | --- |
| Inspect rows in a terminal | `swath list s3://bucket/prefix` | No |
| Pipe TSV | `swath list ... \| command` | No |
| Write JSONL or TSV | `swath list ... --format jsonl -o rows.jsonl --checkpoint none` | No |
| Write partitioned JSONL or TSV | `swath list ... --format jsonl --output-type dir -o rows/ --checkpoint none` | No |
| Create a managed Parquet dataset | `swath list ... --format parquet -o out/` | Yes |
| Create globally key-sorted Parquet | `swath list ... --format parquet --sort -o out/` | Yes |
| Measure listing without writing rows | `swath list ... --format discard --checkpoint none --report run.json` | No |

`--format auto` is the default. It selects an aligned table when stdout is a terminal and
TSV when stdout is redirected. Available formats are `table`, `tsv`, `jsonl`, `parquet`,
and the diagnostic `discard` sink.

`--compression none|gzip|zstd` applies to table, TSV, and JSONL streams, files, or
directory parts. File compression is also inferred from `.gz` or `.zst`; stdout needs the
explicit option. Parquet uses its own compression and rejects this option.

`--writeback-size SIZE` is an off-by-default performance control for open TSV,
JSONL, and Parquet dataset parts, including sorted Parquet final files. It can reduce a
large final-close stall on some filesystems, but it does not finalize a part, publish the
dataset, or shorten the crash-recovery window. Benchmark it before enabling it; see
[Performance](performance.md#writeback-shaping).

### Managed Parquet

Use a directory path such as `out/` when you need checkpoint and resume. A **managed
Parquet dataset** contains data parts plus swath's manifest, completion marker, run report,
and temporary resume state.

Avoid `-o inventory.parquet`. In the current pre-1.0 compatibility behavior, that spelling
selects a one-writer, non-resumable directory layout under a path that looks like a file;
it does **not** create one physical Parquet file. Use `-o inventory/` for a normal managed
dataset and combine parts downstream only when a consumer requires one file.

Text files are published atomically but are one-shot outputs. TSV and JSONL can also use
several background writers to publish a directory dataset, but text directory datasets
are not resumable in this release and require `--checkpoint none`.

The `discard` sink runs and measures the listing pipeline without writing listing rows.
Use `--report PATH` when you want to retain its summary. It does not accept a real output
destination or text compression.

### Directory dataset layout

A completed directory dataset uses one common layout. Parts under `data/` are Parquet,
TSV, or JSONL according to the selected format:

```text
out/
  data/
    part-...
  manifest.json
  .swath-state.json
  _swath_summary.json
  _SUCCESS
  symlink.txt
```

- `data/` contains only parts of the selected format, so a format-specific glob such as
  `out/data/*.parquet` or `out/data/*.jsonl.gz` is safe.
- `manifest.json` lists parts, row counts, checksums, and dataset metadata. Sorted
  datasets also carry key-range metadata.
- `.swath-state.json` stores swath's internal ownership and run identity. Consumers
  should ignore it.
- `_swath_summary.json` is the machine-readable run report. It is automatic for managed
  Parquet and available elsewhere through `--report`.
- `_SUCCESS` is written last. Its presence means swath published the complete result of
  the listing.
- `symlink.txt` lists part paths for Hive-, Athena-, and Trino-style discovery.

While a resumable Parquet run is active, `.swath/checkpoint.sqlite` is present. A sorted
run also uses `_staging/`. Both are internal and disappear after successful publication.

Do not edit, move, or concurrently reuse an active managed directory. swath refuses
symlinked managed paths and non-empty directories that lack durable swath ownership
evidence.

Read all parts as one table:

```bash
duckdb -c "
  SELECT count(*)
  FROM read_parquet('out/data/*.parquet')
"
```

There is no required compaction step. Increase `--parquet-part-size` when a measured
downstream workload benefits from fewer, larger parts.

## Filter rows

```bash
swath list s3://my-bucket/archive/ \
  --include '\.parquet$' \
  --min-size 1mb \
  --modified-since 2026-01-01 \
  --format parquet -o out/
```

Filters cover include/exclude regular expressions, minimum and maximum size,
modification time, and storage class. They compose as an AND chain.

`--include` and `--exclude` use Java regular expressions as substring matches on a UTF-8
view of the key. Add `^` or `$` when an anchored match is required.

Filters run after S3 returns a page. They reduce emitted rows and output size, not LIST
requests or request cost.

The listing-scope `args_hash`, filter specification, and output/run identity are stored
separately. Resume refuses a change to any identity field because combining different
filters, targets, or outputs would produce an incoherent result. Operational settings
classified as free or restorable may be re-supplied as documented by the CLI.

## Sorted output

`--sort` produces a globally key-sorted managed Parquet dataset. Final parts are named in key
order and are strictly disjoint in raw unsigned key order: every key in an earlier non-empty part
is lower than every key in a later part.

Without `--sort`, each S3 page remains ordered, but pages from independent workers can reach
different parts in any order. Neither an individual part nor the complete dataset should be
treated as globally sorted.

```bash
swath list s3://my-bucket/ \
  --format parquet --sort \
  -o sorted/
```

Sorted output has four important constraints:

1. It requires a managed Parquet directory and a durable checkpoint.
2. It writes compressed page-run segments under `_staging/`, then finalizes them through a
   header-scan, reference-router, and encoder pipeline.
3. Staging and final output coexist during finalization, so disk usage scales with captured data.
4. It requires a local filesystem whose Java provider reports file keys. swath captures the
   physical identity of the output and `_staging/` directories before it deletes or renames
   anything, so a run on a provider that reports none — object-store and other non-default
   `FileSystemProvider` implementations — fails during preflight with
   `cannot establish physical identity ... because the filesystem did not provide a file key`
   rather than sweeping a directory it can only identify by pathname.

At finalization, bounded header cursors scan the durable segments, one router assigns complete
ordered part plans, and an admitted pool of encoders reads those plans positionally. The default
maximum encoder count is derived from available processors and capped at eight. Heap and
file-descriptor checks may lower it. `--tune sort.merge-parallelism=1` selects one encoder but
uses the same routing and publication path.

Part count is independent of encoder count: the same input and configuration produce the same parts
whether one encoder or eight ran. `final-file-bytes` is a soft target calibrated from the first
completed Parquet part; rolls happen only between complete staging pages or overlap components,
and an equal-key group is never split. A bounded reference cap can create an earlier roll.

swath checks free space before and during sorted listing. Finalization has no separate free-space
preflight, so retain enough capacity for checkpoint-owned staging, cascade intermediates when fan-in
is constrained, and temporary plus published Parquet parts. A pre-publication failure leaves the
sealed listing resumable. `--tune sort.ignore-disk-check=on` bypasses the startup and listing
checks and is intended only for a volume sized independently.

See [Performance](performance.md#the-sorted-merge) and
[Advanced configuration](configuration.md#sorted-output-jvm-properties).

<a id="sizing-sorted-output"></a>

## Checkpoint and resume

The managed output directory is the public resume handle. With the default
`--checkpoint auto`, swath keeps the live SQLite checkpoint at:

```text
<output>/.swath/checkpoint.sqlite
```

Start and resume:

```bash
swath list s3://my-bucket/ --format parquet -o out/

# after Ctrl+C, a stopped process, a time limit, or a recoverable failure:
swath resume out/
```

The checkpoint records range ownership and committed cursors. Finalized Parquet parts
remain durable. An unfinished part was never published in the manifest; swath removes it
and re-lists from the last durable cursor.

On clean completion the live checkpoint is removed. `swath resume out/` then reports that
the dataset is already complete and exits successfully.

Resume restores the original bucket, prefix, output, filters, and run-shaping identity.
Changing an identity field is refused. Re-run with the original settings, or use
`--restart` to discard an unfinished checkpoint. To replace a completed dataset
deliberately, start a new listing with `--overwrite`.

`--checkpoint none` uses ephemeral state and cannot resume. Stdout, text files, the
legacy `.parquet`-looking one-writer layout, and text directory datasets are one-shot
outputs. The public `resume` command accepts the managed output directory, not an
arbitrary SQLite path.

### Delivery guarantees

| Sink | Interruption behavior |
| --- | --- |
| Managed Parquet dataset | Finalized parts are retained exactly once; an unfinished tail may be re-listed. |
| stdout | One-shot and non-resumable. An interrupted stream can omit a page committed internally before emission. |
| Text file | One-shot and non-resumable; successful publication atomically replaces the destination. |
| TSV/JSONL directory dataset | Non-resumable; `_SUCCESS` is written last, and a failed run has no success marker. |
| `discard` | Diagnostic and non-resumable; it counts rows and can write a JSON report but no listing-output artifact. |
| Legacy `.parquet`-looking destination | One-writer directory layout, non-resumable; avoid this spelling. |

The exact commit, split, and per-sink contracts are in
[Contracts and data model](internals/contracts.md#5-resume-args_hash-and-per-sink-guarantees).

## Parquet schema

Each current object produces one row. The schema reserves columns for future versioned
listing, so several columns are present but unpopulated today.

| Column | Type | Nullable | Meaning |
| --- | --- | --- | --- |
| `key` | `STRING` (physical `BINARY`) | no | UTF-8 object key; physical bytes are preserved byte-for-byte |
| `size` | `INT64` | yes | Object size in bytes |
| `last_modified` | `TIMESTAMP(MICROS,UTC)` | yes | Last-modified time |
| `etag` | `BINARY (UTF8)` | yes | Quotes removed; multipart form retained |
| `storage_class` | `BINARY (UTF8)` | yes | For example `STANDARD` or `GLACIER` |
| `version_id` | `BINARY (UTF8)` | yes | Reserved; null until versioned listing ships |
| `is_latest` | `BOOLEAN` | yes | Reserved; null until versioned listing ships |
| `is_delete_marker` | `BOOLEAN` | no | Currently always `false` |
| `owner_id` | `BINARY (UTF8)` | yes | Populated with `--fetch-owner` |
| `owner_display_name` | `BINARY (UTF8)` | yes | Populated with `--fetch-owner` |
| `checksum_algorithm` | `BINARY (UTF8)` | yes | Present when returned by S3 |
| `checksum_type` | `BINARY (UTF8)` | yes | Present when returned by S3 |
| `row_type` | `BINARY (UTF8)` | no | Currently `OBJECT`; other values are reserved |

The normative schema is in
[Contracts](internals/contracts.md#4-parquet-output-schema--canonical-superset--etag-rule).

### Upgrading from 0.2.4

Before 0.3.0 the `key` column carried no logical type, so query engines surfaced it as an
opaque binary value. The physical bytes, the column statistics, and the sort order are
unchanged, and existing datasets are not rewritten — only the type a reader reports moves.
DuckDB and Spark report `VARCHAR`/`string` where they reported `BLOB`/`binary`, and pyarrow
and pandas yield `str` instead of `bytes`. Drop the conversions that used to be necessary,
such as `decode(key, 'utf-8')` or `CAST(key AS VARCHAR)` in DuckDB, `CAST(key AS STRING)` in
Spark, and `.str.decode('utf-8')` in pandas; a comparison against a blob literal such as
`key = 'prefix/'::BLOB` becomes an ordinary string comparison.

A key whose bytes are not well-formed UTF-8 cannot be represented by this column, so swath
now fails Parquet publication with a typed output error instead of writing a value a reader
would misdecode. A live listing cannot produce such a key, so this affects only
re-publishing or sorting a capture written by an earlier release. See the
[release notes](ops/dev/RELEASE_NOTES.md).

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success, empty result, already-complete resume, or downstream reader closing stdout |
| `1` | Unexpected runtime failure, or a resumable sorted-output disk guard identified by its error marker |
| `2` | Bad argument, URI, configuration, changed resume identity, or guarded output refusal |
| `74` | Output filesystem full (`EX_IOERR`) |
| `75` | Retryable stuck partial (`EX_TEMPFAIL`), such as exhausted transient retries or a liveness watchdog |
| `124` | `--max-duration` elapsed |
| `130` | SIGINT / Ctrl+C |
| `143` | SIGTERM |

Codes 74, 75, 124, 130, and 143 imply resumable work only when the run uses a managed
Parquet directory. A deterministic failure may recur after resume; inspect the terminal
error and `_swath_summary.json`.

The sorted-output disk guards use the markers `sort_disk_precheck_refused` and
`sort_disk_exhaustion_imminent`, with `error_class=sort_disk_exhausted` and
`resumable=true`.

## Progress and reports

Progress and summaries go to stderr so stdout can remain listing data. An interactive
terminal gets a redrawing progress line; appended logs can opt into progress records.

Managed Parquet writes `_swath_summary.json` automatically. Use `--report PATH` for
another destination or another output mode. Automation should parse the JSON report
rather than scrape terminal text.

The complete field reference is in
[Metrics and observability](metrics-and-observability.md).

<a id="progress"></a>
<a id="end-of-run-summary"></a>

## Advanced controls

Everyday runs should use the defaults. Expert `--tune` values, diagnostic engine
ablations, environment precedence, text writer controls, writeback experiments, and
sorted-output JVM properties are documented in [Configuration](configuration.md).

These controls are for measured operator investigations or development experiments, not
a checklist of options to change before the first run.

<a id="tuning---tune"></a>
<a id="diagnostic-tier-ablation---engine-toggle"></a>
