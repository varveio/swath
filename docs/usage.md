# Using swath

This page covers the choices that change a listing's output or durability. For every
visible flag and its current default, use the CLI itself:

```bash
swath list --help
swath resume --help
swath list --tune help
```

For a first run, start with [Getting started](getting-started.md). Credentials, IAM,
endpoints, and request cost are in [Operating swath](operating.md).

## Commands and targets

```text
swath list <s3-uri> [options]
swath resume <output-directory> [options]
```

Verbs are explicit: `swath s3://bucket` is an error and suggests `swath list`.
Targets are `s3://bucket` or `s3://bucket/prefix`. The path is literal;
`s3://bucket/a%20b` means the bytes `a%20b`, not `a b`.

swath currently lists objects from general-purpose S3 buckets. It rejects directory
buckets before the first request because their listing contract does not provide the
global order and `StartAfter` primitive required by the parallel range scan. Version
history and delete-marker listing are not implemented yet.

## Choose an output

| Goal | Command shape | Durable resume? |
| --- | --- | --- |
| Inspect rows in a terminal | `swath list s3://bucket/prefix` | No |
| Pipe TSV | `swath list ... \| command` | No |
| Write JSONL or TSV | `swath list ... --format jsonl -o rows.jsonl --checkpoint none` | No |
| Keep a Parquet inventory | `swath list ... --format parquet -o out/` | Yes |
| Keep globally key-sorted Parquet | `swath list ... --format parquet --sort -o out/` | Yes |

`--format auto` is the default. It chooses an aligned table when stdout is a terminal
and TSV when stdout is redirected. Explicit formats are `table`, `tsv`, `jsonl`, and
`parquet`.

Prefer a directory-shaped Parquet destination such as `-o out/`. It is the normal,
resumable form and supports parallel writers. A known extension selects FILE kind. Text
files are published atomically but are not resumable. A `.parquet` FILE-kind path is also
non-resumable and currently creates a one-writer dataset directory at that path rather
than one physical file; it therefore requires `--checkpoint none`. If you need one
physical Parquet file, write a directory dataset and combine it downstream.

### Parquet dataset layout

A completed `-o out/ --format parquet` run looks like this:

```text
out/
  data/
    part-00000.parquet
    part-00001.parquet
  manifest.json
  .swath-state.json
  _swath_summary.json
  _SUCCESS
  symlink.txt
```

- `data/` contains only Parquet parts, so `out/data/*.parquet` is a safe reader glob.
- `manifest.json` lists the parts, row counts, checksums, and dataset metadata. Sorted
  datasets also carry key-range metadata.
- `.swath-state.json` is swath's internal published-run identity; consumers should ignore it.
- `_swath_summary.json` is the machine-readable run report.
- `_SUCCESS` is written last. Its presence means the complete snapshot was published.
- `symlink.txt` lists part paths for Hive-, Athena-, and Trino-style discovery.

While a resumable run is active, `.swath/checkpoint.sqlite` is present. A sorted run also
uses `_staging/`. Both are internal and disappear after successful publication. Do not
edit, move, or concurrently reuse a managed output directory. swath refuses symlinked
managed paths and a directory that belongs to another or unfinished run.

Read the parts as one dataset:

```bash
duckdb -c "SELECT count(*) FROM read_parquet('out/data/*.parquet')"
```

There is no required compaction step. Increase `--parquet-part-size` if you want fewer,
larger parts.

## Filter rows

```bash
swath list s3://my-bucket/archive/ \
  --include '\.parquet$' --min-size 1mb \
  --modified-since 2026-01-01 \
  --format parquet -o out/
```

Available filters cover include/exclude regexes, minimum and maximum size, modification
time, and storage class. They compose as an AND chain. `--include` and `--exclude` use
Java regular expressions as substring matches on the key's UTF-8 view; add `^` or `$`
when you need an anchored match.

Filters run after S3 returns a page. They reduce emitted rows, not LIST calls or the
request bill. Changing a filter changes the run identity and is refused during resume.

## Sorted output

`--sort` produces a globally key-sorted Parquet directory dataset. The final parts are
range-disjoint and named in key order: every key in an earlier part is lower than every
key in a later part. Without `--sort`, each S3 page remains ordered but pages from
independent workers may reach different parts in any order; neither a part nor the whole
dataset should be treated as globally sorted.

```bash
swath list s3://my-bucket/ --format parquet --sort -o sorted/
```

Sorted output has three important constraints:

1. It requires a directory-shaped Parquet destination and a durable checkpoint.
2. It writes compressed page-run segments under `_staging/`, then merges them into the
   final Parquet parts. During the merge, staging and final output coexist.
3. Disk usage therefore scales with captured data. Measure a representative prefix and
   provision for both staging and final output, with headroom.

swath checks free space before and during a sorted run. It stops with resumable state
rather than continue toward a merge that is already certain to run out of room. The
diagnostic `--tune sort.ignore-disk-check=on` bypasses that protection; use it only when
the volume has been sized independently.

Large merges use several contiguous key ranges by default and reduce that parallelism
when heap or file-descriptor limits cannot carry it. Small merges remain serial. For
resource sizing, staging codecs, merge controls, and how to recognize the binding limit,
see [Performance](performance.md#the-sorted-merge) and
[Advanced configuration](configuration.md#sorted-output-jvm-properties).

<a id="parallel-range-merge"></a>
<a id="sizing-sorted-output"></a>

## Checkpoint and resume

The output directory is the run handle. With the default `--checkpoint auto`, a Parquet
directory run stores its live checkpoint at `<output>/.swath/checkpoint.sqlite`:

```bash
swath list s3://my-bucket/ --format parquet -o out/
# after Ctrl+C, a crash, or a timebox:
swath resume out/
```

The checkpoint records range ownership and committed cursors. Finalized Parquet parts
remain durable. An unfinished part was never published in the manifest; swath deletes it
and re-lists from the last durable cursor. On clean completion the checkpoint is removed,
and `swath resume out/` reports that the dataset is already complete and exits 0.

Resume restores the original bucket, prefix, output, filters, and run-shaping options.
Changing an identity field is refused. Re-run with the original settings, or use
`--restart` to discard the old checkpoint. To replace a completed dataset deliberately,
start a new listing with `--overwrite`.

`--checkpoint none` uses ephemeral state. It runs the same work-stealing engine but
cannot resume. Stdout also uses ephemeral state. The public `resume` command accepts the
managed output directory, not an arbitrary SQLite path.

### Delivery guarantees

| Sink | Interruption behavior |
| --- | --- |
| Parquet directory dataset | Finalized parts are durable and retained exactly once; an unfinished tail may be re-listed. |
| stdout | One-shot and non-resumable. Commit-before-emit means an interrupted stream can omit a page already committed internally. |
| FILE-kind text | One-shot and non-resumable; successful publication atomically replaces the destination. |
| FILE-kind Parquet | One-writer, non-resumable dataset directory in the current release. |

The exact commit, split, and sink contracts are in
[Contracts and data model](internals/contracts.md#5-resume-args_hash-and-per-sink-guarantees).

## Parquet schema

Each object produces one row. The schema is a stable superset reserved for future
versioned listing, so a few columns are present but unpopulated today.

| Column | Type | Nullable | Meaning |
| --- | --- | --- | --- |
| `key` | `BINARY` | no | Raw key bytes, preserved byte-for-byte rather than coerced to UTF-8 |
| `size` | `INT64` | yes | Object size in bytes |
| `last_modified` | `TIMESTAMP(MICROS,UTC)` | yes | Last-modified time |
| `etag` | `BINARY (UTF8)` | yes | Quotes removed; multipart form retained verbatim |
| `storage_class` | `BINARY (UTF8)` | yes | For example `STANDARD` or `GLACIER` |
| `version_id` | `BINARY (UTF8)` | yes | Reserved; null until versioned listing ships |
| `is_latest` | `BOOLEAN` | yes | Reserved; null until versioned listing ships |
| `is_delete_marker` | `BOOLEAN` | no | Currently always `false` |
| `owner_id` | `BINARY (UTF8)` | yes | Populated with `--fetch-owner` |
| `owner_display_name` | `BINARY (UTF8)` | yes | Populated with `--fetch-owner` |
| `checksum_algorithm` | `BINARY (UTF8)` | yes | Present when returned by S3 |
| `checksum_type` | `BINARY (UTF8)` | yes | Present when returned by S3 |
| `row_type` | `BINARY (UTF8)` | no | Currently `OBJECT`; other values are reserved |

The normative schema, including common-prefix and future delete-marker semantics, is in
[Contracts](internals/contracts.md#4-parquet-output-schema--canonical-superset--etag-rule).

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success, an empty result, an already-complete resume, or a downstream reader closing stdout |
| `1` | Unexpected or otherwise unclassified runtime failure |
| `2` | Bad arguments, URI, configuration, changed resume identity, or a guarded output refusal |
| `74` | Output filesystem full (`EX_IOERR`) |
| `75` | A retryable stuck partial (`EX_TEMPFAIL`), such as exhausted transient retries or the liveness watchdog |
| `124` | `--max-duration` elapsed |
| `130` | SIGINT / Ctrl+C |
| `143` | SIGTERM |

Codes 74, 75, 124, 130, and 143 leave resumable work only when the run uses a managed
Parquet directory. A deterministic failure may still recur when resumed; read the terminal
error and `_swath_summary.json` rather than classifying from the code alone.

## Progress and reports

Progress and summaries go to stderr; stdout remains data. A terminal gets a redrawing
progress line, while logs can opt into appended records. Parquet output writes
`_swath_summary.json` by default. Use `--report PATH` for another destination and
`--stats` / `--no-stats` to control the human summary.

Automation should parse the JSON report rather than scrape terminal text. The complete
operator guide and field reference is [Metrics and observability](metrics-and-observability.md).

<a id="progress"></a>
<a id="end-of-run-summary"></a>

## Advanced configuration

Everyday runs should use the defaults. `--tune`, diagnostic engine ablations, JVM sort
properties, environment precedence, and bearer-token behavior are documented in
[Configuration](configuration.md). The engine-toggle surface is for controlled A/B work,
not ordinary tuning.

<a id="tuning---tune"></a>
<a id="diagnostic-tier-ablation---engine-toggle"></a>
