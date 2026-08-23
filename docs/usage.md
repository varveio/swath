# Using swath

This page covers the choices that change a listing's output or durability. For every
visible flag and its current default, use the CLI itself:

```bash
swath list --help
swath resume --help
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
| Write partitioned JSONL or TSV | `swath list ... --format jsonl --output-type dir -o rows/ --checkpoint none` | No |
| Keep a managed Parquet dataset | `swath list ... --format parquet -o out/` | Yes |
| Keep globally key-sorted Parquet | `swath list ... --format parquet --sort -o out/` | Yes |

`--format auto` is the default. It chooses an aligned table when stdout is a terminal
and TSV when stdout is redirected. Explicit formats are `table`, `tsv`, `jsonl`, and
`parquet`. `--compression none|gzip|zstd` compresses table, TSV, or JSONL output to a
file or stdout, and TSV/JSONL parts in a directory dataset. For files it is also
inferred from `.gz` or `.zst`; stdout needs the explicit option. Parquet uses its own
compression and rejects this option.

TSV and JSONL directory datasets use 2–64 bounded writer lanes (`--text-writers`,
default `3`) and rotate independent parts at `--text-part-size` (default `256mb`).
Each compressed part is a complete gzip or Zstandard frame. The dataset publishes a
manifest and writes `_SUCCESS` last, but is non-resumable in this release and therefore
requires `--checkpoint none`. A failed or timed-out text-dataset run therefore has no manifest or
`_SUCCESS`; its already-finalized part files are diagnostic leftovers, not a resumable dataset.
Counts above four are an expert tuning surface: per-lane queue shares
shrink and per-lane rotation can multiply small parts and final-manifest metadata, so benchmark the
`dataset_writer` blocked-time, part, digest, and manifest fields before adopting them.

Use a managed Parquet directory when checkpoint/resume matters. Text file destinations
are published atomically but are not resumable; compressed files are published only
after their gzip or Zstandard frame finishes. TSV/JSONL directories are bounded,
parallel, one-shot datasets.

An interrupted or timed-out managed Parquet run has no consumer manifest or `_SUCCESS` until a
successful resume completes it. Its finalized parts and live/terminal summary metrics may already
exist, but SQLite remains the resume authority; do not treat those parts as a published dataset.

Do not use `-o inventory.parquet` when you expect one physical Parquet file. In the
current release that FILE-kind path creates a one-writer, non-resumable dataset directory
and requires `--checkpoint none`. Write a normal managed dataset and combine it
downstream when a consumer requires one file.

### Directory dataset layout

A completed directory dataset has one common root layout. The parts below are Parquet
for `--format parquet`, `.tsv[.gz|.zst]` for TSV, or `.jsonl[.gz|.zst]` for JSONL:

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
- `manifest.json` lists the parts, row counts, checksums, and dataset metadata. Sorted
  datasets also carry key-range metadata.
- `.swath-state.json` is swath's internal ownership and run identity. It is written
  durably before the first part and refreshed during publication; consumers should
  ignore it.
- `_swath_summary.json` is the machine-readable run report. It is automatic for
  Parquet directories and available for text datasets through `--report`.
- `_SUCCESS` is written last. Its presence means the complete snapshot was published.
- `symlink.txt` lists part paths for Hive-, Athena-, and Trino-style discovery.

While a resumable Parquet run is active, `.swath/checkpoint.sqlite` is present. A sorted
run also uses `_staging/`. Both are internal and disappear after successful publication.
Text datasets never have a live checkpoint. Do not edit, move, or concurrently reuse an
output directory. swath refuses symlinked managed paths and any non-empty directory that
lacks durable swath ownership evidence; a filename such as `data/part-personal.jsonl`
does not establish ownership and will never authorize deletion.

Read the parts as one dataset:

```bash
duckdb -c "SELECT count(*) FROM read_parquet('out/data/*.parquet')"
```

Without a local DuckDB installation, run the same query with its
[official container](https://duckdb.org/docs/current/operations_manual/duckdb_docker):

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace duckdb/duckdb \
  -c "SELECT count(*) FROM read_parquet('out/data/*.parquet')"
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

`--sort` produces a globally key-sorted managed Parquet dataset. The final parts are
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

The output directory is the run handle. With the default `--checkpoint auto`, a managed
Parquet dataset stores its live checkpoint at `<output>/.swath/checkpoint.sqlite`:

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
| Managed Parquet dataset | Finalized parts are durable and retained exactly once; an unfinished tail may be re-listed. |
| stdout | One-shot and non-resumable. Commit-before-emit means an interrupted stream can omit a page already committed internally. |
| FILE-kind text | One-shot and non-resumable; successful publication atomically replaces the destination. |
| Directory-dataset TSV/JSONL | Non-resumable; bounded parallel parts are published with `_SUCCESS` last, and a failed run has no success marker. |
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
| `1` | Unexpected runtime failure, or a resumable sorted-output disk guard (see the markers below) |
| `2` | Bad arguments, URI, configuration, changed resume identity, or a guarded output refusal |
| `74` | Output filesystem full (`EX_IOERR`) |
| `75` | A retryable stuck partial (`EX_TEMPFAIL`), such as exhausted transient retries or the liveness watchdog |
| `124` | `--max-duration` elapsed |
| `130` | SIGINT / Ctrl+C |
| `143` | SIGTERM |

Codes 74, 75, 124, 130, and 143 leave resumable work only when the run uses a managed
Parquet dataset. A deterministic failure may still recur when resumed; read the terminal
error and `_swath_summary.json` rather than classifying from the code alone.

Code 1 is not always fatal. The sorted-output disk guards identify their resumable case in
the terminal or logs with `sort_disk_precheck_refused` at startup or
`sort_disk_exhaustion_imminent` during a run. Both markers carry
`error_class=sort_disk_exhausted`, `stop_reason=sort_disk_exhausted`, and `resumable=true`.
Do not rely on the JSON report for this distinction: a startup refusal may create no report,
and the emergency in-run halt can leave only the last periodic heartbeat.

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
