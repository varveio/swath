# swath guide

This guide covers listing a source, printing directory or prefix totals, and writing a
saved inventory. Start with the command you need, then check authentication and the
source-specific limits below.

> **Unreleased Go implementation on main.** The latest published release is the
> S3-focused Java v0.3.2. Build Go from source for this guide. Go will replace Java at
> release; the [Java release docs](https://github.com/varveio/swath/tree/v0.3.2/docs)
> describe different flags, output and recovery.

## Build and start locally

From a checkout, with Go 1.26 and the toolchain selected by `go/go.mod`:

```bash
cd go
./build.sh -o swath
./swath --version
./swath du /data -d 2
```

Replace `/data` with a directory to inspect. Later examples use `swath` on your PATH;
use `./swath` if you are still in the build directory. A local or mounted filesystem
needs no cloud account. `-d 2` prints totals down to two levels below the root.

Sizes default to apparent bytes in listed metadata. For local sources, `--allocated`
uses allocated bytes and `--count-links-once` counts hard-linked files once, where the
platform supplies the required metadata. `--one-filesystem` keeps a local walk on the
root's filesystem where supported. See [CLI details](cli.md) for metadata tiers,
unsupported combinations and missing-size reporting.

## List and summarize

Print the objects in a small public prefix:

```bash
swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ \
  --anonymous --region us-east-1
```

Use the same source with `du` for prefix totals, or select machine-readable output:

```bash
swath du s3://bucket/prefix/ -d 2 --format jsonl
swath list s3://bucket/prefix/ -o objects.tsv.zst
```

Replace `bucket/prefix/` with your source and configure the appropriate credentials.
Cloud listings make provider requests and may incur charges. `du` does not write an
inventory. Text listing is available for object stores; local `list` requires a dataset
destination. Filters select the output, and do not generally eliminate the underlying
listing work. Prefix narrowing and filter semantics are in [cli.md](cli.md).

## Save and read an inventory

Keep the output outside the tree being scanned and use a new or empty destination:

```bash
swath list /data -o /tmp/swath-inventory
swath show /tmp/swath-inventory
```

A swath dataset is a directory of sorted Parquet parts and a `manifest.json` that commits
them, in the SeekParquet format, with one of swath's schema ids and valid swath metadata.
The [dataset profile](dataset.md) documents the schemas, manifest and coverage contract.
`swath show` displays a summary of a dataset or interrupted run; it is not a query engine.

For a committed dataset whose coverage you have checked, use the exact part list from
`manifest.json` with an external query engine. Read optional columns by name across parts
(`read_parquet([...], union_by_name = true)` in DuckDB). Do not glob the output directory:
unfinished or leftover files are not necessarily part of the dataset. The
[reader requirements](dataset.md#keys-and-order) explain schema, ordering and
coverage checks for DuckDB, Python and other consumers.

Local datasets contain directory rows unless `--no-dirs` was used, so a row count is not
necessarily a file count. Cloud and local schemas differ; check the dataset profile
before choosing columns.

## Choose a source

| Source | Input and authentication | Scope |
| --- | --- | --- |
| Local and mounted filesystem | A directory path; the OS mount and its permissions | Files and directories; direct summaries or dataset output. No local scan resume. Not a native NFS or SMB client. |
| Amazon S3 | `s3://bucket/prefix/`; AWS credential chain, `--profile`, or `--anonymous` | General-purpose buckets. `--all-versions` includes versions and delete markers. No directory buckets. |
| Google Cloud Storage | `gs://bucket/prefix/`; Application Default Credentials, `--gcs-credentials`, or `--anonymous` | Current objects, or live and noncurrent generations with `--all-versions`. |
| Azure Blob Storage | `az://account/container/prefix/`; Azure credentials or `--anonymous` | Current blobs in a confirmed flat namespace. No hierarchical namespace or Azure version listing. |

Authentication and request billing: [operating reference](operating.md#credentials).
Azure credential precedence and namespace checks: [Azure provider](internals/providers/azblob.md).
Use `--azblob-flat` only when you know the account has a flat namespace and the provider
cannot confirm it. S3-compatible endpoints need their own compatibility checks.

## Operate a large scan

Before a long run, check source permissions, request cost, free disk and the output
schema. Start with the defaults; increase concurrency only with measurements. Memory,
spill disk, encoder and request budgets are covered in the
[production checklist](operators.md) and [operating reference](operating.md).

Eligible journaled S3, GCS and Azure Blob dataset scans can continue after interruption:

```bash
swath resume /path/to/interrupted-inventory
```

Local scans and text output cannot resume. Windows currently refuses journaled
object-store scans and `resume` because directory sync is unavailable. A resume retains
durable parts, may reuse validated durable spill where supported, and can repeat work
beyond the recovery point. Keep the original compatible build and source identity.
See [stopping and resuming](operating.md#stopping-and-resuming) before retrying a failed run.

A live walk or multi-request listing is not an atomic snapshot. A resumed run can retain
observations from the earlier attempt. Filters, skipped entries, entry errors and
coverage affect whether a dataset represents the whole source. Read the manifest and
run report rather than treating a written directory as proof of a complete scan.

## Compose a workflow

An inventory can feed SQL or Python analysis, migration planning, or a comparison with
an earlier listing. Versioned listings supply S3 versions and delete markers or GCS
generations for external inspection. swath does not perform the transfer or recovery.

Periodic comparisons, expected-file checks and archive catalogs need additional code for
scheduling, comparisons, rules and retention. Metadata presence and size alone do not
validate a backup or provide full file-integrity monitoring. Parquet is not a direct
`rclone --files-from` input: a handoff must generate and test a compatible path list.

swath produces listings, inventories and basic summaries. Varve records context and
makes storage explorable; check the receiving application's supported schemas before
feeding it a dataset.

## Understand the implementation

The [architecture](architecture.md), [range engine](internals/range-engine.md),
[provider contract](internals/providers.md), [ordering](internals/ordering.md) and
[durability](internals/durability.md) describe Go. Backends have different listing and
ordering semantics. The [measurements](measurements.md) identify each workload, build,
machine and output mode, including the separate billion-object S3 count and inventory runs.

The [historical Java field guide](https://swath.varve.io/field-guide/) describes Java
v0.3.2, including its different splitting, sorting and checkpoint model. Use the Go
references above for current Go mechanics.
