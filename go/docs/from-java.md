# Coming from the Java CLI

For someone who has run the Java `swath` CLI. Go is unreleased on main and will replace
Java at release; Java v0.3.2 remains the latest published distribution. Build Go from
source using the [Go guide](guide.md). The binaries currently have different launchers
and output contracts. Every
Java option, output mode, exit code and report field, with its Go counterpart and the decision behind it, is
in [cli-inventory.md](cli-inventory.md). Readers of the dataset should read [dataset.md](dataset.md) (the swath dataset) and [upgrade.md](upgrade.md) (what changed from the Java layout).

## What stays the same

- **Sources.** `s3://bucket[/prefix]` on general-purpose buckets, with the same credential chain, `--profile`,
  `--region`, S3-compatible endpoints and bearer tokens for GCS's XML API. This tool adds native `gs://`
  through the GCS JSON API, `az://account/container[/prefix]` for Azure Blob flat namespaces,
  and local or mounted directory trees.
- **Output.** Object-store text rows as a table, TSV or JSONL, on stdout or in a `.tsv` or `.jsonl` file
  (optionally `.gz` or `.zst`), and a sorted Parquet dataset in a directory. Local `list` requires
  dataset output; local `du` summaries go directly to stdout. The seven filters (`--include`, `--exclude`, sizes,
  times, storage classes) are kept. Their regular expressions are Go's RE2, not Java's.
- **Resume.** `swath resume DIR` continues an eligible journaled S3, GCS or Azure Blob dataset run.
  Local scans and text output cannot resume. Windows currently refuses journaled object-store runs
  and resume ([platform limits](operating.md#known-limits)). The exit codes keep their meaning:
  0, 1, 2, 74, 75, 124, 130 and 143.
- **Controls.** `--max-duration`, `--request-rate`, `--report`, `--progress`, `--stats`, `-v` and `-q`.

## What behaves differently

- **Always sorted, while listing.** Java writes unsorted parts unless asked for `--sort`, then sorts staged
  runs with a merge at the end. Here every dataset is globally key-sorted, and the order comes from the listing
  itself: rows wait in a memory budget (`--row-budget`, 128 MiB), and rows listed ahead of the output go to
  disposable spill files under the output (`--spill-budget`, by default up to half the free space). There is
  no merge pass at the end. A large sorted run spills almost every row once, so plan disk for it
  ([operators.md](operators.md#disk-sizing)).
- **The durable prefix.** A resume keeps published parts and continues after the last published key.
  Validated durable spill can preserve additional work where supported; gaps are listed again.
  This differs from Java's per-range checkpoints. It does not promise to retain every row already
  fetched ([recovery details](operating.md#stopping-and-resuming)).
- **The dataset.** `manifest.json` installed last is the commit (not `_SUCCESS`). Schemas are source-
  and mode-specific: current objects, local entries, S3 versions and GCS generations have different
  fields ([dataset.md](dataset.md)). Java's reader does not read this format ([upgrade.md](upgrade.md)).
- **The request limiter.** Java cuts concurrency on every throttle; Go uses a recent-answer window and
  at most one cut per round trip. S3, GCS and Azure Blob share the controller, with provider-specific throttle
  signals. See [request controls and tuning](operating.md#throttling).
- **The seed.** Before ranges list, delimiter requests tile the bucket at its folders, in parallel waves, and
  the first wave also fetches the first data page, so a one-page bucket lists in one round trip. The seed is on
  by default. `--seed-from PATH` starts from a previous dataset instead.
- **Splits.** A busy range is split by its owner when workers are idle; there is no thief.
- **Counting is a command.** Java's `--format discard` is `swath diag count SOURCE`; `swath du SOURCE [-d N]`
  totals a source per prefix, as `du`.
- **Text rows** use the S3 API's field names (`Key`, `LastModified`, `ETag`, ...), so a JSONL line matches
  what `aws s3api list-objects-v2` prints for the object.

## Flags

The flags kept, renamed and removed are in [cli-inventory.md](cli-inventory.md). The most common ones:
`--concurrency` is `--workers`, `--no-sign-request` is `--anonymous`, `--endpoint-url` is
`--endpoint`, `--parquet-part-size` is `--part-size` (64 MiB by default, Java's was 256 MB), and
`--format parquet --sort -o DIR` is simply `-o DIR`. Object stores share `--anonymous`,
`--endpoint` and `--request-timeout`; provider-specific credentials and limits are described in the
[operating reference](operating.md#object-store-requests).

## The run report

The report is `run.json` beside a committed dataset and the `--report FILE`; Java's was
`_swath_summary.json`. The fields readers most often used were renamed:

| Java | This tool |
|---|---|
| `schema_version` | `report_version` (3) |
| `run_id` | `attempt.id` |
| `objects` (the dataset's rows) | `output.rows_committed`; this attempt's rows are `output.rows_emitted` |
| `bytes` | `output.object_bytes` (object sizes); the dataset's own size is `output.output_bytes` |
| `cost.api_calls` | `requests.http_attempts` |
| `duration_ms`, `listing_duration_ms` | `timings.wall_ms`, `timings.listing_ms` |
| `error_class` | `outcome.cause` |
| `engine_flags` | `resources.features` |

Java's engine, seed and meter blocks have no counterpart. The engine's counters are under
`source.stats.counters` with their own names ([internals/counters.md](internals/counters.md)). The full schema
is under "Run report" in [cli.md](cli.md).

## What is not here

- **Azure hierarchical namespaces and Azure versions.** Go lists current blobs in flat namespaces;
  it does not list hierarchical namespaces, blob versions, snapshots or deleted blobs.
- **S3 directory buckets and GCS soft-deleted objects.** These are outside the supported listing scope.
  Go does support `--all-versions` for S3 versions and delete markers and GCS live and noncurrent
  generations, under their own schemas. The Java release does not implement these modes.
- **Java's thief splits, unsorted Parquet (`--no-sort`), the SQLite checkpoint, OTLP metrics, partitioned
  text datasets and `--tune`.** Each decision and its reason is in [cli-inventory.md](cli-inventory.md).

## Measured against Java

On 251 production key lists served through a fake at 50 ms per page, at 64 workers each, this tool was
slower than the Java CLI on none and took a median 0.26x Java's wall time (`head1`, 2026-11-07). On the four
replayed recordings it was 3.5x to 13.5x faster (`recordings1`, 2026-11-07). On one core, sorted Parquet of
130M to 145M keys took 246 to 328 s against Java's 362 to 515 s (`delta1b`, 2026-11-07). On a live bucket of
1.09 billion objects at 1,024 workers, Java's sorted run was faster: 654 s against 846 s, with a 16 GB
heap against 2.2 GB RSS (`cogslive2`, 2026-11-07). That run was bound by the single Parquet part being written
after listing ended. The caveats: the fake and replay runs measure the engine's coordination, not S3; most
arms ran once; and the live run was cross-cloud, from GCP us-east1 to a bucket in AWS us-west-2. The tables
are in [measurements.md](measurements.md).
