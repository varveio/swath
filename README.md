[![CI](https://github.com/varveio/swath/actions/workflows/ci.yml/badge.svg)](https://github.com/varveio/swath/actions/workflows/ci.yml)

# swath

**Parallel, resumable S3 listing for very large buckets.**

swath is an open-source CLI for on-demand S3 listings that are too large for a serial
paginator. It learns the bucket's key distribution as it runs, so flat, deeply nested,
and heavily skewed keyspaces can be listed in parallel without pre-partitioning. Stream
the result, or write a crash-resumable Parquet dataset and query it directly.

> **Safety and consistency:** swath reads listing metadata only; it never downloads or
> modifies object contents. The result is complete for the live listing swath performed,
> but it is not a point-in-time snapshot of a bucket that changes during the run.

- **List in parallel without pre-partitioning.** Idle workers take part of the remaining
  key range from busy workers as the scan discovers where objects actually are.
- **Resume after interruption.** A managed Parquet dataset checkpoints progress, retains
  finalized parts, and continues after Ctrl+C, a stopped container, or a crash.
- **Query the result directly.** Write Parquet for DuckDB, Athena, Trino, and other
  engines, or stream a table, TSV, or JSONL.

Use swath when you need an on-demand listing of a very large bucket and a serial paginator
is too slow. If a sufficiently current S3 Inventory or S3 Metadata table already exists
and contains the fields you need, query that instead. For a small prefix or simple one-off
task, the AWS CLI or an SDK paginator is usually simpler.

swath is designed for general-purpose S3 buckets whose listings are globally ordered and
support `StartAfter`. S3 directory buckets use a different listing contract and are not
supported.

<a id="quickstart"></a>

## Quick start

The quickest check streams a few rows from one historical day in NOAA's public
`noaa-gestofs-pds` bucket. It needs Docker but no AWS account:

```bash
docker run --rm ghcr.io/varveio/swath:latest \
  list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ \
  --no-sign-request --region us-east-1 |
  head -n 5
```

To create a small managed Parquet dataset instead:

```bash
mkdir -p out
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest \
  list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ \
  --no-sign-request --region us-east-1 \
  --format parquet -o /out/stofs-20230113
```

A managed dataset is a directory of Parquet parts plus swath's manifest, completion
marker, run report, and temporary resume state. Query all parts as one table:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace duckdb/duckdb \
  -c "SELECT count(*) AS objects
      FROM read_parquet('out/stofs-20230113/data/*.parquet')"
```

The [getting-started guide](docs/getting-started.md) explains the output files, private
credentials, Windows commands, and resume behavior. Before listing a very large or
requester-pays bucket, read the [request-cost guidance](docs/operating.md#request-cost).

## See it at full scale

[![swath demo: interrupt and resume a 39.6-million-object S3 listing, then query the Parquet inventory with DuckDB](docs/assets/swath-demo-v0.2.1.gif)](docs/full-scale-demo.md)

*Run `noaa-gestofs-pds-2026-08-03-505ae26`. The embedded recording was captured on
2026-08-03 with swath v0.2.1 (`505ae26e6019`) against the full public
`noaa-gestofs-pds` bucket, at `--concurrency 128`, and published 39,585,029 objects to a
managed Parquet dataset. It is an interrupted `swath list` followed by the `swath resume`
that finished it; that resume invocation reported 41,582 `ListObjectsV2` attempts, 22
Parquet parts totaling 790.0 MB, a peak of about 1.7 GB RSS, and 1m36s elapsed for
itself. swath prints an elapsed time per invocation, and no clock for the whole capture
was recorded. The bucket and the current release can produce different figures; the
[run facts](site/data/runs/noaa-gestofs-pds-2026-08-03-505ae26.json) record what is known
about this capture and what is not. Read the
[full-scale demonstration](docs/full-scale-demo.md) before reproducing it.*

The [interactive run trace](https://swath.varve.io/runs/noaa-gestofs-pds/) visualizes a
separate capture of the same bucket, not this recording: run
`noaa-gestofs-pds-field-guide-trace` listed 39,651,850 objects, 66,821 more than the
recording above, and its
[run facts](site/data/runs/noaa-gestofs-pds-field-guide-trace.json) leave version,
commit, date, and command unknown.

The [visual field guide](https://swath.varve.io/field-guide/) explains why S3 listing is
hard to parallelize and walks through the range model, safe splitting, work stealing,
checkpointing, and cases where swath is not the right tool.

## How it works

Suppose one worker owns the ordered key range `(A, Z]`. It lists forward from `A`. When
another worker becomes idle, swath chooses a pivot such as `M` and atomically changes
ownership to two adjacent ranges:

```text
before:  worker 1  (A ------------------------------- Z]
after:   worker 1  (A ------------- M]  worker 2  (M - Z]
```

The ranges touch but never overlap, and the boundary belongs to exactly one side. If the
upper range is sparse, it finishes quickly and steals again. If it is dense, it keeps a
worker busy. Real keys and observed density improve later pivots, so an inaccurate initial
guess does not determine the rest of the run.

Checkpointing follows the same ownership model. A page cursor commits before its rows
enter the output pipeline. A finalized Parquet part is durable; after a crash, swath may
re-list an unfinished tail but does not rewrite finalized parts. The
[internals overview](docs/internals/overview.md) is the technical bridge from this model
to the implementation.

## Output and resume

swath can:

- show an aligned table in a terminal;
- stream TSV or JSONL, optionally compressed;
- write non-resumable TSV or JSONL files or directory datasets;
- write a resumable managed Parquet dataset; and
- opt into globally key-sorted Parquet when downstream readers require key order.

Unsorted Parquet is the faster default. Filters run after S3 returns each page, so they
reduce emitted rows and output size but not LIST requests or the request bill.

## Status and limits

swath is **pre-1.0**. The `list` and `resume` commands, managed Parquet output,
checkpoint/resume, filtering, text output, and opt-in global sorting are implemented and
tested. Flags and schemas may still change before 1.0.

Current scope:

- general-purpose S3 buckets;
- current objects only—version history and delete markers are planned;
- local output;
- JDK 25 for the JAR, application archive, and source build; Docker includes Java; and
- S3 as the supported backend. GCS through its S3-compatible XML API is experimental,
  not a native GCS backend.

Every live scan costs approximately one LIST request per 1,000 returned keys, plus probes,
retries, and any unfinished tail re-listed after interruption. swath reports the actual
request count. A fresh precomputed inventory is normally cheaper.

## Documentation

- **Start:** [getting started](docs/getting-started.md) and
  [installation](docs/install.md).
- **Use:** [supported CLI surface](docs/cli.md), [common workflows](docs/usage.md),
  [credentials and cost](docs/operating.md), and
  [troubleshooting](docs/faq.md).
- **Tune and diagnose:** [configuration](docs/configuration.md),
  [performance](docs/performance.md), and
  [metrics and observability](docs/metrics-and-observability.md).
- **Understand:** [visual field guide](https://swath.varve.io/field-guide/),
  [internals overview](docs/internals/overview.md), and
  [architecture](docs/internals/architecture.md).
- **Contribute:** [contribution guide](CONTRIBUTING.md) and
  [testing guide](docs/ops/dev/TESTING.md).

## License

[Apache-2.0](LICENSE). Bundled third-party dependencies and their licenses are listed in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
