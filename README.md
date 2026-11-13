[![CI](https://github.com/varveio/swath/actions/workflows/ci.yml/badge.svg)](https://github.com/varveio/swath/actions/workflows/ci.yml)

# swath

**Parallel listing and inventory for large object stores and filesystems.**

swath is an open-source command-line tool that lists files and objects in local and mounted
filesystems, S3, GCS, and Azure Blob Storage (flat namespace). It can summarize sizes by
directory or prefix (`du`) and write sorted Parquet inventories.

> **Release candidates available.** Download v0.4.0-rc.2 from the
> [releases page](https://github.com/varveio/swath/releases) ([install and verify](docs/install.md)),
> or build from source below. These are candidates for the first Go release. The earlier S3-only
> Java v0.3.2 has another command line and dataset format ([migrating from 0.3](docs/upgrade.md)).

## Measured scale

On 2026-11-07, build `bf5e6d52` listed and counted **1,088,794,418 objects in 177 seconds**
in the public `sentinel-cogs` S3 bucket, without writing an inventory. A **separate run
wrote sorted Parquet in 846 seconds** (14 minutes 6 seconds). Both used 1,024 workers on
one 32-vCPU `n4-highcpu-32` VM with a 400 GB disk in GCP `us-east1`, listing anonymously
across clouds from AWS `us-west-2`.

These are single-run S3 observations, not expected times for every source or machine.
The [measurements and setup](docs/internals/measurements.md) keep the operations, conditions and
other runs separate.

## Supported sources

| Source | Listing and summaries | Limits |
| --- | --- | --- |
| Local and mounted filesystems | Text or dataset listing; direct `du` summaries | No scan resume. Mounted shares use the OS filesystem, not a native NFS or SMB client. |
| Amazon S3 | Text or dataset listing; prefix totals; eligible dataset resume | General-purpose buckets, not directory buckets. `--all-versions` lists versions and delete markers. Check S3-compatible endpoints separately. |
| Google Cloud Storage | Native `gs://` listing; prefix totals; eligible dataset resume | `--all-versions` lists live and noncurrent generations. Their metadata differs from S3 versions. |
| Azure Blob Storage | `az://` listing; prefix totals; eligible dataset resume | Flat namespaces only. `--all-versions` lists versions, snapshots and soft-deleted blobs. Hierarchical namespaces are unsupported. |

Eligible journaled object-store dataset scans can resume. Local scans and text output
cannot. Windows currently refuses journaled object-store dataset runs and recovery through resume
because directory sync is unavailable; local scans work without resume. See
[platform and recovery limits](docs/running.md#known-limits).

<a id="quickstart"></a>

## Quick start

Download a [release candidate](docs/install.md#download-a-release-candidate), or **build from source**
with Go 1.26 or later (the `go` line of [`go.mod`](go.mod)) and git:

```bash
git clone https://github.com/varveio/swath.git
cd swath
./build.sh -o swath
./swath --version
```

### Summarize a local tree

```bash
./swath du /data -d 2
```

Replace `/data` with a directory to inspect. This prints counts and apparent sizes by
directory, two levels below the root, without writing an inventory. Local
`--allocated` and `--count-links-once` options select allocated bytes and count hard-linked
files once where platform metadata supports them. These are size summaries, not a bill.

### Print an object-store listing

This small public NOAA prefix needs no cloud account:

```bash
./swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ \
  --anonymous --s3-region us-east-1
```

Object-store listings go to the terminal or a pipe as text. GCS and Azure Blob use the
same `list` command with their own source and authentication options. Cloud listings make
provider requests and may incur charges; read the [running guide](docs/running.md)
before a large or requester-pays scan.

### Keep an inventory

```bash
./swath list /data -o /tmp/swath-inventory
./swath show /tmp/swath-inventory
```

The destination must be new or empty and should be outside the tree being scanned. `swath show` prints its summary, not SQL results.
A swath dataset is a directory of sorted Parquet parts and a `manifest.json` that commits
them, in the [SeekParquet format](https://github.com/varveio/seekparquet), with a swath
schema and metadata ([dataset profile](docs/dataset.md)). Read it with DuckDB, Python
or another Parquet tool, after checking coverage and schema.

## Output and resume

- `swath du` prints directory or prefix totals as a terminal table, TSV or JSONL.
- `list` can print rows or write TSV/JSONL files, optionally compressed, for every source.
- Dataset output is globally key-sorted Parquet, with a manifest and coverage information.
- `swath resume DIR` continues eligible interrupted S3, GCS or Azure Blob dataset scans.
  It retains durable work and may repeat listing beyond the durable recovery point.

swath reads listing metadata and does not change source contents. A live walk or
multi-request listing is not an atomic storage snapshot. Check coverage, filters,
entry errors and freshness before treating the result as a whole-source inventory.
[Understand your results](docs/results.md) says what each of these covers; the
[running guide](docs/running.md) owns recovery and resource limits.

## When to use it

Use swath when listing itself takes too long, when you need directory or prefix totals,
or when another workflow needs a reusable inventory. An ordinary `find` or `du`, a small
one-off cloud listing, or a sufficiently fresh provider inventory may already answer the
question.

Inventories can feed migration planning, SQL/Python analysis, version inspection, or
periodic comparisons. Scheduling, transfers, diffing, backup validation, alerts and
archive creation belong to other tools or application code. swath produces listings and
inventories; Varve adds context and exploration around storage. Check an integration's
supported schemas rather than assuming every swath dataset is accepted.

## Documentation

The documentation is published at [swath.varve.io](https://swath.varve.io/), built from [`docs/`](docs/README.md).

- **Start:** [start here](docs/start-here.md), [install](docs/install.md),
  [your first inventory](docs/first-inventory.md), [understand your results](docs/results.md).
- **Use:** [command reference](docs/commands/swath.md), [sources](docs/sources.md),
  [output](docs/output.md), [running guide](docs/running.md),
  [production checklist](docs/production.md), [troubleshooting](docs/troubleshooting.md).
- **Read the output:** [swath dataset profile](docs/dataset.md); from Go, the
  [`reader`](reader/README.md) module.
- **Understand:** [architecture](docs/internals/architecture.md), [contracts](docs/internals/contracts.md),
  [providers](docs/internals/providers.md), [measurements](docs/internals/measurements.md).
- **Migrate from 0.3:** [migration guide](docs/upgrade.md); the
  [v0.3.2 documentation](https://github.com/varveio/swath/tree/v0.3.2/docs).
- **Contribute:** [contribution guide](CONTRIBUTING.md), [tests](docs/internals/testing.md),
  [roadmap](ROADMAP.md).

swath is pre-1.0. Flags and schemas can change; keep the build revision with inventories
and run reports.

## History: the Java releases (0.2 and 0.3)

The recordings below were captured with the earlier Java releases. They do not show the
current commands, dataset format or recovery model.

[![swath demo: interrupt and resume a 39.6-million-object S3 listing, then query the Parquet inventory with DuckDB](docs/assets/swath-demo-v0.2.1.gif)](https://github.com/varveio/swath/blob/java-final/docs/full-scale-demo.md)

*Run `noaa-gestofs-pds-2026-08-03-505ae26`. The embedded recording was captured on
2026-08-03 with swath v0.2.1 (`505ae26e6019`) against the full public
`noaa-gestofs-pds` bucket, at `--concurrency 128`, and published 39,585,029 objects to a
managed Parquet dataset. It is an interrupted `swath list` followed by the `swath resume`
that finished it; that resume invocation reported 41,582 `ListObjectsV2` attempts, 22
Parquet parts totaling 790.0 MB, a peak of about 1.7 GB RSS, and 1m36s elapsed for
itself. swath prints an elapsed time per invocation, and no clock for the whole capture
was recorded. The bucket and the current release can produce different figures; the
[run facts](https://github.com/varveio/swath/blob/java-final/site/data/runs/noaa-gestofs-pds-2026-08-03-505ae26.json) record what is known
about this capture and what is not. Read the
[full-scale demonstration](https://github.com/varveio/swath/blob/java-final/docs/full-scale-demo.md) before reproducing it.*

An interactive run trace, no longer served on the website and kept as
[page source](https://github.com/varveio/swath/blob/java-final/site/runs/noaa-gestofs-pds/index.html) at the
`java-final` tag, visualizes a separate capture of the same bucket, not this recording: run
`noaa-gestofs-pds-field-guide-trace` listed 39,651,850 objects, 66,821 more than the
recording above, and its
[run facts](https://github.com/varveio/swath/blob/java-final/site/data/runs/noaa-gestofs-pds-field-guide-trace.json) leave version,
commit, date, and command unknown.

The historical Java visual field guide, likewise kept as
[page source](https://github.com/varveio/swath/blob/java-final/site/field-guide/index.html), explains why S3 listing is
hard to parallelize and walks through the range model, safe splitting, work stealing,
checkpointing, and cases where swath is not the right tool.

For the story behind swath and the study that grew alongside it, read
[How I built swath: listing a billion S3 objects in 4 minutes](https://sagi.org/posts/how-i-built-swath/).
The evidence behind the published large-scale runs is in the
[study findings](https://github.com/varveio/s3-listing-study/blob/main/RESULTS.md).

## License

[Apache-2.0](LICENSE). The Go release archives and images carry `THIRD_PARTY_NOTICES`, the
licenses of every module linked into the binaries, generated at release by
[`notices.sh`](notices.sh).
