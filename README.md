# swath

A fast, resumable object-store lister for the buckets other tools choke on.

We kept running into very large S3 buckets that made every lister we tried slow
to a crawl or fall over — deep prefix trees, totally flat random-key spaces,
badly skewed distributions — so we built swath. It is a **Java 25** CLI
distributed as a self-contained jar and an installable launcher, designed for
very large listings. On supported general-purpose S3 buckets, it discovers how
to partition an opaque keyspace *while* it lists it, using demand-driven
range-stealing across a fixed pool of workers, adapting to backpressure so it
degrades instead of failing. The name fits the method: the keyspace is tiled
into adjacent ranges and swept in parallel, like mown swaths, covering every
object exactly once with no gaps and no overlap.

**Status: pre-1.0.** The `list` and `resume` commands are built and tested,
including globally sorted Parquet output and crash-safe checkpoint/resume. Still
planned: the `inspect` and `diff` subcommands, versioned-bucket listing, and
object stores beyond S3 (there are internal design seams, but no supported
backend SPI in v0.1) — see
[`ROADMAP.md`](ROADMAP.md). Flags and output schemas may still change before 1.0.

## Quickstart

Until the first tagged release, build from source; released artifacts (a Docker
image, an uber-jar, and prebuilt binaries) are on the way. See
[`docs/install.md`](docs/install.md) for every install path and a fuller
quickstart.

```bash
./gradlew :swath-cli:installDist
export PATH="$PWD/swath-cli/build/install/swath/bin:$PATH"
swath list s3://my-bucket/prefix/ --no-sign-request --format parquet -o out/
```

Resume an interrupted run from its output directory:

```bash
swath resume out/
```

## What it does

- **Handles varied general-purpose S3 key distributions** — deep prefix trees,
  flat random keys, heavy skew — with no manual partitioning step, discovering
  the partitioning online as it lists. S3 directory buckets are not supported:
  swath refuses their `--x-s3` naming form before the first LIST request.
- **Is designed for very large listings** without accumulating object rows in
  heap. Active pipeline buffers are configuration-bounded; Parquet output also
  retains metadata proportional to finalized part count, and `--sort` retains
  metadata proportional to staging-segment count. The current public heap gate
  covers 100,000 keys; larger-scale measurements are pending.
- **Resumes managed Parquet directory datasets** after Ctrl+C or a crash.
  Finalized parts stay durable; swath discards an unfinalized tail and re-lists
  it from `durable_cursor`. Stdout and FILE-kind destinations are one-shot and
  non-resumable; commit-before-emit means interrupted text output may omit rows.
- **Keeps keys byte-exact** end to end, and the range set always partitions the
  keyspace: no gaps or overlap between concurrent workers.
- **Adapts to backpressure** — a single controller lowers concurrency under
  sustained S3 throttling and restores it as conditions clear, a safety brake for
  hostile endpoints rather than a throughput knob.
- **Writes Parquet, JSONL, TSV, or an aligned table**, with optional globally
  sorted Parquet output.

## When not to use it

swath is **LIST-only**. It exists for buckets where a precomputed listing (AWS S3
Inventory or S3 Metadata tables) isn't an option — not enabled, too stale, or on
a bucket you don't own. If you already have a fresh Inventory or Metadata table
you can query, use that: reading a precomputed listing is strictly cheaper than
any live `ListObjectsV2` lister, swath included, and swath will tell you so rather
than pretend otherwise.

For how swath compares to other S3-listing tools, the head-to-head benchmarks
and the tool-by-tool mechanism notes belong to a separate
[S3-listing comparison study](https://github.com/varveio/s3-listing-study), so
this repo describes swath rather than grading it against rivals. That study has
committed its methodology and tool roster; the comparative runs have not started
yet.

## Documentation

- **Use it** — [`docs/install.md`](docs/install.md) (install & quickstart),
  [`docs/usage.md`](docs/usage.md) (full flag reference),
  [`docs/operating.md`](docs/operating.md) (credentials, minimal IAM, cost),
  [`docs/configuration.md`](docs/configuration.md) (every flag and knob with its
  default), [`docs/performance.md`](docs/performance.md), and
  [`docs/faq.md`](docs/faq.md).
- **Understand it** — [`docs/internals/overview.md`](docs/internals/overview.md)
  is the entry point for how swath works: the architecture, the listing
  algorithm, the output contracts, and engine walkthroughs. You don't need any of
  it to use swath.

Contributing: [`CONTRIBUTING.md`](CONTRIBUTING.md) · Security:
[`SECURITY.md`](SECURITY.md).

## License

[Apache-2.0](LICENSE). Bundled third-party dependencies and their licenses are
listed in [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

## About

swath is built and maintained by [Varve](https://varve.io/); we wrote it because
our own object-storage work kept running into buckets no existing tool could list.
