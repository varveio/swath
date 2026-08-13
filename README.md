[![CI](https://github.com/varveio/swath/actions/workflows/ci.yml/badge.svg)](https://github.com/varveio/swath/actions/workflows/ci.yml)

# swath

**Lists very large S3 buckets in parallel, working out how to split the keyspace
while it lists.**

Use swath when you need every key in a bucket too big to list one page at a time,
and there is no fresh S3 Inventory or S3 Metadata table to fall back on. It reads
listings, never object contents. If you already have a current precomputed listing,
use that instead: querying it is strictly cheaper than any live
`ListObjectsV2` lister, swath included.

## Quickstart

With Docker installed, this lists a real public prefix without requiring an AWS
account, credentials, a local Swath installation, or a JDK:

```bash
docker run --rm ghcr.io/varveio/swath:latest \
  list s3://noaa-gestofs-pds/estofs.20210101/ \
  --region us-east-1 --no-sign-request --format tsv > /dev/null
```

One day of NOAA's global surge model output on AWS Open Data contains about
5,200 objects and should finish in a few seconds. The object rows are discarded
for this installation and connectivity check; Swath's summary on stderr reports
the objects, elapsed time, API calls, and estimated request cost. Anonymous LIST
requests are not associated with your AWS account. This small slice validates
the workflow, not Swath's scaling claims; the recorded full-bucket run below is
the scale and mechanism evidence.

To keep the same listing as crash-resumable Parquet, create a host directory and
run the container as your user so it can write through the bind mount:

```bash
mkdir -p out
docker run --rm -t --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest \
  list s3://noaa-gestofs-pds/estofs.20210101/ \
  --region us-east-1 --no-sign-request \
  --format parquet -o /out/data
```

This produces `out/data/data/*.parquet` plus `out/data/manifest.json`. Every
object is one row; the most useful columns are the byte-exact `key`, `size`,
`last_modified`, `etag`, `storage_class`, and `row_type`. DuckDB, Athena, Spark,
and Trino can query the directory dataset directly, with no merge step. Resume
an interrupted Docker run from its last committed cursor with:

```bash
docker run --rm -t --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest resume /out/data
```

See [`docs/install.md`](docs/install.md) for every install path, download
verification, private-bucket credentials, and the maintained quickstart. The
complete output schema is in [`docs/usage.md`](docs/usage.md).

![Swath demo: interrupt and resume a 39.7-million-object S3 listing, then query the Parquet inventory with DuckDB](docs/assets/swath-demo-v0.2.1.gif)

**Real 39.7-million-object run:** interrupt the listing, resume it from checkpoint,
then query the Parquet inventory with DuckDB.

S3 lists a bucket one page at a time: 1000 keys per request, strictly in
lexicographic order. You can start anywhere by handing it a `start-after` token,
and that token doesn't have to be a key that exists — but nothing tells you how
many keys sit between two tokens. So parallelism is a guessing problem. Splitting
the keyspace is free; knowing whether the split was balanced costs a listing.

swath guesses disjoint ranges blind, starts workers on the guesses, and corrects
them as real keys come back — stealing work across a fixed pool when a range turns
out denser or emptier than the guess assumed. It needs no user-supplied prefix
hints and no full listing pre-pass; one bounded delimiter probe helps seed the
initial guesses. The name fits the method: the keyspace is tiled into adjacent
ranges and swept in parallel, like mown swaths.

To see the mechanism rather than read it, the
[visual field guide](https://swath.varve.io/field-guide/) walks the range
algebra, the split ladder, and a recorded 39.7-million-object listing where one
guess secretly held 68% of the bucket — with the
[generated trace report](https://swath.varve.io/runs/noaa-gestofs-pds/) of that
run to interrogate. Both live at [swath.varve.io](https://swath.varve.io/).

swath is built and maintained by [Varve](https://varve.io/), a system of record
for object storage.

It is a **Java 25** CLI for general-purpose S3 buckets (directory buckets are not
supported), distributed as a signed multi-arch Docker image, self-contained jar,
and application-distribution archives.

Swath ships `list` and `resume` — crash-safe checkpoint/resume and optional
globally sorted Parquet included. On the roadmap: `inspect`, `diff`,
versioned-bucket listing, and object stores beyond S3. See
[`ROADMAP.md`](ROADMAP.md).

## Behaviour and limits

- **Handles varied general-purpose S3 key distributions** — deep prefix trees,
  flat random keys, heavy skew — with no manual partitioning step, discovering
  the partitioning online as it lists. S3 directory buckets are not supported:
  swath refuses their `--x-s3` naming form before the first LIST request.
- **Is designed for very large listings** without accumulating object rows in
  heap. Active pipeline buffers are configuration-bounded; Parquet output also
  retains metadata proportional to finalized part count, and `--sort` retains
  metadata proportional to staging-segment count.
- **The published large-run evidence is one recorded 39.7-million-object
  listing.** It demonstrates the mechanism under substantial skew, but is not a
  comparative benchmark or a broad characterization of performance. The CI gate
  also pins heap behaviour at 100,000 keys as a regression guard, not a scale
  measurement. See [`docs/performance.md`](docs/performance.md).
- **Costs one LIST request per 1000 keys.** A billion-key bucket is roughly 1M
  requests — about $5 at a $0.005-per-1000-requests reference rate — before probe
  and retry overhead, and before egress if you run it outside the bucket's region.
  Verify current pricing for your region; every run reports its actual
  `cost.api_calls`. See [`docs/operating.md`](docs/operating.md).
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

## Replay server

`swath-replay-server` serves a captured swath listing back as an S3-compatible
`ListObjectsV2` endpoint. Point a lister at it and you get a deterministic,
zero-cost bucket whose key distribution you already know — including the
pathological shapes that are expensive to find and slow to list. It can inject
per-request latency keyed on the shape of the request, so a bucket that only
misbehaves under a particular latency profile can be reproduced on a laptop.

That makes it useful beyond swath itself: reproducing someone's bucket shape
without their credentials or their bill, testing your own tooling against a
listing that would otherwise cost money to enumerate every run, and pinning
listing behaviour in regression tests.

It also works as a **listing cache**. A listing you have already paid to produce
can be served back through the same API your tools already speak: list the bucket
once with swath, then point everything that would otherwise re-list it at the
replay server instead — local speed, no per-request S3 charge, and no load on the
real bucket. Two things to weigh first. It serves a **point-in-time snapshot**,
not live bucket state, so it fits workloads that can tolerate a listing as fresh
as its last capture. And it has **no authentication**, so it belongs inside a
trust boundary you already control. The operational surface is still being
smoothed — see the scope note below.

Scope: path-style `ListObjectsV2` over an existing listing fixture — no object
data, no authentication, not a general S3 emulator. It is built by this repo but
is not part of the swath CLI distribution. See
[`docs/swath-replay-server.md`](docs/swath-replay-server.md).

## Comparisons

The head-to-head benchmarks and tool-by-tool mechanism notes belong to a separate
repo, the [S3-listing comparison study](https://github.com/varveio/s3-listing-study),
which Varve also maintains. Its methodology and tool roster were committed before
any comparative runs began, so the results can be checked against a plan that
predates them. Those runs have not started yet.

## Have a bucket that breaks it

That is the interesting case. Open an issue with the shape — key distribution,
rough object count, what went wrong — or mail oss@varve.io if the shape isn't
something you can post publicly. Bucket names and keys aren't needed; the
distribution is.

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
