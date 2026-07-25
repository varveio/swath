[![CI](https://github.com/varveio/swath/actions/workflows/ci.yml/badge.svg)](https://github.com/varveio/swath/actions/workflows/ci.yml)

# swath

**Lists very large S3 buckets in parallel, working out how to split the keyspace
while it lists.**

S3 lists a bucket one page at a time: 1000 keys per request, strictly in
lexicographic order. You can start anywhere by handing it a `start-after` token,
and that token doesn't have to be a key that exists — but nothing tells you how
many keys sit between two tokens. So parallelism is a guessing problem. Splitting
the keyspace is free; knowing whether the split was balanced costs a listing.

swath guesses disjoint ranges blind, starts workers on the guesses, and corrects
them as real keys come back — stealing work across a fixed pool when a range turns
out denser or emptier than the guess assumed. No prefix hints, no pre-pass, no
prior knowledge of how the keys are laid out. The name fits the method: the
keyspace is tiled into adjacent ranges and swept in parallel, like mown swaths.

swath is built and maintained by [Varve](https://varve.io/) — we catalog the
datasets inside object storage from listing structure alone, never object
contents. swath is the listing layer underneath it, and we wanted it inspectable
by the people who would have to trust it.

It is a **Java 25** CLI for general-purpose S3 buckets (directory buckets are not
supported), distributed as a self-contained jar and an installable launcher.

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

Every object becomes one row. The columns you will use most:

```text
key              BINARY       raw key bytes, byte-exact, never UTF-8 coerced
size             INT64        object size in bytes
last_modified    TIMESTAMP    micros, UTC
etag             UTF8         quotes stripped, multipart ETags kept verbatim
storage_class    UTF8         STANDARD, GLACIER, ...
row_type         UTF8         OBJECT | COMMON_PREFIX (DELETE_MARKER reserved)
```

Owner, checksum, and versioning columns are always present too, reserved for
versioned listing rather than populated by it. The full schema is in
[`docs/usage.md`](docs/usage.md).

Resume an interrupted run from its output directory:

```bash
swath resume out/
```

## When not to use it

swath reads listings, never objects. It is **LIST-only** by design, and it exists
for buckets where a precomputed listing (AWS S3 Inventory or S3 Metadata tables)
isn't an option — not enabled, too stale, or on a bucket you don't own. If you
already have a fresh Inventory or Metadata table you can query, use that: reading a
precomputed listing is strictly cheaper than any live `ListObjectsV2` lister, swath
included, and swath will tell you so rather than pretend otherwise.

## Behaviour and limits

- **Handles varied general-purpose S3 key distributions** — deep prefix trees,
  flat random keys, heavy skew — with no manual partitioning step, discovering
  the partitioning online as it lists. S3 directory buckets are not supported:
  swath refuses their `--x-s3` naming form before the first LIST request.
- **Is designed for very large listings** without accumulating object rows in
  heap. Active pipeline buffers are configuration-bounded; Parquet output also
  retains metadata proportional to finalized part count, and `--sort` retains
  metadata proportional to staging-segment count.
- **Published scale evidence is still thin.** The CI gate pins heap behaviour at
  100,000 keys — a regression guard, not a scale measurement. Larger runs are in
  progress; their figures, and a throughput number, will land in
  [`docs/performance.md`](docs/performance.md).
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
