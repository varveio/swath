[![CI](https://github.com/varveio/swath/actions/workflows/ci.yml/badge.svg)](https://github.com/varveio/swath/actions/workflows/ci.yml)

# swath

**List very large S3 buckets in parallel, without choosing prefixes or partitions first.**

![Swath demo: interrupt and resume a 39.6-million-object S3 listing, then query the Parquet inventory with DuckDB](docs/assets/swath-demo-v0.2.1.gif)

S3 normally exposes a bucket as one ordered sequence of pages. That is simple, but a
single sequence leaves most of a large machine idle. swath starts workers at different
points in the keyspace, watches what they actually find, and moves work from busy ranges
to idle workers. It discovers the useful partitions while it lists.

The result can stream as a table, TSV, or JSONL, or be written as a resumable Parquet
dataset. Global sorting is available with `--sort` when you need it; unsorted output is
the faster default.

## Try it

This lists a small, anonymously readable prefix used by swath's release smoke test:

```bash
docker run --rm ghcr.io/varveio/swath:latest \
  list s3://cmas-smoke-testcase/smoke_example_case/2018gg_18j/inputs/htap/ \
  --region us-east-1 --no-sign-request
```

For a private bucket, remove `--no-sign-request` and provide credentials through the
normal AWS environment, profile, container-role, or instance-role chain.

To keep a resumable Parquet inventory:

```bash
mkdir -p out
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest \
  list s3://my-bucket/prefix/ --format parquet -o /out
```

Query the result directly—there is no conversion or compaction step:

```bash
duckdb -c "SELECT count(*) FROM read_parquet('out/data/*.parquet')"
```

If the listing is interrupted, the output directory is the run handle:

```bash
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest resume /out
```

The [getting-started guide](docs/getting-started.md) walks through the same flow,
including installation choices, credentials, expected files, and common failures.

Building from source requires JDK 25:

```bash
./gradlew :swath-cli:installDist
export PATH="$PWD/swath-cli/build/install/swath/bin:$PATH"
swath list s3://cmas-smoke-testcase/smoke_example_case/2018gg_18j/inputs/htap/ \
  --region us-east-1 --no-sign-request --format parquet -o out/
swath resume out/
```

## The idea

Imagine that one worker owns the key range `(A, Z]`. It lists forward from `A`. When
another worker becomes idle, swath chooses a pivot such as `M` and atomically changes
the ownership to two adjacent ranges:

```text
before:  worker 1  (A ------------------------------- Z]
after:   worker 1  (A ------------- M]  worker 2  (M - Z]
```

The ranges touch but never overlap, and the boundary belongs to exactly one side. If
the upper range is sparse it finishes quickly and steals again; if it is dense it keeps
a worker busy. Real keys and observed density improve later pivots, so a poor initial
guess does not condemn the rest of the run.

Checkpointing follows the same ownership model. A page's cursor is committed before
its rows enter the output pipeline. A finalized Parquet part is durable; after a crash,
swath may re-list an unfinished tail but does not rewrite finalized parts. The exact
range, split, and resume contracts are documented under
[internals](docs/internals/overview.md).

## What it is good at

- Large general-purpose S3 buckets with unknown, skewed, flat, or deeply nested key
  distributions.
- Listings where S3 Inventory or S3 Metadata is unavailable, stale, or controlled by
  somebody else.
- Bounded-memory streaming into Parquet, JSONL, TSV, or a terminal table.
- Long-running inventories that need crash-safe checkpoint and resume.
- Producing globally sorted Parquet when downstream readers require key order.

swath reads listings only. It never fetches object bodies. Filters are applied after
listing, so they reduce output size but not LIST requests.

## When not to use it

If a fresh S3 Inventory or S3 Metadata table already exists, query that instead. A
precomputed inventory is cheaper than any live `ListObjectsV2` scan. For small buckets,
the AWS CLI or an SDK loop may also be simpler.

Every run costs roughly one LIST request per 1,000 returned keys, plus probes, retries,
and any unfinished tail re-listed after interruption. swath reports its actual request
count; see [operating swath](docs/operating.md) before pointing it at a very large or
requester-pays bucket.

## Status and limits

swath is **pre-1.0**. The `list` and `resume` commands, managed Parquet datasets,
checkpoint/resume, and opt-in global sorting are implemented and tested. Flags and
schemas may still change before 1.0.

Current scope:

- general-purpose S3 buckets with globally ordered listings and `StartAfter`;
- object listing only—version history and delete markers are not listed yet;
- local output; and
- JDK 25, with no preview features in shipped artifacts.

S3 directory buckets are rejected because their listing contract does not provide the
ordering this engine requires. See the [roadmap](ROADMAP.md) for planned work.

## Documentation

- **Start:** [getting started](docs/getting-started.md) and
  [installation](docs/install.md).
- **Use and operate:** [common workflows](docs/usage.md),
  [credentials and cost](docs/operating.md), [configuration](docs/configuration.md),
  [performance](docs/performance.md), and [troubleshooting](docs/faq.md).
- **Understand:** [how swath works](docs/internals/overview.md), then the
  [architecture](docs/internals/architecture.md), [algorithms](docs/internals/algorithms.md),
  and [correctness contracts](docs/internals/contracts.md).
- **Contribute:** [contribution guide](CONTRIBUTING.md) and
  [testing guide](docs/ops/dev/TESTING.md).

The repository also ships an unauthenticated, development-only
[S3 listing replay server](docs/swath-replay-server.md). It serves a captured listing as
`ListObjectsV2`, making expensive or pathological key distributions reproducible without
contacting the original bucket.

## License

[Apache-2.0](LICENSE). Bundled third-party dependencies and their licenses are listed in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
