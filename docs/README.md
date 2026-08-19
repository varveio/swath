# Documentation

Choose the path that matches what you are trying to do. You do not need to read the
internals to use swath.

## Start

1. [Getting started](getting-started.md) — make a public listing, save Parquet,
   query it, and resume it.
2. [Installation](install.md) — Docker, the self-contained jar, release archives,
   source builds, and verification.
3. [Common workflows](usage.md) — outputs, filters, sorted Parquet, resume, schema,
   and exit codes.

For a quick lookup of every visible option, run:

```bash
swath list --help
swath resume --help
swath list --tune help
```

## Operate

- [Credentials, IAM, endpoints, and request cost](operating.md)
- [Configuration and advanced controls](configuration.md)
- [Performance and sizing](performance.md)
- [Progress, run reports, metrics, and traces](metrics-and-observability.md)
- [Troubleshooting](faq.md)
- [Docker and packaging reference](packaging-and-docker.md)

The [replay server](swath-replay-server.md) serves a captured listing as an
S3-compatible `ListObjectsV2` endpoint. Use the
[reproduction guide](replay-troubleshooting.md) when investigating behavior that
depends on a bucket's key distribution.

## Understand

Start with [how swath works](internals/overview.md). It explains the problem, range
ownership, work stealing, output, and resume in plain language. Continue only as deep as
you need:

- [Architecture](internals/architecture.md) — components and the flow of a run.
- [Listing algorithms](internals/algorithms.md) — pivots, stealing, seeding, AIMD,
  sorting, and the correctness argument.
- [Contracts and data model](internals/contracts.md) — invariants I1–I12, types,
  persistence schemas, output schemas, and delivery guarantees.
- [Worked bucket shapes](internals/walkthroughs.md) — step-by-step traces through
  difficult distributions.
- [Instrumentation internals](internals/metrics-internals.md) — engagement-counter
  registry and trace schema.
- [S3 implementation compatibility](internals/s3-implementation-compatibility.md)
  and [probe budgets](internals/probe-budgets.md) — focused protocol details.

## Contribute

- [Contributing](../CONTRIBUTING.md)
- [Build and module structure](internals/build-and-modules.md)
- [Testing](ops/dev/TESTING.md)
- [Decision-trace goldens](ops/dev/decision-trace-goldens.md)
- [Field investigations](ops/dev/field-investigations.md) — dated supporting
  evidence, not product semantics.
- [Release process](../RELEASING.md) and the current
  [release notes](ops/dev/RELEASE_NOTES.md)

## Documentation contract

User and internals pages describe current behavior; code remains the final authority when
they disagree. Dated field evidence and release notes explain why behavior changed but do
not override current contracts. Rewrite stale guidance instead of adding correction banners,
and give each detailed fact one canonical owner so linked summaries cannot drift.
