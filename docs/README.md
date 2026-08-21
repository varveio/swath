# Documentation

Choose the path that matches what you are trying to do. You do not need to read the
internals to use swath.

## Start

1. [Getting started](getting-started.md) — try a public listing with Docker, save a
   managed Parquet dataset, query it, and learn how resume works.
2. [Installation](install.md) — choose Docker, the self-contained jar, a release archive,
   or a source build.
3. [Common workflows](usage.md) — choose an output, filter rows, sort Parquet, resume a
   run, and consume the result.

For the current command-line options, ask the installed version:

```bash
swath list --help
swath resume --help
```

## Operate

- [Credentials, IAM, S3-compatible endpoints, and request cost](operating.md)
- [Troubleshooting and FAQ](faq.md)
- [Configuration and advanced controls](configuration.md)
- [Performance and resource sizing](performance.md)
- [Progress, run reports, metrics, and traces](metrics-and-observability.md)

The defaults are intended for ordinary runs. Use [Common workflows](usage.md) and
[Operating swath](operating.md) first; open the configuration, performance, or
observability references when a specific run gives you a reason to.

## Understand

The [visual field guide](https://swath.varve.io/field-guide/) is the clearest explanation
of the problem and the algorithm. It shows why an S3 listing is difficult to parallelize,
how swath divides an unknown keyspace, how safe splits move work to idle workers, and where
a live parallel scan is the wrong choice.

For the repository-level technical model, continue with
[the internals overview](internals/overview.md), then go only as deep as you need:

- [Architecture](internals/architecture.md) — components and the flow of a run.
- [Listing algorithms](internals/algorithms.md) — seeding, pivots, stealing, AIMD,
  sorting, and the correctness argument.
- [Contracts and data model](internals/contracts.md) — invariants I1–I12, types,
  persistence schemas, output schemas, and delivery guarantees.
- [Worked bucket shapes](internals/walkthroughs.md) — step-by-step traces through
  difficult key distributions.
- [Instrumentation internals](internals/metrics-internals.md) — engagement-counter
  registry and trace schema.
- [S3 implementation compatibility](internals/s3-implementation-compatibility.md)
  and [probe budgets](internals/probe-budgets.md) — focused protocol details.

## Contribute

- [Contributing](../CONTRIBUTING.md)
- [Build and module structure](internals/build-and-modules.md)
- [Testing](ops/dev/TESTING.md)
- [Packaging and release engineering](packaging-and-docker.md)
- [Replay server reference](swath-replay-server.md) and the
  [reproduction workflow](replay-troubleshooting.md)
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
