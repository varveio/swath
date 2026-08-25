# Documentation

Choose the path that matches the job in front of you. You do not need the internals to
use Swath.

## Start

1. [Getting started](getting-started.md) — verify the image, list a small public prefix,
   create a managed Parquet dataset, query it, and learn the resume model.
2. [Installation](install.md) — choose Docker, the runnable JAR, a release archive, or a
   source build.
3. [Full-scale public demonstration](full-scale-demo.md) — reproduce the
   39.6-million-object README recording and practice interruption and resume.

For the exact options and defaults in the installed release, ask the binary:

```bash
swath list --help
swath resume --help
```

## Use and operate

- [Common workflows and output choices](usage.md)
- [Credentials, IAM, endpoints, and request cost](operating.md)
- [Troubleshooting and FAQ](faq.md)
- [Configuration and advanced controls](configuration.md)
- [Performance and resource sizing](performance.md)
- [Progress, run reports, metrics, and traces](metrics-and-observability.md)

The defaults are intended for ordinary runs. Start with the workflow and operating
guides. Open the configuration, performance, or observability references when a real run
gives you a reason to.

## Understand

The [visual field guide](https://swath.varve.io/field-guide/) is the clearest explanation
of the problem and algorithm. It shows why an unknown S3 keyspace is difficult to divide,
how adjacent ranges preserve correctness, and how idle workers take part of a busy
range's remaining work.

Continue with the [internals overview](internals/overview.md), then go only as deep as
needed:

- [Architecture](internals/architecture.md) — components and the lifecycle of a run.
- [Listing algorithms](internals/algorithms.md) — seeding, pivots, stealing, adaptive
  concurrency, sorting, and the correctness argument.
- [Contracts and data model](internals/contracts.md) — invariants I1–I12, persistence
  schemas, output schemas, and delivery guarantees.
- [Worked bucket shapes](internals/walkthroughs.md) — step-by-step traces through
  difficult key distributions.
- [S3 implementation compatibility](internals/s3-implementation-compatibility.md) and
  [probe budgets](internals/probe-budgets.md) — focused protocol details.
- [Instrumentation internals](internals/metrics-internals.md) — contributor-facing
  metric identities, engagement reasons, and trace schema.

## Contribute

- [Contributing](../CONTRIBUTING.md)
- [Documentation style and terminology](style.md)
- [Build and module structure](internals/build-and-modules.md)
- [Testing](ops/dev/TESTING.md)
- [Packaging and release engineering](packaging-and-docker.md)
- [Replay toolkit](swath-replay.md) and
  [reproduction workflow](replay-troubleshooting.md)
- [Decision-trace goldens](ops/dev/decision-trace-goldens.md)
- [Field investigations](ops/dev/field-investigations.md) — dated evidence, not current
  product semantics
- [Release process](../RELEASING.md) and
  [release notes](ops/dev/RELEASE_NOTES.md)

## Documentation sources of truth

User and internals pages describe current behavior. Code is the final authority when a
difference is found, and the documentation should be corrected in the same change.

Each detailed fact should have one owner. Other pages may summarize it briefly and link
to that owner. Release notes and dated investigations explain how behavior changed; they
do not override the current guides or contracts.
