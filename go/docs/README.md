# Go documentation

This documentation describes the unreleased Go implementation on main. The latest
published release is Java v0.3.2. Go will replace Java at release.

- **First run:** [getting-started.md](getting-started.md), with local summaries, object-store text listings,
  dataset output and source-specific limits.

Shared object-store authentication and request controls are in [running.md](running.md#object-store-requests);
provider pages linked there describe each provider's support and differences.

Who reads what. Each page owns its facts; the others link to it.

- **Operator** (schedules and watches runs): [production.md](production.md), then [running.md](running.md)
  for the full reference and [cli.md](commands/swath.md) for every flag, exit code and report field.
- **Coming from the Java CLI:** [from-java.md](from-java.md), then [internals/cli-inventory.md](internals/cli-inventory.md) (every
  Java option against this tool) and [upgrade.md](upgrade.md) (from the Java layout to the swath dataset, for Java users and for readers).
- **Reading or integrating a dataset** (any language): [dataset.md](dataset.md), the swath dataset profile: what
  a swath dataset is, the schema ids and their columns, `extensions.swath`, key order and evolution.
- **Contributor**, in reading order: [internals/architecture.md](internals/architecture.md),
  [internals/contracts.md](internals/contracts.md), [internals/ordering.md](internals/ordering.md) (the order
  stage, its budget, waits and spill), [internals/providers.md](internals/providers.md) (the provider contract,
  registry and capabilities, with a page per provider, [internals/clients.md](internals/clients.md) for the list
  clients and [internals/range-engine.md](internals/range-engine.md) for the range engine), [internals/parquet-output.md](internals/parquet-output.md),
  [internals/durability.md](internals/durability.md) (journal and resume),
  [internals/counters.md](internals/counters.md), [internals/dependencies.md](internals/dependencies.md),
  [internals/testing.md](internals/testing.md).
- **The replay server** (`cmd/swath-replay`: a recorded listing served through the S3, GCS and Azure APIs for
  tests and benchmarks): [internals/swath-replay.md](internals/swath-replay.md).
- **Reading a dataset in Go** (`go/reader`, a nested module, the Go API over [dataset.md](dataset.md): the schema
  descriptors, the manifest's `extensions.swath`, typed rows decoded from SeekParquet rows, and opening, schema checks, whole-source
  acceptance and current-object streaming): [../reader/README.md](../reader/README.md).
- **Reading a dataset from an object store** (`go/objfile`, a nested module: an object opened as a file read by
  byte range, one version pinned, over S3, GCS or Azure): [../objfile/README.md](../objfile/README.md).
- **Reviewer:** [internals/contracts.md](internals/contracts.md) (what is promised and which test holds it),
  [internals/measurements.md](internals/measurements.md) (the evidence, by run and date), [internals/testing.md](internals/testing.md).

The root [README.md](../../README.md) introduces the Go implementation. The root
[Java algorithm](../../docs/internals/algorithms.md),
[Java architecture](../../docs/internals/architecture.md) and historical
[Java v0.3.2 field guide](https://swath.varve.io/field-guide/) describe the Java engine.
Use [release-tagged Java docs](https://github.com/varveio/swath/tree/v0.3.2/docs) for that release. The
language-neutral contracts in [docs/internals/contracts.md](../../docs/internals/contracts.md) bind both.
