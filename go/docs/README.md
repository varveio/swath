# The Go port's documentation

Who reads what. Each page owns its facts; the others link to it.

- **Operator** (schedules and watches runs): [operators.md](operators.md), then [operating.md](operating.md)
  for the full reference and [cli.md](cli.md) for every flag, exit code and report field.
- **Coming from the Java CLI:** [from-java.md](from-java.md), then [cli-inventory.md](cli-inventory.md) (every
  Java option against this tool) and [upgrade.md](upgrade.md) (the SeekParquet dataset, and format 2 before it, for readers).
- **Contributor**, in reading order: [architecture.md](architecture.md),
  [internals/contracts.md](internals/contracts.md), [internals/ordering.md](internals/ordering.md) (the order
  stage, its budget, waits and spill), [internals/providers.md](internals/providers.md) (S3, GCS, the local
  tree and the range engine), [internals/parquet-output.md](internals/parquet-output.md),
  [internals/durability.md](internals/durability.md) (journal and resume),
  [internals/counters.md](internals/counters.md), [internals/dependencies.md](internals/dependencies.md),
  [testing.md](testing.md).
- **The replay server** (`cmd/swath-replay`: a recorded listing served through the S3, GCS and Azure APIs for
  tests and benchmarks): [replay.md](replay.md).
- **Reading a dataset in Go** (`go/reader`, a nested module: the schema ids and their columns, the manifest's
  `extensions.swath`, typed rows decoded from SeekParquet rows): [../reader/README.md](../reader/README.md).
- **Reading a dataset from an object store** (`go/objfile`, a nested module: an object opened as a file read by
  byte range, one version pinned, over S3, GCS or Azure): [../objfile/README.md](../objfile/README.md).
- **Reviewer:** [internals/contracts.md](internals/contracts.md) (what is promised and which test holds it),
  [measurements.md](measurements.md) (the evidence, by run and date), [testing.md](testing.md).

The repository's root [README.md](../../README.md), [docs/internals/algorithms.md](../../docs/internals/algorithms.md),
[docs/internals/architecture.md](../../docs/internals/architecture.md) and the public
[field guide](https://swath.varve.io/field-guide/) describe the Java engine, not this port. The
language-neutral contracts in [docs/internals/contracts.md](../../docs/internals/contracts.md) bind both.
