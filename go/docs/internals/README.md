# Internals

How the Go implementation works, what it promises and how that is tested and measured. The user's pages are
[../README.md](../README.md). In reading order:

1. [architecture.md](architecture.md): the pipeline, packages, goroutines and ownership.
2. [contracts.md](contracts.md): what is promised and which test holds it.
3. [ordering.md](ordering.md): the order stage, its budget, waits and spill.
4. [providers.md](providers.md): the provider contract, registry and capabilities, with a page per provider
   ([S3](providers/s3.md), [GCS](providers/gcs.md), [Azure Blob](providers/azblob.md),
   [local trees](providers/local.md)); [clients.md](clients.md) for the list clients, request admission and
   retries; [range-engine.md](range-engine.md) for the range engine.
5. [parquet-output.md](parquet-output.md): schemas, parts and the writer.
6. [durability.md](durability.md): the journal, publication and resume.
7. [run-lifecycle.md](run-lifecycle.md): outcome classification and recording, liveness, the forced exit,
   side-path checks, encoder sizing.
8. [counters.md](counters.md): every counter in the run report.
9. [diagnostics.md](diagnostics.md): `swath diag` and its options, tracing and profiling.
10. [dependencies.md](dependencies.md): the modules swath depends on and why.
11. [testing.md](testing.md): test tiers, CI jobs, stress repeats, fixtures.
12. [measurements.md](measurements.md): the evidence, by run and date.
13. [cli-inventory.md](cli-inventory.md): every Java CLI option, output mode, exit code and report field
    against this tool, with the decision behind it.

The replay server (`cmd/swath-replay`: a recorded listing served through the S3, GCS and Azure APIs for tests
and benchmarks): [swath-replay.md](swath-replay.md) (using it), [replay.md](replay.md) (how it is built) and
[replay-java.md](replay-java.md) (its differences from the Java server).

The nested Go modules: [`go/reader`](../../reader/README.md) reads a dataset in Go (schema descriptors, the
manifest's `extensions.swath`, typed rows and current-object streaming);
[`go/objfile`](../../objfile/README.md) opens an object in S3, GCS or Azure as a file read by byte range.

The language-neutral contracts in [docs/internals/contracts.md](../../../docs/internals/contracts.md) bind both
implementations. The root [Java algorithm](../../../docs/internals/algorithms.md),
[Java architecture](../../../docs/internals/architecture.md) and the historical
[Java v0.3.2 field guide](https://swath.varve.io/field-guide/) describe the Java engine.
