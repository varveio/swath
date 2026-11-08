# swath agent guide

Read this before changing swath. General contribution rules are in
[`CONTRIBUTING.md`](CONTRIBUTING.md).

## What swath is

swath is a Java 25 CLI for high-performance object-store listing. The supported product
currently targets general-purpose S3 buckets and uses an adaptive work-stealing scan with
bounded active buffers, crash-safe checkpoint/resume, and optional globally sorted
Parquet output.

Start with [`README.md`](README.md). The
[visual field guide](https://swath.varve.io/field-guide/) is the best plain-language
algorithm explanation.

## Sources of truth

- Current user behavior belongs in `README.md` and `docs/`.
- [`docs/internals/contracts.md`](docs/internals/contracts.md) owns the load-bearing
  invariants I1–I12, persistence/output schemas, and per-sink guarantees.
- [`docs/internals/algorithms.md`](docs/internals/algorithms.md) owns the listing
  algorithm.
- [`docs/internals/architecture.md`](docs/internals/architecture.md) maps the design to
  modules and runtime flow.
- Code is the final authority when a difference is found. Correct the documentation in
  the same change rather than adding a banner that leaves both versions in place.
- Visible options and defaults come from the CLI implementation and generated help.
  Narrative documentation teaches choices and exceptional constraints instead of
  maintaining another exhaustive option list.

Use [`docs/style.md`](docs/style.md) for public terminology, project casing, consistency
language, evidence claims, and documentation ownership.

## Correctness spine

The highest-risk areas are:

- byte-exact key ordering;
- range boundaries and the split/steal protocol;
- checkpoint and resume ordering;
- Parquet part durability and publication;
- cancellation and shutdown order; and
- concurrency changes that can create gaps, overlaps, deadlocks, or unbounded memory.

Changes to this spine need adversarial tests. A high-risk unit's no-gap/no-overlap,
mid-crash resume, or concurrency guard should be independently authored or reviewed when
possible. Test tiers are documented in
[`docs/ops/dev/TESTING.md`](docs/ops/dev/TESTING.md).

## Instrument algorithm changes

Every new algorithm path—pivot choice, split trigger, seed path, pacing gate, retry
branch, or router decision—must emit an engagement reason plus any cheap classification
signal it observes.

The test is practical: can a completed run report show whether the path engaged and what
constrained it? If not, the path is under-instrumented.

The contributor reference is
[`docs/internals/metrics-internals.md`](docs/internals/metrics-internals.md).

## Build and test

- `./gradlew build` is the integration gate.
- The Docker-free per-commit loop is `./gradlew build -PnoIntegration`.
- The root `:test` task does not exist. Scope filtered tests to a subproject, for example:

  ```bash
  ./gradlew :swath-core:test --tests 'io.varve.swath.SomeTest'
  ```

- Deep and performance tiers are opt-in. See
  [`docs/ops/dev/TESTING.md`](docs/ops/dev/TESTING.md).
- Docker is available for Testcontainers, LocalStack, MinIO, and replay checks.
- Use DuckDB or the repository verifier for Parquet output.

## Go port

- The Go port lives in [`go/`](go/): module `github.com/varveio/swath/go`, command
  `go/cmd/swath`, packages under `go/internal/`. It is a separate tool from the Java
  distribution until the Java code is retired.
- The language-neutral documents bind it: [`docs/internals/contracts.md`](docs/internals/contracts.md)
  (invariants, schemas, dataset layout), the algorithms, and the usage semantics.
  [`go/docs/`](go/docs/) is the source of truth for the Go code (architecture, its
  statement of the invariants, ordering, Parquet output, providers, testing, CLI); a change
  to `go/` is not done until its page there is current. Measurements are recorded in
  [`go/docs/measurements.md`](go/docs/measurements.md).
- The gate is `cd go && go vet ./... && go test -race ./...`; CI runs it sharded. Locally
  run the tests you change, not the whole suite; repeats go through `go-stress.yml`. Static
  builds use `CGO_ENABLED=0`.
- A commit that touches `go/` needs the Go jobs of `ci.yml` green (`go-lint`, the `go-race`
  shards, `go-nonrace`, `go-build`, `go-xos`, `go-fixtures`; `ci-gate` requires them).
  [`go/docs/testing.md`](go/docs/testing.md) says what each job and tier runs and when.

## Repository discipline

- Use one integration branch per effort and one pull request to `main`.
- Keep commits focused and signed off.
- Never commit directly to `main`, merge a pull request, force-push shared work, commit
  secrets, or delete outside build and scratch directories.
- Never commit a red build.
- Reuse existing helpers, registries, and dependencies instead of creating another source
  of truth.
- Avoid magic strings, telescoping constructors, and abstractions introduced only to
  narrate a small change.
- Add options, comments, metrics, or documentation only when they clarify a behavior the
  code actually supports.
