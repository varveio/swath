# swath — agent guide

The entry point. Read this before touching swath.

This file carries this repo's settled law, the parts of the code to treat carefully, and
how to build and test. General contribution norms are in [`CONTRIBUTING.md`](CONTRIBUTING.md).

## What swath is

`swath` is a high-performance object-store lister (Java 25), currently shipped as an
S3 CLI for general-purpose buckets and designed for future ordered/start-after-capable
stores. It lists very large buckets via a work-stealing scan, with bounded active
buffers, crash-safe checkpoint/resume, and optional sorted output. This repo holds
both the `docs/` design pack and the implementation. See [`README.md`](README.md).

## Settled law (do not relitigate)

- **The design docs are the spec — and the code wins.** Implement to `docs/internals/`:
  [`contracts.md`](docs/internals/contracts.md) (core types, SQLite/Parquet schema, config,
  per-sink guarantees; its §0 lists the load-bearing invariants **I1–I12**),
  [`algorithms.md`](docs/internals/algorithms.md) (the engine), and
  [`overview.md`](docs/internals/overview.md) / [`architecture.md`](docs/internals/architecture.md)
  (the shape and the why). These are re-derived from the shipped code, so **the code is
  the final authority — verify any doc claim against the source before trusting it.**
  Change a settled decision by changing the docs alongside the code, never silently.
- **JDK 25, no `--enable-preview` in shipped artifacts.**
- **The `io.varve.swath` package root stays.**
- **`--sort` is production-grade and opt-in** — unsorted is the default.
- **Some decisions are deliberately reserved.** Where the current design docs mark a
  choice as settled or flag a tradeoff, don't quietly reverse it — raise it in an issue
  or PR discussion first.

## The correctness spine (handle with care)

The correctness spine is the **split/steal protocol**, **checkpoint/resume ordering**
(e.g. commit-before-emit **I1**, CAS split **I4**), and **concurrency**. Changes here
carry the most risk; make them deliberately and expect close review.

- **A high-risk unit's adversarial correctness guard must be independently authored or
  reviewed by someone other than the code's author** (PROP no-gap / no-overlap, RES
  mid-crash, CONC). See the test tiers in
  [`docs/ops/dev/TESTING.md`](docs/ops/dev/TESTING.md).

## Instrument every new algorithm path

swath does **not** bake bucket-shape detection into the engine; it emits rich raw
per-path signals so buckets are classified and tuned **post-hoc**. So **every new algo
path** (pivot strategy, split trigger, seed mode, backoff, router branch) MUST emit a
`recordStealReason(category, reason)` engagement counter **plus any cheap
keyspace-classification signal** it observes. The test: *can post-analysis tell from the
metrics alone whether this path engaged, and whether it helped?* If not, it's
under-instrumented. How-to: [`docs/internals/metrics-internals.md`](docs/internals/metrics-internals.md) §5.

## Build / test

- **`./gradlew build`** is the integration gate.
- **Scoped test filters must be subproject-scoped** — the root `:test` task does not
  exist in this multi-module layout:
  `./gradlew :swath-core:test --tests 'io.varve.swath.SomeTest'`. For the module graph
  see [`docs/internals/build-and-modules.md`](docs/internals/build-and-modules.md).
- Deep/perf legs (not part of a plain build): `-Pdeep`, `-PonlyPerf`, the kill-9 resume
  suites, replay conformance.
- Docker is available (Testcontainers/LocalStack/MinIO); the `duckdb` CLI verifies
  Parquet output.
- Test tiers, the `MockPageFetcher` no-mass-populate discipline, and the named-test
  contract: [`docs/ops/dev/TESTING.md`](docs/ops/dev/TESTING.md).

## House discipline

- **One integration branch per effort, one PR to `main`** — commit per unit with
  pathspecs; don't branch-switch the working tree mid-session.
- **Never commit red. Never merge to `main`** — merging is a human gate. **No AI
  attribution in commits or PRs** — no `Co-Authored-By`, no "Generated with …" footers.
- **Hygiene:** never force-push, commit to `main`, commit secrets, or `rm -rf` outside
  build/scratch dirs.
- **Quality bar:** match the surrounding code's style, naming, and structure; keep a
  single source of truth (reuse existing helpers and dependencies rather than
  hand-rolling utilities or duplicating logic); no magic strings, no telescoping
  constructors; add options, comments, or abstractions only when a change genuinely
  needs them.
