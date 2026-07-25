# Build & module structure

How the `swath` repository is organized as a Gradle build: the module graph, the
dependency rules, the shared build config, and the decisions behind them. For the *runtime*
architecture (the engine, listing flow, checkpoint/resume) see
[`architecture.md`](architecture.md); for the test tiers see
[`ops/dev/TESTING.md`](../ops/dev/TESTING.md); for the naming/package decision (the
`swath` name describes the parallel-swath technique; package root `io.varve.swath`)
see the Key design decisions table in [`architecture.md`](architecture.md); for how
these modules become a runnable jar/`installDist`/Docker image see
[`packaging-and-docker.md`](../packaging-and-docker.md).

## Module graph

`swath` is a **multi-module Gradle build** (Gradle 9, JDK 25). The repository root is an
**aggregator** — it holds no production `src/`; every unit of code lives in a module. Modules
sit flat at the root (the `swath-` prefix groups them), which is the standard Java-OSS
convention at this module count (cf. Kafka, Netty, JUnit 5, Spring Framework).

```
                 ┌─────────────┐
                 │ swath-model │  entity/value types (leaf: KeyBytes, ListEntry, ByteMidpoint)
                 └──────▲──────┘
                        │ api
                 ┌──────┴──────┐
                 │  swath-core │  the internal core implementation (engine, runtime, output,
                 └──▲───────▲──┘  sort, checkpoint, filter, pipeline, observability, store seams)
             api │         │ impl
        ┌─────────┴──┐   ┌──┴──────────────────┐
        │  swath-s3  │   │ swath-replay-server │  dev/test replay server (non-published)
        └─────▲──────┘   └─────────────────────┘
       impl │  │ impl
        ┌────┴──┴────┐
        │  swath-cli │  the `swath` binary (application)
        └────────────┘
```

All edges point one way (no cross-module cycles). The `engine`↔`runtime` package cycle is
**internal to `swath-core`** and therefore harmless.

## The modules

| Module | Gradle type | Contains | Depends on | v0.1 release status |
|---|---|---|---|---|
| **`swath-model`** | `java-library` | The `io.varve.swath.model` package — a true leaf (imports no other internal package): `KeyBytes`, sealed `ListEntry`, `PageBatch`, `ByteMidpoint`. testFixtures: `ScalarSafety`. | — | internal; not published or supported |
| **`swath-core`** | `java-library` | The internal core implementation: `engine`, `runtime`, `output` (incl. `output.parquet`), `sort`, `checkpoint`, `filter`, `pipeline`, `observability`, `error`, `concurrent`, and the **store abstraction** (`store/*` except `store/s3`). **No AWS SDK, no picocli.** testFixtures: `testkit` (MockPageFetcher, Keyspaces, EngineHarness…). Owns the JMH bench source set. | `api` → swath-model | internal; not published or supported |
| **`swath-s3`** | `java-library` | The S3 backend: `io.varve.swath.store.s3` (`S3PageFetcher`, `S3ClientFactory`, `S3Config`) + the AWS SDK. testFixtures: `LocalStackSupport`. Future `swath-gcs`/`swath-azure` sit beside it. | `api` → swath-core | internal; not published or supported |
| **`swath-cli`** | `application` | The `swath` binary: the `io.varve.swath.cli` package (`App`, `ListCommand`, `ResumeCommand`, …). `mainClass = io.varve.swath.cli.App`, `applicationName = swath`. | `impl` → swath-core, swath-s3 | binary/dist |
| **`swath-replay-server`** | `application` | The listing replay server + `sort-fixture` + conformance harness (`io.varve.swath.replay`). Serves swath Parquet fixtures as a fake S3 `ListObjectsV2` endpoint. | `impl` → swath-core | ❌ (dev/test tool) |

v0.1 is CLI-only. No Java module is published to Maven Central and no Java
package, class, interface, SPI, source shape, or binary ABI is a supported API.
The `java-library` plugin and Gradle `api` edges below describe only this
repository's compile-classpath structure. `swath-cli` ships as a binary/dist;
`swath-replay-server` is a non-release developer tool.

## Dependency rules

- **`swath-model` is a leaf** — it must never import another internal package. Guarded in CI.
- **`swath-core` is AWS-free and picocli-free** — the S3 backend and the CLI are separate modules,
  so a future `swath-gcs` build won't drag the AWS SDK. Guarded in CI
  (`grep import software.amazon` / `import picocli` over `swath-core/src` → 0).
- **`swath-core` is terminal-free** — no JLine, no `isatty`, no notion of a tty at all. Core knows
  *what* the run is doing and publishes it as a neutral `ProgressEvent` to a `ProgressSink`; whether
  any of it reaches a terminal, in what form, and how wide that terminal is are decided entirely in
  `swath-cli` (`ProgressDisplay`, `TerminalCapabilities`, `TerminalGeometry`, `StderrCoordinator`).
  This is why the progress lifecycle carries no terminal booleans across the module edge: an
  embedding application gets the events without inheriting swath's opinions about stderr.
- **`api` vs `implementation`** is chosen per internal build edge: an edge is `api` when the downstream module's
  Java-visible surface exposes the upstream module's types (so sibling modules see them transitively),
  otherwise `implementation`. Concretely:
  - `swath-core → swath-model` is **`api`** (e.g. `RunContext`/`PageFetcher` expose model types).
  - `swath-s3 → swath-core` is **`api`** (`S3PageFetcher`'s Java-visible surface exposes core types), and
    the **AWS SDK itself is `api` on `swath-s3`** because `swath-cli`'s `ListCommand` wires
    `S3Client`/credentials/`Region` directly while depending on `swath-s3` only via `implementation`.
  - `swath-cli → {swath-core, swath-s3}` and `swath-replay-server → swath-core` are **`implementation`**
    (apps expose nothing).
- **§0.7 compile-classpath purity** — `swath-replay-server`'s *main* code must not import any
  `org.apache.parquet`/`org.apache.hadoop` type; parquet reaches it only *transitively at runtime*
  via `implementation(project(":swath-core"))`. Enforced by the module's
  `verifyNoParquetOrHadoopOnCompileClasspath` task, wired into `:swath-replay-server:check`.
- **A module declares every dependency it directly uses**, even one that would also arrive
  transitively. `jackson-databind` on `swath-cli` tests is the necessity case: it reaches
  `swath-core` only transitively via `parquet-hadoop`, `implementation`-scoped there, so it never
  crosses to `swath-cli` on its own and must be declared directly. `micrometer-core` is the
  clarity case: it is `api`-scoped on `swath-core` (already on every consumer's compile classpath),
  but `swath-s3`/`swath-cli` still redeclare it because each imports Micrometer types directly.

## Shared build config: `build-logic/` convention plugin

Shared Gradle config lives in a **composite build** at `build-logic/`, exposed as the
`swath.java-conventions` precompiled script plugin and applied by every module
(`plugins { id("swath.java-conventions") }`). It carries: `group = io.varve.swath`, the JDK-25
toolchain (`--release 25`, **no `--enable-preview`**), compiler args (`-parameters`,
`-Xlint:deprecation`), and the shared `Test` task tuning — 10-minute timeout, JUnit-platform with
`junit-jupiter` + `jqwik` engines, the `@Tag` filtering (`-Pperf` opt-in, `-PnoIntegration` skip),
Testcontainers `DOCKER_HOST`/`api.version`, and `testLogging`.

This replaces what would otherwise be a copy-pasted `subprojects {}` block or 5× duplicated build
config — the Gradle-9-idiomatic way to share convention across modules. Dependency versions come
from the single version catalog `gradle/libs.versions.toml`.

## Building & testing

The Gradle root is the **repo root** (`./gradlew build` works at clone-root; CI, IDE import, the
justfile, and Docker context all assume this). A [`justfile`](../../justfile) wraps the common
invocations — see it and [`ops/dev/TESTING.md`](../ops/dev/TESTING.md) for the full tier story. Highlights:

| Command | What |
|---|---|
| `just build` | Full build, all modules, incl. Docker/LocalStack integration tests. |
| `just build-fast` | Compile + fast tests only (`-PnoIntegration`, no Docker). The PR gate. |
| `just build-notest` | Compile + package, no tests (quickest). |
| `just test-one MODULE CLASS` | One test class, e.g. `just test-one swath-core SeedStepTest`. |
| `just test-perf` | Opt-in heavy scale/throughput tier (`-Pperf`). |
| `just run -- <args>` | Run the CLI (`:swath-cli:run`). |
| `just jmh` | Run the JMH micro-benchmarks (`:swath-core:jmh`). |

`gradle.properties` enables `org.gradle.parallel` + `org.gradle.caching` (safe build-level
parallelism across the module task graph). JUnit's own concurrent-within-JVM execution is
deliberately **unset** — several integration tests share Docker/LocalStack and haven't been
verified safe to run concurrently. Gradle's `maxParallelForks` (separate JVM-level sharding) is
tiered instead: `swath-core` — the sole critical-path module for the fast tier — forks across
`min(4, cores/2)` JVMs, while the `deep`/`perf`/`onlyPerf` tiers and every other module stay
serial (`forks=1`), since their timing-sensitive assertions and Testcontainers ITs are
schedule- and contention-sensitive under cross-fork CPU pressure. `-PtestMaxParallelForks=N`
overrides the tier default.

The instrumentation-drift guard (`scripts/ci/check-instrumentation-drift.*`, self-contained
stdlib Python) scans every module's main source that formerly lived under root
`src/main` (model/core/s3/cli) and cross-checks the §5a counter registry in
[`metrics-internals.md`](metrics-internals.md).

## Key decisions & rationale

- **Package root `io.varve.swath`; internal Gradle group `io.varve.swath`** — swath is a
  varve.io project. These coordinates are build organization, not a present
  Maven-publication promise; a future library release would first need a curated
  supported API, compatibility policy, and publication design.
- **Config-key prefix `swath.*`** (`swath.sort.*`, `swath.replay.*`) — deliberately *decoupled* from
  the reverse-DNS package root: a short, distinctive, collision-safe operator surface (the
  `spring.*`/`server.*` idiom), read through a `fromProperties(lookup)` seam so it stays library-safe
  for embedded use.
- **Flat module layout** (no `modules/` or `java/` grouping) — the Java-OSS convention at 5 modules;
  keeps the Gradle root at the repo root (zero tooling friction). A `java/` language-dir was
  considered for polyglot future-proofing and rejected: a cross-language reimplementation would be a
  separate repo (gRPC/OpenTelemetry precedent), and `java/` would move the Gradle root off repo-root
  for a hypothetical. It's a cheap `git mv` later if ever genuinely needed.
- **This 5-module split** separates the *apps* (cli, replay-server) and the *S3 backend* from the
  internal reusable code (core + model), which is what the build/Docker pipeline needs and what enables the
  embeddable-library + `swath-server`/`swath-gcs` futures. Module names are keyed by function:
  `-core` (library), `-model` (foundation), `-s3`/`-gcs` (backend drivers), `-cli` (one-shot tool),
  `-server`/`-replay-server` (long-running services).

## Roadmap: further decomposition (deferred)

The reusable-foundation split — extracting `swath-parquet` (the pure Parquet writer/reader/schema)
and `swath-sort` (the external merge sort, reusable standalone) out of `swath-core` — is a **future
effort**, not yet done. It is blocked on a real refactor: `output.parquet`'s streaming
`ParquetOutputStage`/`ParquetWriterPool` (which import `runtime`/`pipeline`) must be separated from
the pure writer first, or `runtime → sort → output.parquet → runtime` becomes a module cycle. When
done, `swath-replay-server` repoints to `swath-sort` (it compiles against only the sort types today)
and stops dragging the engine at runtime. New store backends (`swath-gcs`, `swath-azure`) and a REST
`swath-server` slot in as `→ swath-core` (+ backend) without disturbing the graph.
