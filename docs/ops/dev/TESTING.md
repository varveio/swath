# Testing — tiers, speed, and the no-mass-populate rule

This is the **operational** view of swath's test suite: how it is partitioned for
speed and the hard rule that keeps it from "going insane." The contracts the suite
tests against (the invariants tagged `I1`–`I12` and friends) are authoritative in
[`contracts.md`](../../internals/contracts.md); this file only covers tiering,
speed, and the `MockPageFetcher`-driven no-mass-populate discipline.

## The golden rule: never mass-populate a real store
Listing is cheap; **populating** a store object-by-object is not — 50M `PutObject`
calls would take *hours* before a single list runs. So scale is **never** seeded into
LocalStack/MinIO/AWS one object at a time. Instead:

| Need | How | Setup cost |
| --- | --- | --- |
| Correctness + **edge cases** (no-gap/no-overlap, resume, ordering, `0xFF`/supplementary-plane/1000-1001 boundaries, skew) | **`MockPageFetcher`** — in-memory, no SDK, no store; adversarial `Keyspaces.*` generators | instant |
| **Memory/throughput at scale** (I11, PERF-1/2) | `MockPageFetcher` streams 100k–millions through the **real pipeline** | instant (no puts) |
| **"Millions of objects" realism** | **swath-replay** (`:swath-replay`) — serves synthetic `ListObjectsV2` XML from a fixture (the LIST-only high-fidelity oracle; swath is LIST-only, no inventory path) | fixture-backed, cheap |
| **Simulator policy/model checks** | `:swath-sim` deterministic defaults (`SimKernelTest`, `PolicyInvariantsTest`), store/pager differential checks (`SimStoreDifferentialTest`, `SimListingViewProtocolTest`), and opt-in `@Tag("perf")` corpus/real-listing runs (`CorpusSweepRunTest`, `RealListingRunTest`) | in-memory or local fixture; no object-store population |
| **Real-SDK integration** (pagination, continuation tokens, real byte order) | LocalStack/MinIO with **modest** counts (~1–2k zero-byte objects = a few pages) | seconds |
| **Real-world ground truth** | the public-bucket `aws s3api --no-sign-request` differential at the **gates** | none (read-only) |

Zero-byte bodies everywhere — listing only reads keys + metadata (size/etag/mtime),
never object content. If a real-store mass-populate is ever truly unavoidable, it must
be: opt-in (`@Tag("perf")`), **capped** (small default, env override for the big run),
parallel/batched, and the populated volume **snapshotted and reused** — not re-seeded.

## Speed tiers (how the default suite stays ~1 min)
- **Default** `./gradlew build` / `test` — unit + property (jqwik) + concurrency +
  the LocalStack integration ITs + a fast `MockPageFetcher` work-stealing smoke
  (`WorkStealingScanSmokeTest`, ~2k keys). Fast. **Excludes the `perf` and `deep` tiers**
  (see below) so it stays the ~1-min per-commit gate.
  `:swath-sim`'s deterministic kernel, policy, model, and fixture-differential tests run here too;
  its corpus sweeps, throughput benches, and real-listing fixtures are opt-in `@Tag("perf")` tests.
- **`-Pperf`** `./gradlew test -Pperf` — adds the gated `@Tag("perf")` tier. This is
  where the **full PERF-1 / PERF-2** live (mock-driven at 100k, per the no-mass-populate
  rule above — NO LocalStack), plus the heavier ≤50M-key throughput/memory sweeps and
  future endpoint matrices. On-demand / at gates only.
  - **PERF-1** (`WorkStealingScanPerf1Test`): a 99%-skewed 100k keyspace — asserts the
    stealer **balances** (work distribution: ~40 ranges, busiest ≈10%), **probe/steal
    overhead is `O(W·log ρ)`** not `O(N)` (the effective range partition stays under an
    `O(W·log N)` ceiling at both `N` and `2N`), and **exactly-once**. Balance is asserted
    *structurally* (work distribution), not by wall-clock: the in-memory mock computes each
    page by scanning its keyspace, so a wall-clock speedup would measure mock probe-CPU, not
    real-store parallelism.
  - **PERF-2** (`ParquetPerf2Test`): 100k keys through the real listing→parquet pipeline —
    **measured peak heap** < the §7.2 Parquet budget (1 GB), **measured peak RSS**
    (`/proc/self/status` `VmRSS`) under a bounded budget, and **no hot-path virtual-thread
    pinning** (`jdk.VirtualThreadPinned` JFR events == 0). Its direct pool leg activates all four
    release-envelope writers with the production 64-slot lane queues.
  - Fast writer-pool coverage separately constructs eight active Parquet lanes and a resume that
    shrinks from eight lanes to three, asserting exact row union, unique part names, complete
    manifests, sequence continuation for surviving lanes, and clean shutdown under the fixed queue
    budget. This is correctness coverage, not evidence that eight writers improve throughput.
  - `DatasetWriterScalingBenchmark` is an opt-in 4/8/16 diagnostic (`-PonlyPerf
    -Dswath.bench=on`, filtered to that class). It prints `WRITER_BENCH_RESULT` rows for an encode/dispatch arm and a
    row-rotation arm, including submit/HOL blocking plus digest/manifest time. Results are host and
    workload evidence, never a relative-throughput assertion in CI.
  - `PageBlockAllocationCharacterizationTest` is the exact opt-in guard for persisted page-body
    copy removal. It forks a small interpreted child JVM with `-XX:-UseTLAB`, verifies that VM flag,
    requires a named 8 KiB positive-control `byte[]` allocation to appear in JFR, and only then
    accepts zero >=2 KiB byte arrays under legacy/current PageBlock parse/decode stacks. Run it as:

    ```bash
    JAVA_TOOL_OPTIONS='-Dswath.profile.allocations=exact' ./gradlew \
      :swath-core:test \
      --tests 'io.varve.swath.sort.PageBlockAllocationCharacterizationTest'
    ```

    The default build skips it; a zero target count without the positive control is a failure, not
    evidence.
- **`-Pdeep`** `./gradlew test -Pdeep` — runs **only** the `@Tag("deep")` tier:
  schedule-sensitive probe-budget tests + latency-injecting retry/AIMD/throttle/timeout
  timing tests, demoted off the per-commit gate because they are slow (real `Thread.sleep`)
  and/or flaky under CI contention. Runs on every **main merge** (ci.yml `deep-tests`,
  serial via `-PtestMaxParallelForks=1`) and **nightly** (`nightly.yml`). The *invariants*
  these guard are pinned per-commit by fast deterministic smokes (see the Tag convention
  below).
- **`-PnoIntegration`** `./gradlew test -PnoIntegration` — Docker-free quick inner
  loop (skips the Testcontainers ITs).
- **`-PonlyIntegration`** `./gradlew test -PonlyIntegration` — runs **only** the
  `@Tag("integration")` ITs (used by the CI `integration-tests` job so it does not re-run
  the fast tier that `fast-tests` already ran). Mutually exclusive with `-PnoIntegration`.
- **`-PtestMaxParallelForks=N`** — override each module's JVM-fork count for the run. The
  default is tiered, not off: `swath-core` (the fast tier's sole critical-path module) forks
  across `min(4, cores/2)` JVMs; the `deep`/`perf`/`onlyPerf` tiers and every other module stay
  serial (`forks=1`), because their timing-sensitive assertions and Testcontainers ITs are
  schedule- and contention-sensitive under cross-fork CPU pressure.

The `fast-tests` CI job (`.github/workflows/ci.yml`) also runs a seconds-cheap,
build-independent step, **Instrumentation-drift guard**, before the Gradle build:
`scripts/ci/check-instrumentation-drift.py` fails the gate if a `recordStealReason`/
`stealReasonCounter` counter pair in `swath-model`/`swath-core`/`swath-s3`/`swath-cli`'s
`src/main/java` is missing from the §5a registry
table in `docs/internals/metrics-internals.md`, or if that table carries a ghost row no
code emits.

### Before pushing an engine change

`./gradlew build -PnoIntegration` is **not** sufficient — two CI gates live outside it, and each
fails on a change the default build happily accepts:

```bash
./gradlew build -PnoIntegration          # fast tier + spotless
./gradlew :swath-core:test -Pdeep        # deep tier is OPT-IN; it pins seed cut counts
                                         #   (ShapeRegressionCorpusTest) among other things
./scripts/ci/check-instrumentation-drift.sh   # a shell step, NOT a Gradle task
```

Both were learned the hard way on the same PR: a new counter that was not yet in the §5 registry,
and a deep-tier `W`-cap assertion, each caught only after pushing. Any change that adds a counter
or moves a seed cut count should assume the default build cannot see it.

> **Doc gap — RSS budget.** Contract §7.2 documents a per-format peak-**heap** budget but
> **no RSS budget**. PERF-2 therefore asserts RSS against a *derived* bound (heap budget +
> ~2 GB JVM/native headroom), flagged in `ParquetPerf2Test` for a spec follow-up so the test
> can assert against the pack instead of a derived bound.

## Parallel range-merge harness

`ParallelMergeBenchmark` measures the production merge over page-run staging only; every result
is labelled `arm=MERGE_BENCH_PAGE_RUN` and records zero listing fetches. It is never evidence for
live `swath list --sort` listing throughput.

By default it generates and validates a non-empty corpus. Create external staging through the
organic diagnostic lifecycle (never by editing SQLite):

```
swath list s3://<bucket>/<prefix> --format parquet -o /path/to/out --sort \
  --tune sort.keep-staging=on
```

The resulting `<out>/_staging` is accepted only alongside its retained co-located checkpoint:
the harness reads `<out>/.swath/checkpoint.sqlite` and
the run identity through an immutable read-only SQLite connection, requiring matching
`args_hash`/`run_id`, the current checkpoint schema, OBJECTS mode, completed/PUBLISHED state, and
`_SUCCESS`, to snapshot exactly the checkpoint-tracked original listing segments. It rejects live
SQLite journal/WAL companions, symlinked authority directories, and untracked `*.pageseg` files
(including stale cascade or fixture debris), and hashes every regular file to verify the retained
tree is byte-identical after every arm.

```
./gradlew :swath-core:test --tests 'io.varve.swath.sort.ParallelMergeBenchmark' \
  -Dswath.bench=on -Pperf -Dswath.bench.staging-dir=/path/to/_staging
```

The harness snapshots and validates the catalog once, then materializes every arm with
same-filesystem hard links; it refuses physical-copy fallback so a cold-storage result cannot
measure copying instead of merging. Before timing, it fully reads and CRC-validates the source into
a constant-memory row-count/multiset oracle, then opens every input for the per-stream heap probe.
The source oracle hashes the canonical Parquet row representation: timestamps are epoch
microseconds, and a versionless object's schema-omitted `is_latest` decodes as false. It still
includes every representable field and exact multiplicity; this normalization prevents the live
OBJECTS mapper's in-memory `isLatest=true` convenience value from being misreported as output loss.
Every arm is therefore explicitly `cache_state=warm_primed`; this harness cannot produce a cold
result. A true cold bracket needs a separate fresh-process protocol that prepares first, drops
caches under external control, and launches exactly one measured arm without the oracle/heap probe
in that process.

The warm-cache sweep first runs one complete **untimed** R=1 transform to absorb class loading, JIT,
Parquet initialization, and RSS-sampler first use. Measurements then run serial A, candidates in
ascending order, serial B, candidates in descending order, and serial C. Speedups use the median of
the three serial brackets and the two candidate samples. `swath.bench.max-variance-pct` defaults to
`15.0`; a baseline or candidate spread above it produces `status=invalid_variance` and
`speedup=unavailable`, never a publishable speedup. An inconsistent clamp/engagement disposition is
likewise invalid. Every output must be physically sorted and match the independent source oracle.
Every `BENCH_*` line carries cache state, retained run identity (or generated sentinels), `git_sha`,
`corpus_id`, and the stable ordered logical-output fingerprint when output exists.
The row reports proof-spool logical extent, preallocation operations/attempted bytes, mapped
operations/bytes, and summed service time with the same scope as the live log and run summary.
`PageRunZoneProofAdversarialTest` pins fixed extent/preallocation while requiring mapped work to grow
with pages/source switches. `PageRunProofSpoolLargeMapTest` performs an actual sparse >2-GiB FFM
first/last touch, arena unmap, and delete. The opt-in `PageRunProofSpoolRssCharacterizationTest`
touches a representative mapping, bounds its RSS rise with an explicit noise caveat, and checks
post-arena-close RSS behavior:

```bash
./gradlew :swath-core:test -PonlyPerf \
  --tests 'io.varve.swath.sort.PageRunProofSpoolRssCharacterizationTest'
```

Its `PROOF_SPOOL_RSS_RESULT` line is characterization evidence, not a portable memory promise.

## JMH micro-benchmarks

Three JMH benchmarks live under `swath-core/src/jmh/java/io/varve/swath/benchmarks/` and cover
the hot-path primitives called on every list page:

| Class | Methods | What it measures |
|---|---|---|
| `CompareUnsignedBench` | `lastByteDiffers`, `highByteUtf8`, `prefixRelationship`, `supplementaryPlane` | `KeyBytes.compareUnsigned` over 4 key-pair shapes |
| `ControlCharEscaperBench` | `fastPath`, `escapePath` | `ControlCharEscaper.escape` — no-alloc fast path vs. StringBuilder escape path |
| `ByteMidpointBench` | `adjacentDense`, `straddleC0Block`, `controlSliver`, `highByteUtf8`, `longKeys` | `ByteMidpoint.between` over 5 bound-pair shapes |

**How to run:**

```
# All benchmarks (default: 2 warmup + 3 measurement iterations, 1 fork):
./gradlew jmh

# Compile only (verify the JMH sources build — fast):
./gradlew compileJmhJava
```

Benchmarks are **never** executed by `./gradlew build` or `./gradlew test` — they
are strictly opt-in via `./gradlew jmh`. Results are written to
`build/results/jmh/results.txt` by default.

## Hang safety

All Gradle `Test` tasks have a 10-minute wall-clock timeout. This is the hard
backstop: if a test deadlocks or spins in a way JUnit cannot interrupt, Gradle
kills the test task instead of letting the build hang indefinitely.

JUnit also has a same-thread default per-test timeout:

```properties
junit.jupiter.execution.timeout.default = 60s
```

Do not set `junit.jupiter.execution.timeout.thread.mode.default = SEPARATE_THREAD`
globally. Some swath tests exercise thread-bound context (`ctx.runWhereBound`), so
same-thread timeouts preserve production-like execution; the Gradle task timeout
is the universal hang-stop.

## Known coverage gaps

Gaps we know about and have chosen to live with, so nobody rediscovers them as
surprises:

- **The AIMD throttle path is covered mock-only.** The reduce/pause/recover cycle
  is driven entirely through `MockPageFetcher`-injected `503`s. A real anonymous
  (`--no-sign-request`) request against a public bucket cannot be made to return a
  sustained `SlowDown` on demand, so nothing proves the cycle end-to-end against
  actual AWS throttling. `--request-rate` does **not** close this gap — it is
  client-side self-throttling that never touches AIMD's `T`, added to cap request
  volume proactively, not a substitute for exercising the reactive path against a
  real 503. Closing it needs a fault-injecting proxy or LocalStack fixture that can
  force a `503` on a real round-trip; the alternative is accepting mock-only as the
  permanent shape here.

## Test inventory (selected named tests)

| Class | Tag | What it guards |
|---|---|---|
| `SeedStepTest` | default | `SeedStep` tiling correctness: seed ranges partition keyspace (no gap/overlap), `subsampleEvenly` cap, atomic `insertNodes`, multi-level descent stops at truncated/exploding sub-level, `swath resume` skips re-seed |
| `ThiefTest` | default | `Thief` steal logic: bounded `delimiter=/` structure discovery (median-CommonPrefix split, reached only after a probe exposes an empty upper), empty-upper bisection, lock hand-off re-validation |
| `LivelockUnderLatencyTest` | default | with 64 workers + 20ms page latency + instant probes, the scan completes within timeout (livelocked pre-fix); progress-gated victim eligibility |
| `IdleThiefProbeScalingTest` | default | with high concurrency + deep keyspace, `api_calls` stays bounded (idle-steal backoff) |
| `IdleStealSlotOwnershipTest` | default | CONC: the fleet-wide one-attempt slot is owned by its acquirer — `reset()` from unrelated workers never hands it away, a release admits exactly one successor, and a denied worker parks on the in-flight backstop |
| `IdleStealProbeConcurrencyTest` | `deep` + default | the same bound measured at the store across a live scan: max **concurrent** probe fetches is exactly 1 while every worker's page commits fire resets (`deep`); the head-of-line characterization — four victims + one slow probe serialize fleet-wide without starving splits (`deep`); and the release-exactly-once guard, an unchecked throw inside the acquired region still frees the slot (per-commit) |
| `WorkStealingScanSmokeTest` | default | Fast PERF-1 smoke: ~2k skewed keyspace, work-stealing balances, exactly-once |
| `WorkStealingScanPerf1Test` | `perf` | Full PERF-1: 99%-skewed 100k keyspace, balance + O(W·log ρ) probe overhead |
| `ParquetPerf2Test` | `perf` | Full PERF-2: 100k keys, measured peak heap < §7.2 budget, no VT pinning |
| `DecisionTraceGoldenTest` | default | Policy-seam safety net: `Thief.steal`/`OwnerSelfSplit.maybeOwnerSelfSplit`/`WorkerState.stealPaced`/`SeedStep.seedSpecs` (view, decision) sequences replayed against committed JSONL goldens, diffed on drift — see [`decision-trace-goldens.md`](decision-trace-goldens.md) |

## Tag convention (so heavy tests can't sneak into the default suite)
- `@Tag("integration")` — needs a real store/Docker (Testcontainers: LocalStack, and
  the planned MinIO matrix). Runs by default but kept at modest object counts.
- `@Tag("perf")` — heavy scale/throughput/memory; **excluded by default**, opt-in via
  `-Pperf`. Tag any test that seeds or streams large counts, or that you wouldn't want
  in every-PR CI.
- `@Tag("deep")` — **excluded by default**; runs on main-merge + nightly (`-Pdeep`). Tag a
  test that is (a) **latency-injecting / wall-clock-bound** (real `Thread.sleep`, waits on a
  recovery window — slow by construction), or (b) **schedule-sensitive** (asserts absolute
  API/probe counts while real worker threads race, so it flakes under CI contention).
  **Rule: a `deep` test must not be the *only* guard of a contract line.** Whatever invariant
  it guards must also be pinned per-commit by a fast, deterministic smoke, so demoting it off
  the per-commit gate opens no coverage gap. **Tag at METHOD granularity, not whole-class** — a
  class that mixes slow/flaky methods with fast correctness contracts must `@Tag("deep")` only the
  slow/flaky methods, leaving the fast contract methods per-commit. Current deep methods + their
  per-commit backstops: the retry/throttle tests (`TransientTimeoutRetryEngineContractTest` [4 of
  its 5 methods], `TransientRetryFetcherTest.retriesTransientThrottlesThenSucceeds`,
  `Thr1SustainedThrottleTest.fullEngine_realThrownThrottle…`)
  are backstopped per-commit by `EngineThrottleRetrySmokeTest`, a full-engine smoke that asserts:
  a thrown 503 is retried to exactly-once completion **and** drops effective concurrency (live
  503→AIMD wiring); an ATTEMPT_TIMEOUT is retried to exactly-once completion but casts **no** AIMD
  vote and keeps full concurrency (the transient-timeout engine wiring, distinct from a 503); and an over-cap
  ATTEMPT_TIMEOUT storm **rides out** rather than cancelling (never `CancelledException`, casts no
  AIMD vote). The BOUNDED-policy cap-exhaustion abort (`CancelledException`/`StopReason.STUCK`,
  resumable `RUNNING` checkpoint) is itself a fast, no-op-sleeper test
  (`TransientTimeoutRetryEngineContractTest.persistentAttemptTimeout_boundedPolicy_abortsResumablyStuck_castsNoAimdVote`)
  and needs no separate backstop. `AimdAttemptTimeoutSignalContractTest` now drives every method off
  an injected fake clock (no real sleep), so none of its methods are `@Tag("deep")`; the no-vote-on-timeout
  policy it guards is additionally pinned by
  `ConcurrencyGaugeTest.onTransientTimeout_doesNotVoteAimdDown_norRecordAnEvent`. `MaxDurationResumeTest`'s single
  wall-clock method → `DeadlineCancellerTest` + `MaxDurationNoProgressTest`.
  `OwnerSelfSplitContractTest` demotes only `ownerSplitAddsNoProbesAndStaysWithinTheApiBudget` (the
  schedule-sensitive ablation-relative probe-budget guard); its six CONC/PROP byte-exact methods
  stay per-commit, and `StealStructureProbeTest`'s O(pages) structure-probe bound catches the gross
  probe regression per-commit. (The probe-budget tests — `StealStructureProbeTest` et al. —
  are **kept per-commit**, made robust with a documented contention margin instead of being
  demoted to `deep`.) `IdleStealProbeConcurrencyTest` (sleeps to open the probe-overlap window,
  races real workers) → `IdleStealSlotOwnershipTest`, which drives the same
  handover interleaving deterministically off real threads with no injected latency.

If you write a PERF-tier or large-matrix test, it **must** be `@Tag("perf")` and follow
the no-mass-populate rule above. If you write a latency-injecting or schedule-sensitive test,
`@Tag("deep")` it **and** add its per-commit deterministic smoke in the same change.
