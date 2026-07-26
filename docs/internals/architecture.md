# swath — architecture overview

`swath` is a high-performance Java 25 CLI designed for very large listings of
general-purpose Amazon S3 buckets. It tiles their globally ordered keyspace into
disjoint half-open byte ranges `(A, B]` that are scanned in parallel by a pool
of virtual-thread workers — the `WorkStealingScan` engine — whose load imbalance is corrected
on-the-fly by demand-driven range stealing. Each page advances the listing
`cursor` before its rows are sent downstream. In one-shot stdout and FILE-kind
text runs, that ordering can omit a committed page if the process stops before
emission; those destinations are not resumable. Managed directory-dataset
Parquet separately advances `durable_cursor` only when a part is finalized
(footer-fsynced); resume discards the unfinalized tail and re-lists it, giving
exactly-once durable dataset output.
Part files are **not** globally key-sorted — each part concatenates
several nodes' key-ranges in listing order, not key order. Global key order is
opt-in `--sort` (`--format parquet` only), which forces a single sorted output
file instead of unordered parts.

For CLI flags, options, and output formats see [`docs/usage.md`](../usage.md).
For the design shape — the problem, scope, requirements, and the two design laws
— see [`overview.md`](overview.md). For the full engine algorithm (pseudocode,
correctness proof, `byteMidpoint`, versioned listing, AIMD) see
[`docs/internals/algorithms.md`](algorithms.md).
The authoritative contracts and invariants live in
[`contracts.md`](contracts.md).
For how the repository is organized as a Gradle build — the module graph,
dependency rules, and the decisions behind them — see
[`docs/build-and-modules.md`](build-and-modules.md).

---

## Component map

| Package | Source path | Responsibility |
|---|---|---|
| `model` | `io.varve.swath.model` | Core types: `KeyBytes` (raw byte key + unsigned comparator), sealed `ListEntry` (`ObjectEntry`, `CommonPrefixEntry`, `DeleteMarkerEntry`), `PageBatch`, `ByteMidpoint` (UTF-8-safe pivot math) |
| `store` | `io.varve.swath.store` | Internal, unsupported v0.1 seams: `PageFetcher.fetchPage(PageRequest) → ListPage`; `StoreCapabilities` descriptor (v0.1 reads only `maxKeysCap` in `RangeScanner`; no router ships); `PaginationKind` (KEY vs OPAQUE_MARKER) |
| `store.s3` | `io.varve.swath.store.s3` | S3 implementation: `S3PageFetcher` (SDK v2 sync, `encoding-type=url`; the SDK's `DecodeUrlEncodedResponseInterceptor` decodes response keys, read via `getBytes(UTF_8)`), `S3ClientFactory` |
| `engine` | `io.varve.swath.engine` | `WorkStealingScan` (worklist + fixed VT pool), `RangeScanner` (`runRange` loop), `Thief` (steal executor mechanics: pool→view/snapshot translation, RPC issuing, the lock-guarded CAS hand-off, metrics/trace emission — the pivot cascade itself is `engine.policy`'s), `StealMath` (`byteMidpoint`, `extrapolate`, victim-selection math), `WorkerState` (per-worker cursor/hi/lock), `ConcurrencyGauge` (AIMD), `SeedStep` (shallow `delimiter=/` seed pass), `SeedMode` (`SHALLOW`/`NONE`/`HINTS`) |
| `engine.policy` | `io.varve.swath.engine.policy` | The policy seam (swath-notes' 2026-07-26 simulator campaign): `StealPolicy`/`StealAttempt`/`ThiefPolicy` — victim selection and the full pivot cascade (§3 below) as a source-agnostic decision interface (views/decisions/probe outcomes carry keys as bytes, counts, streaks, and policy-domain enums only — no `store.ListPage` or other protocol type). `Thief` drives `ThiefPolicy` through a request/response loop, issuing every RPC it requests; `Thief.java` itself contains no cascade branch. First (and so far only) slice of the wider policy-seam track — `OwnerSelfSplit`/`WorkerState` pacing/`SeedStep` are unextracted. |
| `checkpoint` | `io.varve.swath.checkpoint` | `CheckpointStore` interface; `SqliteCheckpointStore` (single writer thread, WAL, `commitPage`, `splitNode`); node/run state types |
| `output` | `io.varve.swath.output` | `EntryFormatter` (sealed), text formatters (`JsonlFormatter`, `TsvFormatter`, `AlignedFormatter`), `OutputStage`, `ControlCharEscaper` |
| `output.parquet` | `io.varve.swath.output.parquet` | `ParquetWriterPool` (2–4 writers, decoupled from listing concurrency), `PartWriter` (size-rotated parts, footer fsync), `Manifest` (atomic `manifest.json`), `ParquetSchema`, `ParquetResume` |
| `filter` | `io.varve.swath.filter` | Sealed `Filter`: `IncludeRegexFilter`, `ExcludeRegexFilter`, `SizeFilter`, `MtimeFilter`, `StorageClassFilter`; `FilterChain` |
| `pipeline` | `io.varve.swath.pipeline` | `Channel` (bounded blocking queue), sealed `Msg` envelope (`Item`, `End`, `Failure`), `Pipeline` wiring |
| `runtime` | `io.varve.swath.runtime` | `RunContext` (token, metrics, config — bound as a `ScopedValue`), `ListRunner` (pipeline orchestration), `ScanProducer`, `CheckpointedScanProducer`, `ArgsHash` |
| `concurrent` | `io.varve.swath.concurrent` | `Scope` — the in-house structured-concurrency helper over virtual threads (no `--enable-preview`) |
| `cli` | `io.varve.swath.cli` | `App` (Picocli root), `ListCommand`, `ResumeCommand`, `ExitCodes`, `S3Uri` |
| `error` | `io.varve.swath.error` | Sealed `SwathException` hierarchy (`ListingException`, `CheckpointException`, `OutputException`, `InvalidArgsException`, …) |
| `observability` | `io.varve.swath.observability` | `RunMetrics` (Micrometer counters/gauges/timers), `RunSummary`/`JsonRunSummaryWriter` (end-of-run + `--report` sidecar), `RunProgressReporter` (the run's single progress lifecycle) + `ProgressSink`/`ProgressEvent` (the neutral seam a presentation layer renders through), `ResourceMetrics` (peak RSS/heap, CPU seconds), `RunFingerprint`, `StopReason` |

**Known seam exceptions:** `engine.policy`'s convention is that a policy is a deterministic
function of its view (no I/O, no ambient randomness) and returns reason enums for the executor to
record (so AGENTS.md's counter-per-path law stays mechanically checkable against the decision
enum) — two pre-existing gaps against that convention were carried into `ThiefPolicy` unchanged
(moved verbatim, not introduced, and not fixed here — both are behavior-adjacent-refactor territory,
out of scope for an extraction slice):
- `AlphabetDigest` (carried through in `StealAttemptView`, consumed by
  `StealMath.interpolate(..., digest)`) holds its own `RunMetrics` reference and fires `ALPHABET.*`
  fallback counters directly from inside `chooseScalar`. Issue #19.
- `ThiefPolicy`'s structure-probe suppression recovery reaches for ambient
  `ThreadLocalRandom.current()` (the 1-in-64 escape hatch), so that one decision is not
  reproducible from the view alone. Issue #20.

Both fixes belong in the determinism-audit slice, which already owns injected-clock/seeded-RNG
purity for this interface.

**Dormant seams (built but not active in v0.1):**
- `ExpressionFilter` — in the sealed `Filter` permits; JEXL evaluation deferred to v1.1.
- `output_journal` / `--resume-output` — at-least-once stdout replay; deferred to v1.1.
- Multi-store (`gs://`, `az://`, …) — `StoreCapabilities`, `OPAQUE_MARKER`
  pagination kind, and `PREFIX` node kind are internal design seams, not a
  supported SPI; only the S3 fetcher ships in v0.1. The range engine additionally
  requires global lexical ordering and a `StartAfter`-equivalent lower bound.

**No longer dormant:** `--sort` / external merge sort (opt-in, `--format parquet` only)
ships today; see the flow paragraph above. The sorted **text**-sink path and the
default-on flip remain deferred.

---

## How a list run flows

```
S3
 │  ListObjectsV2 pages
 ▼
[S3PageFetcher]  ──page──►  [RangeScanner / runRange]
                                 │  PageBatch
                                 ▼
                          [Channel<Msg<PageBatch>>]  (bounded)
                                 │
                                 ▼
                          [OutputStage / ParquetWriterPool]
                                 │  finalized part files
                                 ▼
                          [manifest.json + SQLite checkpoint]
```

### Step by step

1. **Seed.** `SeedStep` runs a shallow `delimiter=/` probe to discover top-level
   common prefixes `p1 < p2 < … < pk` and converts them into initial range
   cut-points `(⊥, p1], (p1, p2], …, (pk, null]`. Cut-points are capped at
   `min(1000, 4×W)` and then `subsampleEvenly` so the seed set is proportional to
   the worker count. The insert is atomic (`CheckpointStore.insertNodes`). If the
   top level has no common prefixes (flat bucket) the entire keyspace starts as one
   range `(⊥, null]`. `SeedMode` is `SHALLOW` (default), `NONE`, or `HINTS`
   (not yet implemented — throws); controlled by `--tune seed.mode=shallow|none|hints`.

2. **Workers claim ranges.** The fixed virtual-thread pool in `WorkStealingScan`
   picks `PENDING` nodes off the ready queue. Each worker runs `RangeScanner.runRange`
   — a tight pagination loop that calls `S3PageFetcher.fetchPage`, filters each key
   against the node's current upper bound `hi` (re-read volatile on every key), and
   accumulates a `PageBatch`.

3. **Commit, then emit.** Before pushing a `PageBatch` downstream the worker calls
   `CheckpointStore.commitPage` (invariant I1 — commit-before-emit). The checkpoint
   writer thread advances `cursor` and, on the final page, flips the node to
   `COMPLETED`. Only after the commit future resolves does the worker enqueue the
   batch onto the channel.

4. **Stealing.** When a worker finds the ready queue empty but `outstanding > 0`,
   it becomes a thief. It picks the live worker with the largest estimated remaining
   range, computes a UTF-8-safe pivot `m` via `byteMidpoint` (bounded ranges) or
   density extrapolation (the open frontier), validates it with a 1-key probe, then
   under `victim.lock` narrows the victim's `hi` to `m` and atomically commits a new
   `PENDING` child `(m, oldHi]` via `CheckpointStore.splitNode`. The thief then runs
   that child.

5. **Output.** `PageBatch`es flow through the `Channel` to the `OutputStage`. For
   Parquet, `ParquetWriterPool` sticky-routes each node's batches to one writer
   (`node_id % numWriters`). When a part file reaches its size target the writer
   closes it (footer fsync), marks it `finalized` in `part_file`, advances each
   node's `durable_cursor`, and atomically updates `manifest.json`.

6. **Quiescence.** An `AtomicLong outstanding` counter is decremented only after a
   node's `COMPLETED` status and any split child are durably committed. The run ends
   when `outstanding == 0` and the channel drains.

### Two concurrency shapes

The run has two distinct concurrency shapes by design:

- **The static pipeline DAG** (filter → sort → output) is a small fixed set of
  siblings under the in-house `Scope` helper. It mirrors `s3ls-rs`: the terminal
  `OutputStage` is awaited first so a downstream error (broken pipe, full disk)
  propagates fast; drop the downstream receiver before joining the producer, and
  `shutdownNow()` before `close()` (invariant I8).
- **The listing engine** is the `WorkStealingScan` worklist — a fixed pool of N
  workers draining `PENDING` `listing_node` rows, stealing to generate new ranges,
  all DB writes funnelled through one checkpoint-writer thread. It is **not**
  structured concurrency; it is a worklist, which is what caps threads at N
  regardless of tree width and eliminates the permit-across-`join()` deadlock class
  (invariant I7).

---

## The seed+steal hybrid engine — conceptual

The engine combines an **up-front seed pass** with **demand-driven range stealing**,
giving early parallelism across varied supported general-purpose-bucket key
distributions.

The engine's core invariant is a **partition of the keyspace**: at all times the set
of live and completed ranges tiles `(⊥, ⊤]` — pairwise disjoint, union covering
every key. This is what the project name encodes: adjacent swaths cover the whole
field with no gaps and no overlap.

**Seeding.** Before any worker starts listing, `SeedStep` issues a shallow
`delimiter=/` pass to discover top-level prefixes and creates one range per
cut-point — so every worker has a structurally disjoint slice from page 1. This
eliminates the "near-serial start" that byte-midpoint stealing alone suffers on
deep-tree buckets (`in_flight` was ~1 without seeding). The seed insert is atomic
(`CheckpointStore.insertNodes`, invariant I2).

**Range model.** A range `(A, B]` means: send `start_after=A` to S3 (exclusive),
emit keys `k` while `k <= B`, stop at the first `k > B`. `B = null` is the open
frontier. **The boundary key belongs to the LEFT interval** — this is invariant I3
and the most load-bearing convention in the whole system: the wrong choice (exclusive
on both sides) drops one key at every split.

**Splitting.** When an idle worker targets a busy victim with `cursor = c`, `hi = H`,
it places the pivot from what the run has already observed: for a bounded range an
interpolation inside `(c, H]` at a density-derived far-ahead fraction (≥ 0.5); for the
open frontier a density extrapolation toward the prefix ceiling. Pivot synthesis is
code-point-aware, so it always emits valid UTF-8 — and a `null` means one of two
things. On a bounded range it is **terminal**: no key exists strictly between, the
range is genuinely unsplittable, and it is cached as such. On an un-started frontier
it means only that the owner has not committed page 1 yet, so the steal retries and
the victim must **not** be cached — caching it would exclude the seed victim, which
owns the whole keyspace, from every later steal and collapse the scan to one worker.

Only when the single `max_keys=1` probe of `(m, H]` comes back **empty** does the
thief spend more calls, and the two pathologies take different ladders. An **empty
upper** steps back to the plain midpoint, then tries a bounded `delimiter=/` structure
probe for a real populated directory boundary (which works well on hierarchical
keyspaces where a uniform pivot lands in an empty gap), then the density-reflected
pivot, then a budgeted bisection back toward the cursor. A pivot that degenerates into
a **cursor-adjacent sliver** takes its own two rungs instead — an adaptive structure
back-out that searches for the coarsest level yielding a far-ahead boundary, then a
flat-leaf density pivot — and commits the byte-exact sliver if neither lands. The full
ladder and its probe budgets are in [`algorithms.md`](algorithms.md) §3. Whichever rung produces `m`, the victim's range
narrows and a new `PENDING` child is created atomically. The victim re-reads `hi` on
every key (volatile), so an in-flight page fetched under the old bound stops at the
new `m` without double-emitting.

**Victim selection.** `estRemaining(w) = localDensity(w) × remaining_span(w)` — the
worker with the largest estimated remaining work is stolen from first. The frontier
worker (`hi = null`) scores +∞ until it gets a finite bound. A worker is eligible
to be a victim only after it has emitted at least one page since its last steal
(**progress-gated victim eligibility**) — this prevents idle thieves
from narrowing a victim's `hi` into an empty gap above its cursor faster than the
page returns, which caused a livelock at default concurrency on real multi-page buckets.

**Idle backoff.** When steal attempts repeatedly find nothing splittable,
idle workers back off exponentially (`IdleStealBackoff`, 5→50ms) rather than
re-probing every 5ms. This bounds API calls on deep/serial-ish buckets where adding
concurrency adds only probe overhead.

**Quiescence.** Termination is signaled by `outstanding == 0`, not by an empty ready
queue (a worker may be mid-page and about to spawn a child or complete).

Full algorithm, `byteMidpoint` pseudocode, correctness proof, cost analysis, and
edge-case checklist: see [`docs/internals/algorithms.md`](algorithms.md).

---

## Checkpoint and resume

The checkpoint store (`CheckpointStore` → `SqliteCheckpointStore`) doubles as the
worklist. Each `listing_node` row is one unit of work: `PENDING → IN_PROGRESS →
COMPLETED`. All checkpoint writes funnel through a **single writer thread** (SQLite
WAL is single-writer) that batches commits for throughput.

Key properties:

- **Commit-before-emit (I1).** `commitPage` advances `cursor` and sets `status` in one
  transaction, and `WorkStealingScan` awaits that commit's durability *before* it runs
  the filters and pushes the page to the channel. The checkpoint therefore never
  reflects a page that was not durably committed — but the window has a deliberate
  cost in the other direction: a stop between the commit and the emit leaves a page
  that was **committed and never emitted**. Stdout and FILE-kind text have no public
  resume path, so that page can simply be absent from their one-shot output — the
  **at-most-once** behavior described in [`contracts.md`](contracts.md) §5. Managed
  directory-dataset Parquet recovery instead resumes from `durable_cursor`, which lags
  `cursor`, so the tail is re-listed and the non-finalized part discarded
  (**exactly-once durable dataset**, I6 below).

- **Cursor = last committed key.** The ordinary checkpoint reload primitive sets
  `IN_PROGRESS → PENDING` while preserving `cursor` (I5), so a node would re-list
  via `start_after=cursor`. The shipped CLI exposes resume only for managed
  directory-dataset Parquet, whose recovery resets to `durable_cursor` as described
  above. No opaque continuation token is stored for S3 range nodes.

- **Split transaction (I4).** `splitNode` is its own transaction, guarded by
  `cursor < pivot AND range_end IS oldHi AND status <> COMPLETED`. This rejects a
  stale second-thief split and a victim that finished via an empty page (which
  completes without advancing cursor). A crash mid-split leaves the transaction
  atomically absent (the victim keeps the full range) or committed (child appears);
  the range set remains a valid partition either way.

- **Parquet exactly-once (I6).** Part files are tracked in `part_file`. A finalized
  part (footer fsynced) is never discarded. On resume non-finalized parts are
  discarded and each node re-lists from its `durable_cursor` — the highest key whose
  pages are all in finalized parts — so no finalized row is lost or duplicated.

- **`args_hash`.** The run's scope identity (store scheme + endpoint + bucket +
  prefix + recursive flag + `--all-versions` + hints) is hashed into `run_meta`.
  Resuming against a mismatched hash is refused; `--restart` discards the old run.
  Output format and filters are excluded from the hash (they don't change what is
  listed).

---

## Adaptive concurrency (AIMD)

The live concurrency target `T` (the number of active worker permits in
`ConcurrencyGauge`) is adjusted dynamically:

- **Decrease on stress:** a 503 `SlowDown` / `ServiceUnavailable` or a `Retry-After`
  triggers `T := max(1, floor(0.7 × T))`. Workers above the new `T` finish their
  current page then park. Per-call retries are swath's own bounded, jittered exponential
  backoff — SDK-internal retry is disabled (`maxAttempts=1`, see [`contracts.md`](contracts.md) §7) so the AIMD
  gauge sees every real 503/5xx immediately; swath's AIMD on `T` is the only adaptive control
  loop (two loops reacting to the same 503 would over-correct).
- **Increase on health:** after a clean window (no throttles for ~10 s),
  `T := min(Tmax, T + 1)`, and one parked stealer is unparked.
- **Floor `T ≥ 1`** — the run always makes forward progress.

On a healthy endpoint the decrease loop never triggers: `T` ramps from a slow-start floor
(`min(4, Tmax)`) up to `Tmax` and stays there. AIMD is a **safety brake** for throttling
storms — degrade gracefully, recover, never collapse — not a throughput lever; a clean run's
speed comes from the engine, not from adaptivity.

See [`docs/internals/algorithms.md`](algorithms.md) §5 for the full AIMD spec.

---

## Key design decisions

| Decision | Rationale |
|---|---|
| Project/binary name is `swath`; package root is `io.varve.swath` | The name describes the parallel-swath / no-gap-no-overlap technique; multi-store support is design intent, not v0.1 product support |
| One engine: `WorkStealingScan` replaces the four-strategy router | Eliminates mis-routing; demand-driven stealing handles varied key distributions within the ordered general-purpose S3 support envelope; no overlap unlike S3P's 2× reads |
| The worklist *is* the checkpoint table; fixed worker pool; termination by quiescence | Eliminates the semaphore-permit deadlock class; makes resume nearly free; single-writer thread scales |
| JDK 25 LTS; no `--enable-preview`; in-house `Scope` helper over virtual threads instead of `StructuredTaskScope` (still preview) | `ScopedValue` and non-pinning `synchronized` are final in 25; uber-jar must run without flags |
| Internal `StoreCapabilities` / `PageFetcher` seams and `PREFIX` node kind; other stores are post-v0.1 | Records the design intent without promising source/binary compatibility or claiming that an opaque-token store can use the range engine unchanged |

---

## Load-bearing invariants

The following invariants have named tests (see
[`docs/ops/dev/TESTING.md`](../ops/dev/TESTING.md)) and must not be violated:

| # | Invariant | Test |
|---|---|---|
| I1 | Commit-before-emit: checkpoint commits before the page is pushed downstream | RES-1 |
| I2 | The range set always partitions the keyspace: pairwise-disjoint, union = full scope | PROP-1 |
| I3 | Boundary key belongs to the LEFT interval (`(A,B]`: emit `k<=B`; right worker `start_after=B`) | PROP-1 |
| I4 | A split is its own atomic transaction guarded by `cursor < pivot AND range_end IS oldHi AND status <> COMPLETED` | RES-3 |
| I5 | Ordinary resume preserves `cursor`; Parquet resets it to `COALESCE(durable_cursor, range_start)` and reopens any not-output-complete node | RES-2 |
| I6 | A part file's rows are durable iff it is `finalized` (footer fsynced); a node is output-complete iff `COMPLETED` and `durable_cursor == cursor` | RES-4 |
| I7 | Worker permit / slot is never held while waiting on child work (worklist model eliminates this deadlock class) | CONC-2 |
| I8 | Drop the downstream receiver before joining the producer; `shutdownNow()` before `close()` | CONC-1 |
| I9 | Stuck continuation token / truncated-without-token → error, never an infinite loop | INT-3 |
| I10 | Keys are byte-exact end-to-end; never `String.compareTo` (UTF-16 ≠ S3 byte order for supplementary code points ≥ U+10000 — the surrogate pairs reorder) | UNIT-1 |
| I11 | Active row/page/merge buffers are functions of configured knobs, not object count. Finalized-part metadata is `O(parts)` and sorted staging metadata is `O(segments)`; these scale terms are outside the active-buffer bound. | PERF-2 |

Full invariant definitions and the `args_hash` / per-sink delivery guarantees:
[`contracts.md`](contracts.md) §0 and §5.

---

## Implementation status

The core listing path is built and green: the pipeline skeleton, the core types and
`byteMidpoint`, `S3PageFetcher` and `runRange`, the output formatters, the filter
chain, the Parquet writer pool, the checkpoint store and resume, and the
`WorkStealingScan` engine all ship.

Seeding is partial. The shallow `delimiter=/` seed pass (`SeedStep`, `SeedMode`,
`--tune seed.mode=shallow|none|hints`) and the demand-side `delimiter=/` structure
probe in `Thief` (median-common-prefix split) ship. The `--hints` file seed is not yet
implemented (`--tune seed.mode=hints` reports "not yet implemented"). Versioned listing,
dataset inspection, diff mode, and the live TTY progress display are not yet wired; see
[`ROADMAP.md`](../../ROADMAP.md).

Features deliberately deferred to v1.1: `-o s3://` output, `--resume-output` /
`output_journal`, `--expr` JEXL filter, `--metrics-port` Prometheus endpoint. `--sort`
(external merge sort) ships today (`--format parquet` only, opt-in); only its sorted
**text**-sink path and the default-on flip remain deferred.
