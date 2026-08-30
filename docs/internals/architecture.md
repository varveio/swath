# Architecture

swath is a bounded pipeline around one adaptive range engine. This page maps the design
to the repository and follows a run from the CLI to publication.

For the motivation, read [Internals overview](overview.md). For exact types, schemas, and
guarantees use [Contracts](contracts.md); for engine pseudocode and correctness arguments
use [Algorithms](algorithms.md).

## Component map

The Gradle module graph and allowed dependencies are documented separately in
[Build and modules](build-and-modules.md).

| Area | Main packages/classes | Responsibility |
| --- | --- | --- |
| Model | `io.varve.swath.model` | Byte-exact keys, entries, pages, and pivot primitives. |
| Store | `store`, `store.s3` | Ordered-page abstraction and the AWS SDK `ListObjectsV2` adapter. |
| Engine | `engine` | Worker pool, range scanner, stealing executor, owner splits, seeding, and adaptive concurrency. |
| Policy | `engine.policy` | Source-agnostic views and deterministic decisions for seed, victim, pivot, pacing, and owner-split policy. |
| Checkpoint | `checkpoint` | Worklist state and single-writer SQLite transactions. |
| Pipeline | `pipeline`, `runtime`, `concurrent` | Bounded message channel, lifecycle, cancellation, and orchestration. |
| Output | `output`, `output.parquet`, `sort` | Text formatting, managed Parquet, staging, merge, manifest publication. |
| Filters | `filter` | Key, size, time, and storage-class predicates. |
| Observability | `observability` | Meters, progress, run summaries, fingerprints, and traces. |
| CLI | `cli` | Picocli commands, option validation, target parsing, and exit codes. |

`swath-sim` drives the policy layer in virtual time. `swath-replay` serves a
captured listing through an S3-like API for repeatable integration and performance work.
Neither sits on the production listing path.

## Run lifecycle

```text
CLI
 │ validate target, output, filters, resume identity
 ▼
SeedStep ──► SQLite worklist ──► WorkStealingScan workers
                                      │ PageBatch
                                      ▼
                               bounded Channel
                                      │
                  ┌───────────────────┴───────────────────┐
                  ▼                                       ▼
             text / Parquet                         sort staging
                  │                                       │
                  │                                finalization
                  │                              (ranges | pipeline)
                  │                                       │
                  └──────────────► publish ◄───────────────┘
                                         │
                              report + manifest + checkpoint
```

### 1. Construct the run

`ListCommand` validates options before opening a checkpoint or contacting the store.
`ListRunner` creates the S3 client, metrics, stop token, checkpoint/output resources, and
pipeline. `RunContext` carries run-scoped state without thread-local plumbing.

A resumable run persists an `args_hash` covering listing identity. Resume refuses changes
that would alter the keyspace or output identity while allowing documented soft context to
be re-supplied.

### 2. Seed the worklist

`SeedStep` performs the configured shallow discovery and atomically inserts initial
`listing_node` rows. `seed.mode=none` starts from one open range. A normal resume skips
seeding and reopens incomplete nodes from persisted state.

### 3. Scan ranges

`WorkStealingScan` owns a fixed number of virtual-thread workers. A worker claims a
`PENDING` node and `RangeScanner` paginates after its cursor. Keys beyond the node's current
upper bound are not emitted, including when a split narrowed that bound while an API call
was in flight.

For each accepted page the worker:

1. records the next cursor in the checkpoint;
2. waits for the writer acknowledgement;
3. applies filters and sends a `PageBatch` to the bounded channel.

That commit-before-emit order is invariant I1. Output durability is layered on top rather
than changing the range cursor semantics.

### 4. Rebalance live work

When ready work is empty but outstanding nodes remain, an idle worker becomes a thief.
`Thief` snapshots live executor state into policy views; `ThiefPolicy` selects a victim and
requests any needed probes. The executor performs those store calls, feeds the results back,
and finally attempts the guarded split transaction.

The policy package does not perform I/O, mutate executor state, emit metrics, or read ambient
time/randomness. Decisions return reason enums; executors perform mutations and record the
matching engagement. `DecisionPathPurityTest` enforces the boundary. Random choices and clocks
enter through explicit interfaces so simulation and focused tests can be deterministic.

`OwnerSelfSplit` uses the same separation but consults a zero-probe governor after page
commits. It can publish an upper child before an idle thief asks, subject to demand, density,
confetti, and pacing gates.

### 5. Write and publish

The output consumer determines the delivery contract:

- stdout and file text are one-shot streams;
- direct managed Parquet routes each node consistently to one of a small fixed number of
  writers. Each lane owns encoding and durable close; after its checkpoint callback commits, one
  synchronous publication coordinator owns the monotone part set and atomically replaces the
  manifest. The coordinator is a serialized object, not another queued worker or shutdown path;
- sorted Parquet packs pages into bounded segments and performs a resource-bounded external
  merge before publication.

Direct and sorted Parquet deliberately have separate lifecycle owners but one physical writer
boundary in `output.parquet`. That lower layer owns the pinned parquet-mr configuration, the
caller-supplied geometry and `WriteSupport`, emitted-byte and digest accounting, optional
data-only sync on the writer's existing channel, and the final file-plus-parent durability step.
Above it, direct output retains sticky lanes, part rotation, checkpoint callbacks, and monotone
manifest publication; sorted output retains staging, mechanism-specific finalization, late global
footer stamps, ordered final-file rolling, and sorted publication. Sharing the byte transport must
not merge those state machines or let a physical sync advance either one's durable checkpoint
boundary.

Page-run staging is outside that Parquet boundary. It remains a checkpoint-tracked framed format
with its own strict seal-order durability protocol; any periodic writeback there is an adapter to
the shared cadence policy, admitted only by measurement, not a reason to route page-run bytes
through the Parquet writer abstraction.

The sorted merge keeps one public façade, `SortTransform(SortRun)`. `SortFinalization` selects the
default `ranges` path or the experimental `pipeline` path without changing durable staging or
publication. Shared owners are `PageRunCatalog`, which validates names and preflights trailers and
indexes; `MergePlanner`, which owns fan-in and mechanism-specific heap/FD admission; and
`DatasetPublisher`, which owns temporary parts, cross-part verification, stale-final replacement,
rename/fsync/listener ordering, and staging completion.

On `ranges`, `ParallelRangeMerge` owns boundary and seek planning, executor lifecycle, global
physical-zone proof, writer registration, and failure cleanup; each `ParallelRangeWorker` executes
one key range. On `pipeline`, `PipelineFinalization` owns the common failure domain:
`SegmentHeaderCursors` produces bounded header-only reference streams, `MergeRouter` is the single
global order and part-boundary owner, and `PartEncoders` positionally reads complete plans from one
shared bounded work queue. Encoder completion order is reconciled by dense plan ordinal before the
shared publisher sees any part.

`DatasetPublisher` deliberately stops at the listener seam—`ListRunner` remains the owner of
consumer `manifest.json`, state, symlink, and last-written `_SUCCESS`. After that listener returns,
`DatasetPublisher` owns only disposable-intermediate and staging reconciliation. A failure in that
suffix is typed as committed-publication cleanup pending; the runtime records PUBLISHED and retains
the completed transform facts for the unwound summary. PUBLISHED re-entry revalidates identity plus
`_SUCCESS` before cleanup, so retries clean without LIST work.

The terminal output stage is observed first so broken pipes, full disks, and writer failures
cancel producers promptly. On shutdown, downstream receivers close before producer joins;
executors receive `shutdownNow()` before `close()` (I8).

### 6. Detect completion

An `outstanding` count covers every uncompleted range. A split adds its child within the
same durable transition that narrows the parent; completion decrements only after the node
state is durable. The scan ends at `outstanding == 0`, not merely an empty ready queue.

The pipeline then drains, writers finalize, sorted output merges if requested, manifests and
reports publish, and the run records its terminal phase and exit classification.

## Two concurrency shapes

The listing engine and outer pipeline intentionally use different structures:

- The engine is a fixed work-stealing pool over a mutable worklist. Tree-shaped task joins
  would risk holding permits while waiting for children and would make thread count scale
  with split width.
- The outer pipeline is a small fixed set of sibling stages with coordinated cancellation.
  It uses the repository's `Scope` helper over virtual threads because the shipped JDK 25
  artifact does not require preview features.

All SQLite mutations pass through one writer thread. Parquet encode/write concurrency is a
separate bounded pool (default 3; expert range 2–64 with heap admission above 4), so object-store
concurrency does not multiply file writers. A fixed whole-pool submission budget likewise prevents
additional lanes from multiplying queued page batches.

## Checkpoint and publication boundaries

| Boundary | Durable fact |
| --- | --- |
| `commitPage` acknowledged | Range cursor/status transaction committed. |
| `splitNode` acknowledged | Parent bound and child insertion committed together under CAS. |
| Parquet part finalized | Footer fsynced, part recorded, covered node cursors may advance durably. |
| Completion manifest replaced | Readers can discover the complete finalized dataset; `_SUCCESS` follows last. |
| Sorted publish completes | Final ordered parts and metadata replace the staging result. |

A crash between boundaries may repeat work but must not break the keyspace partition or
duplicate a row in the published managed dataset. The recovery procedure and per-sink
qualifications are in [contracts §5](contracts.md#5-resume-args_hash-and-per-sink-guarantees).

## Adaptive concurrency

`ConcurrencyGauge` maintains the live request target. It begins at a small floor, increases
after clean windows, and multiplicatively decreases on service throttling. Workers above a
reduced target finish their current page before parking. The configured `--request-rate`
cap is independent and applies before requests are issued.

Adaptive concurrency is a safety control, not the source of parallelism. Seed and split
supply determine whether useful work exists; AIMD only limits how much of that work may call
the store at once. See [algorithms §5](algorithms.md#5-adaptive-concurrency-aimd).

## Change map

| If you change… | Also inspect… |
| --- | --- |
| range boundaries, cursor order, or split transaction | invariants I1–I6, PROP/RES tests, SQLite schema |
| a seed/pivot/gate/backoff path | policy/executor seam, engagement reason, trace event, simulator |
| output buffering or sorted merge | I11–I12, heap/FD/disk gates, resume and publish tests |
| metrics or summary fields | public metrics page, internal registry, drift guard, schema compatibility |
| CLI options affecting run identity | `ArgsHash`, resume classes, generated help, configuration docs |

The concise invariant list in this page is intentionally replaced by one canonical source:
[contracts §0](contracts.md#0-load-bearing-invariants).
