# swath-sim

`swath-sim` runs swath's **real listing policies against a modelled store in virtual time**;
its sibling [`swath-replay-server`](../swath-replay-server) does the opposite — it runs the
**real engine against a fake S3 endpoint in real time**. Neither is a shipped artifact: both
are development/analysis tools that live in this repository only.

That one-line split is the whole reason the two modules coexist. The replay server exists to
prove the shipped engine behaves correctly end-to-end over HTTP, so it pays real socket, real
Parquet and real clock costs per page. A simulator exists to answer *"what would a different
split/steal policy have done on this bucket?"* thousands of times, so it must pay none of
them — but it must still answer every list request exactly as the real thing would, or its
answers are fiction. Two parts of the module make that possible: the **ground-truth store**, and
the **discrete-event kernel** that drives it.

## What "simulation in virtual time" means here

The simulator does not run a listing and time it. It **computes** what the timings would be.

The clock is a plain number of nanoseconds, and it only moves when the model says something
happened. Alongside it is a queue of events scheduled for future instants — "this page request
completes at t + 4.1 ms", "this worker's backoff expires at t + 5 ms". The kernel loops: take the
earliest event in the queue, jump the clock straight to its instant, run it (which typically
schedules more events), repeat until the queue is empty. All the time in between, in which nothing
happens, is skipped for free. That is what makes it fast: a modelled ten-minute listing costs
whatever its few million events cost to process, not ten minutes.

Virtual time buys two things beyond speed.

- **Reproducibility.** The kernel is single-threaded and events are totally ordered by
  `(instant, scheduling sequence)`, so there is nothing for a host scheduler to decide. Every random
  draw comes from a stream derived from the run's single seed, one stream per actor and purpose. Two
  runs of one scenario at one seed therefore produce byte-identical event logs — and a policy change
  shows up as a difference in the log rather than as noise. (This is determinism *inside the
  simulator*; a real multi-worker run's thread scheduling stays outside anyone's control.)
- **Inputs you state rather than inherit.** Request latencies, the client-side cost of processing a
  page, and the engine's own time budgets (probe timeouts, pacing windows, run duration ceiling) are
  all **declared parameters** of a scenario, not properties of the machine the simulation runs on.
  A benchmark on a loaded laptop silently changes the ratio between a three-second timeout and the
  calls it bounds; a scenario states that ratio exactly, and a sweep can vary it on purpose.

The price is honesty about what is modelled. Store answers are ground truth — real keys, real
pagination, real truncation — but every duration in a result is only as good as the model that
produced it, which is why the module refuses to run without an explicit client-cost term and why its
own tests pin closed-form answers in the mode where the arithmetic is exact.

### Two instruments, not two attempts at one

|  | `swath-replay-server` | `swath-sim` |
|---|---|---|
| Runs | the real engine, over HTTP | the real decision logic, in-process |
| Store | a fake S3 endpoint serving a captured fixture | a modelled store serving the same fixture |
| Time | real: real sockets, real clock, real duration | virtual: computed from declared inputs |
| Answers | *does the shipped engine behave correctly end to end?* | *what would a different policy have done, and how fast?* |

They are complementary. The replay server is the realism instrument — it exercises the actual code
paths and can catch anything a model leaves out, at the cost of one real-time run per question. The
simulator is the speed-and-reproducibility instrument — thousands of runs, exactly repeatable, at
the cost of only being as right as its model. Results from the second are worth what the first
says they are worth, which is why the two live side by side.

## The seam

`swath-sim` does not define a second listing protocol. It plugs into the two layers the replay
server already owns and reuses both verbatim:

| Layer | Owns | Lives in |
|---|---|---|
| `ListObjectsV2Pager` | *All* S3 `ListObjectsV2` semantics — max-keys accounting, truncation, delimiter rollup, common-prefix resume, prefix/`start-after` interplay, continuation tokens. | `swath-replay-server` |
| `ListingStore` | Ordered **range reads** only: the first `limit` rows in `[from, toExclusive)`, ascending unsigned key order. Plus the one optional `delimitedRollup` fast path. | `swath-replay-server` |

A simulator backend is therefore *only* a `ListingStore`. If a change here starts re-deriving
when a page is truncated or where a continuation token resumes, it is in the wrong layer.

## Backends

Backends are selected explicitly through `SimStoreFactory.open(fixturePath, backend, config)`
(`SimStoreBackend`), following the replay server's `--serving-mode` / `ReplayServingFactory`
precedent. Explicit selection is the point: an automatic choice would only ever exercise one
backend per fixture, so a conformance or differential run could never compare them.

| Backend | What it is | Metadata |
|---|---|---|
| `ARENA` | Keys-only in-memory arena: segmented key-byte blocks plus long offsets, built once from any other `ListingStore` and then answered entirely from memory. For a fixture whose whole key set fits the configured budget. | **Stubbed** (sim-mode projection, below) |
| `STREAMING` | Keys-only **decode-once** tier: each row group of a sorted fixture is decoded to off-heap keys the first time a cursor reaches it, then served from memory, and evicted behind the cursors. For fixtures too large for the arena. | **Stubbed** |
| `WINDOWED` | The replay module's sequential-window prefetch decorator (`WindowedListingStore`) wrapping its sorted-Parquet store, in process. **Forced-only** — see below. | Full |
| `PARQUET` | The replay module's Parquet-backed store, unchanged. The differential reference. | Full |
| `AUTO` | `ARENA` when the fixture's encoded key bytes fit the configured budget; else `STREAMING` when the fixture is sorted-eligible; else `PARQUET`. | Depends on the resolution |

Which backend actually served a fixture is recorded, not just returned: every resolution bumps
`swath.sim.store.backend{backend}`, an `AUTO` resolution that declines the arena bumps
`swath.sim.store.arena.decline{reason}`, and one that goes on to decline the streaming tier bumps
`swath.sim.store.streaming.decline{reason}` — each logs why. A sweep's results therefore carry a
record of what produced them, so a threshold regression cannot masquerade as a throughput
regression.

`STREAMING` and `WINDOWED` both require a **sorted-eligible** fixture: a stamped, `mode=objects`,
strictly-sorted, pure-`OBJECT` capture, the same eligibility the replay server's own
`--serving-mode sorted` checks (`SortedEligibility`, shared code). A fixture that is not
sorted-eligible fails fast under either forced request, and `AUTO` falls back to `PARQUET`.

**Why `WINDOWED` is forced-only.** It decodes Parquet *inside* the serving loop: every window
refill is a bounded range query, and because the `key` column is a `BLOB` with no usable zonemaps
that query scans the whole key column — a cost proportional to the fixture, not to the window.
Amortising it over a window divides that cost by a constant and leaves the fixture-size term
intact, so on a giant fixture it is orders of magnitude off a simulated run's budget. It stays as
the memory-bounded path that carries **full metadata** and as the conformance comparison for
`STREAMING`, chosen deliberately rather than by default. Its `enabled`/`window-rows`/`max-windows`
come from the replay server's `swath.replay.prefetch.*` system properties — one tuning surface for
both callers, not a second sim-only knob for the same thing — so
`swath.replay.prefetch.enabled=false` serves the bare sorted-Parquet store here too, exactly as it
does for `--serving-mode sorted`.

### The sim-mode projection

The two keys-only tiers (`ARENA`, `STREAMING`) load **keys only**. Every metadata column they
return is a stub: size `0`, epoch-`0` last-modified, and `null` etag / storage class / owner /
checksum fields (`SimModeRows`).

This is deliberately **not** `Projection.KEYS_ONLY`, which only drops the two owner fields —
under `KEYS_ONLY` a store still materialises size, etag and dates because the replay server's
XML renderer needs them. A simulator never renders XML, and metadata is dead weight it would
pay for on every key of every fixture, so these tiers skip loading it altogether. The tradeoff
is explicit: **their responses are ground truth for keys, pagination and truncation only.**
Anything that needs real metadata must use a `PARQUET`-backed backend.

### The streaming tier: decode once, serve from memory, evict behind the cursors

A simulated run issues on the order of 150,000 store calls, so anything the serving loop does per
call is multiplied by 150,000. The streaming tier's whole design is to make *decode* not one of
those things.

- **A segment is one row group**, decoded keys-only the first time a read reaches it. A fixture's
  row groups hold hundreds of thousands of rows, so one decode is amortised over hundreds of
  1000-row pages; served calls after that are a binary search and a walk, the same shape (and the
  same cost class) as the arena tier.
- **Mid-keyspace starts seek, they do not scan.** A steal or a split starts a fresh cursor at an
  arbitrary key. The derived row-group index (`SortedFixtures`/`SortedEligibility`, shared with the
  replay server's sorted-serving path) locates the one row group containing that key by binary
  search, and decode begins there — nothing earlier in the file is touched. The
  `swath.sim.store.streaming.segment.fault{kind}` counter separates these (`seek`) from a cursor
  walking off the end of its current segment (`forward`), so a workload's access shape is readable
  from the metrics alone.
- **Memory is bounded by configuration, not by fixture size.** Decoded segments live in an
  access-ordered map bounded by `swath.sim.streaming.max-resident-bytes`; the least recently used is
  released once the budget is exceeded, which for N mostly-sequential cursors is exactly "evict
  behind the cursors". A segment costs its row group's key bytes plus 8 bytes per key: for a fixture
  whose groups hold ~300K keys of ~100 bytes, ~33 MB each, so the 1 GiB default holds ~30 of them —
  headroom for ~30 concurrent cursors on a fixture of any size. A fault transiently exceeds the
  budget by the block it is decoding (built before the eviction it triggers frees anything), so size
  a run for the budget plus one row group.

#### Why the decoded segments are off-heap, and why the layout is a file format

Segments are `java.lang.foreign.MemorySegment`s allocated from an `Arena`, not `byte[]` blocks.
Three reasons, in order of weight:

1. **`long` indexing.** The arena tier splits its key bytes across fixed-size blocks purely because
   a single Java `byte[]` caps at 2 GiB. A `MemorySegment` has no such cap, so the streaming tier
   has no block bookkeeping, no per-block addressing, and no key that can straddle a boundary.
2. **Deterministic release.** A tier whose claim is a bounded working set wants memory freed when it
   evicts, not when a collector next runs. Closing a segment's `Arena` releases its bytes at the
   moment the residency budget says they are gone — and keeps a multi-hundred-megabyte working set
   out of the heap the rest of the JVM is using.
3. **The layout is dumpable.** A segment is exactly two buffers: an `int64` offset per key, and one
   contiguous run of key bytes — Arrow's `LargeBinary` shape. Nothing in it is an in-process
   pointer, an object reference, or any other state that would not survive a round trip through a
   file. That is deliberate and is the one forward-looking constraint in the tier: decoding a
   fixture *once ever* into a sidecar and memory-mapping it on later runs
   (`FileChannel.map(..., Arena)` returns a `MemorySegment` over the same two buffers) would turn
   the per-run decode into a per-fixture one. **That sidecar is not built here** — it pays for
   itself only across many runs over the same fixture, which is a different decision than this
   tier's own single-run budget. Keeping the layout mappable costs nothing now and is what makes it
   an addition later rather than a redesign.

`KeyArena` (the arena tier) and `KeyBlock` (the streaming tier) are therefore separate on purpose:
one on-heap whole-fixture column built once at load, one off-heap per-row-group block faulted and
dropped as cursors move. They share a shape — sorted keys plus an offset table, binary-searched —
but not a lifecycle, an allocation strategy, or an addressing scheme.

### Arena sizing, and why the tier threshold is in bytes

A single Java `byte[]` cannot exceed 2 GiB, so arena key bytes are held in fixed-size
**segments** addressed by `long` offsets; a key may straddle a segment boundary and is
reassembled on read. Capacity is therefore governed by **estimated encoded bytes**
(`key bytes + (n + 1) × 8` for the offset table), never by a key count: keys are legal up to
1024 bytes each, so two fixtures with identical key counts can differ by three orders of
magnitude in footprint. `SimStoreConfig.arenaMaxEncodedBytes` is the configurable threshold;
`SimStoreFactory.open(fixturePath, backend)` reads it from the `swath.sim.arena.max-encoded-bytes`
system property, so an operator can retune the tier without a code change, while the
config-taking overload lets a caller pin it outright.

Keys are stored raw. Front-coding a sorted key set often buys 3–5×, but that is a *measured*
optimisation to make later against a real fixture, not an assumption to build in now.

## The kernel and the models

Three packages, in dependency order:

| Package | Holds |
|---|---|
| `sim.kernel` | The clock, the event queue, the scheduler, the per-actor SplitMix64 draw streams, the event log, and a FIFO server for modelling a shared resource. A few hundred lines, deliberately: a general-purpose simulation framework would bring its own clock and its own randomness, which is precisely what the closed-form invariants below need to own. |
| `sim.model` | The physics, all of it pluggable: `LatencyModel` (constant, fitted per call class, or scaled by how many calls are in flight), `ClientCostModel` (independent per page, contended through a shared server, or the measured composite of both), and `EngineTimeBudgets`. |
| `sim.driver` | A scenario, and a trivial "list every range with T workers" driver that exercises the whole stack against a real store. The real split/steal policies are not wired up here. |
| `sim.executor` | The real policies, wired: the seed planner, the owner-side split governor, the thief's victim selection and pivot cascade, the idle-steal pacing, and the simulator's own adaptive-concurrency controller. |

### What a page costs the client, and why it is three things

Charging a page one number is the mistake this model exists to avoid. Direct measurement of a real
client found the per-page cost split across stages that behave differently under load: the fetch
worker's own conversion work is independent per page and parallel across workers; the durability
commit is a single serial writer every page waits on before it may emit; the output sink is another
serial stage whose service rate is a real ceiling on how fast pages can leave. A columnar sink adds a
fourth, measured to run on its own threads and off the page's critical path.

`CompositeClientCost` charges the first three in series on the page's own timeline and the fourth in
parallel, because that is the order the engine does them in — and getting it wrong in the other
direction would let a simulated fleet emit pages faster than any real client could absorb, which is
exactly the impossible strategy a client-cost term exists to prevent the simulator from "discovering".
Two of the stages have a mean several times their median, so those are **sampled** from their measured
quantiles rather than averaged: a policy that bursts should pay for its bursts, not pay the average
twice.

Every term carries where it came from and how far it can be trusted. A term is never defaulted and
never silently zero; zeroing one is legal only through the constructor that records the zero as a
deliberate choice, which marks the run as an arithmetic check rather than a prediction.

### The timeouts are inputs, not constants

A timeout only means something relative to the latencies it bounds. Under a scaled-down real-time
experiment those ratios move, which is how a timeout pathology disappears from a reproduction — the
budget stayed at three seconds while the call it bounds got ten times faster. Virtual time can state
the ratio exactly, so every one of them is a declared input: probe and worker attempt timeouts, the
transient-retry ceilings, the pacing windows, and the adaptive controller's own windows. Defaults
restate what the shipped engine uses, so a scenario that wants "today's engine" says so, and a
scenario that wants a different ratio states the difference against a written reference.

An action body runs atomically in virtual time — the clock does not move and no other actor runs
inside it — which is how a lock hold is expressed without a lock. State read in one event and used
in a later one is correspondingly exposed to whatever other actors do in between, which is how a
widened read window, and the race it permits, is expressed without a thread.

## Running the real policies

The point of the module. `SimExecutor` runs swath's **actual** decision code — the seed planner, the
owner-side split governor, the thief's victim selection and pivot cascade, the idle-steal pacing —
against a fixture, in virtual time. Nothing is reimplemented: those modules are consumed exactly as the
engine consumes them, as decisions over views that return actions and mutations. What the simulator
supplies is the other half of that seam: it builds the views, issues what the decisions ask for,
applies the mutations they return, and owns everything the policies deliberately never see — the clock,
the concurrency target, the ranges' bookkeeping, and the check that decides whether a proposed split
survives.

One mechanism is a **port** rather than the real thing: the adaptive-concurrency controller. It is the
most timing-coupled code in the engine — jittered windows, paced valves, a decaying latency baseline,
all racing under compare-and-set — and carving it out from under its concurrent callers would have been
a far larger risk than writing a faithful equivalent whose every signal carries its own timestamp. That
is a reimplementation, and it is treated as one: it is reviewed against the controller's own documented
guarantees, a change to one is a change that has to be made to both, and no test here can prove they
agree.

**A simulated split can fail, and that is the point.** A thief reads its victim's cursor, spends probes
placing a pivot, and by the time it proposes the split the victim may have drained past it. The
proposal is then refused — exactly as in a real run, and for exactly the same reason. A simulator that
let it through would report splits a real fleet never gets, on precisely the workloads where stealing
is hardest. The full ordering contract, including the two disclosed timing widenings the executor
models and what a cancelled timer costs a run's event budget, is in
[`docs/executor-ordering.md`](docs/executor-ordering.md).

**What a run reports.** A duration on its own is not a result: the same fixture yields a different one
under a different store backend, a different client-cost term, or different declared budgets, and all
three are choices. So a run record carries them — which store served it, which cost term it was charged
against and how far that term can be trusted, which budgets it declared — alongside the phase shape,
the counters every policy path engaged, and how many events the kernel dispatched to produce it.

### What a run costs to produce

Two different numbers, and confusing them is the classic simulator mistake. A run's **virtual
duration** is the modelled system's answer. Its **wall time** is what this machine spent computing that
answer, and the only thing it says about swath is whether sweeping over many runs is affordable.

Measured on one 8-core arm64 development box, 2M keys under the real policies, 32 workers, 1000-key
pages, no seed:

| | |
|---|---|
| modelled store calls | 2,872 |
| events dispatched | 44,752 (**15.6 per call**), of which 16,712 were retired park timers |
| wall time | 0.34 s (**117 µs per modelled call**, including materialising every page from the fixture) |
| virtual duration | 6.8 s |

The shape that matters is **events per modelled call**, because that is what does not change with the
fixture's size. At ~15 events per call, a 150,000-call run — the scale of a very large bucket —
dispatches roughly 2.3M events and costs on the order of 15–20 s here, which is inside the budget such
a run has to fit for sweeping to be practical. Two caveats travel with that, and neither is small: the
measurement above reads its pages from an in-memory fixture, where a real large-fixture backend serves
a call in ~175 µs and would dominate the total; and a timeout-heavy scenario arms events a healthy one
never does, so a run's budget has to be sized including the ones that turn out not to matter. The
bench that produces these numbers is `PolicyRunBudgetBenchTest` (`@Tag("perf")`, opt-in).

### Why the kernel's own tests assert equalities

With a constant latency and the client-cost term explicitly zeroed, a run's wall time is pure
arithmetic, so the tests assert it exactly rather than within a tolerance: a range of `n` keys costs
`floor(n / pageSize) + 1` calls (the last, short page is how a lister learns it is finished); the
total call count does not depend on the worker count at all; one worker costs the sum of the
latencies; a worker per range costs the largest range. Scaling is asserted **monotonic and not
proportional** — a range is claimed whole, and pacing intervals and client costs are fixed
durations, so more workers legitimately stop helping.

## Fixtures

A fixture is a **local path** — a swath Parquet capture file, or a directory of them — supplied
by the caller (config or CLI argument). Nothing in this module hardcodes, or is allowed to
hardcode, a remote object-store location; fetching a fixture to local disk is the caller's job,
outside this module.

### Keyspace shapes, generated

A policy's behaviour is decided almost entirely by *shape* — where the directories are, whether mass
sits in a few of them or spreads evenly, whether a split can find a populated pivot — so the tests also
generate small keyspaces shaped like real buckets: a deep date-partitioned observation archive, a
hash-fanned content-addressed corpus, a tree with one object per directory, and a single dense flat
leaf. Each runs in milliseconds and each provokes different policy paths, which a fixture of uniformly
named keys does not.

One of them is adversarial on purpose. **Concurrency poison** is a store whose latency rises with the
number of calls in flight: the fleet's own success makes every call slower. It exists because one
control rung — the adaptive controller's latency freeze — reacts to nothing else, so a store that
degrades under load is the only thing that can exercise it. Staging that against a real store is not
something anyone can do deliberately; here it is a constructor argument, and the fleet's reaction is
observable event by event.

## Building and testing

`swath-sim` is part of the ordinary build:

```shell
./gradlew :swath-sim:test
./gradlew :swath-sim:test --tests 'io.varve.swath.sim.store.SimStoreDifferentialTest'
```
