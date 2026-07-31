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

The clock is a plain number of nanoseconds. Alongside it is a queue of events scheduled for future
instants — "this page request completes at t + 4.1 ms", "this worker's backoff expires at t + 5 ms".
The kernel takes the earliest event, jumps the clock straight to its instant, and runs its body
atomically; events at the same instant run in scheduling order. It repeats until the queue is empty
or the scenario's duration or event ceiling stops the run. All the time in between, in which nothing
happens, is skipped for free. That is what makes it fast: a modelled ten-minute listing costs
whatever its events cost to process, not ten minutes.

Virtual time buys two things beyond speed.

- **Reproducibility.** The kernel is single-threaded and events are totally ordered by
  `(instant, scheduling sequence)`, so there is nothing for a host scheduler to decide. Randomness
  is derived from the run's single seed and isolated by actor (or the reserved fleet identity) and
  purpose or policy mechanism. Adding actors does not re-seed existing actors, and consuming more
  draws from one stream does not shift another stream. Within a stream, draws are consumed in event
  order, so changing when that mechanism draws can change its later values. Two runs of one scenario
  at one seed therefore produce byte-identical recorded event logs; recording can also be disabled
  without changing the simulated result. This is determinism *inside the simulator*; a real
  multi-worker run's thread scheduling stays outside anyone's control.
- **Inputs you state rather than inherit.** Request latencies, the client-side cost of processing a
  page, and the engine's own time budgets (probe timeouts, pacing windows, run duration ceiling) are
  all **declared parameters** of a scenario, not properties of the machine the simulation runs on.
  A benchmark on a loaded laptop silently changes the ratio between a three-second timeout and the
  calls it bounds; a scenario states that ratio exactly, and a sweep can vary it on purpose.

The price is honesty about what is modelled. Store answers are ground truth — real keys, real
pagination, real truncation — but every duration in a result is only as good as the model that
produced it, which is why the module refuses to run without an explicit client-cost term and why its
own tests pin closed-form answers in the mode where the arithmetic is exact.

> **Current model limits.** Ordinary `SimExecutor.run` defaults to `SensingVariant.CURRENT`, which
> leaves the estimator seam `null` and therefore selects the legacy `WINDOW` sensor. Scenario toggles
> still select `REACH_FLOORED` and the other current mechanisms, so an ordinary default scenario is a
> `WINDOW` + `REACH_FLOORED` hybrid, not the production 0.2.0 defaults (rate-anchored sensing plus
> `REACH_FLOORED`). `RATE_ANCHORED_FLOOR_QUARTER` delegates to production's promoted sensor. Also,
> `EngineTimeBudgets.engineDefaults()` uses one 3 s probe-attempt budget for seed, pivot, and structure
> calls; production uses 3 s for point-class pivot probes and 10 s for scan-class seed/structure calls.
> Historical experiment results below remain measurements of the explicitly stated arms and budgets.

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
| `STREAMING` | Keys-only **on-demand decoded-block** tier: a sorted fixture's row groups are decoded to off-heap keys when reached, cached while resident, and evicted behind the cursors. An evicted group is decoded again if revisited. For fixtures too large for the arena. | **Stubbed** |
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
sorted-eligible fails fast under either forced request — as a typed `IneligibleFixtureException`
carrying the eligibility reason and the file set, so a sweep over a corpus classifies the refusal
without reading a message — and `AUTO` falls back to `PARQUET`.

### An unsorted fixture is a corrupt input, and where that is caught

Eligibility proves the ascent of row-group **first** keys. The rows *inside* a group are proved
where they are decoded, in the loop each tier already runs — never as a separate validation pass,
which on a 300 MB fixture would cost as much as the run:

| Tier | Unsorted input |
|---|---|
| `STREAMING` | **Hard fail** on the first violation in any row group a run faults in, naming file, row group and the offending key's position within that group — the message spells it `key N`, the position the block counted (`KeyBlock` on the way in, file and row group added by the tier). |
| `WINDOWED` | Hard fail along the `delimiter=/` skip-scan, which proves every row it steps over (`SortedRowGroupReader.KeyCursor`). Its plain range reads are **not** guarded, and what they do is worse than a short page — see below. |
| `ARENA` | Loaded through the Parquet store, whose reads are `ORDER BY key`: disorder is normalised on the way in and only a **duplicate** key trips the arena's check. |
| `PARQUET` | Not checked, by design — that store exists to serve arbitrary unsorted captures and re-sorts at query time. |

So a corpus fixture large enough for a real sweep — which `AUTO` puts on `STREAMING` — is guarded,
and one small enough to fit the arena is served in key order whatever its file holds. A sweep runner
treats the hard failure as "exclude this bucket and record why", not as a reason to stop — and it can
read the *why* mechanically: the failure is an `io.varve.swath.sort.RowGroupOrderException` carrying
`reason() == "row_group_disorder"`, and the decode bumps
`swath.sim.store.streaming.segment.refused{reason}` **before** it rethrows, so the exclusion survives
into the metrics of a run that ended in an exception. Nothing downstream has to match a message. (The
replay server raises the same typed failure from its own skip-scan and counts it as
`swath.replay.serving.refused{reason}`; see `docs/swath-replay-server.md`.)

**What `WINDOWED`'s unguarded range reads actually do is lose keys, silently.** They are routed off
the derived index, which describes an order the file does not have, so on a multi-file fixture the
plan for a range can leave out the file a misplaced in-range key physically sits in — and then that
key is never read at all. Nothing is short and nothing is out of order: the client walks to a clean,
**un-truncated** end of listing missing a key it was entitled to. Worse, a whole-listing read can
find the key anyway (its plan spans every file), so the loss appears only once a cursor *starts*
above the misplaced key's own file — which is every steal and every split a simulated run performs.
`UnsortedFixtureGuardTest` asserts both halves, the refusal and the loss, so the masked behaviour is
a pinned fact rather than a claim in prose.

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

### The streaming tier: decode on demand, serve from residency, evict behind the cursors

A simulated run issues on the order of 150,000 store calls, so anything the serving loop does per
call is multiplied by 150,000. The streaming tier's whole design is to make *decode* not one of
those things.

- **A segment is one row group**, decoded keys-only when a read faults it into residency. A fixture's
  row groups hold hundreds of thousands of rows, so one decode is amortised over hundreds of
  1000-row pages while resident; served calls are then a binary search and a walk, the same shape
  (and the same cost class) as the arena tier. A later revisit after eviction decodes it again.
- **Mid-keyspace starts seek, they do not scan.** A steal or a split starts a fresh cursor at an
  arbitrary key. The derived row-group index (`SortedFixtures`/`SortedEligibility`, shared with the
  replay server's sorted-serving path) locates the one row group containing that key by binary
  search, and decode begins there — nothing earlier in the file is touched. The
  `swath.sim.store.streaming.segment.fault{kind}` counter records `forward` when the preceding row
  group is resident and `seek` otherwise. These are observable access-shape signals, not proof of
  which cursor caused the fault.
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
probe retry cap, the worker retry threshold, the pacing windows, and the adaptive controller's own
windows. The worker threshold is a stopping ceiling only under simulated `BOUNDED`; simulated
`RIDE_OUT` ignores it, while probe chains stay bounded under either disposition. Most defaults restate
what the shipped engine uses, so a scenario can state its reference point and vary ratios deliberately.
The current single probe-attempt field is the approximation disclosed in the model-limit note above,
not an exact restatement of production's point/scan split.

An action body runs atomically in virtual time — the clock does not move and no other actor runs
inside it — which is how a lock hold is expressed without a lock. State read in one event and used
in a later one is correspondingly exposed to whatever other actors do in between, which is how a
widened read window, and the race it permits, is expressed without a thread.

## Running the real policies

The point of the module. `SimExecutor` runs swath's **shared decision policies** — the seed planner,
the owner-side split governor, the thief's victim selection and pivot cascade, and the idle-steal
pacing — against a fixture, in virtual time. Those modules are consumed exactly as the engine consumes
them, as decisions over views that return actions and mutations. What the simulator supplies is the
other half of that seam: it builds the views, issues what the decisions ask for, applies the mutations
they return, and owns everything the policies deliberately never see — the clock, the ranges'
bookkeeping, and the guarded split outcome. Adaptive concurrency is the explicit port described next,
not shared implementation.

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
is hardest. The full ordering contract, including the three disclosed timing widenings the executor
models and what a cancelled timer costs a run's event budget, is in
[`docs/executor-ordering.md`](docs/executor-ordering.md).

**There are two guard stages, and the first is where ordinary footrace losses land.** The engine
re-validates its pivot against the victim as it stands *now*, immediately before proposing the split,
and then the ledger guards the expected bound and cursor again and additionally rejects an already
completed node. The first check is where a thief that spent its probes on a drainer it cannot catch
usually finds out — `RETRY.cursor_passed_pivot`, `RETRY.bound_moved`, reported as
`splits_lost_revalidation`. The completion guard needs no mutation between that re-validation and the
serial ledger call: a final non-empty page marks the ledger node complete before its client-side cost
is charged, while its progress-eligible worker remains in `livePool` until that cost finishes. A
proposal whose pivot is still above the final cursor can pass the in-memory checks during that interval
and be rejected by the ledger. The single steal-attempt slot makes a concurrent second proposer
unavailable in the modelled fleet. Thus `splits_rejected` is a counter for this late guard, not for all
lost races; the ordering doc reports the measured rejection and zero-rejection cases. Both stages are
in the run record, named apart, for exactly that reason.

**How often that race is lost turns on one ratio the scenario declares: what a probe costs relative to
a page.** This README used to say the opposite — that the loss share is a property of the keyspace and
not of the declared timings — on the evidence that moving from a 100-key page in 30 ms to the
1,000-key page in 110 ms leaves it in the same place (81% and 66% on the same fixture). Both of those
regimes price a probe at ~0.32 of a page, so what that pair of runs actually held fixed was the ratio,
and the claim generalised from it was wrong. The live store was measured at 121 ms against 223 ms
(0.54). A thief spends a cascade of probes placing a pivot and loses if the victim drained past it
meanwhile, so `probes × probe/page` *is* the race window in owner pages: about two at the live ratio,
under one at 0.32. On a wide-flat tail that difference is the whole outcome — at 0.54 the engine loses
essentially every race on the tail range (measured on a real bucket: 460 of 461 attempts), and a
fleet modelled at 0.32 keeps winning them and reports a bucket carved up that a real run leaves to
drain serially.

So the loss share is ratio-sensitive, and *mass* moves it too: the same geometry with a twentieth of
the keys per directory loses 52%. The real-listing instruments (`RealListingRunTest`, the corpus
sweep, `SingleLegRunTest`) therefore run at the live profile — `PolicyRunFixtures.LIVE_S3_LATENCY`,
223 ms page / 121 ms pivot probe / 223 ms delimited probe, measured 2026-07-28 — and
`ProbeToPageRatioTailTest` pins the mechanism: one fixture, one fleet, one page size, one arm, with
only the ratio changed, and the heavy leaf goes from carved-up to a single unstealable range holding
the fleet for half the run.

**The bench tables published at the 35 ms / 110 ms regime remain valid as characterisations of that
regime** and are still pinned there — a bench number states its regime, and re-running them at the
live ratio would replace measurements, not correct them. What does not survive is quoting one of them
as what a fleet would do against the live store on a race-sensitive shape; that needs a run at the
live ratio. Every regime is a named pair of inputs so either claim can be re-checked rather than
believed.

Two denominators are in circulation and they are not interchangeable. The shares above are proposals
lost over proposals that reached the re-validation; a deployment's own numbers are usually quoted per
steal *attempt*, counting attempts that never got that far. The measured runs read 85% and 93% per
attempt, which is 96% and 90% per proposal. On either, this bench loses fewer races than the
deployment does — the conservative direction for an instrument whose job is to make a proposed cure
prove itself.

**What a run reports.** A duration on its own is not a result: the same fixture yields a different one
under a different store backend, a different client-cost term, or different declared budgets, and all
three are choices. So a run record carries them — which store served it, which cost term it was charged
against and how far that term can be trusted, which budgets it declared — alongside the phase shape,
the counters every policy path engaged, and how many events the kernel dispatched to produce it.

Two parts of that record are worth naming, because a counter total cannot express either.

- **The phase timeline.** Three instants — the seed's end, the last split of any kind, the run's own
  end — plus what happened between the last two: the keys emitted in that tail, and the *occupancy*,
  meaning how many ranges were actually being drained while they were. A run that divides its keyspace
  and one that gives up early can have identical split counts and completely different shapes, and the
  difference is the whole subject. The run's end here is **quiescence**, not the kernel's last event.
  The kernel cannot cancel a timer, so a park that has already been retired still costs a dispatch when
  it fires, and a dispatch moves the clock. The longest of them is the steal-attempt-slot backstop —
  one second under the engine's own defaults, the park of a worker that found the fleet's single steal
  slot busy — so the clock keeps advancing for up to that second after the last worker has retired.
  Traced after quiescence, the residue is *only* retired parks: no call timeout, no retry, no key.
  Both instants are reported, and the gap between them (0.75–1.0 s on the in-repo fixtures) is an
  artifact of the kernel rather than a property of the modelled fleet — **so a comparison between runs
  belongs on the timeline's end, not on the kernel's.**
- **Position-sensor readings.** Whether the arithmetic the policies steer on can see a given keyspace
  at all: how many page commits moved the cursor without moving the run's position metric, and how many
  scanned victims had a degenerate estimate — one that read zero, or one that discarded the emitted keys
  it was given. Nothing in swath-core changes to produce them, because whether a sensor works is a
  property of the keys, and a fixture that defeats it needs to be able to say so in numbers. They are
  read **through whichever sensor the run steers on** (below): the question a counter answers is whether
  *this* run's sensor could see *this* keyspace, so reading the legacy-window arithmetic under a candidate
  would report the disease while the cure was running.

### Dumping what the gates read

A counter says a gate fired *n* times; it cannot say which reading made it fire, and when a simulated
run and a real one disagree that is the only question worth asking. `-Dswath.sim.gate-dump=<path>`
turns on a per-decision dump: one TSV row per `OwnerSplitGovernor.decide` past the open-frontier
early-out, and — at `<path>.scans.tsv` — one per `ThiefPolicy.selectVictim` pass. The columns are the
engine's own `OwnerSplitGateInputs` and `VictimScan` payloads, which also feed the engine's
`owner_split_decision` and `victim_scan` trace events (metrics-internals.md §7). The schemas are not
identical: the simulator adds key bounds and omits production's `worker_count`, so comparison requires
normalization. No end-to-end same-fixture golden comparison currently validates row-for-row parity;
the dump provides the inputs needed to build and localize one. The decision file carries
`virtual_time_ns`, `node_id`,
`reason`, `est`, `pages_since_last_self_split`, `outstanding`, `far_ahead_fraction`, `density_ratio`,
`keys_emitted`, then the decided range's own `lo`, `cursor_to` and `hi` — the bounds, because sim node
ids and a replay run's node ids are different id spaces and a tail range is matched by *keys*. The scan
file carries `virtual_time_ns`, `seen`, `skipped_paced`, `skipped_unsplittable`, `skipped_no_span`,
`chosen_node_id`, `best_est`, `reason`, and the chosen victim's `chosen_lo`/`chosen_cursor`/`chosen_hi`.
A double the short-circuiting gate chain never computed is written as `NaN`, and an unseeded argmax as
`-Infinity`: a TSV carries what the run held, without JSON's ban on a non-finite number.

The dump is a **write-only observer** — a run dumping takes exactly the decisions it takes not
dumping, and with the property unset no row is formatted and no key is rendered. Its instants are the
run's own virtual clock, never the host's. It is **one run per dump**: both files are created
`CREATE_NEW` and every IO failure fails the run, because an artifact that silently truncates would
have its missing rows read as a finding — so point a multi-leg harness at a fresh path per invocation
rather than at one it has already written.

### Swapping the position sensor

Victim choice, pivot mass floors, the owner's self-split and the density feedback all steer on one
quantity: estimated remaining work on a range. The legacy `WINDOW` sensor computes a local density
times a remaining span in the range-bounds window and is degenerate on deep-nested keyspaces.
`SensingVariant` makes the quantity swappable here so alternatives remain reproducible:

| variant | reading |
|---|---|
| `CURRENT` | the legacy `WINDOW` control selected by the simulator's null estimator seam; it delegates to the engine's `StealMath`, but is not production's 0.2.0 default sensor |
| `RATE` | remaining work is what the range has already produced. No key-shape inference; the only byte comparison is the exact test for a cursor that has reached its bound |
| `CURSOR_ANCHORED` | the same density-times-span reading, in a window anchored at the cursor's own divergence from `lo`. Byte-identical to the legacy window exactly where the cursor leaves `lo` at the byte `hi` does, and not one byte wider — a cursor that diverges deeper still, but inside that window's own width, reads an order of magnitude lower |
| `RATE_CURSOR_ANCHORED` | the rate estimate, which the anchored geometry may adjust within a stated band |
| `RATE_ANCHORED_FLOOR_QUARTER` | the same, with the band's lower half cut short at a quarter — **the arm the corpus race promoted and production now defaults to**. Its composition lives in the engine (`io.varve.swath.engine.RateAnchoredEstimator`) and this arm delegates to it, so neither side can drift from the reading that was measured |
| `RATE_ANCHORED_LIFT_ONLY` | the same, with the band's lower half removed: geometry may **lift** a range's proven mass and never cut it. A factor below one asserts that less remains than has already come out, which is the inference the rate reading exists to refuse — and it is what refused a straggler's carve at the owner's remaining-work floor until the range had emitted 64 pages (`CarveAdmissionRaceProtocol`) |

`SimExecutor.run` takes one, defaulting to `CURRENT`; an unqualified run is therefore on the legacy
control sensor, with its scenario's independently selected toggles. The geometry-floor rungs that the
sweep rejected (`..._EIGHTH`, `..._HALF`, the symmetric `RATE_CURSOR_ANCHORED` and lift-only ends) are
kept so those races stay reproducible; they run through the promoted arm's engine object at their own
floor rather than through a second implementation of it. Victim selection, the owner-split gate chain
and the whole pivot cascade are **the engine's own classes**, consumed through the estimator seam they
already have (#68): an arm is a `RemainingWorkEstimator` handed to `OwnerSplitGovernor` and
`ThiefPolicy`, and no mirror of either remains in this module. Within those policy objects, variant
arms differ only in the sensor. `SensingVariantParityTest` drives the battery through that
chain under every arm (decisions complete and stable; the observed-mass floor's structural zero visible
under each), and pins `CURRENT` against an unsteered legacy-window chain. The route itself is
instrumented like every other algo path here: a
run counts `SENSING_ROUTE.owner_split_*` and `SENSING_ROUTE.thief_*` once each, off whether an estimator
was actually installed, so which sensor a table's legs were taken through is a reading of the run rather
than an inference from its `sensing` field. A run on a rate+anchored arm also emits the engine's own
per-reading classification rows for that sensor (`SENSING_OWNER.*` / `SENSING_STEAL.*`,
docs/internals/metrics-internals.md §5a) — the same rows a deployed run under
`rate_anchored_sensing=on` emits, because it is the same object reporting them.

**What the first race found** (`SensingRaceTest`, protocol pre-registered in `SensingRaceProtocol`;
four seeds, three keyspaces, two page regimes). On the deep-nested mass-concentrated bench at a 100-key
page all three candidates remove the serial tail — 0.33 down to 0.0003–0.0090 at every seed, ~28% less
virtual time, mean occupancy 5.5 → 7.6–7.9 of 8 workers, and *fewer* store calls. The mechanism is the
one the estimate gates directly: owner carves refused by the remaining-work floor fall by an order of
magnitude per page committed, and what replaces them is the demand gate declining to carve because
there is already enough work queued. **At the measured 1,000-key page every candidate is worse, at every
seed**, because an estimate with no sense of position places the owner's carve on a nearly-drained range,
the child comes back confetti-sized, and the feedback gate that watches for that shuts owner splitting
down for the rest of the run. The rate variant alone also damages the hash-fanned healthy shape, at one
of the four seeds — read on the same yardstick for all three candidates, the control's own reading at
the same seed — for the same reason. None of the three is shippable as it stands; what the race
establishes is that the sensor, and not the split policy above it, is what gates division on that
keyspace.

**The degeneracy counters are not the evidence they look like.** Both go to zero for `RATE` and
`RATE_CURSOR_ANCHORED`, but that is arithmetic and not a cure: an estimator whose estimate *is* the
emitted count cannot discard it, and the only score that can read zero is a cursor already at its
bound, which is a finished range and never a scanned candidate. Only `CURSOR_ANCHORED`'s readings there
are measured — and its zero-estimate share is **not** an improvement (0.015–0.050 against the control's
0.036–0.039, worse at one seed). What the counters do establish is that they follow whichever sensor is
installed.

### What a run costs to produce

Two different numbers, and confusing them is the classic simulator mistake. A run's **virtual
duration** is the modelled system's answer — its time to quiescence, per the timeline note above. Its
**wall time** is what this machine spent computing that answer, and the only thing it says about swath
is whether sweeping over many runs is affordable. (The two virtual durations below are ~0.9 s shorter
than the same runs reported before the duration was re-pointed off the kernel's last event; the
difference is that run's retired-park drain, and it was a near-constant, so nothing that compared two
runs was affected by it.)

Measured on one 8-core arm64 development box, the same keyspace shape at two sizes, 32 workers,
1000-key pages, no seed:

| | 500K keys | 2M keys |
|---|---|---|
| modelled store calls | 1,001 | 2,813 |
| events dispatched | 30,345 | 46,936 |
| of those, retired park timers | 13,195 (43%) | 18,056 (38%) |
| **events per modelled call** | **30.3** | **16.7** |
| wall time | 0.21 s (206 µs/call) | 0.13 s (48 µs/call) |
| virtual duration (to quiescence) | 2.7 s | 5.0 s |

**Events per call is not a constant, and the second size point is what shows it.** A large share of a
run's events are park timers — an idle worker waiting to be woken — and their number is driven by how
long the run lasts and how many workers are idle in it, not by how many calls it makes. So the figure
*falls* as a fixture grows and calls come to dominate: the small-fixture number is an upper bound, not
the rate. Extrapolating the larger point, a 150,000-call run is on the order of 2.5M events and single-
digit seconds of compute here, which is comfortably inside what sweeping requires; extrapolating the
smaller one would have overstated it by nearly twice, which is exactly why a single point was not left
to stand. Two further caveats: these read pages from an in-memory fixture, where a real large-fixture
backend serves a call in ~175 µs and would dominate the total; and the two runs above are in one JVM, so
the first pays warmup the second does not. The bench is `PolicyRunBudgetBenchTest` (`@Tag("perf")`,
opt-in).

### Why the kernel's own tests assert equalities

With a constant latency and the client-cost term explicitly zeroed, a run's wall time is pure
arithmetic, so the tests assert it exactly rather than within a tolerance: a range of `n` keys costs
a fixed number of calls; the total call count does not depend on the worker count at all; one worker
costs the sum of the latencies; a worker per range costs the largest range. Scaling is asserted
**monotonic and not proportional** — a range is claimed whole, and pacing intervals and client costs
are fixed durations, so more workers legitimately stop helping.

The "fixed number of calls" is two different closed forms, and the difference is the point:

* The **policy executor** (`PolicyInvariantsTest`) issues real `ListObjectsV2` requests through
  `ListObjectsV2Pager`, so a range of `n` keys costs `ceil(n / pageSize)` calls. S3 looks one key
  past the page it returns, so the page that consumes the last key comes back full **and not
  truncated** — a range whose size divides by the page size ends there, not on an extra empty call.
* `SequentialListingDriver` (`ExactModeInvariantsTest`) costs `floor(n / pageSize) + 1`, because it
  is a load generator that reads a **bounded range** straight off the store seam and stops on a short
  page. It is deliberately not a model of the listing protocol; it exists so the kernel's clock and
  ordering can be checked without a policy or a protocol in the way.

A test that pins the second form on the first path is pinning a simulator artefact, which is what
`SimListingViewProtocolTest` exists to prevent.

## Fixtures

A fixture is a **local path** — a swath Parquet capture file, or a directory of them — supplied
by the caller (config or CLI argument). Nothing in this module hardcodes, or is allowed to
hardcode, a remote object-store location; fetching a fixture to local disk is the caller's job,
outside this module. That rule is mechanical rather than conventional: a source scan fails the build
on an object-store URI in `src/main`, alongside the scans for ambient clocks and for reads of row
metadata a keys-only fixture does not have (`SimAmbientSourceGuardTest`).

### Running the real policies against a real listing

A generated keyspace is a hypothesis about what a bucket looks like. `RealListingRunTest`
(`@Tag("perf")`) runs the same fleet, seeds, page regimes and measured client cost over a **captured
listing** instead, and prints the sensing race's own table so the two read side by side — plus a run
cost table (wall time, events, store calls) that is how a corpus sweep gets sized, and a "where does
the tail live" leg that reports the longest common prefix of the keys committed after the last split.

Its legs run at the **live store's call-class latency profile** (`LIVE_S3_LATENCY`), not the synthetic
benches' — an answer quoted as "what the fleet would do on this bucket" has to be taken at the ratio
that bucket's store actually charges, and that ratio decides the steal race (above).

```shell
./gradlew :swath-sim:test -PonlyPerf -Dswath.sim.listing.fixture=/path/to/sorted-fixture
# a listing of tens of millions of keys wants more than the perf tier's 2 GB:
./gradlew :swath-sim:test -PonlyPerf -PsimTestHeap=6g -Dswath.sim.listing.fixture=...
# eight workers by default (comparable with the synthetic benches); model the fleet the
# listing's own capture ran at when the question is why THAT run behaved as it did:
./gradlew :swath-sim:test -PonlyPerf -Dswath.sim.listing.fixture=... -Dswath.sim.listing.workers=64
```

The path is the operator's, supplied per invocation: **the repo never names a fixture, a bucket or a
location**, and with the property unset the run skips itself. One store handle (resolved through
`AUTO`, with the arena budget at a third of the heap) serves every leg, because opening a
multi-million-key fixture costs more than the runs do. Nothing there asserts a magnitude — a
threshold invented against a real bucket's numbers would be a threshold fitted to them — but every
leg must complete and every leg must emit **the fixture's own key total**, which the resolved tier
reports (`SimStoreFactory.Result#keyCount`); only under `PARQUET`, which cannot supply one without a
scan, does it fall back to comparing the legs with each other, and it says which check it used. The
per-leg `heap_peak_mb` column is the heap high-water mark **during that leg** (pool peak usage, reset
between legs) — an upper bound on what the leg held, not its live set. It necessarily includes the one
shared store handle, which is held across every leg: on an `ARENA` resolution that is nearly all of
it and the column reads flat, and it is on `STREAMING` — whose decoded segments are off-heap and so
absent from this figure — that the number is mostly the run's own.

**Which tier `AUTO` lands on depends on the heap this invocation was given** — including the heap the
line above raises. The arena budget is a third of `-Xmx`, so `-PsimTestHeap=6g` gives it ~2 GB and a
listing whose encoded keys fit that is served by `ARENA`, **not** `STREAMING`. That matters beyond
speed: `ARENA` and `PARQUET` are the order-masked tiers (they serve the fixture in an order they
imposed), so a run on either proves nothing about whether the capture is in order. The harness prints
a `WARNING` line next to the resolved backend when it lands on one. To exercise the guarded tier,
give the run a smaller heap so the arena declines by budget.

### Sweeping a whole corpus of captured listings

`CorpusSweepRunTest` (`@Tag("perf")`, mechanics in `CorpusSweep`) is the same four arms over a
*directory* of staged captures, in one JVM. One process, because the fixed costs are what decide
whether a corpus sweep is affordable at all: a JVM start plus a Gradle invocation dominates a small
fixture, and each fixture's own open dominates every run over it.

```shell
./gradlew :swath-sim:test -PonlyPerf -PsimTestHeap=6g \
  -Dswath.sim.listing.corpus=/path/to/staged/captures \
  -Dswath.sim.listing.results=/path/to/sweep.tsv
```

A fixture is a subdirectory holding a `data/` directory of capture Parquet; its sibling `summary.json`
is the capture's own run record. Both paths are the operator's, supplied per invocation — **the repo
names no corpus, bucket or location**, and with the first property unset the sweep skips itself. The
results path is required, not optional: a corpus produces thousands of numbers and its per-fixture
tables scroll past on the way, so a run whose output lived only in a console buffer would have to be
repeated to be read. Rows are appended and flushed per fixture, so the file is complete for every
fixture finished so far at any moment — an hour-long sweep that dies at the ninetieth fixture still
yields the eighty-nine before it. **The results file must not already exist**, and a sweep pointed at
one fails before it opens a fixture: a sweep's rows are cited raw, and a re-run that truncated the
previous run's file would destroy the evidence for a finding already published.

**Which directories the sweep covered is itself an output.** A corpus is staged by hand, so a fixture
silently dropped reads exactly like a fixture with nothing to say. Two kinds are passed over, and each
is named with its reason in the result and on the console (`phase=skipped`): a directory holding no
`data/*.parquet`, and a capture whose footers declare more rows than
`-Dswath.sim.listing.corpus-max-keys` allows (default 20M). That ceiling is there because a staged
corpus outlives the tier staged into it: a leftover fixture an order of magnitude larger than the rest
would be swept at whatever fleet it could name, for hours, and its rows would be comparable with
nothing. Reading it costs one Parquet footer per part and no decode at all.

Three choices decide what its numbers mean, and all three are visible in the output:

- **`STREAMING`, forced** rather than `AUTO`. The arena declines by budget on a corpus-scale fixture
  anyway, and `AUTO` pays a DuckDB materialization to find that out once per fixture; forcing also
  fixes the tier across the corpus, and it is the tier whose decode is order-guarded. Note what this
  moves rather than removes: a forced-streaming open reads footers only, so it is near-instant, and
  the decode is paid lazily by the runs — which then amortizes across the legs sharing the handle.
- **Each fixture at its own capture's `max_parallel_listings`.** How hard a keyspace is to divide is a
  statement about a fleet size, not about the bucket alone, so a corpus comparison at one arbitrary
  fleet size compares the wrong thing. A capture with no usable summary is swept at the race's eight
  and its rows say `fleet_source=fallback`, because those rows are then not comparable with the rest.
- **Two screening seeds, four where it matters.** Four seeds is the verdict standard; the sweep
  screens at two and escalates to all four on any fixture whose screen *diverged* — a collapsed leg
  (serial fraction above ½) at any arm or seed, steals mostly finding no victim at any arm and either
  seed, or a duration gap either way between the `CURRENT` control and **any** candidate arm, read both
  over the two screening seeds' mean **and at each seed on its own**. Both halves of that last one
  matter: screening against a single candidate leaves a divergence only the other candidate shows
  unexamined, and a mean over two seeds is the reading least able to see a bimodal fixture — one leg
  55% down and one leg level average to a 23% gap that clears no threshold, so the fixture reads as
  uninteresting exactly because it is unstable. The `escalated` column says which fixtures got the
  full four, so **no two-seed row can be quoted as a verdict**.

A capture the streaming tier **refuses** is recorded and the sweep continues to the next fixture.
There are two typed refusals and the exclusion carries what each of them knows: an eligibility failure
at open — disorder across row groups, a missing stamp, an incomplete multi-file set — carries the
eligibility reason and the file set, and a disorder *inside* a row group, which fails the first run
that faults that group in, carries its file, row group and row. Neither carries the operator's
directory tree: the exclusion's `where` is the failure's redacted report, and the full path stays in
the console log. That exclusion list is an output, not a failure: it is corpus QA data, and one
unreadable capture must not cost the other hundred.

**Nothing else is corpus data.** Any other runtime failure — a bug in the sweep, a malformed
`swath.sim.*` property — is recorded as a *problem* against that fixture, and problems fail the run.
So does a leg that produced a number nobody may use: one that stalled, or one that did not emit its
fixture's own key total. Both are collected across the whole sweep and raised at the end, so a single
bad fixture costs no measurements either. (A fixture that is then excluded mid-run drops the problems
its earlier legs recorded: those describe the exclusion, not a defect the run must fail on.)

**Reading the table: check the fleet before comparing two rows.** The sweep closes by printing the
fleets it actually ran at (`phase=fleet`) and naming every fixture off the corpus's modal one
(`phase=fleet_mismatch`), because a `workers` column that varies is a comparability limit and not a
measurement. A row at `fleet_source=fallback`, or at a capture fleet the rest of the corpus did not
run at, is a reading about that bucket at that concurrency — quotable on its own, not against its
neighbours in the same table.

### Keyspace shapes, generated

A policy's behaviour is decided almost entirely by *shape* — where the directories are, whether mass
sits in a few of them or spreads evenly, whether a split can find a populated pivot — so the tests also
generate small keyspaces shaped like real buckets: a deep date-partitioned observation archive, a
hash-fanned content-addressed corpus, a tree with one object per directory, a single dense flat leaf,
and a deep-nested shared prefix. Each runs in milliseconds and each provokes different policy paths,
which a fixture of uniformly named keys does not.

**The deep-nested shared prefix is the one aimed at a specific mechanism.** It is taxonomy-shaped —
`species/<Genus_epithet>/<accession>/<dataset>/<stage>/<file>` — and its numbers are the point: sibling
species directories diverge ten bytes in, while everything inside either of them varies only from byte
39 on. Position is measured by `StealMath.fracIn` over the twelve bytes *after* the longest common
prefix of a range's own bounds, so on a range spanning sibling subtrees the measured window is bytes
10–22 and a cursor draining a subtree changes nothing inside it. Ninety-odd per cent of that fixture's
page commits therefore advance the cursor without moving the fraction at all, the consumed span reads
zero, and `estRemaining` falls back to a raw width with the range's emitted keys discarded — which is
what every layer downstream (victim choice, pivot mass floors, the owner's self-split, the density
feedback) is then steering on.

**A high invisible-advance share on its own is not diagnostic of any of that**, and the measurements
say so: the flat hex-named control reproduces it too (95.0% at scale, against this fixture's 94.1%).
Twelve bytes is simply narrower than the ground a hundred-page range covers, whatever its keys are
named, so once ranges are that wide the counter is reporting range width rather than taxonomy depth.
What separates the two shapes is the **consumed span** — whether the window can see the cursor move
*away from its own `lo`*, which is what decides if the estimate keeps the range's emitted keys or
throws them away. That is where the fixtures diverge by a factor of two to eleven
(`est_ignores_keys`, `est_zero`), and it is the reading a cure has to move.

How much each subtree holds is a separate parameter, because the
geometry decides what can be *measured* and the mass distribution decides whether being unable to
measure it costs anything: `UNIFORM` isolates the first, and the heavy-tailed law a real archive
follows is what makes the second visible.

**Where that mass sits is a third property, and it is the one that reaches the run.** Real deep-nested
buckets do not thin out as the tree descends: a third of a bucket's objects sat in one subtree and 90%
in five, and the chain leading down to them had a fan-out of one or two the whole way, ending in a
single directory holding some 1.8 million objects. `LEAF_CONCENTRATED` reproduces that last step —
the same species ranks as the heavy-tailed law, with each accession's whole file count in *one* of its
data directories and a token file in the others. Two things follow that a keyspace whose leaves hold
thousands cannot produce. Structure discovery stops rescuing the run: the fan-out it finds is real and
carries no mass, so a cut at it sheds a directory holding one file. And once a range lies inside the
heavy directory there is no structure below it at all, so every pivot has to come from arithmetic over
the frozen position window. Both show up as measurements: at a million keys not one thief child is
placed by a structure probe (219 probes, zero wins), against three on the same mass spread across the
accession, and the estimate discards its victim's emitted keys for 72% of the victims it is computed
over.

Its readings are pinned in two places, both as characterizations of *legacy-window control* behaviour that a change
to how remaining work is measured is expected to break. `PositionSensorCharacterizationTest` runs it
against the hash-fanned corpus at the same size, and against *itself under a uniform mass* — which is
what separates the two claims: the same geometry is just as blind (94% of commits invisible) and costs
nothing at all, because equal subtrees leave the seed's own division balanced and the fleet is never
left having to divide anything. `PositionSensorAtScaleTest` (`@Tag("perf")`) runs the heavy-tailed pair
ten times further up, where the difference reaches the run: 8.4% of it after the last split at a mean of
1.6 ranges in flight, against 0.8%. Note what that second test does *not* show — the deep-nested run
publishes 205 split children there against the control's 64. The division is not missing; it is late,
driven by thieves that spend 691 attempts to place 118 of them, and refused four thousand times over by
the owner's own estimate.

`MassConcentrationAtScaleTest` (`@Tag("perf")`) is the same geometry at a million keys with the mass
where a real archive's is, and it is the fixture that reaches the régime worth curing: a third of the
post-seed run with at most one range in flight, a third of it after the last split anything managed to
make, and four split proposals in five lost to the victim's own cursor. That is within a factor of two
of the 60% of a run a real deep-nested bucket spends serial, against the 3.5% the same shape produces
with its mass spread over 20,000-key leaves. Its control differs in one property only — the same eight
subtrees holding the same number of keys each (to within the rank law's integer division), spread
across an accession instead of concentrated in one directory — which is what makes the pair a
comparison rather than two experiments.

**That tail number is quoted at a 100-key page, and the page size is doing work.** What a fleet has to
serialise is round trips, not keys, so the scaling variable is *pages per range*. This fixture's heavy
directory holds 400,000 keys: 4,000 pages at 100 keys a page, 400 at a real 1,000-key page, against
roughly 1,800 pages for the 1.8-million-object directory measured on a real bucket. The 100-key run is
therefore page-faithful and mass-short — its biggest range is 4,000 of the run's 11,019 pages, where
the real one was ~1,800 of ~8,800 — and the same keys at the measured 1,000-key page are mass-faithful
and page-short, with a biggest range of 400 pages out of 1,179 and a serial fraction of **0.001**
rather than 0.332. Both are asserted, in the same test class, for the same reason the spread control
is: the honest statement is that reproducing this tail needs a bucket about ten times this fixture's
size, and the small page buys that at a tenth of the memory while buying nothing else. The 36.8% /
84.0% concentration shares it deals (against a measured 32.6% / 90.7%) are pinned in
`KeyspaceFixturesTest`.

**And that 0.001 is quoted at a probe:page ratio too.** The same fixture, the same 1,000-key page and
the same fleet, run at the live store's ratio instead of the bench's, does not have its heavy leaf
absorbed at all: the leaf drains as one range that holds the fleet for **49%** of the run, 668 steal
attempts against it produce a single child that carries no keys, and the run publishes 18 splits where
the bench ratio publishes 39–61. `ProbeToPageRatioTailTest` (`@Tag("perf")`) pins that, and it is the
leg to read before quoting any tail or loss number from this module as what a real fleet would do:
the page size decides how many round trips a range is, and the probe:page ratio decides whether anyone
can take it away.

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
