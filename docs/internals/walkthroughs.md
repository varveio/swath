# How swath handles hard buckets — five walkthroughs

These are worked examples: concrete, step-by-step traces you can follow with the
engine source open. Each takes one bucket *shape*, shows what a naive lister does
and why it is slow or expensive, then walks the actual engine event sequence —
seed, pages, steals, splits — naming the real classes and methods, the
observability counters that fire at each step, and the invariants that are
load-bearing where.

This document assumes the mechanism reference in
[`algorithms.md`](algorithms.md) and the "why parallel listing is hard" primer in
[`overview.md`](overview.md). The counters named
below (`PIVOT.*`, `OWNER_SPLIT.*`, `RETRY.*`, `STRUCTURE.*`, `CHILD_MASS`, …) are
defined in [`metrics-internals.md`](metrics-internals.md) §5.
The invariants (I1–I12) are in [`contracts.md`](contracts.md)
§0; the two design laws (L1 placement, L2 triggers) they express are in
[`overview.md`](overview.md) under "Design laws".

A one-line orientation to the vocabulary: a worker owns a half-open key range
written `(A, B]`, paginates it with S3's `start_after=A` (keys strictly greater
than `A`), and stops at the first key past `B`. An **idle worker steals** by
splitting a busy worker's remaining range at a **pivot** key `m`, taking the far
half. A **cursor** is the last key a worker emitted — its resume point.

---

## 1. Deep-nested tree (Hive/Spark-partitioned)

**Shape.** A directory tree with a handful of top-level partitions and a small
number of objects per leaf — the classic `yyyy/mm/dd/…` layout produced by Hive,
Spark, or a data-lake writer.

```
s3://bucket/
  yyyy=2023/mm=01/dd=01/part-0000.parquet
  yyyy=2023/mm=01/dd=02/part-0000.parquet
  ...
  yyyy=2024/mm=12/dd=31/part-0000.parquet
       (thousands of leaf directories, ~1 object each)
```

**What a naive lister does, and why it's slow or expensive.** A recursive
`delimiter=/` walker descends the tree directory by directory: one `ListObjectsV2`
call to enumerate the years, one per year to get the months, one per month for the
days, one per day for the files. On a one-object-per-leaf tree that is roughly one
API call *per directory* — order N calls to list N objects, at real cost (S3 bills
per 1000 LIST requests). A plain `start_after` paginator avoids the call explosion
but lists the whole bucket through a single cursor: strictly serial, one worker,
no parallelism no matter how many you configure.

**What swath does.**

1. `SeedStep.shallow` (algorithms.md §8) issues **one** `delimiter=/`
   `ListObjectsV2` at the listing prefix. S3 returns the top-level common prefixes
   `p1 < p2 < … < pk` (the "directories") for that single call. The seed step uses
   only the `CommonPrefixes` as cut-points and **discards the `Contents`** — it is
   a structure probe, not an emitting pass.
2. It tiles the keyspace into seed ranges `(⊥, p1], (p1, p2], …, (pk, null]` as
   `NodeSpec`s and installs them with `CheckpointStore.insertNodes`, an
   **all-or-nothing** insert. **I2 is load-bearing here:** the seed set is a valid
   partition of `(⊥, ⊤]` from its first durable moment, or it does not exist at
   all — there is never a window where the persisted ranges leave a gap or
   overlap. The cut-point count is capped at `min(1000, 4×workerCount)`
   (`subsampleEvenly`), so the seed cost is ~1 RPC regardless of how many
   directories exist.
3. `WorkStealingScan.produce` seeds the ready queue from those nodes, sets
   `outstanding` to the seed count, and starts `workerCount` workers. Every range
   except the last now has a finite upper bound, so all workers have bounded work
   to claim at *t = 0* — the bucket parallelizes to full width from the first
   round of pages, not after a slow ramp.

**Counters that fire.** `SeedStep.recordSeedSummary` emits the `seed_shallow`
shape (probe count, cut-points, seed ranges) and `recordSeed("delimiter_seeded")`.
Post-run you read `seed_ranges` climbing from 1 to k and `peak_in_flight` reaching
`workerCount`.

**Why this is the API-call-efficiency win.** The delimiter-recursion trap this
shallow seed avoids matters because swath issues **≈ `ceil(N/1000)` LIST calls, not
≈ N**, on exactly this one-object-per-leaf shape. The bounded seed is what earns
that: it probes structure once and hands the leaves to range workers that flat-scan
them, and it never recurses per directory. (History: before this shallow seed step existed, `ListCommand` seeded a single root range and
deep-nested buckets ran near-serial; wiring the seed took a deep-nested bucket from a
low single-digit `in_flight` to the full configured worker width.)

**The `--tune seed.mode=none` contrast.** With `--tune seed.mode=shallow` disabled the engine starts
from a single `(⊥, null]` root range and must *discover* the structure by
stealing and open-frontier extrapolation alone — a near-serial ramp while the
first workers slowly learn where the mass is. It is still correct (stealing
converges), just slow to fan out. This is why `shallow` is the default.

*Mechanism references:* algorithms.md §8 (seeding), §2 (the worker loop).

---

## 2. Dense flat directory tail (a single hot prefix)

**Shape.** After the seed, most ranges finish quickly, but one covers a single
flat directory holding a large fraction of the bucket — a `uuid`-per-object dump
with no sub-structure. This is the shape that produces swath's hardest residual.

```
s3://bucket/data/
  data/0a1f...e9/obj      ← ~110k objects, one flat prefix,
  data/0a2c...77/obj        no delimiters below `data/`,
  data/0b03...12/obj        names spread thinly across the byte space
  ...  (the "mega-day")
```

**What a naive lister does, and why it's slow.** There is nothing to recurse into:
`delimiter=/` under `data/` returns no sub-directories. So every lister — naive or
not — is reduced to `start_after` pagination: ~110 sequential network round-trips,
one per ~1000-key page. No extra workers help *by themselves*, because to hand a
second worker a valid starting point deep inside the directory you must already
know a key that far in — which costs the very listing you were trying to split.

**What swath does — the steal sequence on a bounded range.** An idle worker
becomes a `Thief` and calls `Thief.steal(pool)` against this range's owner. The
sequence, in order, is a ladder of increasingly mass-aware placement attempts
(this is design law L1 in action — climb the evidence ladder, don't stay blind):

1. **Victim selection under the progress gate.** `steal` picks the victim with the
   largest `StealMath.estRemaining` **among `eligibleVictims()` only** — a worker
   is eligible as a victim only once it has committed a non-empty page since it was
   created or last stolen from (`WorkerState.stealEligible` / the
   `emittedSinceSteal` flag). This paces re-splitting to **≤1 split per emitted
   page** and closes the latency livelock; it is a balance/liveness
   heuristic only — I2–I4 hold on *any* victim.
2. **Far-ahead placement.** The thief snapshots `(c, H)` and computes a pivot
   `m = StealMath.interpolate(c, H, f)` with `f` skewed past 0.5 from the victim's
   own drain density (`WorkerState.densityFraction`, `PIVOT.far_ahead`). It probes
   once — `probeNonEmpty(m, H)`, a single `maxKeys=1` `ListObjectsV2`.
3. **Step-back on an empty probe.** If the far pivot lands beyond the mass the
   probe is empty (`PIVOT.step_back`); the thief walks the pivot back toward the
   cursor by bisecting `(c, m]`, bounded by a **log-scaled budget**
   `B = ceil(log2(bandWidthBytes)) + 6` (`emptyUpperBisectionBudget`). Each hop
   records an empty-upper bisection; exhausting the budget bails with
   `RETRY.bisect_budget_exhausted` rather than committing a doomed near-cursor
   pivot.
4. **Demand-driven structure probe.** When byte-midpoint bisection can't find
   populated space, `structurePivot` issues one bounded `delimiter=/` probe scoped
   to `(c, H]` and splits at a discovered `CommonPrefix` — the median when the probe
   saw the whole directory (`PIVOT.structure_probe`), or the furthest boundary it proved
   when the page truncated (`PIVOT.structure_capped`) — recursive structure discovery at any
   depth, not just at seed time.
5. **Adaptive coarse→fine back-out.** `adaptiveStructurePivot` picks the probe
   prefix from the cursor∧`H` divergence direction, coarse first, so the boundary
   lands far ahead of the fast drainer (`PIVOT.adaptive_structure`, or
   `PIVOT.adaptive_structure_capped` under the same truncated-page regime as step 4).
6. **Flat-leaf density pivot.** When the leaf genuinely has no sub-directories,
   `flatLeafDensityPivot` bisects the filename space via `StealMath.extrapolate`
   against a real deep key (one floor probe, or zero using the child's own `lo`)
   — `PIVOT.flat_leaf`. This is the mass-aware pivot that collapsed
   `encode-public`'s serial tail — a wall-clock win, not a cost one.
7. **Zero-fan-out suppression.** After `K=8` consecutive `delimiter=/` probes
   return no structure, the thief stops issuing them
   (`STRUCTURE.suppressed_zero_fanout`, `consecutiveZeroFanoutProbes`), with a
   1-in-64 recovery probe — restoring the "never unbounded LISTs per
   attempt" budget on a genuinely structureless directory.

**The hand-off and the CAS.** Any pivot that survives is committed under
`victim.lock`: re-check `victim.hi == H` (`RETRY.bound_moved` if a second thief
already narrowed it) and `victim.cursor < m` (`RETRY.cursor_passed_pivot`), then
`WorkerState.narrowHi(m)` and `CheckpointStore.splitNode`. **I4 is load-bearing
across this entire ladder:** the split is one atomic, CAS-guarded transaction
(`cursor < m AND range_end IS oldHi AND status<>COMPLETED`), so *every* placement
heuristic above is free to be wrong — a bad pivot can only cost a wasted probe,
never a gap or a double-emit. Winning splits record `CHILD_CREATED.split_committed`
plus the `PIVOT.*` mechanism tag and `CHILD_MASS` (the emitted mass of the range,
a small:large histogram that reveals zero-transfer splits).

**The cursor-passes-pivot race, and the owner-side self-split that kills it.** The
whole ladder above is *reactive*: a thief probes near the drainer's moving cursor.
On a fast dense directory the drainer advances a full page (~1000 keys) during the
probe window — a stride that rivals the entire shrinking remaining span — so the
cursor passes `m` before the CAS commits, and the split aborts with
`RETRY.cursor_passed_pivot`. The structural fix is to move the *trigger* from the
thief to the drainer itself: `WorkStealingScan.maybeOwnerSelfSplit` runs **at
page-commit, under `ws.lock`, with the cursor just advanced** (see walkthrough 4).
Because the owner holds its own lock and picks `m > cursorTo`, the CAS guard holds
*by construction* — there is no probe window for the cursor to cross. This kills
the race (`RETRY.cursor_passed_pivot` all but disappears) — though as a wall-clock
lever on a single dense directory it is a cost win, not a speed win.

**Where serial residue is genuinely irreducible.** Inside *one* dense directory
the only primitive S3 offers is `start_after` pagination — one page per round-trip,
strictly sequential — and handing a worker a valid deep starting point costs
exactly the listing you were trying to parallelize. So a single dense directory
has a serial LIST floor no partitioner can beat. This is a **single-directory**
bound on that directory's tail, not a whole-bucket property: a bucket with many
such directories still parallelizes fine across them; only the last, densest one
serializes at the very end of the run.

*Mechanism references:* algorithms.md §3 (stealing), §3.1 (pivots), §3.3
(dense-tail placement mechanisms), §6 (why skew is handled).

---

## 3. Skewed mass (a thin low sliver of the keyspace)

**Shape.** A bucket whose keys are heavily concentrated — high mass-skew (Gini
≥ 0.7) — with deep common prefixes and no directory fan-out. A time-partitioned
archive whose station identifier appears late in the key is a canonical example.

```
s3://bucket/
  2024/03/01/KABC/...   ← nearly all keys share a long low prefix,
  2024/03/01/KABD/...     diverging only at depth ≥ 15,
  2024/03/01/KABE/...     delimiter=/ fan-out ≈ 0 (no usable "directories")
  ...                     real mass sits in a thin low sliver of byte space
```

**Why uniform code-point pivots probe vacuum.** `byteMidpoint(c, H)` computes the
arithmetic midpoint of the *byte values* between `c` and `H`. That is the median
*key* only if keys are spread uniformly across the byte range. In plain words, the
error is a missing change-of-variables: the map from "position in the byte-value
space" to "position in the key population" is wildly non-linear on a skewed bucket,
and a uniform-prior pivot ignores that stretch factor entirely — so when the
observed `mass_skew_gini` is high, the value-midpoint can land far above where
the keys actually are and the 1-key probe comes back empty. Every uniform-prior estimator
(`interpolate` at any fixed fraction, the geometric bisection, `estRemaining` over
a mostly-empty code-point span) makes the same mistake; the visible exhaust is a
probe storm. This is exactly the failure design law **L1** names.

**What the counters look like.** On a thin-prefix bucket the fingerprint is
unmistakable: `PIVOT.step_back` fires on most steal attempts (the far-ahead pivot
probed empty), `empty_upper_bisections` runs to several hops per attempt walking
back toward the cursor, and even the splits that commit move almost nothing —
`CHILD_MASS` lands overwhelmingly in the *small* bucket. Average page-concurrency
sits far below the configured peak: the engine is busy, but busy probing vacuum.

**The density-reflected pivot — shipped, default on.** The engine
already owns a skew-immune estimator on the open frontier: `ByteMidpoint.forwardReflect`
(via `StealMath.extrapolate`) reflects the *consumed* span forward, placing the
pivot where roughly as many keys lie ahead as were just drained, under the
*observed* density. `Thief.steal` applies that same estimator to a **bounded** range,
in exactly the state the counters above fingerprint: the upper-half probe came back
empty, the far-ahead step-back re-probed empty too, and the `delimiter=/` structure
probe found no boundary. Rather than let the blind bisection walk back toward the
cursor one halving at a time, the thief computes `StealMath.extrapolate(lo, c, H)` —
pure math, no extra I/O — and spends **one** probe on it:

- **hit** — the reflected pivot landed inside the mass, so the split commits there
  (`PIVOT.reflect_hit`, and `PIVOT.reflect` as the winning mechanism at the hand-off)
  and the bisection never runs;
- **miss** — the reflected pivot still becomes the bisection's new upper end
  (`PIVOT.reflect_empty`), so the existing budgeted walk-back starts from the much
  shorter interval `(c, m_r]` instead of `(c, m)`.

Cost is one extra constant probe per empty-upper attempt, spent *on top of* the
`1 + emptyUpperBisectionBudget` ceiling the bisection already carries — the walk-back
budget itself is unchanged, only its starting point moves. The mechanism
is **default on**; `--engine-toggle reflect=off` is the kill-switch and restores the
plain uniform-midpoint bisection byte-for-byte, which is how the counters above are
read as an ablation. It is placement, never correctness — every pivot is CAS-guarded
(I4), so a reflected pivot can only change balance. Engagement is readable straight
off `PIVOT.reflect_hit` / `PIVOT.reflect_empty` — how often the reflected pivot landed
in the mass versus merely shortened the bisection interval. No measured before/after
for this mechanism is on record yet; algorithms.md §3.3 is the register where a
placement mechanism's measured status lands, and it carries no reflect entry.

*Mechanism references:* algorithms.md §3.1 (the pivot math), §3.2 (victim
selection / probe caps), §3.3 (dense-tail placement mechanisms).

---

## 4. Saturated wide bucket (owner-split with a demand gate)

**Shape.** A large bucket whose keys are well spread across many prefixes —
`pmc`-like open-data archives. Stealing fans it out easily; the worklist stays
full, and every worker always has claimable work.

```
s3://bucket/
  oa_package/00/00/PMC.../   ← thousands of well-distributed prefixes,
  oa_package/00/01/PMC.../     uniform-ish mass, always more PENDING
  oa_package/ff/fe/PMC.../     ranges than workers to run them
  ...
```

**The mechanism: owner-split at page-commit.** `WorkStealingScan.runClaim`, on each
non-empty page, holds `ws.lock`, advances the cursor to `cursorTo`, enqueues the
page commit, and then calls `WorkStealingScan.maybeOwnerSelfSplit`. The draining
worker carves its *own* far-ahead tail — pivot
`m = StealMath.interpolate(cursorTo, H, f, alphabetDigest())` at density fraction
`f` — and hands the child `(m, H]` to the ready queue via the **unchanged** split
transaction (`narrowHi` + `splitNode` + `enqueueChild`). Because the owner holds
its own lock and picks `m > cursorTo`, the I4 CAS guard holds by construction (no
thief can interleave); `WorkerState.markStolen()` then makes the worker briefly
thief-ineligible so it is not *also* carved this page.

**The demand gate — design law L2 made concrete.** On a *saturated* bucket the
worklist already holds enough live nodes to keep every worker busy, so an owner
self-split buys **zero** extra parallelism and only over-fetches its bounded
child's terminal page (fetched full, then trimmed per key). So `maybeOwnerSelfSplit`
suppresses the carve once live work reaches the worker count:

```
if (workerCount > 1 && outstanding.get() >= (long) workerCount) {
    metrics.recordStealReason("OWNER_SPLIT", "demand_gated");
    return;
}
```

This is the L2 pattern exactly: the split *mechanism* is global and shape-blind,
but its *trigger* reads a genuinely global **demand** quantity — all live nodes
(`outstanding`, queued plus active) versus the fixed configured worker count
`Tmax` — not a failure or pacing signal. During ramp (`outstanding < Tmax`) the
gate stays open. The adaptive concurrency gauge's effective `T` is diagnostic
here and never changes the threshold; load shedding therefore cannot close this
gate merely by shrinking `T`. A `2·Tmax` threshold was tried first and measured a production NO-OP
(idle thieves drain children as fast as the owner creates them, so `outstanding`
plateaus at ~Tmax and never reaches 2Tmax); `Tmax` is the threshold that engages. The gate
is skipped when `workerCount == 1` (no thief exists, so "buys zero parallelism" is
moot).

**The 2-page confetti floor.** Even below the gate, an owner-split must never
fission into a one-page "confetti" child. The code floors the child's tail above
two pages before carving:

```
double f = ws.densityFraction();
if ((1.0 - f) * est <= 2.0 * (double) maxKeys) {
    return;   // child tail below two pages — not worth a proactive carve
}
```

**What over-fetch looked like, before and after.** The engagement counters make
the win legible: `OWNER_SPLIT.self_published` (carves that fired),
`OWNER_SPLIT.demand_gated` (carves the gate suppressed), and the derived
`overfetch_ratio` (API calls per `ceil(N/1000)` objects). Adding the gate on a
dense multi-prefix regression fixture drove `overfetch_ratio` down toward its 1.0 floor at unchanged
peak in-flight, and keys/s up with it. Post-hoc, `demand_gated ≫ 0` with a low
`overfetch_ratio` is the gate's win fingerprint; `demand_gated ≈ 0` means the run
never saturated to T live nodes and the gate was a no-op.

**Invariants at work.** **I1** (commit-before-emit): `runClaim` awaits the page
commit's durability *outside* `ws.lock` before pushing the batch downstream. **I4**
(atomic CAS split): the owner-split reuses the identical split transaction as a
thief, so no-gap/no-overlap holds whether the split was owner- or thief-initiated.

*Mechanism references:* algorithms.md §3.3 (owner-side split + demand gate), §4.1–4.3
(commit / split protocol); metrics §5 (over-fetch caps).

---

## 5. Crash mid-split, and resume

**Shape.** Not a bucket shape — a fault. A `SIGKILL` (or a crash, or a hard
power-loss) lands in the middle of a managed directory-dataset scan, and the next
`swath resume` must pick the listing up from it. Two guarantees are at stake here, and they are **not** the same
guarantee. **Split/resume integrity** — the range set still tiles the keyspace, no
gap and no overlap, every key owned by exactly one node, and each node resumes only
within its own range — holds unconditionally. **Output delivery** is per sink
(contracts.md §5): non-resumable stdout / FILE-kind text is **at-most-once**,
finalized Parquet is **exactly-once**. Everything below states which of the two it is
claiming.

**The source of truth is the worklist.** The SQLite `listing_node`
rows *are* the checkpoint — there is no separate journal to reconcile. All DB
writes funnel through one checkpoint-writer thread in enqueue order. A page
commit is enqueued asynchronously; while still holding the node lock, the owner
may submit a synchronous split, and only after releasing the lock does it await
the page-commit future before emission. Two orderings carry both guarantees:

- **I1 — commit before emit.** A page's checkpoint commits (cursor advanced,
  durably) *before* its entries are pushed downstream. So anything a downstream
  sink saw is a subset of what the checkpoint already recorded.
- **I4 — the split is one atomic CAS-guarded transaction.**
  `SqliteCheckpointStore.doSplitNode` runs **both** statements on one connection in
  one transaction:

  ```sql
  -- (1) narrow the victim, CAS-guarded
  UPDATE listing_node SET range_end = :m, generation = generation + 1
   WHERE id = :victim AND (cursor IS NULL OR cursor < :m)
     AND range_end IS :oldHi AND status <> 'COMPLETED';
  -- (2) insert the child, only if (1) updated a row
  INSERT INTO listing_node (... range_start, range_end, cursor, status ...)
       VALUES (:m, :oldHi, :m, 'PENDING' ...);
  ```

  A crash leaves **either both rows or neither** — never a half-split. So the range
  set always tiles the keyspace (I2 survives the crash).

**A concrete timeline — SIGKILL between `narrowHi` and `splitNode`.** A worker owns
`(lo, H]`; the checkpoint currently has listing cursor `c0`. It finishes a page
whose last in-range source key is `c` and considers an owner split at `m`:

1. Under `WorkerState.lock`, it advances the **in-memory** cursor to `c` and
   enqueues `commitPageAsync(... cursor=c ...)`. The future is not awaited yet.
2. Still under that lock, `WorkerState.narrowHi(m)` sets only the in-memory,
   volatile `hi` to `m`.
3. **`SIGKILL` lands here**, before `store.splitNode(...)` is submitted.

The in-memory narrow evaporates and no child exists. The asynchronous page commit
may already have landed, however: the durable row still owns `(lo, H]` and records
either `cursor=c0` (commit absent) or `cursor=c` (commit present). If the crash had
instead landed after the split request reached the writer, I4 makes that split's
victim update and child insert durable together or absent together.

The shipped CLI behavior now diverges by destination:

- **Stdout / FILE kind:** these are one-shot, non-resumable runs. Their checkpoint
  store is ephemeral (`auto` on stdout, mandatory `none` for FILE kind), so
  `swath resume` cannot reload `q ∈ {c0, c}`. Commit-before-emit still matters:
  if `q=c` committed before the process stopped but emission did not happen, the
  text output can omit that page — its promised **at-most-once** behavior.
- **Managed directory-dataset Parquet:** `swath resume <dir>` clears the lease,
  bumps the generation, reopens the node, resets its listing cursor to
  `COALESCE(durable_cursor, range_start)`, and discards every unfinalized part.
  `durable_cursor` advances only when a part is finalized, so it can lag both
  `c0` and `c`; resume deliberately re-lists that bounded nondurable tail and
  produces **exactly-once durable dataset output**.

In both subcases the unsplit victim still owns the whole `(lo, H]` range, so no key
falls out of the partition and no second node overlaps it. What may repeat is the
LIST work for a nondurable tail (necessarily so for Parquet), not a finalized
Parquet row. This separates unconditional split/resume integrity from the
sink-specific output guarantee (algorithms.md §4.5–§4.6, I5–I6).

**The other cut of the timeline — SIGKILL *after* `splitNode` commits.** Now both
rows are durable (one transaction, atomic): the victim `(lo, m]` at cursor `c`, and
the child `(m, H]` `PENDING` at cursor `m`. A managed Parquet resume loads both and
re-runs stealing from that seed set, potentially resetting the victim farther back
to its `durable_cursor` and discarding the corresponding unfinalized part. The
tiling `(lo, m] ∪ (m, H] = (lo, H]` is intact, and any re-list
stays within the node that owns that key range. Both guarantees hold in this cut
for the same two reasons: I2 tiling survived the crash, so integrity is untouched,
and the two-cursor recovery rule gives each sink exactly its contract — a
committed-but-unemitted page can be absent from stdout, while Parquet re-lists its
nondurable tail into a fresh part without duplicating finalized rows.

**Why the CAS guard's other clauses matter on resume.** Within one process the
`cursor < m` clause cannot fail *because the cursor passed `m`* (the thief checked
the leading in-memory cursor under `victim.lock`). But `range_end IS :oldHi` and
`status <> COMPLETED` *can* legitimately abort a split — a second thief's stale
snapshot, or a victim that completed via an empty/terminal page — returning
`SPLIT_ABORTED` so the initiator restores `hi` and retries. Across a crash, those
same clauses are the durable backstop that keeps a resumed run from re-deriving an
inconsistent split; reconciliation of a genuinely concurrent (future multi-host)
split is the resume reload's job via `generation`/`owner_lease`, not a
process-local restore.

*Mechanism references:* algorithms.md §4.1 (writer protocol), §4.3 (split
transaction), §4.5 (output durability / two-cursor model), §4.6 (resume);
the worklist-is-checkpoint model (contracts.md §3).

---

## Where to go next

- The full mechanism spec: [`algorithms.md`](algorithms.md).
- The "why parallel listing is hard" primer and the current-limit discussion:
  [`overview.md`](overview.md).
- The counters every step above emits, and the instrumentation discipline behind
  them: [`metrics-internals.md`](metrics-internals.md) §5.
- The design laws these traces illustrate: [`overview.md`](overview.md) under "Design laws".
