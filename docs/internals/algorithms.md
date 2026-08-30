# The listing engine — `WorkStealingScan`

This is the authoritative specification of swath's listing algorithm. The
internals overview (`overview.md`) and the contracts pack (`contracts.md`) defer to
this document for the engine.

The engine has **one job**: enumerate every object in a prefix scope of a
supported general-purpose S3 bucket, exactly once, as fast and call-efficiently
as possible. It handles varied globally ordered key distributions (deep tree,
flat random, badly skewed) using S3's `StartAfter` contract, with no manual
partitioning and with crash-resume. Active buffers are configuration-bounded;
finalized-part metadata is `O(parts)` and sorted staging metadata is
`O(segments)`.

> **Why one engine.** Earlier designs had three live-LIST strategies
> (recursive discovery, adaptive bisection, hinted partition) and a router
> that picked among them. `WorkStealingScan` subsumes all three: recursive
> `delimiter=/` discovery becomes its *seed* step (`--tune seed.mode=shallow`, shipped),
> hints become an *alternate seed* (`--tune seed.mode=hints` — **not yet wired**;
> `SeedStep` throws "not yet implemented"), and bisection is replaced by
> *demand-driven range stealing*. The router can no longer mis-route.

## Contents

- [1. Foundations](#1-foundations)
  - [1.1. Keys are bytes — `KeyBytes`](#11-keys-are-bytes--keybytes)
  - [1.2. The range model — half-open `(A, B]`](#12-the-range-model--half-open-a-b)
- [2. The worker loop (`runRange`)](#2-the-worker-loop-runrange)
  - [2.1. Intra-range speculative readahead (opt-in)](#21-intra-range-speculative-readahead-opt-in)
- [3. Stealing — demand-driven rebalancing](#3-stealing--demand-driven-rebalancing)
  - [3.1. Choosing the pivot `m` over raw bytes](#31-choosing-the-pivot-m-over-raw-bytes)
  - [3.2. Victim selection](#32-victim-selection)
  - [3.3. Dense-tail placement mechanisms](#33-dense-tail-placement-mechanisms--measured-status)
  - [3.4. Intra-range speculative readahead — engagement and tuning](#34-intra-range-speculative-readahead--engagement-and-tuning)
- [4. Checkpoint, commit, and the split transaction](#4-checkpoint-commit-and-the-split-transaction)
  - [4.1. The writer protocol (request → ack)](#41-the-writer-protocol-request--ack)
  - [4.2. Per-page commit (`commitPage`)](#42-per-page-commit-commitpage)
  - [4.3. Split transaction (`splitTxn`) — standalone, CAS-guarded](#43-split-transaction-splittxn--standalone-cas-guarded)
  - [4.4. Quiescence / termination](#44-quiescence--termination)
  - [4.5. Parquet output durability (the two-cursor model)](#45-parquet-output-durability-the-two-cursor-model)
  - [4.6. Resume (listing)](#46-resume-listing)
- [5. Adaptive concurrency (AIMD)](#5-adaptive-concurrency-aimd)
- [6. Correctness argument](#6-correctness-argument)
- [7. Cost](#7-cost)
- [8. Seeding (the HYBRID)](#8-seeding-the-hybrid)
- [9. Versioned listing (`ListObjectVersions`)](#9-versioned-listing-listobjectversions)
- [10. Express One Zone (directory buckets)](#10-express-one-zone-directory-buckets)
- [11. Edge-case checklist (must be handled / tested)](#11-edge-case-checklist-must-be-handled--tested)
- [12. Parallel sort boundary selection](#12-parallel-sort-boundary-selection)

---

## 1. Foundations

### 1.1. Keys are bytes — `KeyBytes`

S3 returns objects in **UTF-8 binary order** and compares `start-after`
**byte-by-byte, unsigned**. Java `String.compareTo` uses UTF-16 order, which
**diverges** from UTF-8 byte order only for **supplementary** code points
≥ U+10000 (UTF-8 byte order matches code-point order across the whole BMP; it
is the UTF-16 surrogate pairs that reorder). Using `String` comparison for
boundaries is therefore a silent correctness bug.

swath represents every key as **`KeyBytes`**: the raw key bytes plus an
unsigned-byte lexicographic comparator. A `String` view is derived lazily,
only at text-formatter and filter boundaries (where the operator wants
text). Keys flow through the engine, the checkpoint, and the Parquet/CBOR
sinks as bytes, so byte-exactness is preserved end-to-end (see
`contracts.md` §1.1).

```
KeyBytes:
  byte[] raw                       // the key, exactly as S3 returned it (after url-decode, §1.1/§11)
  static int compare(KeyBytes a, KeyBytes b)   // unsigned lexicographic over raw bytes
  String asString()                // lazy UTF-8 decode for output/filtering only
```

Request listings with `encoding-type=url` and **URL-decode** each returned
key before comparing/splitting/emitting (keys may contain bytes `< 0x20`
that otherwise break XML parsing in some SDK/endpoint paths). `start_after`
is sent as the raw decoded key.

### 1.2. The range model — half-open `(A, B]`

A worker owns a half-open range written **`(A, B]`**:

- `start_after = A`. S3 returns keys **strictly greater** than `A`
  (`start-after` is exclusive).
- **Client stop check:** emit each returned key `k` while
  `compare(k, B) <= 0`; at the first key with `compare(k, B) > 0`, **stop
  and do not emit it**. `B = null` means unbounded (the frontier).
- So a worker emits exactly the keys `k` with `A < k <= B`.

**The boundary key belongs to the LEFT interval.** Adjacent workers
`(A, B]` and `(B, C]` share boundary `B`: the left worker emits `k == B`;
the right worker has `start_after = B` and emits only `k > B`. This is *the*
load-bearing invariant — the wrong convention yields a one-key **gap**
(`start_after` exclusive on both sides drops `B`) or a one-key **overlap**
at every split.

Boundary keys may be **synthetic / non-existent** byte strings (this is
legal and what the proven `s3-fast-list` engine relies on —
`tasks_s3.rs`/`data_map.rs`: "end could be a non-exist key/prefix"). When
`B` is synthetic, no key equals it, so `<= B` and `< B` coincide and the
boundary is automatically gap-free.

Each worker publishes, under a tiny per-worker lock taken only at
page-commit and at steal (never per key):

- `cursor` — the last in-range source key accepted for checkpointing
  (= `start_after` for the next page; it may precede final output filtering).
  Held as an `AtomicReference<KeyBytes>` so a thief reads
  a coherent snapshot.
- `hi` — the current upper bound `B`, lowered by a thief. **Declared
  `volatile` (an `AtomicReference<KeyBytes>`)**: the worker re-reads it
  lock-free on every key (§2), so it must have a happens-before with the
  thief's narrowing write. The thief writes `hi` under `victim.lock` *and*
  the field is volatile — the lock orders thieves against each other; the
  volatile publishes the new bound to the lock-free per-key reader. Without
  the volatile the reader could miss the narrowing and double-emit.

---

## 2. The worker loop (`runRange`)

`runRange` is the proven `s3-fast-list` inner loop (`tasks_s3.rs:108`) with
the `s3ls-rs` pagination defenses:

```
runRange(node):                         // node = (lo, hi], cursor, mode
  startAfter = node.cursor              // = node.lo on a fresh node (OBJECTS)
  loop:
    if cancelled: return PAUSED
    rateLimiter.acquire()               // §5; cancellable
    page = fetchPage(prefix=P, startAfter=startAfter, maxKeys=1000, mode=node.mode)
    batch = []; reachedBound = false
    for k in page.keys:                 // already in byte order
        hiNow = node.hi                 // volatile re-read every key: a thief may have lowered it
        if hiNow != null and compare(k, hiNow) > 0:
            reachedBound = true; break  // reached our (possibly newly-narrowed) bound; do NOT emit k
        batch.add(k); lastKey = k
    done = reachedBound or not page.truncated
    // ATOMIC, before emit. cursor advances ONLY if batch non-empty (lastKey set this page).
    commitPage(node, batch, advanceTo = (batch non-empty ? lastKey : node.cursor), completed = done)
    emit(batch)                         // push downstream (page-batch)
    if done: return DONE
    startAfter = lastKey                // OBJECTS: paginate by the last in-range source key
    // progress / stuck defenses (port s3ls-rs mod.rs:506-544):
    if page.truncated and batch is empty and not reachedBound: fail("truncated page returned no keys ≤ hi")
    if startAfter == previousStartAfter: fail("no forward progress (stuck listing)")
    previousStartAfter = startAfter
```

Notes:

- **Re-read `hi` per key** (volatile, not once per page): the **first of two
  independent defenses** that keep a narrow from double-emitting when a page
  fetched under the old, wider bound is still in flight. Walking the page, the
  worker re-reads the volatile `hi` before each key and stops at the new bound,
  leaving `(hi, oldHi]` for the new worker — this catches every key **not yet
  pulled** from the in-flight page when the narrow lands. It cannot retract a key
  **already read into the batch** under the old bound: the narrow may land after
  that key passed its per-key check. That residual window is closed by the
  **second** defense — the **commit-time, under-lock in-range re-trim**.
  `commitPage` runs under the same per-worker lock a thief takes to narrow `hi`
  (§1.2, §3), so it observes a coherent bound, and before it advances the cursor
  it re-checks the batched keys against the current `hi` and drops any now `> hi`
  (those keys are the child's). The two are **not redundant**: the volatile
  re-read structurally cannot catch an already-batched key — only the under-lock
  re-trim can — and together they make the hand-off double-emit-free under
  pipelined requests.
- **Pagination is by `startAfter = last in-range source key`** (OBJECTS), full stop —
  there is no `continuation_token` in the model. The key is the single source
  of truth for both pagination and resume (survives token expiry and
  re-splitting; portable across hosts). VERSIONS uses `(key_marker,
  version_id_marker)` instead (§9).
- **Empty batch never nulls the cursor.** A page can be empty (end of bucket)
  or yield no key `≤ hi` (first key already past a narrowed bound). In both
  cases `completed = (reachedBound or !truncated)` and the cursor is left
  unchanged — there is no key to advance to. Completion is decided by
  `reachedBound`/`!truncated`, **independent of emptiness**.
- **`cursor reached range_end`** means `compare(cursor, range_end) ≥ 0`.
- Stop on `IsTruncated == false`, never on `count < 1000` (page size is not
  guaranteed to be exactly 1000).

### 2.1. Intra-range speculative readahead (opt-in)

When a **bounded** dense range collapses to a serial owner drain — one page per
RTT, no structure to split on — the worker can hide the page-to-page latency
chain with **intra-range speculative readahead**: fire `K` concurrent guessed
`start-after` fetches ahead of the cursor, hold them in a bounded buffer, and
adopt them back into this same serial loop as the cursor reaches them. It is
**opt-in and off by default** (`--tune engine.readahead=on`); off, the loop is
exactly the pure serial pagination above, with no added state and no counters.
When engaged it changes only *how* the next contiguous page's entries are
obtained — never *what* or *when* the loop emits — so I1 commit-before-emit
(§4.1) and byte-exact in-order emission are preserved by construction.

- **Exactly-once adoption (the linchpin).** Readahead never emits or commits
  anything itself. A page fetched at guess `G` is handed back to the loop **only
  when `G <=ᵤ cursor`** (unsigned), and then trimmed to just the keys `> cursor`.
  This is the whole safety argument: `start-after=G` returns a contiguous page
  over `(G, last]`, and when `G <= cursor <= last` the suffix `(cursor, last]`
  has no gap — so the committed cursor still advances **only through contiguous,
  fetched keys**, exactly as a serial fetch would. The other dispositions never
  adopt: a guess still ahead of the cursor (`G > cursor`) stays buffered (HOLD)
  until the cursor reaches it; a fully-overlapped page (`last <= cursor`) is
  discarded; a guess a split has narrowed past (`G >= hi`) is cancelled. There
  is never a jump-ahead adoption. An adopted page then takes the **identical
  per-key volatile `hi` check** as a serial page (§2), so a page fetched
  speculatively under a wider `hi` that a split has since narrowed completes the
  node exactly as a serial page would.
- **Engage gate.** The mechanism engages only when a range has genuinely
  collapsed to a sustained bounded drain — five AND-ed conditions: (1) the toggle
  is on; (2) **the range is bounded (`hi != null`) — readahead NEVER engages on
  the open frontier**, the rightmost unbounded worker, whose forward density is
  unknown; (3) both endpoints of the just-drained page are known; (4) the range
  has drained **≥ 6 consecutive full, un-split serial pages** (a *full* page = a
  non-adopted, truncated page returning `maxKeys` keys); and (5) that page spans
  forward (`firstKey <ᵤ lastKey`). The streak counts full pages drained **without
  `hi` being narrowed by a steal or self-split, and resets to 0 on every such
  narrow** before engagement. This is deliberately not a knob to raise to reduce
  engagement: under normal work-stealing an owner is narrowed frequently, so a
  longer required streak would make engagement rare for *every* bucket rather
  than targeting collapsed tails, sacrificing the genuine sustained-drain win it
  exists to capture.
- **Off-gauge, fail-soft, bounded.** Speculation fetches through a **dedicated
  off-gauge fetcher**, never the worker's own slot-gated one, for two reasons: a
  disposable guess must never (a) acquire an AIMD concurrency slot or cast a
  happy-path growth vote — otherwise `K` guesses per engaged worker would amplify
  permit-pool pressure during exactly the 503 storm the engine (§5) is trying to
  shed (a genuine voting 503 on a guess still drives AIMD *down* — real
  backpressure counts regardless of which fetch observed it) — nor
  (b) reach the worker fetcher's bounded-retry give-up path, which cancels the
  **whole run** as a side effect. Every speculative fetch is additionally bounded
  by a wall-clock budget (30 s default), because a genuine AIMD-voting 503
  retries *unbounded* inside the fetcher regardless of gauging; without the timed
  await plus a drain-time reclaim of any past-budget guess, the owner's own
  forward progress could block indefinitely on a disposable guess — worse than no
  readahead. Any speculative fault (throttle, listing error, cancellation) is
  **absorbed and dropped**, never propagated: the loop just falls back to an
  ordinary serial fetch for that page, exactly as if the guess had missed.
  Speculation is therefore **fail-soft — never run-cancelling.**
- **Bounded memory and self-tuning.** At most `K` speculative pages exist at any
  instant (in-flight + buffered ≤ `K`, ≈ `K × maxKeys` keys); once the budget is
  full no new guess launches (demand back-off). While engaged, each page's
  outcome (adopted vs serial-fallback) is folded into a small **tumbling** window;
  if the adopted fraction over a full window falls to a floor, the guesses are
  not paying off and the range **disengages and reverts to plain serial** — a
  later fresh streak can re-engage a drain that resumes. All readahead state is
  process-local and never durable: a crash discards it harmlessly, and resume
  reopens at the sink's durable resume position (`cursor` for ordinary
  checkpoint resume, `durable_cursor` for Parquet) and re-lists forward,
  re-doing at most `K` un-adopted fetches (nothing speculative was emitted or
  committed out of contiguous cursor order).

---

## 3. Stealing — demand-driven rebalancing

> Everything in this section is governed by the two design laws in
> [`overview.md`](overview.md) under "Design laws": pivot placement is conditioned
> on observed mass (L1), and every steal/split trigger is local and demand-driven (L2).

A worker that has no `PENDING` node to claim, while other workers are still
busy, becomes a **thief**.

**Implementation split (the policy seam, contracts.md §2.1):** the
pseudocode below is a decision-logic description, not a call-graph — victim selection and the
whole pivot cascade (everything from "PLACE the pivot" through the structure/reflect/bisect/
flat-leaf fallbacks) live in `io.varve.swath.engine.policy.ThiefPolicy`, a source-agnostic
`StealPolicy` with no lock, clock, or RPC of its own. `io.varve.swath.engine.Thief` is the
executor: it snapshots `(cursor, hi)` under the victim's lock, drives `ThiefPolicy` through a
request/response loop (issuing every probe it asks for), then re-validates and runs the durable
split CAS under the same lock. See `architecture.md`'s component map for the package split.

```
steal():
  victim = argmax over live workers w of estRemaining(w)        // §3.2
  if victim == null or victim.unsplittable: return null         // nothing to steal → maybe done
  (c, H) = snapshot(victim.cursor, victim.hi)                   // coherent read
  // 1. PLACE the pivot from observed mass (L1) — never from a bare midpoint:
  //      bounded  → interpolate inside (c, H] at the victim's far-ahead density fraction
  //                 f >= 0.5, synthesizing against its observed alphabet (§3.1, §3.3);
  //                 f == 0.5 with a flat digest IS byteMidpoint, byte for byte.
  //      frontier → reflect the consumed span forward toward prefixCeil(P).
  f = (H == null) ? 0.5 : farAheadFraction(victim)              // §3.3
  m = (H == null) ? extrapolate(victim.lo, c, prefixCeil(P))    // §3.1
                  : interpolate(c, H, f, victim.alphabetDigest())
  if m == null:                                                 // TWO distinct nulls — discriminate:
    if H == null and isUnstartedFrontier(victim.lo, c):         //  un-started frontier (cursor still at lo,
      return RETRY                                              //   no consumed span yet) — TRANSIENT, do NOT cache:
                                                                //   the owner hasn't committed page 1; re-steal later.
    victim.unsplittable = true; return UNSPLITTABLE             //  no SAFE key strictly between — TERMINAL, cache.
                                                                //   A null pivot never reaches the structure probe:
                                                                //   an empty interval has no structure to discover.
  // 2. PROBE the upper half once. Every rung below fires only on real probe EVIDENCE.
  upperEmpty = not probe(m, H)                                  // §3 probe: one key, "empty" iff none <= H
  if upperEmpty and H != null and f > 0.5:                      // far-ahead step-back: re-probe ONCE at the plain
    m = interpolate(c, H, 0.5)                                  //   code-point midpoint (no digest) before paying
    upperEmpty = not probe(m, H)                                //   for the costlier machinery — so far-ahead is
                                                                //   never worse than byte-midpoint (PIVOT.step_back)
  parentEmpty = H != null and not upperEmpty                    // the SYMMETRIC degenerate: m is c's immediate
                and isCursorAdjacentSliver(m, c)                //   MIN_SAFE successor, so (c, m] is empty and the
                                                                //   child would inherit the ENTIRE tail
  if upperEmpty or parentEmpty:
    // 3. STRUCTURE discovery — BOUNDED ranges only (it is over a finite far H that a
    //    uniform pivot can only sliver; the open frontier is balanced by extrapolate) and
    //    only while this victim's zero-fan-out suppression permits (§3.2).  Issue ONE bounded
    //    ListObjectsV2(prefix=Q, delimiter=/, start_after=c, max_keys=STRUCTURE_PROBE_MAX_KEYS)
    //    and take a CommonPrefix strictly in (c, H): the MEDIAN when the page came back
    //    complete, else — the page truncated, so the boundaries are only a PREFIX of the
    //    directory's children and their median would sit near the cursor — the FURTHEST one
    //    the probe proved.  max_keys is small because a delimiter listing costs ~linearly in
    //    the CommonPrefixes it returns, and a probe that outruns its attempt timeout yields no
    //    cut point at all (the dense-tail starvation leg).  Either way it is a real, populated
    //    boundary, so victim and child run in DIFFERENT regions.  Q is
    //    the lo∧c directory for the empty-upper funnel; for the parent-empty sliver it is a
    //    coarse→fine back-out from the c∧H divergence directory (<= 4 probes, §3.2 caps).
    m_s = structureProbeMedianPrefix(victim, c, H, P)           // null on a flat region / no boundary in (c, H)
    if m_s != null: m = m_s; goto found_pivot                   // skip the bisection entirely
    if upperEmpty:
      // 4. DENSITY REFLECTION (bounded ranges) — ships DEFAULT ON; `--engine-toggle
      //    reflect=off` is the kill-switch (off ⇒ the plain uniform-midpoint bisection,
      //    byte for byte).  The mass sits BELOW the uniform pivot, so reflect the consumed
      //    span (lo, c] forward under OBSERVED density and spend ONE probe on it before
      //    any blind halving.
      m_r = extrapolate(victim.lo, c, H)                        // §3.1 forwardReflect — pure math, no I/O
      if m_r != null and compare(c, m_r) < 0 and compare(m_r, m) < 0:
        m = m_r
        if probe(m_r, H): goto found_pivot                      //  HIT  → commit at the reflected pivot
                                                                //  MISS → fall through, bisecting the SHORTER
                                                                //         interval (c, m_r] instead of (c, m)
      // 5. BISECT back toward the cursor, log-scaled budget B (§3.2 probe-storm caps).
      for B iterations:                                         // upper half still empty → retry NEARER the cursor,
          m = byteMidpoint(c, m)                                //   bisecting the lower half (c, m]; do NOT cache
          if m == null: return RETRY                            //   unsplittable — c and the live head are adjacent
                                                                //   now; re-steal later (a left-skewed dense head is
                                                                //   not permanently disabled)
          if probe(m, H): goto found_pivot                      //   a populated upper half — hand it off
      markNonProductiveSteal; return RETRY                      // probe-storm cap: bisect_budget_exhausted (§3.2)
    else:
      // 6. A parent-empty sliver with NO discoverable structure — one flat, high-entropy
      //    LEAF directory of files.  Only density extrapolation divides it; a rejected
      //    pivot falls through to the byte-exact sliver UNCHANGED (still tiles).
      m_f = flatLeafDensityPivot(victim, c, H)                  // §3.1, ceiling clamped inside the leaf
      if m_f != null: m = m_f
  found_pivot:
  // lock-guarded RE-VALIDATE-and-narrow hand-off. The snapshot (c,H) and the
  // probe are speculative — RE-CHECK *both* the bound and the cursor under the
  // lock. The per-victim lock also serializes two thieves that picked the same
  // victim; without the hi re-check the second would split against a stale H.
  under victim.lock:                                            // serializes thieves; gates the victim's commit-enqueue (§4.1)
    if victim.hi != H: return RETRY                             // another thief already lowered the bound → our m/probe are stale; re-snapshot
    if compare(victim.cursor, m) >= 0: return RETRY             // victim already at/past m; re-snapshot
    victim.hi = m                                              // volatile narrow to (lo, m]; publishes to §2 reader
    child = splitTxn(victim, m, oldHi=H)                       // §4 — durable BEFORE returning; SQL guards cursor<m AND range_end IS oldHi AND status≠COMPLETED
    if child == ABORT:                                         // guard refused (victim advanced/completed, or range_end already moved)
        victim.hi = H                                          // RESTORE the just-validated bound (H was current under the lock ⇒ single-process safe)
        return RETRY
    return child                                               // thief now runs (m, H]
```

- **`probe(m, H)`** is the pivot-testing speculative call: `ListObjectsV2(prefix=P,
  start_after=m, max_keys=1)`, "empty" iff it returns no key `<= H`. One
  key, not a page. Every attempt spends exactly one of these on its placed pivot;
  the step-back, the reflection, and each bisection halving add at most one more
  apiece, all inside the §3.2 budget. **`unsplittable` is cached only on a
  terminal null pivot** — a range with no safe key strictly between its bounds, so
  repeated steals don't re-probe a dead range every cycle — never on a merely empty
  probe, which says only that the pivot was placed too high.
- **Idle probe pacing is allowed and expected.** A non-productive steal outcome
  (`NO_VICTIM`, unchanged/transient `RETRY`, or an empty probe that cannot
  produce a child yet) does not change the range partition, so the driver may
  exponentially back off the next idle steal attempt and skip re-probing a
  victim whose `(cursor, hi)` snapshot is unchanged since the last
  non-productive attempt. A created child, claimed work, or non-empty page
  commit resets this pacing; enqueue/decrement/progress signals still wake
  parked workers, so quiescence detection and the progress-gated liveness fix
  are unchanged.
- **Per-victim futility pacing is a separate, narrower mechanism — deliberately
  not merged with the fleet-wide backoff below.** `WorkerState.recordFutileSteal`
  trips a per-victim cooldown after `FUTILITY_PACE_THRESHOLD` (4) *consecutive*
  futile outcomes against THAT victim (`cursor_passed_pivot`/`bound_moved`/
  `bisect_budget_exhausted`), for a bounded-exponential number of steal-selection
  skips (cap 64), reset only by that victim's own productive progress
  (`markStolen`). A productive sibling stays fully stealable throughout — this
  paces hammering one racing drainer, never the fleet. **Implementation split
  (the policy seam, contracts.md §2.1):** the trip/
  bounded-exponential-growth/decay/reset arithmetic is
  `io.varve.swath.engine.policy.FutilityPacingPolicy`, pure functions of one
  `int` at a time; `WorkerState` still owns every `AtomicInteger` read/write, in
  the same order and with the same lock-free-per-field discipline as before —
  see `docs/internals/contracts.md` §2.1 for the concurrency argument for why
  this stays per-field rather than one combined view.
- **At most one speculative steal attempt is in flight fleet-wide, and the bound
  is strict.** *Pacing state* (backoff level, next-attempt instant) and *slot
  ownership* (`attemptInFlight`) are separate concerns in `IdleStealBackoff`. The
  pacing arithmetic (the exponential growth/cap and the park-remaining
  computation) is `io.varve.swath.engine.policy.IdleStealPacingPolicy`, which
  owns no clock: `IdleStealBackoff` reads the ambient clock through its
  `DecisionClock` (mirroring `DecisionRng`'s treatment of randomness, and
  supplying the live `System::nanoTime` default) and passes the resulting
  `nowNanos` in as a policy argument. The one-attempt SLOT itself — its ownership,
  release, and the `RunMetrics` reference — is executor infrastructure and does
  **not** move into the policy package. The
  slot belongs to the worker that acquired it and is released only by that worker,
  in a `finally` covering the whole acquired region — so no escape from the
  metrics, logging, victim curation or child enqueue inside it can strand the slot
  and disable stealing for the rest of the run. The pacing reset is called by
  *unrelated* workers on every claim and every non-empty page commit and therefore
  must never touch ownership; when it did, the effective bound was
  `1 + (reset rate × attempt duration)` — tens of concurrent probes under load.
  Concurrent attempts would remain *safe* (`victim.lock` + the I4 CAS guard); they
  are simply not *efficient*, because N thieves converge on the same argmax victim
  and all but one lose the CAS. Measured on a 6.6M-key bucket, honouring the bound
  raised steal success from ~4% to ~25% and cut API calls ~35%.
- **Waiting on the slot is release-driven, not poll-driven.** A denied worker
  re-reads the state under the ledger gate and acts on what it finds there — it may
  claim a child that became ready meanwhile, or park on whatever pacing window the
  attempt's own outcome left behind. When it does park *because the slot is still
  held*, the backstop is seconds-scale, not the ~5 ms pacing base, because the
  release itself broadcasts on the ledger. That backstop
  bounds the wait for an attempt that *outlives* it — it is not the mechanism that
  ends an ordinary wait, and not merely a lost-signal fallback. The release
  must be signalled *outside* the backoff monitor: `Worklist.park` holds its gate
  across the `parkNanos()` call, so gate→backoff is the only safe lock order.
  That same gate hold is what makes the signal unlosable — a denied worker either
  reads the cleared flag under the gate and parks briefly, or is already awaiting
  when the broadcast lands. Quiescence is unaffected; enqueue, decrement and
  progress all still broadcast.
- **In-memory `cursor` leads the durable cursor — this is what makes the
  hand-off race-free.** A worker advances its in-memory `victim.cursor`
  (`AtomicReference`, §1.2) **under `victim.lock` at the instant it *enqueues*
  a page commit** (§4.1), never when the future later resolves. So the
  in-memory cursor never lags what is — or is about to be — committed. A thief
  that reads `victim.cursor < m` under the same lock is therefore reading the
  *leading* edge: any commit already enqueued (and arrival-ordered before this
  `splitTxn`, §4.1) advances the DB cursor to at most that same key, still
  `< m`. Hence the `cursor < m` clause of the `splitTxn` guard (§4.3) **cannot
  fail *because the cursor passed `m`*** in a single process. (The guard's other
  two clauses — `range_end IS oldHi` (NULL-safe) and `status ≠ COMPLETED` — *can* legitimately
  fire and ABORT: a second thief's stale split, or a victim that completed via an
  empty/terminal page, §4.3. Those ABORTs are handled by the restore-and-RETRY
  above; multi-host reconciliation is the resume reload's job, §4.6, not the
  local restore.) The thief holds `victim.lock` across the hand-off, which blocks
  only *this* victim's next commit-enqueue (a short, batched burst) — not other
  workers, and not the victim's already-enqueued in-flight commit.
- **`splitTxn` is synchronous**: it returns only after the checkpoint-writer
  thread has *committed* the child as `PENDING` (§4). So the child node is
  durable before the thief lists or emits a single key of `(m, H]` — a crash
  before the commit simply leaves the victim owning the whole `(lo, H]`,
  which it re-lists. Combined with per-key volatile `hi` re-reads (§2), this
  is overlap-free and gap-free under pipelined requests **and** across
  crashes.

### 3.1. Choosing the pivot `m` over raw bytes

**Bounded range (`H` finite — the common case after seeding): a key
strictly between.** Compute `m` with `c < m < H` over Unicode **code points**
(== unsigned-byte order for valid UTF-8), or `null` when no **safe** key exists
strictly between (the region up to `H` is only a control/noncharacter sliver):

**Safe synthesized pivot code points (the asymmetry).** A pivot is sent to S3 as a
`start-after` value, which **real S3 validates server-side against XML 1.0** and
rejects (HTTP 400) for XML-illegal code points. So the ONE code point a pivot
*synthesizes* (the divergence / appended / reflected scalar) is drawn only from the
**safe set** `E^c`, where the **excluded set** is
`E = {U+0000..U+001F} ∪ {U+007F} ∪ {U+0080..U+009F} ∪ {U+FDD0..U+FDEF} ∪
{every plane's trailing pair xFFFE/xFFFF} ∪ {U+0025}`. This is a conservative superset of
the XML-1.0-illegal code points: it also excludes the XML-legal TAB/LF/CR and
DEL by choice, keeping the C0 block contiguous and with zero synthesis value.
`MIN_SAFE = U+0020` is the smallest safe scalar. The C1 block `U+0080..U+009F`
and every noncharacter (the standalone BMP block `U+FDD0..U+FDEF` and the
trailing `xFFFE`/`xFFFF` pair in EVERY plane, not just the BMP one) are
XML-legal but were pulled into `E` too: a synthesized pivot is
sent to real S3 as a `start-after` cursor, and several implementations 400 on
a C1 control or a noncharacter in that role. `U+0025` (`%`) was pulled into
`E` for a **different** reason than every other member: it is
XML-1.0-legal and real S3 never 400s on it. It is excluded because
the tested LocalStack build echoes an S3 request parameter back verbatim (not
re-percent-encoded, unlike real S3 and the tested MinIO build), and the AWS SDK's own response
interceptor strict-decodes that echo with `URLDecoder`, which throws on a
lone/trailing `%` and aborts the listing — a self-inflicted crash against
such endpoints only. See `docs/internals/s3-implementation-compatibility.md`
for the full deviation and its limits. This is an
**asymmetry**, not a collapse: **bound interpretation** (decode / common-prefix /
compare) still ranges over the FULL scalar space — real keys/cursors may carry any
code point including controls — while **synthesis** draws only from `E^c`,
re-verifying strict betweenness at the synthesis point so the no-gap/no-overlap
tiling is preserved.

The safe-set guarantee applies to the **synthesized** divergence/append character,
not to bytes copied from the bounds' common prefix. For buckets whose keys are
themselves XML-safe (the overwhelming common case), every pivot is a valid S3
`start-after`. swath does not, and cannot via pivot construction, sanitize bytes
copied from the bounds.

```
// Operates on Unicode CODE POINTS so the pivot is INHERENTLY valid UTF-8 (assembled
// only from scalar values — never a lone continuation byte). Returns m with a < m < b
// (unsigned-byte order == code-point order for valid UTF-8) whose SYNTHESIZED scalar
// is in E^c, or null IFF no safe key exists strictly between (the successor region up
// to b is a control/noncharacter sliver). Precondition: a, b valid UTF-8, compare<0.
//
// Scalar values omit the surrogate block U+D800..U+DFFF, so they form a contiguous
// index space:  idx(x) = x < 0xD800 ? x : x - 0x800 ;  cp(j) = j < 0xD800 ? j : j + 0x800.
// isSafe(cp) = cp ∉ E and cp not a surrogate.
// safeBetween(lo, hi) = the SAFE scalar nearest the natural midpoint index
// ⌊(idx(lo)+idx(hi))/2⌋ that lies STRICTLY in the open interval (lo, hi), found by
// scanning OUTWARD from the midpoint; the scan only ever visits indices strictly
// inside (lo, hi), so it always terminates regardless of how wide an excluded
// block is — NONE iff no safe scalar lies strictly between (the whole interval,
// however wide, is excluded). lo may be the sentinel BELOW
// (index −1, "no code point here" — sorts before every scalar).
// safeAbove(cp) = the smallest safe scalar > cp (NONE if none).
safeBetween(lo, hi):
  jl = (lo == BELOW) ? -1 : idx(lo);  jh = idx(hi)
  if jh − jl ≤ 1: return NONE             // scalar-adjacent ⇒ nothing strictly between
  jm = ⌊(jl + jh) / 2⌋                    // jl < jm < jh
  for delta = 0, 1, 2, …:                 // scan outward; both ends leave (lo,hi) ⇒ NONE
    for j in {jm − delta, jm + delta} ∩ (jl, jh):
      if isSafe(cp(j)): return cp(j)
    if neither jm±delta is in (jl, jh): return NONE

byteMidpoint(a, b):
  A = decodeUtf8(a);  B = decodeUtf8(b)   // arrays of Unicode scalar values
  i = length of the common code-point prefix of A and B
  if i == A.length:                       // Path B: A is a proper prefix of B ⇒ B[i] exists
    v = safeBetween(BELOW, B[i])          // a SAFE scalar with U+0020 ≤ v < B[i]
    if v != NONE:
      return encodeUtf8(A ++ [v])         // A·v : v < B[i] ⇒ A < A·v < B (MIN_SAFE fallback if A·v overflows the cap)
    // No safe scalar below B[i] (B[i] ≤ U+0020): recover only the safe-boundary sliver
    if isSafe(B[i]) and B.length > A.length + 1:
      return encodeUtf8(A ++ [B[i]])      // A·B[i] < B (B strictly longer) — e.g. B[i]==U+0020
    return null                           // unsplittable: only a control/noncharacter sliver lies between
  else:                                   // Path A: scalars differ at i — A[i] < B[i]
    v = safeBetween(A[i], B[i])           // a SAFE scalar strictly between A[i] and B[i]
    if v != NONE:
      return encodeUtf8(A[0..i] ++ [v])   // shortest m, well-balanced; A[i] < v < B[i]
    return encodeUtf8(A ++ [MIN_SAFE])    // no safe scalar between ⇒ A·U+0020 < B (m[i]=A[i] < B[i])
```

(Cap fallback when the ideal pivot exceeds `MAX_KEY_LEN`: bump `A`'s right-most
code point to the next **safe** scalar via `safeAbove`, drop the tail, accept the
first that fits the cap **and** stays `< b`; else `null` — balance-only, never a
coverage risk.)

Correctness sketch (verify each branch): `byteMidpoint` decodes both
valid-UTF-8 keys to scalar values, so every branch assembles `m` **only from
scalar values** ⇒ `m` re-encodes to **valid UTF-8** by construction, and its
synthesized scalar is in `E^c`. Each branch yields `compare(a,m) < 0` and
`compare(m,b) < 0` (a strict safe scalar at the first differing position, or an
appended `MIN_SAFE`/safe scalar that extends `A` while staying below `B`). The
`null` cases are exactly the **bounded** "no safe key strictly between" slivers
(e.g. `b == a ++ [U+0000]`, `b == a ++ [U+0001]`, or `b == a ++ [U+0020]` with `b`
not longer) — they route to `setUnsplittable` (terminal), affecting only load
**balance**, never the no-gap/no-overlap invariant. The earlier "pad-with-0x00 /
sentinel-256" version was **wrong** (it returned `m > b` for `a=[]`,`b=[0x00]`),
the raw-byte `a ++ [0x80]` branch emitted **invalid UTF-8**, and the prior
`a ++ [U+0000]` successor synthesized XML-illegal control code points that real S3
rejects as `start-after` — do not use them. **Must** be property-tested on: `a`
empty, `a` a proper prefix of `b`, `b == a++[U+0000]` / `b == a++[U+0001]` (→ null),
`b == a++[U+0020]` (→ null if `b` not longer, else `a·U+0020`), scalar-adjacent
`A[i]`/`B[i]`, bounds straddling the C0 block (→ append `MIN_SAFE`), bounds
straddling `U+FFFD ↔ U+10000` (→ never `U+FFFE`/`U+FFFF`), code points straddling
the surrogate gap (U+D7FF ↔ U+E000), supplementary code points (≥ U+10000), long
max-scalar runs, 1024-byte keys — **and that every non-null output is valid UTF-8
and its synthesized scalar carries no code point in `E`.** Whole-output
XML-safety is asserted only when the copied bound prefix is itself XML-safe.

**Pivots must be UTF-8-safe — the simple, correct choice (supersedes the earlier
"byte-exact interceptor" note).** Every real S3 key is valid UTF-8 (AWS defines a
key as a Unicode string whose UTF-8 encoding is ≤1024 bytes) — **there are no
non-UTF-8 keys.** A split pivot is only a *boundary strictly between two real
keys*, never a key itself, so it can always be chosen to be **valid UTF-8**:
between any two distinct UTF-8 keys `a < b` a valid-UTF-8 `m` with `a < m < b`
exists unless they are UTF-8-adjacent, in which case `byteMidpoint` returns
`null` (unsplittable — affects only load **balance**, never the no-gap/no-overlap
invariant). So `byteMidpoint` **MUST emit only valid-UTF-8 pivots** — no lone
continuation byte (`a ++ [0x80]`), no truncation mid-multibyte — and return
`null` when no valid-UTF-8 boundary exists. Then `start_after` transmits
correctly through the SDK's ordinary `startAfter(String)` and **no
`ExecutionInterceptor` is needed**: real keys and UTF-8 pivots both round-trip
byte-exact; the U+FFFD corruption only happens to a *non*-UTF-8 string, which we
now never produce.

**`start-after` encoding is unchanged — the fix is pivot SYNTHESIS, not transport.**
The SDK already percent-encodes the `start-after` query parameter (`U+0000` → `%00`,
`U+000F` → `%0F`), so swath does **not** under-encode; `encoding-type=url` is a
*response* knob and cannot make an XML-illegal decoded value acceptable. Real S3
validates the decoded `start-after` server-side against XML 1.0 and returns HTTP 400
for a code point in `E` (LocalStack accepted them — the gap that hid this). The
remedy is therefore to never *synthesize* an `E` code point into a pivot (the safe
set above); when the copied bound prefix is XML-safe, the resulting valid-UTF-8
pivot round-trips byte-exact and passes validation. Do **not** change
`toRequestParam` / `encoding-type`. (Empirically, S3 itself *is*
byte-oriented — it accepts and byte-orders even non-UTF-8
`start-after` — so the raw-byte KEY model is sound; we simply don't need
non-UTF-8 *pivots* to use it, and avoiding them removes the SDK String-impedance
problem entirely. Property-test `byteMidpoint` output is always valid UTF-8.)

**Capability limitation: XML-illegal real cursors.** Buckets whose keys
legitimately contain XML-illegal control bytes cannot be fully paginated via
`start_after`: after such a key is emitted as a real cursor, sending that cursor as
the next `start-after` would 400 on real S3 for the same XML-validation reason.
The pivot safe-set change does not and cannot fix this residual cursor risk. The
future mitigation is `ContinuationToken` pagination, tracked as future work.

**Open-ended range (`H = null` — only the single rightmost frontier worker):
density extrapolation,** never blind galloping. This is a *heuristic*: any
`m` with `c < m < ceil` is correct (an empty probe just means "don't split"),
so precision doesn't affect correctness, only balance. The implementation
reflects the consumed span `(lo, c]` forward in **code-point** space
(`forwardReflect`), sharing `c`'s prefix so the retry-nearer-cursor can refine an
overshoot, and **snaps the one synthesized scalar (the reflected bump, or the
appended high scalar for a `U+10FFFF` run) into the safe set `E^c`** via
`safeAbove`. For XML-safe cursors, the frontier pivot is therefore a valid
`start-after` (the prior reflection could land on `U+000F` and the prior append on
an unverified mid-scalar).

```
// K = precision in bytes (default 12). frac(key) interprets the first K bytes
// as a base-256 fraction in [0,1): frac = Σ key[i]·256^-(i+1); ⊥→0.0.
// unfrac(f,K) renders a fraction back to a K-byte key (floor). ceil = prefixCeil(P)
// (§ below), or 1.0 for the whole-bucket scope.
extrapolate(lo, c, ceil):
  fl = frac(lo); fc = frac(c); fcap = frac(ceil)
  fm = fc + (fc - fl)                      // reflect the consumed span forward (= 2·fc − fl)
  fm = min(fm, fcap - 256^-K)              // stay strictly below the ceiling
  if fm <= fc: return null                 // no forward room ⇒ owner finishes the frontier
  m = unfrac(fm, K)
  if compare(m, c) <= 0 or compare(m, ceil) >= 0: return null   // guard against rounding
  return m
```

The `spanIn` used by victim selection (§3.2) measures positions **relative to
each range's own `[lo, hi]` window** (see §3.2), not the global 12-byte `frac`
above — it is only an estimate. `prefixCeil(P)` = the smallest key
strictly greater than every key with prefix `P`: drop trailing `0xFF` bytes
of `P`, then increment the last remaining byte; if `P` is empty or all
`0xFF`, the ceiling is `⊤` (unbounded / `1.0`). The frontier worker keeps all
boundaries within `[P, prefixCeil(P))`.

### 3.2. Victim selection

```
// All spans are measured RELATIVE to each range's own [lo, hi] window — never as a
// global 12-byte frac, which collapses to ~0 on a deep shared prefix (e.g.
// `crawl=2024-…/pid=…/`) and would divide by zero. Let d = |longest common prefix of
// w.lo and w.hi|; fracIn(key) reads the K bytes AFTER offset d as a base-256 fraction
// (a key ending before d sorts at 0). spanIn(x, y) = fracIn(y) − fracIn(x) ≥ 0.
estRemaining(w):
  if w.hi == null: return +infinity            // frontier scores highest until bounded
  remaining = spanIn(w.cursor, w.hi)
  consumed  = spanIn(w.lo, w.cursor)           // share of the range already listed
  if consumed <= 0: return remaining           // cursor == lo (no density signal yet, 0/0):
                                               //   rank by remaining width alone
  return (w.keysEmitted / consumed) * remaining   // localDensity · remaining span
```

- **The reading above is the pre-0.2.0 rollback/control behind a seam.**
  `estRemaining` is what victim choice, the owner-side self-split's remaining-work floor
  and its pivot mass floors all steer on, so which arithmetic computes it is a run-time
  choice: `RemainingWorkEstimator` (`swath-core`, `io.varve.swath.engine`) is that
  quantity, `RemainingWorkEstimator.WINDOW` is `StealMath.estRemaining` itself expressed
  through it, `rate_anchored_sensing=off` selects it, and
  `EngineToggles.remainingWorkEstimator(maxKeys)` is the only place a run picks one. The
  engine builds exactly one per run and shares it (it is pure and
  stateless) between `ThiefPolicy`'s selection, `OwnerSplitGovernor`'s gate chain, and the
  `slow_ranges[]` diagnostic dump, so a run's reported estimate is the one its decisions
  were taken on. A **fourth** reader sits deliberately outside the seam: `RangeScanner`'s
  readahead engage gate calls `StealMath.estRemaining` directly, because its frame is the
  drain STREAK's own `lo` rather than the range's, it converts the result to pages against
  `maxKeys`, and its precision guard is written against the window reading's exact
  degenerate branch (a consumed span that rounds to zero, which it fails OPEN on). Which
  range the fleet steals from and when an owner carves is a fleet-wide ranking decision;
  whether one owner has the local runway to earn back a speculative fetch is not, so
  `readahead=on rate_anchored_sensing=on` leaves that gate on the window reading.
- **The default reading since 0.2.0: `RateAnchoredEstimator`
  (`rate_anchored_sensing=on`).** The window reading above is degenerate on a
  deep-nested keyspace: a cursor that agrees with `hi` across all `K = 12` window bytes
  makes `consumed` underflow to exactly `0.0`, and the estimate collapses to a raw width
  with the range's emitted keys discarded entirely (`NO_VICTIM.all_no_remaining_span`'s
  measurement-artefact warning in metrics-internals §5a is this). The ported sensor —
  raced against the shipped one over a captured-listing corpus in `:swath-sim` and promoted
  out of that race (`SensingVariant.RATE_ANCHORED_FLOOR_QUARTER`, which delegates to this
  same object) — reads

  ```
  estRemaining(w) = max(w.keysEmitted, maxKeys) × clamp(anchoredGeometricFactor(w), 1/4, 16)
  ```

  where the magnitude is the range's own proven mass (mean residual life of a Pareto-2
  law, floored at the page in flight so an un-started range is not scored zero) and
  `StealMath.anchoredGeometricFactor` is the same remaining-over-consumed ratio as above,
  measured in a window anchored at the **cursor's own divergence from `lo`** instead of at
  `[lo, hi]`'s. The band is what keeps geometry an adjustment rather than the estimate: a
  factor below one asserts that less remains than has already come out, which is what
  refused a straggler's owner-side carve at the remaining-work floor until it had emitted
  16 pages — the quarter floor puts that boundary at 16 pages, and is the rung the sweep
  promoted. Both bounds stay exact: an open frontier still scores `+infinity` and a cursor
  at its bound still scores `0`. **This changes which range is stolen from and when an
  owner carves. It remains an A/B arm after promotion to the default**; a run on it
  marks itself
  `TOGGLE.rate_anchored_sensing_on` and emits the sensor's own classification counters,
  one namespace per decision site (`SENSING_OWNER.*` at the owner gate, `SENSING_STEAL.*`
  at victim selection — the two count against different denominators, metrics-internals §5a).
  `rate_anchored_sensing=off` selects the unmarked pre-0.2.0 window control.
- Pick the victim with the largest `estRemaining`. A left-skewed (dense-head)
  victim is attacked by the **density-reflected pivot** and, behind it, the
  **retry-nearer-cursor** bisection in `steal()` (§3) — both walk the pivot back
  into the mass when the upper-half probe is empty, so no separate pivot heuristic
  is needed.
- **Thief probe-storm caps (over-fetch).** On big skewed buckets the thief's
  speculative probes, not pages, can dominate the API bill.
  Two bounded caps curb them without touching correctness (the split CAS/transaction
  tiles regardless of any pivot/probe choice — I4): (1) the empty-upper
  retry-nearer-cursor bisection is bounded to a **log-scaled** per-attempt budget
  `B = ceil(log2(bandWidthBytes)) + MARGIN(6)` (`RETRY.bisect_budget_exhausted` on
  exhaustion) rather than a blunt fixed cap — a fixed B=2 would also cut off the
  legitimate `O(log band width)` wide-gap convergence (a band wider than its content
  genuinely needs multiple halvings to find the dense sub-window); the log-scaled
  budget scales with the gap's own byte width, so real convergence always completes
  well inside it (a closed-form, provable ceiling, not literally unbounded), while a
  pivot whose true convergence depth exceeds even that generous estimate still bails
  rather than committing a doomed near-cursor pivot that loses the CAS race; (2) after
  **K=8** consecutive zero-fan-out `delimiter=/` structure probes the thief stops
  probing (falling to the byte-midpoint/sliver fallback used when no structure probe
  runs; `STRUCTURE.suppressed_zero_fanout`), re-enabling on any non-zero fan-out and via a
  1-in-64 recovery probe. Both restore the "never unbounded LISTs per attempt"
  budget. See `docs/internals/metrics-internals.md` §5.
- **Progress-gated victim eligibility (required — closes the latency livelock).**
  A worker is only a steal victim once it has **committed a non-empty page since it
  was created or last stolen from** (the in-memory `emittedSinceSteal` flag: set
  under `victim.lock` when `commitPage` advances the cursor, §4.2; cleared under
  `victim.lock` by the thief at a successful `splitTxn`). The driver curates the
  eligible pool and `steal()` picks `argmax estRemaining` among **those**. This
  paces re-splitting of one worker to **≤1 split per emitted page**, so the page
  fetch always wins a round before the worker is carved again, and a **freshly
  created child is never carved before it lists its first page**. Without it, when
  workers ≫ available work and pages carry real latency, idle thieves narrow each
  owner's `hi` into the empty gap just above its cursor *faster than the page that
  would advance the cursor returns* — the owner then completes its now-empty range
  with no emit, hands the real keys to a child the waiting thieves re-narrow the
  same way, and `splits`/`api_calls` climb without bound while `total_emitted`
  freezes (reproduced on real public S3 at the default `T=64`, never by
  zero-latency mocks). The gate is a **balance/liveness heuristic only** — splitting
  is correct on *any* victim; the no-gap/no-overlap and termination invariants
  (§6, I2–I4) are untouched — so a stale eligibility read can at worst admit one
  extra (still-correct) split, never a missed key. It generalizes the
  un-started-frontier RETRY (§3) to bounded ranges: the open frontier already
  refuses to split before its first page (`extrapolate → null` ⇒ owner lists page 1);
  the flag extends that "list before you are split" rule to every range.
- Optionally allow a brief over-subscription burst (`active` slightly above
  the target `T`) when `max estRemaining > k · median` to attack a straggler
  without waiting for an idle worker.

### 3.3. Dense-tail placement mechanisms — MEASURED STATUS

Four pivot-*placement* mechanisms attack the dense single-directory tail.
**Correctness is unaffected by all of them** — every pivot
is CAS-guarded (§4.3), so any placement heuristic can only change balance/speed,
never the no-gap/no-overlap tiling (all 13 generality-matrix shapes pass
byte-exact). The mechanisms are **general, but their triggers are overfit to the
two buckets they were measured on**; per-mechanism status follows.

- **Owner-side proactive split at page-commit.** A draining worker
  self-splits its own range at a far-ahead pivot at commit time, instead of
  waiting for a reactive thief probe. This changes *who initiates* a split, not
  the split transaction — the I4 CAS, I1 ordering, and the bounded-LIST-per-attempt
  budget are unchanged. It structurally kills the cursor-passes-pivot race
  (`RETRY.cursor_passed_pivot` all but disappears; nearly every split becomes
  owner-side) and cuts API calls and cost. **Ships DEFAULT ON; `--engine-toggle
  owner_split=off` is the kill-switch.** On low-latency infra it is a **wall-clock
  regression**: it sheds thin ~1-page far-slices off the one hot worker and
  `markStolen()` makes those pages thief-ineligible, so thieves starve
  (`NO_VICTIM` dominates the steal outcomes) and the dense tail runs near-serial
  (`in_flight` collapses toward one) — a real trade-off the kill-switch exists
  to back out.
  **Implementation split (the policy seam, contracts.md §2.1):** the gate chain below it — the remaining-est floor, the page
  rate-limit, the demand gate, the observed-mass child-tail floor, the confetti
  feedback gate, then pivot synthesis, the reflection clamp, and the reflect-lift —
  is `io.varve.swath.engine.policy.OwnerSplitGovernor`, a source-agnostic
  `OwnerSplitPolicy` with no lock/clock/RPC of its own: one page-commit's view in,
  `Skip(reason)` or `Carve(pivot)` out. `io.varve.swath.engine.OwnerSelfSplit` is the
  executor: it translates `WorkerState` into that view, then runs the durable
  split CAS (`splitTxn`) and the child hand-off under the owner's own lock — the
  same primitives §4.3 describes. See `architecture.md`'s component map for the
  package split.
  **Demand-gated:** on a *saturated* bucket the worklist already has enough live
  nodes to keep every worker fed, so an owner self-split adds no parallelism and
  only over-fetches its child's terminal page. It therefore skips the carve when
  `outstanding ≥ Tmax`, where `Tmax` is the fixed configured worker count
  (recording `OWNER_SPLIT.demand_gated`), and floors the child
  mass at two pages (the exact reading is the observed-density one below, not the
  plain `(1−f)·est` span); the split transaction is unchanged (only
  *whether* to split, never *how*), so no-gap/no-overlap and ramp
  (`outstanding < Tmax`) are preserved. The threshold is `Tmax`, not `2·Tmax`: idle
  thieves drain owner-split children as fast as they are created, so
  `outstanding` plateaus near `Tmax` and a `2·Tmax` gate never engages, while at `Tmax`
  an extra split once every worker already has claimable work buys nothing. The
  gate is skipped when `workerCount == 1` (no thief exists there, so "buys zero
  parallelism" is moot rather than true). The adaptive concurrency gauge's
  effective `T` is recorded for diagnosis but does not change this gate.
  **The child-tail floor's exact reading, and its wide-flat blindness
  (`--engine-toggle tail_floor=MODE`, default `reach_floored` since 0.2.0).** The
  pre-0.2.0 `current` floor is not
  `(1−f)·est` but its observed-density correction
  `est × max(0, min(1, densityRatio) − f) > 2·maxKeys`, where `densityRatio =
  trailingEwmaDensity / averageDensity` (`StealMath.childTailBelowObservedMassFloor`;
  `min(1, ·)` means a region *denser* than average never inflates the child's share,
  and the no-signal fallback `+∞` reduces the whole thing to the plain span floor).
  On a thinning tail this is the correction it was built for. On a **wide-flat**
  tail it is structurally blind: measured on `nara`'s single tail range, the ratio
  reads 0.0002–0.0008 (median 0.0003) against `f` pinned at ~0.5, so the reach term
  is exactly `0` and the product is `0` **for any `est`** — all 5,326 owner-split
  attempts over that tail ended `OWNER_SPLIT.floor_reflected_blocked` while the
  position sensor was honestly reporting 322,500–1,653,750 keys remaining, and the
  range drained serially to the end of the run. Because the term is right on some
  shapes and blind on this one, the two candidate cures remain **selectable arms**
  raced from one binary; `reach_floored` is the promoted default, `est_direct` is the
  other raced arm, and `current` is the rollback/control:
  - `tail_floor=est_direct` — block iff `est <= 2·maxKeys`; the window product is
    dropped entirely. Under `rate_anchored_sensing` the estimate is already the
    range's own realized mass, so multiplying it by a byte-geometry window
    double-counts geometry (the same double-count removed one gate upstream at the
    remaining-est floor). Its disclosed cost: a range whose *total* mass clears two
    pages but whose far share honestly does not is now carved.
  - `tail_floor=reach_floored` — keep the product, floor the reach term at
    `TAIL_REACH_MIN = 1/16` (`est × max(1/16, min(1, densityRatio) − f)`). Geometry
    still shrinks the child's share; it can no longer erase it. The 1/16 floor puts
    the admit boundary at 32 pages of estimate, and wherever the real reach exceeds
    1/16 the arm is byte-for-byte `current`.
  Both cure arms are monotonically more permissive than `current` (they can only admit
  carves it refuses), both leave pivot placement and the split CAS untouched
  (I2/I3 unaffected — only *whether* an owner carves changes), and both compose with
  `rate_anchored_sensing` without being coupled to it, so the race keeps its
  factorial of controls. The mode is threaded through the governor to all three of
  the floor's consult sites (the gate, the reflection clamp, the reflect-lift), and
  at each one a cure mode evaluates `current`'s verdict too and records the
  difference (`TAIL_FLOOR.<site>_admit_current_blocks` /
  `TAIL_FLOOR.<site>_would_block_current_admits`, metrics-internals §5) — which is
  what makes a live A/B attributable to the toggle instead of to the run.
- **Reactive far-ahead interpolated pivot** (`StealMath.interpolate(c, H, f)`
  over a bounded range, feeding a per-worker density estimate). The
  foundation the owner-side split reuses; same structural-win / no-speed-win
  verdict. **Keep the far-ahead 0.5–0.75 skew** — dropping it regresses the
  matrix on sustained-occupancy shapes.
- **Rank-space (alphabet-aware) pivot synthesis.** Infers the per-position
  alphabet from pages already fetched and interpolates in rank space so pivots
  land on populated hex/UUID values instead of sparse-alphabet dead zones. As
  built it **never engages** (`ALPHABET.alphabet_chosen=0` across all 13 matrix
  shapes): per-position scalar choice is anchored at static birth
  bounds and consulted exactly where no strictly-between scalar exists (adjacent
  bounds) or the cursor has drained past the stale `base+8` window → `NO_SCALAR`
  → the same pivot-fallback cascade as the two mechanisms above. Dead weight as
  built.
- **Double-buffered (prefetched) owner page fetches.** Removed: never hit, since
  a page commit is negligible against a wide-area RTT. Could matter for
  millisecond-RTT stores (MinIO-class), which would need re-measurement there
  first.

### 3.4. Intra-range speculative readahead — engagement and tuning

§2.1 gives the *safety* argument for readahead — how a speculative page is adopted
back into the serial loop exactly-once, off-gauge, and fail-soft. This section covers
**when readahead engages, how it is tuned, and why**; it is what the
`ReadaheadConfig` tuning knobs point back to.

Readahead attacks the one thing stealing cannot: a **single** bounded dense range that
has collapsed to a serial owner drain — one page per RTT, its next `start-after` being
this page's last key, no structure left to split on. Splitting parallelizes *across*
ranges; on such a tail all remaining mass lives *inside* one range, so the fix is to
pipeline the fetch chain itself — fire `K` guessed `start-after` fetches ahead of the
cursor and adopt them as the cursor reaches them (§2.1). It is **opt-in and off by
default** (`--tune engine.readahead=on`); off, the scanner is exactly the pure serial
loop of §2 with no readahead state and no `READAHEAD.*` counters.

- **Engagement discipline — a consecutive-full-page streak, not a position estimate.**
  A range engages only after it has drained a fixed number of **consecutive full,
  truncated, un-split serial pages** (a *full* page = a non-adopted, `maxKeys`-returning,
  forward-spanning page) — six by default. The streak counts full pages drained
  **without `hi` being narrowed by a steal or self-split, and resets to 0 on every such
  narrow**. This is what distinguishes a *transient* dense stretch (which the plain serial
  scan absorbs at zero extra cost, and whose ~3-RTT warm-up would be pure waste) from a
  genuinely sustained drain worth pipelining. **This streak is deliberately not a knob to
  raise to suppress engagement.** It is the *only* config value that reduces engagement
  count, but raising it does so indiscriminately: because an owner is narrowed frequently
  under normal work-stealing, requiring a much longer un-interrupted streak makes
  engagement rare for *every* bucket rather than targeting collapsed tails, sacrificing the
  sustained-drain win it exists to capture.
- **Why a remaining-pages floor was rejected as the primary discipline.** An engage-time
  floor on estimated remaining pages `(cursor, hi]` — engage only if enough work is left
  to amortize the guess overhead — was tried and is **disabled by default**. It defers an
  *already-decided* engagement based on a position-in-range estimate, so on a short
  late-range tail it keeps re-deferring and re-timing engagement, eroding the very
  sustained drain's throughput more than it saves. (Estimating it correctly is also subtle:
  the whole-range `estRemaining` (§3.2) anchors its "consumed" sentinel at a null lower
  bound, which — since real keys occupy only a narrow high sub-band of the byte space —
  undercounts remaining work by orders of magnitude; a usable floor must re-anchor on the
  streak's own observed start.) The consecutive-full-page streak instead gates each
  engagement on a fixed, re-earnable signal and never re-times a decided one — that is the
  duration discipline in force. The floor mechanism is retained (a non-zero config value
  re-enables it) as a discriminator for the residual case the adoption rate alone cannot
  separate (below), but it is off by default.
- **Disengage on low adoption.** Once engaged, each processed page's outcome
  (adopted vs serial-fallback) folds into a small **tumbling** window — `2·K` pages by
  default, two replenish cohorts: long enough to be statistically meaningful and ride
  warm-up jitter, short enough to abandon a non-paying engagement within tens of pages and
  render its verdict *during* a typical engagement rather than after it. If the adopted
  fraction over a full window falls to a floor (0.40 by default), the guesses are not
  paying off and the range **disengages and reverts to plain serial**; a later fresh streak
  can re-engage a drain that resumes. The floor sits with wide margin between a
  transient-stretch adoption rate (~0.25, where readahead is pure overhead) and a healthy
  sustained drain's (~0.7–0.8), so it separates the two on adoption alone — **except** when
  two bucket shapes share an indistinguishable adoption rate but differ in how much a hit is
  *worth* (an expensive slow fetch hidden vs a cheap fast one); the disabled remaining-pages
  floor above is the retained discriminator for exactly that case.
- **`K` guess-ahead cursors and a bounded adoption buffer.** `K` (8 by default — a
  measured aggregate-pages/s win on a real dense tail over the serial chain) is both
  the guess-ahead depth and the memory bound: **at most `K` speculative pages exist at any
  instant** (in-flight + buffered ≤ `K`, ≈ `K × maxKeys` keys). Guesses that land far ahead
  simply occupy `K`-slot budget as HOLD pages rather than bloating; once the budget is full
  no new guess launches (demand back-off). All of this state is process-local and never
  durable (§2.1).
- **An isolated, wall-clock-bounded fetch budget.** Speculation fetches through a
  **dedicated off-gauge fetcher**, never the worker's own slot-gated one — so a disposable
  guess neither competes for an AIMD permit nor can reach the worker fetcher's bounded-retry
  give-up path that cancels the whole run (§2.1). Because a genuine voting 503 retries
  *unbounded* inside the fetcher regardless of gauging, each speculative fetch carries a
  wall-clock budget (30 s default) covering *all* its internal retries; past budget the
  guess is abandoned as a `speculative_fault` and reclaimed, so the owner's forward progress
  can never block indefinitely on a disposable guess. The default is set comfortably above a
  healthy RTT and above the worker's own per-attempt timeout plus a couple of backoff
  retries, so a genuinely recovering fetch is not discarded prematurely, while a persistent
  throttle storm still costs only a small fixed bound per guess.

---

## 4. Checkpoint, commit, and the split transaction

The worklist **is** the checkpoint table. Each range is one
`listing_node` row (schema in `contracts.md` §3).
All DB writes funnel through **one checkpoint-writer thread** (SQLite WAL is
single-writer), so `commitPage` and `splitTxn` are **serialized** against
each other — no two transactions interleave, which is what lets the split be
a standalone transaction (below) rather than being folded into a page commit.

### 4.1. The writer protocol (request → ack)

Workers don't touch SQLite directly. They enqueue a `CommitRequest` /
`SplitRequest` carrying a **completion future**, and block on it where the
invariant demands durability:

- The writer drains its queue and executes requests **in arrival order**,
  batching consecutive requests into one transaction (flush on queue-empty,
  or every `N` requests / `M` ms — whichever first), then completes each
  request's future. Batching may merge transactions but **never reorders a
  node's commits** (arrival order is preserved).
- **`commitPage` future** must complete (durable) **before** the worker emits
  that page (I1, commit-before-emit). A `splitTxn` future must complete
  before the thief lists the child.
- **The worker advances its in-memory `cursor` under `victim.lock` at the
  moment it *enqueues* the commit — not when the future resolves** — then
  releases the lock and awaits the future *outside* it (the commit-before-emit
  wait does not hold `victim.lock`). This keeps the in-memory cursor a
  non-lagging *leading* edge for a concurrent thief (§3): a steal that observed
  `cursor < m` can never be invalidated by an already-enqueued commit, because
  that commit advances the DB cursor only to a key the thief already saw.

### 4.2. Per-page commit (`commitPage`)

In one transaction:
1. (if `--resume-output`) append the page's raw, **pre-filter**, byte-exact
   entries to `output_journal` at `(node_id, MAX(page_seq)+1)`;
2. update the node: if the batch was non-empty `cursor = advanceTo`
   (= last in-range source key, before final output filters); set `status = COMPLETED` when `completed` (i.e.
   `reachedBound` or `!truncated`), else `IN_PROGRESS`. **An empty batch
   leaves `cursor` unchanged** (§2). **This includes the post-narrow case:** a
   page fetched under the old wider bound whose keys all now exceed the new `m`
   yields an empty in-range batch with `reachedBound = true` → the node
   **completes without advancing `cursor`** (it does not spin or retry forever);
3. commit. Only then is the page emitted.

### 4.3. Split transaction (`splitTxn`) — standalone, CAS-guarded

A split is its **own** transaction (not merged with a page commit), made safe
purely by serialization through the writer plus a SQL guard:

```sql
UPDATE listing_node
   SET range_end = :m, generation = generation + 1
 WHERE id = :victim AND cursor < :m            -- victim hasn't passed m
   AND range_end IS :oldHi                      -- bound unchanged since snapshot (rejects a 2nd thief's stale split)
   AND status <> 'COMPLETED';                   -- victim not already done (an empty/terminal page completes w/o advancing cursor)
-- if rowcount == 0: ABORT (victim advanced/completed, or range_end already moved) → thief restores hi and RETRYs
INSERT INTO listing_node (run_id, parent_id, range_start, range_end, cursor,
                          status, generation)
     VALUES (:run, :victim, :m, :oldHi, :m, 'PENDING', 0);
```
(`range_end IS :oldHi` uses SQLite `IS` so it also matches the frontier
`oldHi = NULL`.)

The guard has **three** clauses, each closing a distinct hazard:

- **`cursor < m`** — cannot fail *because the cursor passed `m`* in a single
  process: the thief checked the *leading* in-memory cursor under `victim.lock`
  (§3, §4.1), and any enqueued page commit (arrival-ordered before this split)
  advances the DB cursor only to a key `< m`.
- **`range_end IS :oldHi`** — rejects a **second thief** whose pre-lock snapshot
  of the bound is stale. The in-memory `victim.hi != H` re-check (§3) catches
  this first; this clause is the durable backstop (and the multi-host guard).
- **`status <> COMPLETED`** — rejects splitting a victim that **finished via an
  empty/terminal page**, which advances `status` without advancing `cursor` (so
  `cursor < m` alone would wrongly admit the split).

Either of the latter two firing is a legitimate `ABORT` (rowcount 0): the child
is **not** inserted, the thief restores `victim.hi = H` (safe — `H` was the
bound it *validated under the lock*) and retries; the victim keeps owning the
whole `(lo, H]`. A **crash** mid-split leaves either both rows or neither (one
transaction), so the range set always tiles the keyspace; resume reconciles from
the durable rows (§4.6). For a **future multi-host** writer the same guard
rejects a stale split, but reconciliation is the resume reload's job
(`generation` / `owner_lease`), **not** a process-local `volatile` restore.
**The victim's `cursor` is never advanced by the split** — only its `range_end`.

### 4.4. Quiescence / termination

`outstanding` (an `AtomicLong`) counts live nodes:
- **Incremented when a node is created** — for a seed node, at seed time; for
  a split child, **inside `splitTxn`, before `victim.lock` is released**, so
  the child is counted before the victim can advance to its new bound and
  decrement.
- **Decremented only after** a node is `COMPLETED` *and* any split it spawned
  is committed.
- The run ends when `outstanding == 0`. An empty ready queue alone is **not**
  termination (a worker may be mid-page about to split/complete).
- **On resume, `outstanding` = the number of non-`COMPLETED` nodes loaded.**

### 4.5. Parquet output durability (the two-cursor model)

Streaming output to **rotating** Parquet/file parts needs a durability
boundary distinct from listing progress, because a part file's rows are not
durable until its footer is fsynced:

- **Sticky node→writer:** every page of a given node goes to one writer (of
  the configured bounded pool, by `node_id % writers`). A writer serves many nodes and rotates
  its part file by target size, or, whichever fires first, by time-open or
  row count (the bounded-cadence triggers, evaluated on the
  writer's own thread — on write and on an idle-timeout wakeup); a
  node's pages thus occupy a *contiguous* sequence of that writer's parts,
  which finalize in creation order.
- **`durable_cursor`** (a second per-node column): the highest key all of
  whose pages are in **finalized** (footer-fsynced) parts. When a writer
  closes a part, one transaction marks it `finalized`, records it in checkpoint
  `part_file`, and advances `durable_cursor` for each node whose pages it held
  (safe because earlier parts of those nodes already finalized). The consumer
  manifest is written once, after every lane drains successfully.
- A node is **output-complete** iff `status = COMPLETED` *and*
  `durable_cursor == cursor`.
- **Resume (Parquet):** discard every non-finalized part; for each node
  set `cursor := durable_cursor` and `status := PENDING` unless
  output-complete. The not-yet-durable tail re-lists into **new** parts.
  Finalized parts are never discarded or rewritten ⇒ **exactly-once** (no
  finalized row is lost or duplicated). For stdout (at-most-once),
  `durable_cursor == cursor` always — the machinery degrades to the simple
  commit-before-emit case.

### 4.6. Resume (listing)

Load every non-`COMPLETED` node; `IN_PROGRESS → PENDING` **preserving
`cursor`** (and resetting to `durable_cursor` for Parquet as described above), **clearing `owner_lease`**
and **bumping `generation`**. `COMPLETED` nodes are skipped. Re-run stealing
from this seed set. No re-probe, no re-seed (the `args_hash` gate,
`contracts.md` §5, guarantees the same scope). Note
`generation`/`owner_lease` exist for
crash/resume idempotence and a future multi-host mode; within one process the
in-memory `victim.lock` is authoritative.

---

## 5. Adaptive concurrency (AIMD)

`--concurrency` supplies the ceiling `Tmax`; the live target `T` is a
**resizable permit gauge**, not a constant:

- **Slow-start ramp** — a fresh run's effective `T` starts at
  `min(4, Tmax)`, not at `Tmax`: seeding every worker at `Tmax` from `t=0`
  storms a shared endpoint immediately (every attempt-timeout destroys its
  TLS connection, so the storm self-amplifies), and after a tail-stall
  `STUCK` the remaining `swath resume` work IS the wedged tail — a naive
  `Tmax`-pinned restart would re-storm it. While no congestion has been
  observed (no WORKER, i.e. `slotGated=true`, attempt-timeout, no 503
  down-vote, no sustained-timeout shed) a clean-window success grows `T`
  **multiplicatively**
  (`T := min(Tmax, T*2)`) instead of the additive `+1` below, so a healthy
  endpoint ramps 4→8→16→32→64 in a handful of paced steps (~4 s). The
  **first** congestion signal of the run latches this off for good — growth
  reverts to the cautious additive `+1` for the rest of the run.
  A probe-class transient (the thief's
  slot-free pivot/structure probe hitting an attempt-timeout or a network
  fault) does NOT count as a congestion signal here or feed the transient
  growth-freeze window below — only a WORKER (slot-gated) attempt-timeout
  does, mirroring the shed-side exclusion at the same call site. The latch
  and the growth step (double-vs-`+1` decision, the `T` update, the permit
  release) are one atomic step, so a signal can never land *between* the
  decision and the update and let a success double after congestion was
  already seen. Engagement counters: `swath.steal_reason{AIMD,
  slow_start_double}` per doubling step, `swath.steal_reason{AIMD,
  slow_start_exit_congestion}` once, on the latch.
- **Decrease on stress** — a 503 `SlowDown` / `ServiceUnavailable` or a
  `Retry-After`: `T := max(1, floor(0.7·T))` and
  **pause new steals**. Workers above the new `T` finish their *current
  page* (never killed mid-page — that would lose the cheap cursor advance)
  then park. The
  10 s clean-window cool-down re-arm (`lastThrottleNs`, gating the `+1`
  recovery and the relaxation valve below) is **conditional on the decrease
  actually reducing `T`** — a floor no-op (`T` already at 1, or rounding
  produces no change) does not re-arm it, only a real reduction does. This
  preserves the brake: the valve's own starvation gate, not the cool-down, holds
  `T` at the floor under a genuine sustained storm.
  **Per-call backoff is the SDK's `RetryStrategy.standard()`**
  (exponential backoff + jitter, **no** client-side rate limiter): swath's
  AIMD on `T` is the *single* adaptive controller. Do **not** use
  `RetryStrategy.adaptive()` — its client-side token bucket also throttles on
  503, and two adaptive loops reacting to the same `SlowDown` over-correct (the
  run dips harder and recovers slower than either alone; a run that merely
  "completes" would still pass, hiding the regression). ("No client-side rate limiter" here
  is scoped to this REACTIVE control loop — the 503-driven `RetryStrategy`/AIMD
  pair — not a statement about the CLI surface as a whole: `--request-rate`
  (`contracts.md` §7) is a separate, opt-in, off-by-default PROACTIVE cap a user
  requests ahead of time via Bucket4j, never triggered by or wired into this
  adaptive loop. See `docs/metrics-and-observability.md` §1.1 for the
  proactive-vs-reactive distinction.) Every `multiplicativeDecrease` (both
  this trigger and the sustained-timeout shed below, kind-agnostic) emits
  a `T`-band engagement counter (`AIMD/decrease_at_floor|low_t|mid_t|high_t`,
  tagged by the `T` read at entry) plus `AIMD/floor_noop_rearm` when the CAS
  produced no numeric change (the vote/shed and stealing-pause side effects
  still fire unconditionally; the cool-down re-arm does not — see the bullet
  above), and `onSuccess()`'s cool-down gate emits
  `AIMD/growth_blocked_cooldown` per suppressed growth opportunity
  (`docs/internals/metrics-internals.md` §5a).
- **Sustained-timeout shed** — a *second, distinct* decrease trigger for a
  sustained client attempt-timeout storm, which a 503 vote never catches (a
  timeout is not store backpressure) and the growth-freeze cannot brake
  at `Tmax`. On a rolling ~30 s window (per-process jitter [25,40] s, RED
  desync): if `timeouts >= max(3, ceil(0.3·T))` **and** progress is starved
  (`successes <= max(1, floor(T/32))`), shed once per window
  `T := max(1, floor(0.5·T))` and **pause new steals**. The starvation gate is
  the load-bearing clause: a timeout tail on a still-progressing run never
  sheds. Reuses the 503 decrease machinery but records `swath.aimd.timeout_shed` (never
  `swath.aimd.votes`, which stays real-503-only).
- **Increase on health** — a clean window (e.g. no throttles for 10 s):
  `T := min(Tmax, T+1)`; unpark one parked stealer. During slow-start, before
  the congestion latch above has fired, this step is multiplicative
  (`T*2`) instead — see the slow-start bullet.
- **Latency-inflation freeze — a damper, not a latch.** A growth GATE (never a
  decrease): while the successful-attempt
  latency EWMA exceeds `LATENCY_FREEZE_FACTOR` (2×) the Vegas-style rolling-
  minimum baseline, the `+1`/doubling growth step above is held. On a dense
  tail (intrinsically heavy pages, not real overload) this rung can latch
  frozen indefinitely, because the baseline is a rolling *minimum* and any
  occasional fast page keeps it low relative to the inflated average, so the
  latch is demoted to a **paced additive relaxation valve**: while
  latency-frozen **and** the worker-timeout growth-freeze is NOT also active
  **and** the run is making progress (successes above the shed's
  starvation gate — the exact complement), admit one paced additive `+1`,
  at most once per `VALVE_PACE_NANOS` (~30 s, the shed-window scale). The
  valve never doubles (a frozen-latency relaxation must stay gentle) and
  never fires under a worker-timeout storm (`growthFrozen()` is checked
  first — the hard latch is preserved there). `FREEZE/latency_inflation`
  still fires on every frozen opportunity, valve-admitted or held, so the
  opportunity count is not distorted by the valve. Net effect: a dense tail
  ratchets `T` out of a collapsed floor at `+1` per ~30 s window, while a
  genuine overload's 503/timeout decrease paths (multiplicative, per-window
  capacity ≫ 1) still dominate the valve's additive-only `+1`, so the loop
  recovers without defeating those explicit stress signals.
  A completed successful attempt feeds its latency sample before that same
  completion may claim a paced growth step; the decision never evaluates a
  request's status first and publishes its latency evidence afterward. This is
  an event-ordering precondition for the latency gate; it does not aggregate
  evidence across concurrent completions or make latency vote the target down.

**Capacity boundary.** AIMD is a reactive backpressure controller, not an online
throughput optimizer. A 503 or a starvation-gated timeout storm can reduce `T`;
successful-latency inflation can only hold future growth. Consequently, a clean
endpoint can reach a high `T` before queue latency catches up, and the controller
does not search back down merely because a lower `T` would deliver the same keys/s
with less CPU, memory, connection pressure, or latency. `Tmax` therefore remains an
operator resource ceiling. Size it from repeated runs and inspect the trajectory's
achieved `in_flight`, worker pages/keys per second, successful latency, and live AIMD
target; do not infer capacity from `Tmax` alone. A replay experiment on one bucket or
latency regime is not a universal S3 cap.

Uneven progress needs no separate mechanism: a finishing worker becomes a
thief that targets the laggard (`argmax estRemaining`) — stealing *is* the
rebalancer. The retry-nearer-cursor probe (§3) makes it bite faster on a
dense head.

**Per-fetch transient-retry cap disposition — single liveness owner.**
Independent of `T`, each WORKER fetch (`slotGated=true`) retries
a non-AIMD-voting transient (attempt-timeout / network fault) up to
`MAX_TRANSIENT_RETRIES = 8` times with jittered backoff before this
cap-shaping threshold engages. What happens once a fetch crosses it depends
on a `RetryPolicy` resolved once at CLI wiring time from whether a real
`LivenessWatchdog` is armed — chosen so liveness death is owned by exactly
ONE mechanism, never a cap racing the watchdog:
- **`RIDE_OUT`** (a watchdog is armed, the default) — the cap no longer
  cancels the run. The fetch retries indefinitely; crossing it only raises
  the full-jitter backoff ceiling (5 s → 15 s, fewer TLS handshakes during a
  self-amplifying storm) and records `swath.steal_reason{TRANSIENT,
  storm_ride_out}`. The watchdog alone decides when a genuinely-wedged run
  ends.
- **`BOUNDED`** (both watchdog windows disabled by flags) — nothing else
  could ever stop an unbounded ride-out, so cap exhaustion keeps the legacy
  disposition: cancel the run `STUCK` (resumable exit-75, attributing
  `CancelSource.TRANSIENT_RETRY_CAP`), recording `swath.steal_reason{TRANSIENT,
  retry_cap_stuck}`.

A fetch with no `CancellationToken` wired (embedded use) is unaffected by
this policy and stays count-bounded, escaping as the fatal `ListingException`
contract on exhaustion. This drives the run's `stop_source`/`error_class`
marker fields.

**Probe fetches are NOT governed by `RetryPolicy`.** The thief's speculative probe fetches (`slotGated=false`
— structure probe, empty-upper/reflect/flat-leaf pivot probes) get their own,
independent, much smaller fixed cap (`PROBE_TRANSIENT_RETRY_CAP = 1`, one
grace retry) — neither `RIDE_OUT` nor `BOUNDED` applies, and cap exhaustion
never touches the `CancellationToken`. Measured evidence: letting a probe
ride out the SAME schedule a worker uses let ONE camping probe — mid-flight
against an already-`stealEligible` victim when a storm starts — consume the
MAJORITY of total request volume, because it holds the sole idle-steal
in-flight slot for the whole storm; idle-worker count never
multiplies this (at most one probe is ever in flight), but ONE unbounded
probe already dominated. A probe is disposable (a later steal attempt
retries once the idle-steal backoff allows, against the same or a different
victim), so it fails fast instead: the `ThrottleException` is caught inside
`Thief.steal` and folded into the SAME non-productive-steal `RETRY` outcome
an ordinary retry takes, recording `swath.steal_reason{TRANSIENT,
probe_retry_cap_failfast}` (fetcher side) and `swath.steal_reason{RETRY,
probe_retry_cap_failfast}` (thief side). Single-liveness-owner is unchanged:
a failed probe ends nothing.

At run end the implementation emits a `list_run_diagnostics` fingerprint line,
read back from a bounded set of Micrometer counters (`swath.steal_reason{outcome,
reason}` plus the sibling `swath.probe.*`/`swath.split.*`/`swath.page.*`/
`swath.throttle.*`/`swath.aimd.*` scalars — metrics-and-observability.md §1,
docs/internals/metrics-internals.md §5; these are unified onto Micrometer rather than a parallel hand-rolled
counter map). It records the selected strategy and reason, steal outcomes by
branch reason, 1-key probe count, empty-upper bisections, split
commits/aborts/unsplittables, parallelism ramp timings, page shape, and AIMD
throttle/reduction counts. This distinguishes flat, deep, probe-heavy,
serial-paced, parallelizable, and throttled buckets; the tag shape
(`{outcome,reason}`, a bounded ~30-50 value enum) keeps the meter set's
cardinality low even though it now does extend the public meter contract.

**Simulator port (deferred extraction).** `ConcurrencyGauge` above is, and remains, the only
implementation of this section swath ships — nothing here is extracted or wired to a policy
interface. `io.varve.swath.engine.policy.ConcurrencyPolicy` (contracts.md §2.1)
instead documents the PORT a simulator's own faithful reimplementation carries: the
reactive inputs above (success / 503 / timeout-shed / latency, each arriving with its own explicit
timestamp rather than reading a clock), the two outputs (`effectiveT`, `isStealingAllowed`), and the
internal windows/latches enumerated in that interface's javadoc that a faithful port must reproduce.
Extraction of the real controller behind it is deliberately deferred — AIMD is the most
timing-coupled mechanism in the engine (the clean-window cooldown, the jittered shed window, the
relaxation valve, and the decaying latency baseline above all race under CAS), and the divergence
risk of a simulator-side port was judged low enough not to justify carving the live controller out
from under its concurrent callers.

---

## 6. Correctness argument

Let the prefix scope be `(⊥, ⊤]`. Let the **range set** be all live ranges
`{(lo_w, hi_w]}` plus all `COMPLETED` ranges.

**Invariant I (partition).** The range set is always pairwise disjoint and
its union is `(⊥, ⊤]`.
- *Base:* a single range `(⊥, ⊤]`, or the seed partition, which tiles
  `(⊥, ⊤]` by construction (consecutive cut-points, boundary-belongs-left).
- *Step (split):* a range `(lo, H]` whose owner has completed `(lo, c]` and
  with `c < m < H` is replaced by `(lo, m]` (victim) and `(m, H]` (child).
  `(lo, m] ∪ (m, H] = (lo, H]`, disjoint. Committed atomically (§4), so the
  invariant holds across crashes.

**No overlap.** By Invariant I every key lies in exactly one interval;
`start_after` exclusivity + boundary-belongs-left place each boundary key in
exactly one interval; the per-key `hi` re-read + CAS hand-off (§2, §3)
prevent a victim from emitting a key now owned by the child. So no key is
listed by two workers.

**No gaps (completeness).** By Invariant I the union of live + completed
ranges is always `(⊥, ⊤]`. When every range is `COMPLETED`, the completed
union is `(⊥, ⊤]`, so every key present throughout the scan is emitted by
exactly one owner.

**Termination.** Every non-empty page advances some cursor by ≥ 1 key; keys
are finite; the stuck-token defense forbids an infinite single chain.
Splitting cannot subdivide below a safe pivot (pivot synthesis returns `null` →
terminal `unsplittable`), and a committed child was first proven populated, so
#splits ≤ #keys. An empty upper probe instead invokes the budgeted fallback
ladder and is not cached as terminal. When no splittable victim remains and
`outstanding == 0`, the engine joins and completes.

**Under S3 non-snapshot pagination.** Correctness here is on the **key axis**
(ranges), so it is robust. A key present for the whole scan is in exactly
one interval → listed once. A key *inserted into an already-passed region*
(`< owner.cursor`) is missed — identical to any paginated lister including
`aws s3 ls`; documented, not introduced here. A key inserted ahead of all
cursors is found. Because overlap is structurally zero, swath **never
double-emits** — unlike S3P, whose overlapping reads can return a key twice
under churn and need a dedup pass. Work-stealing adds no consistency anomaly
beyond S3's inherent behavior.

**Why skew is handled.** A bad pivot is not permanent: the instant a worker
idles it targets the *current* busiest worker's *remaining* range. On an empty
upper probe, the thief steps back, can consult structure or density-reflection
candidates, and then bisects toward the cursor within a log-scaled probe budget.
Homing in on a density-skew ratio ρ can therefore cost `O(log ρ)` 1-key probes
within and across steal attempts. Wall-clock is
bounded by the granularity you keep splitting to, not by the luck of one
split.

---

## 7. Cost

`N` objects, `P = ⌈N/1000⌉` pages, `W` target concurrency, `ρ` density-skew
ratio.

| | API calls | CPU | Memory | Balance under skew |
| --- | --- | --- | --- | --- |
| **`WorkStealingScan` (HYBRID-seeded)** | `P` + (seed: 1–few) + `O(W·log ρ)` 1-key probes | `O(N)` parse | `O(W)` active ranges/buffers + `O(parts)` output metadata + `O(segments)` when sorting | **good** — re-steal self-corrects to `O(log ρ)` |
| S3P overlap-bisection | `≈2P` (2× bill) + dedup | higher (≈50% wasted keys) | `O(W)` + dedup | good |
| Galloping bisection (old §5.3) | `P` + `O(W)` probes | low | `O(W)` | poor (one-shot peel) |
| Recursive `delimiter=/` | `P` + `D` prefixes | low | `O(tree frontier)` | great on trees, ~serial on flat/skewed |

`WorkStealingScan` matches the cheapest options on API cost (`≈P`), beats
S3P ~2× while avoiding its dedup CPU and double-emit risk, and matches S3P
on balance — with no hints and no overlap. Active range state, queues, and
writer buffers are configuration-bounded; finalized-part metadata is
`O(parts)`, and sorted staging metadata is `O(segments)`.

---

## 8. Seeding (the HYBRID)

Controlled by `--tune seed.mode=shallow|none|hints` (default: `shallow`). `SeedStep` runs
before any worker claims a range; the resulting nodes are inserted atomically via
`CheckpointStore.insertNodes` (all-or-nothing, invariant I2 — the seed set is itself
a valid partition from the first durable moment or it does not exist at all). On
`swath resume` the seed step is skipped (nodes already present).

**Implementation split (the policy seam, contracts.md §2.1):** the
whole shallow-mode descent below — the span-priority frontier, probe-budget accounting,
per-level classification (narrow / partition fan-out / flat-wide radix banding / tiny-leaf
explosion vs. heavy-cut via the sampled-sibling prior), and cut-set assembly plus the
mass-weighted subsample to the target seed count — lives in
`io.varve.swath.engine.policy.HybridSeedPlanner`, a source-agnostic `SeedPlanner` with no RPC,
page decode, or node insertion of its own; a future seed-diet policy or hints-file planner is
meant to slot in as an alternative `SeedPlanner` implementation behind the identical
`SeedDescent` request/response contract. `io.varve.swath.engine.SeedStep` is the executor: it
drives `HybridSeedPlanner`'s `SeedDescent` through a request/response loop (issuing every
bounded `delimiter=/` probe it asks for, decoding each page into a source-agnostic
`SeedProbeOutcome`), then tiles the finished cut set into fresh `NodeSpec` ranges and inserts
them. Unlike the thief/owner-split policies, the seed descent has no `View` to read and no
mutation to apply back — its frontier and probe budget are private state it owns outright,
never shared, since seeding runs single-threaded before any worker starts (see `contracts.md`
§2.1 for why that makes its shape a third, deliberately different one). See `architecture.md`'s
component map for the package split.

- **Default — `--tune seed.mode=shallow` (`delimiter=/` pass).** One (or, for very broad tops,
  1–2 levels of) `delimiter=/` listing returns top-level common prefixes
  `p1 < p2 < … < pk`. Seed ranges `(⊥, p1], (p1, p2], …, (pk, null]`.

  **Scope-closing sentinel.** That final `(pk, null]` range is the one the owner-split governor
  refuses outright (`hi == null` ⇒ `OPEN_FRONTIER`), so any mass behind it drains on a single
  worker however large it is — measured at 95.2% of one real bucket, whose mass sat under its
  LAST top-level prefix. When the top scope was listed to completion AND its greatest returned
  item is a `CommonPrefix pk`, every key in scope is provably `< prefixCeil(pk)`, so that bound
  is appended as one further cut: `…, (pk, u], (u, null]` with `u = prefixCeil(pk)`. The final
  range is then EMPTY by construction and the mass-bearing range has a finite `hi`, making it
  eligible for owner self-split and for the thief's delimiter-structure path. Costs no probe and
  adds exactly one cut, so the static tiling is not enlarged — the region is handed to the
  dynamic splitter. Declined (each refusal with its own counter, §5) when the top is truncated
  and unrecovered, when the scope returned no `CommonPrefixes` at all, when it reported no
  combined greatest returned item to verify the bound against, when a direct object sorts past
  `pk` so the ceiling would not bound it, when the cut set already fills `targetSeeds`, when
  `prefixCeil(pk)` is unbounded (an all-`0xFF` prefix), or when the bound is not strictly above
  the last cut.
  Note the empty final range is not free — see the open-tile follow-up issue. Now
  every range except the last has a finite `hi`, so exact byte-midpoint
  applies almost everywhere and only one range is ever open-ended. This is
  the old `RecursiveDiscovery`, demoted to a seed step (the
  release-permit-before-fan-out detail still applies if the seed pass itself
  parallelizes).
  - **The seed is bounded — never an exhaustive prefix enumeration.** The
    cut-point count is capped at `min(1000, 4×W)` where `W` is the worker
    count (e.g. W=64 → cap=256). If the first delimiter page returns more
    prefixes than the cap, `subsampleEvenly` picks a representative subset so
    the seed set stays proportional to available parallelism without exploding
    split count. If the top level is itself truncated (>1000 prefixes — e.g. a
    flat `pid=<hex>/file` layout), **do not keep paging it** (default): use
    what you have and let the final open range `(p_last, null]` be
    flat-scanned and work-stolen — the ONE exception is `mass_aware_seed`
    (default ON; `--engine-toggle mass_aware_seed=off` is the documented
    opt-out), which reads at most one extra top page so
    mass-weighting/tiling can see the overflow prefixes past the first page;
    sub-levels are never paged further either way. Seed probe cost is bounded
    by `maxProbes` (worker-count-derived, `≤256`), spent best-first by the
    span heuristic below rather than walked per-directory — a directory
    explosion cannot blow the wallet, only spend it. On shallow/narrow
    buckets the frontier exhausts long before the wallet does, so the old
    ~one-probe-per-level cost still describes the common case (e.g. a
    1-object-per-leaf Hive/Spark tree costs one seed call + a flat scan).
  - **The seed's cost is bounded in calls; its wall clock is bounded in round
    trips.** The two are not the same budget, and only the first is capped
    above. The descent is **serial** — it polls one frontier node, probes it,
    and enqueues that probe's children before choosing the next — so seed wall
    time is approximately `maxProbes × probe RTT`, and raising `--concurrency`
    lengthens it rather than shortening it (a higher worker count buys a larger
    `maxProbes`, and the probes still run one at a time). A `delimiter=/`
    structure probe is also the most expensive request shape S3 serves, since
    the store must scan and aggregate to return `CommonPrefixes` rather than
    hand back the next page of keys. For an anchor: a 256-probe descent under
    the replay server's `prod-commoncrawl` latency injection
    (`223ms + 55ms per CommonPrefix`) took **~2m48s**, during which the run
    emits no objects at all. This is why the live progress display carries a
    `seeding` phase with a probe count and the age of the last completed probe
    (`docs/metrics-and-observability.md` §4): every listing-shaped counter reads zero for
    that whole span, so a healthy seed and a wedged one are otherwise
    indistinguishable. Nothing here parallelizes the descent today; the probes
    are independent and could be batched, at the cost of the frontier's
    per-probe feedback, and that trade is unmade.

    Multi-level adaptive descent: descend one additional delimiter level only
    when a sub-level's prefix count is *non-truncated and narrow* (below the
    cap). An exploding or truncated sub-level is classified and disposed of
    **per cut** (tiled/banded/left whole, per the shapes below) and the
    descent **continues** over the rest of the frontier — an unrelated
    sibling exploding first must not strand a sibling with real mass at one
    giant, near-serial range. Under `mass_aware_seed` (default on), the
    not-yet-probed prefixes are held in a best-first frontier — ranked by the
    keyspace-gap span to each cut's next sibling — that is **maintained across
    every insertion**, not sorted once before the descent starts: a wide
    subtree discovered several probes into the descent still outranks (and is
    probed before) a narrow one already queued, so a probe budget too small
    for the whole tree biases toward the widest-spanning regions instead of
    draining itself on whichever narrow siblings happened to enqueue first. A cut with
    no successor *yet* in the global cut set falls back to the upper bound of
    the scope it was discovered in — the ceiling of the directory being probed
    (or the scan's own prefix ceiling at the top level), never the whole
    keyspace — so an unmeasured tail scores as a genuine, bounded gap instead
    of unconditionally outranking everything else; without that scoping, a
    lexically-last cut's own lexically-last child would inherit the same
    unconditional top priority at every depth and the frontier would chase a
    single rightmost spine instead of spreading across the wallet. The ranking
    stays a *span* proxy, not a mass one — the keyspace gap between cuts
    correlates with, but does not measure, the objects behind a prefix — so it is
    a best-effort bias toward heavy regions, not a guarantee of reaching every
    one ahead of a narrower sibling: a heavy-but-narrow subtree can still be
    out-ranked by a wide-but-light one. Closing that gap needs mass feedback the
    seed pass does not yet carry. The
    descent itself is bounded only by the probe
    budget (`maxProbes`) and natural frontier exhaustion — reaching the
    cut-point cap (`targetSeeds`) no longer stops it, since that would again
    be first-come-first-served: whichever heavy region the frontier reached
    first would keep every cut, stranding an equally heavy sibling reached
    only after the cap was already hit. `targetSeeds` instead bounds the
    *final* range count: once the descent completes, a cut set over
    `targetSeeds` is reduced by the same mass-weighted subsample used for an
    over-cap wide top (below), so cut density stays proportional to each
    region's sampled mass regardless of probe order. **The
    seed pass uses only the `CommonPrefixes` as cut-points and discards its
    `Contents`** (the scope-closing sentinel above is the one cut-point NOT taken
    verbatim from a `CommonPrefix` — it is `prefixCeil` of the greatest one, and its
    derivation reads the combined greatest returned item, prefix or object, only to
    verify that greatest `CommonPrefix` bounds everything the scope returned)
    (the top-level objects it also returns): it is a structure probe, not an
    emitting pass. Every object — including top-level ones — is (re-)listed by
    a range worker, so there is no double-emit between the seed and the ranges.
  - **Dense-flat-region radix banding.** A *truncated* `delimiter=/` level is
    classified by its shape — using no probes beyond the page already in hand —
    into three cases:
    - **Tiny-leaf explosion vs. heavy cut** — the level's `CommonPrefixes` are
      plain (non-`key=value/`) directory names. One page cannot tell a 1:1
      tiny-leaf explosion (~1000 `<hex>/` dirs each holding ~1 object) from a
      heavy deep subtree, so under mass-aware seeding (default on) a bounded
      sample of child prefixes disambiguates: a confirmed 1:1 explosion is **left
      whole** and handed to work-stealing (a 1:1 tree flat-scans in ~`⌈N/1000⌉`
      LIST calls, so enumerating it would be pure waste), while a sample that
      proves real mass is **banded** — tiled along the child prefixes already in
      the probed page (distinct from the leading-byte radix bands below). With
      mass-aware seeding off, such a level is always left whole.
    - **Partition fan-out** — the `CommonPrefixes` are Hive/Spark `key=value/`
      partition directories, each holding real data mass. **Tiled at seed time**
      along a `W`-capped subset of the partition prefixes already in the probed
      page (zero extra probes), instead of collapsing the fleet to a serial tail.
      Detected by a zero-probe *syntactic* signal — a **majority** of the level's
      `CommonPrefixes` carry `=` in their final path segment — because the
      N-objects-per-partition shape and the 1:1 one-object-per-leaf explosion
      produce **identical page shapes** (per-leaf density is unobservable without
      a leaf probe). This partition path is taken **before** mass-aware banding
      and short-circuits it, so it composes identically whether `mass_aware_seed`
      is off or on. Only the first page (≤1000 prefixes) is ever read, so a table
      with **more than 1000 partitions** leaves everything past the first page as
      one final open range — still an exact tiling, just under-parallelized for
      that tail (mass-aware seed descent is the intended superseding mechanism).
    - **Heavy dense range** — the level has **no `CommonPrefixes`, only direct
      objects** (a flat leaf, e.g. a `YYYY/MM/DD/<uuid>` mega-day, or a single
      dense directory at the root). **Pre-cut at seed time into leading-byte radix
      bands** so the fleet parallelizes the tail from the first moment instead of
      draining it near-serially.

    The **radix-banding trigger** is precisely the heavy-dense case: a truncated
    flat level with **no `CommonPrefixes` and non-empty direct objects**
    (`isFlatWide`). A flat top that is *not* truncated is a small bucket → one
    `(⊥, null]` range (same as `--tune seed.mode=none`), never banded.

    **Classified from the page shape alone.** A flat-wide level is decided by
    `isFlatWide` (truncated, no `CommonPrefixes`, at least one direct object) — the
    seed inspects **no key bytes** and issues **no further probe** before banding.
    There is no entropy test and no separator re-probe: a delimiter probe on such a
    level would only return the same ~1000-way truncated explosion, so the seed
    falls straight to leading-byte radix bands.

    **Band construction.** Bands are cut-points `dir·<scalar>` for single
    printable-ASCII scalars spread **uniformly and endpoints-inclusive** over the
    safe printable-ASCII alphabet `[0x21 '!', 0x7E '~']` minus `'%'`. The spread
    is **blind** — an even partition of the leading alphabet, *not* a
    mass-weighted one, because **no keys are observed at seed time** (contrast the
    runtime observed-alphabet rank-space pivots of §3.3, which apply only after a
    worker has fetched pages). The endpoints **must** be enumerated, not
    interpolated strictly *between* two anchors — strict betweenness could only
    emit interior scalars (`0x22..0x7D`) and would leave the first and last
    printable characters unbanded. Flat-region alphabets (hex, base64, UUID, ISO
    timestamps) live in printable ASCII, so one scalar per band isolates each
    leading bucket with no disproportionate range, and any subset of the
    synthesized cuts still tiles exactly (I2/I3) — so over-cutting a sparse region
    is correctness-harmless. `'%'` is excluded for the same reason it is excluded
    from synthesized pivots — a lone `%` in a `start-after`/`prefix` echoed back
    verbatim by some emulators crashes the SDK's `URLDecoder` — see
    `docs/internals/s3-implementation-compatibility.md`.
  - **Band count.** `K = min(SPAN, max(MIN_BANDS, min(1000, 4×W)))`, where
    `SPAN = 93` is the usable single-byte radix alphabet (the `0x21..0x7E` span
    less `'%'`) and `MIN_BANDS = 8`. `K` is **worker-proportional** (same spirit
    as the `min(1000, 4×W)` seed cap) and **independent of how much cut-point
    budget the directory descent already spent** — a dense leaf is banded even if
    the descent exhausted the seed budget, because a dense flat region is a
    serial-tail risk in its own right. The `SPAN` cap keeps a sparse region from
    ever exploding into a probe/seed storm.
- **`--tune seed.mode=hints` / `--hints` file** — seed cut-points from a file (the old
  `HintedPartition`). **NOT YET WIRED** — `SeedStep` throws "not yet implemented"
  for `SeedMode.HINTS`; reserved for a future release. Reuses the identical
  range structure once implemented.
- **`--tune seed.mode=none` / flat top (no prefixes)** — start with one range `(⊥, null]`
  and rely on stealing alone. Correct; just a few early extrapolated probes.

The engine also emits its observed prefix→count distribution as a **bounded**
`.ks` summary (top-level prefixes / a sketch, never an unbounded
prefix→count map) so a future run can seed from it.

---

## 9. Versioned listing (`ListObjectVersions`)

> **Planned — not yet wired.** `S3PageFetcher` throws for a `VERSIONS` request and
> `StoreCapabilities.supportsVersions` is `false`; v1.0 lists `OBJECTS` only. This
> section is the design of record for when `ListObjectVersions` lands (see
> [`contracts.md`](contracts.md) §1.2). One enablement gate remains explicit: the intended
> product order is newest-first within a key, but the dormant synthetic sort machinery currently
> compares and stamps null-first, lexicographic `version_id`. Those orders are not equivalent because
> version IDs are opaque. Do not expose `VERSIONS` until chronology and deterministic ties are defined
> and the comparator, footer/manifest order value, compatibility version, and independent oracle tests
> are changed together; the provisional lexicographic order is not the product contract.

Versioned mode changes the ordering and the split granularity:

- Ordering is **by key, then by version within the key (newest-first, not
  lexicographic on `version_id`).** `start_after` does not apply — use
  `key_marker` + `version_id_marker`.
- **Split only on key boundaries** (`key_marker = m`, `version_id_marker =
  null`); never split inside a key's version list.
- The stop check compares **key only** (`k.key > hi`).
- A single key with millions of versions is an **unsplittable atom** — the
  estimator/probe treats one key as the floor granularity. Final output rolling likewise never splits
  an equal-key group: `final-file-bytes` is a soft target and this atom may make one file exceed it,
  while the writer stays streaming and retains only the previous key.
- **All pivot / stop / span / `byteMidpoint` computations operate on
  `cursor.key` only** (never the `version_id` component); a split pivot `m`
  must satisfy `compare(m, cursor.key) > 0` (strictly past the current key),
  so a split never lands inside the version list the worker is mid-way
  through. `range_start` / `range_end` are keys.

The resumable position (the node "cursor") for versioned nodes is the pair
`(key_marker, version_id_marker)` — the worker paginates by sending both
markers; the stuck-marker defense compares the pair (port `s3ls-rs
mod.rs:535`). The key axis (`range_start`/`range_end`/`durable_cursor` and all
splitting) is separate from this intra-node version pagination.

---

## 10. Express One Zone (directory buckets)

> **Unsupported in v0.1.** swath detects the directory-bucket `--x-s3` naming
> form and refuses it before seeding, checkpoint creation, or the first LIST
> request. The opaque-token sequential path described below is planned, not
> built; its operator override is likewise not a current flag.

Directory buckets (`--x-s3` suffix) do **not** expose a single global
lexicographic order the way general-purpose buckets do, and don't support
the same cross-chain parallel listing. Range-splitting is therefore unsafe. The
planned implementation will **force sequential** operation (one `(⊥, null]`
worker, no stealing).
In that future design, `--allow-parallel-listings-in-express-one-zone` is the
proposed explicit override for users who know their layout; it is not a v0.1
option.

**The sequential fallback paginates by opaque continuation token, not
`start_after`.** Directory buckets advance only by `ContinuationToken` — they
do **not** honor `StartAfter` — so this single worker cannot use the §2
last-key pagination/resume model. It carries the SDK's opaque
`NextContinuationToken` as its cursor (the `opaque_token` / `OPAQUE_MARKER`
pagination path); resume reloads that token rather than a
`start_after` key. The §2 key-as-cursor model and the §3 range hand-off apply
only to general-purpose buckets.

---

## 11. Edge-case checklist (must be handled / tested)

1. Arbitrary key bytes → `encoding-type=url` + URL-decode before
   compare/split/emit.
2. Byte order ≠ UTF-16 order → unsigned `byte[]` compare/midpoint only.
3. `start_after` exclusivity + boundary-belongs-left (one-key gap/overlap
   bug). Synthetic boundaries legal and gap-free.
4. Page size not guaranteed 1000 → use `IsTruncated`, never `count == 1000`.
5. `IsTruncated == true` with no continuation token → bail.
6. Stuck continuation token / repeated key+version marker → bail.
7. In-flight page vs concurrent steal → per-key `hi` re-read + CAS hand-off.
8. Versioned listing → §9 (key-only split, version-atom).
9. Express One Zone → §10 (fail closed today; opaque-token sequential planned).
10. `prefix` + `start_after`: keep all boundaries within `[prefix,
    prefix-ceil)`; `start_after` below `prefix` is ignored by S3.
11. Empty / directory-marker keys (`/`-terminated) are normal objects.
    **Empty batches/pages do NOT advance the cursor** (§2 — there is no key to
    advance to); completion is decided by `reachedBound`/`!truncated`. An
    An empty upper probe does not cache the range as unsplittable: it triggers
    step-back, structure/reflection, and budgeted bisection fallbacks; only a
    populated candidate can spawn a child.
12. Range too small to split (pivot synthesis returns `null`) → `unsplittable`;
    owner finishes it.
13. Very long keys (≤ 1024 bytes) → property-test `byteMidpoint` `0xFF`-run
    and prefix-of cases.
14. Probe discipline → one initial 1-key probe plus bounded step-back,
    structure/reflection, and bisection probes per attempt; cache
    `unsplittable` only when no safe pivot can be produced.
15. Resume re-split consistency → ordinary checkpoint resume is keyed on the
    last committed source-key `cursor`; Parquet resets to `durable_cursor` and
    deliberately re-lists the unfinalized tail. Either path resumes a node
    split in a prior run without relying on a stale token; covered by the
    fault-injection resume tier.

---

## 12. Parallel sort boundary selection

When the range-merge gates admit `R > 1`, each page-run original listing-phase segment contributes
the systematic page-minimum sample in contracts §6.1. Boundary selection deduplicates those keys and
retains the deterministic bottom-hash 16,384 candidates across the whole run (1,024 per range at
the supported 16-range maximum); this bounds retained
candidate state independently of segment count and is invariant to segment/input order. It sorts the
retained keys unsigned and chooses ranks `j * candidates / R` for `j = 1..R-1`. Cascade
intermediates are produced after this phase and therefore carry no sample. A valid embedded sample
is committed only after its complete block passes structural, CRC, count, and ordering validation;
otherwise that segment alone is scanned through `PageFrontierReader`, preserving legacy behavior. Repeated
minima remain in the per-segment sample and are deduplicated by the whole-run sampler. Embedded,
legacy, and mixed inputs therefore feed the same deterministic selection rule.

Boundary choice affects balance only: every range still filters every retained page row against its
exact `[lo, hi)` bounds. The final unbounded range drains every original segment to EOF, retaining
the whole-input CRC/order/count proof even though all-new boundary selection reads no page bodies.

The experimental `sort.finalization=pipeline` arm bypasses boundary selection entirely. After the
usual cascade reduces the input to the admitted fan-in, bounded sequential segment readers publish
decoded pages to one ordered router. The router forwards a page without row materialization when its
maximum raw key is below the next page minimum; transitively overlapping pages enter the shared
page-aware row heap and leave in batches of at most 4,096 rows. The router owns calibrated part
geometry and raw-key-atomic boundaries, while bounded striped encoders only execute its stamped
batch stream. This preserves a single global merge order without `R` duplicate range frontiers.
