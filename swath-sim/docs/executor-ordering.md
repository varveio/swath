# What the simulated executor does, in what order

This is the ordering contract of `io.varve.swath.sim.executor`. It exists because the policies the
simulator drives are pure decisions over views, and *when* each view is built — relative to every
other actor's writes — is the entire difference between a simulator that reproduces the engine's
races and one that quietly assumes they never happen.

The rule underneath everything here: **one event body is one atomic region.** The kernel is
single-threaded and never interleaves two bodies, so anything done inside one body cannot be observed
half-finished. State read in one body and used in a later one is correspondingly exposed to
everything other actors do in between. Those two facts are how a lock hold and a widened read window
are expressed without a lock, a thread, or a memory model.

## The run

1. **Seed.** Before any worker exists, the seed planner is driven to a plan: it asks for bounded
   directory probes, each one a modelled store call with its own latency, and returns a cut set. The
   phase therefore costs virtual time, which matters because on a deep keyspace it is a real part of
   the run. A run seeded `NONE` skips it and starts with one range over the whole keyspace.
2. **Tile and start.** The cuts become seed ranges in the ledger — `(⊥, c1], (c1, c2], …, (ck, null]`,
   which tile the keyspace exactly — and every worker is started at the instant the plan landed.
3. **Claim, drain, split, complete.** Each worker claims a range, lists it page by page, and may carve
   its own far tail as it goes. When it finishes, it decrements the outstanding count and wakes
   whoever is parked.
4. **Steal.** A worker with nothing to claim becomes a thief: it selects a victim, probes, and
   proposes a split. If the fleet's single steal-attempt slot is taken, or the fleet's pacing window
   has not elapsed, it parks instead.
5. **Quiesce.** The run ends when nothing is claimable and nothing is outstanding.

## One page, in order

Everything from "the response arrives" to "the owner-split decision is made" happens in **one body** —
the region the engine holds the worker's lock across:

| Step | Why it is inside the atomic region |
|---|---|
| Read the page from the store | The store is consulted on arrival, never at issue, so a model whose store can change gets the faithful answer without a redesign. |
| Trim the page to the current bound | A thief may have narrowed `hi` while the call was in flight. The keys above the new bound belong to the child; emitting them would be a double-emit. |
| Advance the cursor, add the emitted keys, fold the page into the density digest | The digest has to be coherent with the cursor, or the far-ahead fraction is computed against a page the cursor has not reached. |
| Commit the page to the ledger | The durable cursor advance. |
| Run the owner-side split decision | It reads the cursor this commit just advanced, and the bound nothing else can have moved, so its pivot is ahead of its own cursor **by construction** — which is why an owner's split never loses the race a thief's can. |

The page's client-side cost is charged *after* that region, in stages, on the worker's own timeline:
its conversion work, then the durability commit it waits on before emitting, then the consumer stage.
Only then does the worker ask for the next page or complete the range.

## One steal, and the race it can lose

| Body | What happens |
|---|---|
| 1 | The victim pool is built from live workers that have committed a non-empty page since they were last split (the progress gate). Selection runs. The chosen victim's cursor and bound are read — **the snapshot**. |
| 2..n | The cascade's probes are issued and answered, one modelled call each. The victim is draining the whole time, in bodies of its own. |
| n+1 | The proposal is re-validated against the victim **as it stands now**: the bound must still be the one the snapshot saw, and the cursor must still be below the pivot. Then the durable split guard checks the same facts again, and only then is a child published. |

A proposal that fails either check is refused and recorded as futile against that victim, which is
what eventually paces attempts against a drainer nobody can catch. That refusal is not a defect to be
engineered away: it is the fidelity the simulator exists for, and there is a test that fails if the
re-validation is removed.

The two checks in body `n+1` are not two chances at the same thing, and a run's counters read very
differently across them. The re-validation is where proposals die: it is the first place the victim's
current cursor is compared with a pivot placed against a snapshot taken several bodies ago
(`splits_lost_revalidation`). The durable guard sees only what that check has already passed, so it can
reject only if something changed between the two — inside one body, where no other actor runs. That
needs a second in-flight proposer, and the fleet allows one steal attempt at a time.

What is left is narrow enough to describe exactly. The guard's third condition is completion, and a
victim can complete while a steal attempt against it is in flight, its claim not yet retired (the page's
client-side cost is charged before the range is released). For the earlier check to pass anyway, the
victim's final cursor has to still be *below* the pivot — so the pivot has to sit above the last key in
the range, which the cascade only produces where it commits a pivot it never probed: a structure
boundary, or the flat-leaf reflection, landing in the empty span between a directory's last key and a
bound synthesised above it. That combination was observed exactly once, in a sweep configuration that
is not in the repository: the concentrated deep-nested shape at `(8, 8, 1, 160_000)` — 759,188 keys —
under eight workers, a 1,000-key page at 110 ms with 35 ms probes, the measured client cost and seed
`20260727`, which rejected one durable split. Fifty-four flat-leaf configurations swept across worker
count, probe latency and the width of the client-cost window rejected none. The in-repo run of the
concentrated shape pins `splits_rejected == 0`, which is the ordinary case. So `splits_rejected` reads zero on runs
losing most of their steals, and it is not the number to read when asking how often a fleet loses this
race.

## Who wakes a parked worker, and how often

A worker with nothing to claim parks on a timer. Four things cut that park short, and they are the
engine's four, not a simplification of them:

| Wake | Fired when | How often |
|---|---|---|
| A split child is published | the ledger enqueues a child, owner-side or thief | tens to hundreds a run |
| A range completes | the ledger's outstanding count is decremented | once per range |
| A steal attempt finishes | the fleet's single attempt slot is released, whatever the outcome | once per attempt, thousands a run |
| **A non-empty page commit** | **every page any worker commits that emitted a key** | **once per page — the dominant one by an order of magnitude** |

The last one also **resets the idle-steal backoff ladder**, and both halves of that are the engine's
behaviour: its page-commit path resets the fleet-wide backoff and broadcasts on the worklist for
exactly the same reason — a fleet that has just been handed fresh progress should not sit out a full
backoff window before looking for it. The consequence is worth stating plainly rather than
rediscovering: **while any worker commits pages steadily, the ladder is structurally pinned near its
bottom rung**, because the interval it needs to climb is an interval in which nothing anywhere commits.
And during a single-owner serial tail the same broadcast fires on every page of the one range still
draining — waking every parked thief, each of which attempts, is denied, and re-parks, only to be woken
by the next page. That is where a large share of a run's events come from (retired park timers are
38–43% of all events dispatched in the measured runs), and it is a cost the modelled fleet is supposed
to be paying, because the real one pays it.

The rung is not thereby dead: refusals are recorded (`IDLE_SLOT.paced`) in both regimes, because a
thief whose attempt just returned non-productive re-enters the idle path in the same instant and is
turned away by the backoff it has itself only just re-armed. What the per-commit reset governs is how
high the ladder climbs, not whether it is consulted. `IdleStealPacingReachabilityTest` pins that the
rung is reached at all.

## Timeouts, and what a cancelled timer costs

The kernel has no cancellation. So:

- **When the completion instant is known at issue** (the ordinary store, which answers independently),
  the executor schedules the response *or* the timeout — never both. A timeout costs no extra event.
- **When it is not** (a modelled store with a queue, whose answer depends on what else is in flight),
  both are armed and whichever fires second finds its subject already retired and returns. Those
  second firings are dispatched events: they count against the run's event budget, and they are
  counted (`events.stale`) so a budget can be sized including them rather than despite them.

A timed-out call keeps one further event either way, at the instant the *store* would have answered,
which is where its occupancy is retired. A call the client has given up on is still work the store is
doing and still crowds out the next one, so retiring it at the client's timeout would understate
occupancy by exactly the calls a struggling store is struggling with — the one regime where a latency
model that reads occupancy has anything to say. The two paths answer that question the same way.

The stale mechanism also retires a park timer whose worker was woken early by any of the four signals
above.

## The three disclosed widenings this reproduces

Extracting the policies widened three read windows, all benign, all disclosed. The simulator models the
**current** semantics, not the pre-extraction ones, because the point of the exercise is to predict the
engine that ships:

- **The per-victim futility cooldown is now read and consumed as two steps.** Whether a candidate still
  has a cooldown skip is read while the pool is being scanned, without a lock; the skip is consumed
  afterwards, for exactly the candidates the policy reports it skipped. The pre-extraction code checked
  and decremented in one call, so a cooldown can now end a call or two later than it used to. The
  executor models the split pair, which is what production runs.
- **A victim's structure-probe suppression streaks are read once, when the attempt's view is built.** A
  streak that changes mid-cascade is not observed until the next attempt.
- **The zero-fan-out streak lands one step late.** The policy returns it as a mutation attached to the
  action it decided, so the executor applies it after that step rather than during it.

The **fleet-wide idle-steal pacing window is not one of them.** Its arithmetic moved behind the seam
unchanged and is still consulted under the same monitor, so this executor's single check-then-act at the
top of an attempt is the engine's own shape rather than a widening of it.

## Divergences from the engine, deliberately not modelled

Distinct from the widenings above, which are the engine's current behaviour reproduced. These are
places where the executor knowingly does something the engine does not, and the entry exists so a
result that turns on one is recognisable as such rather than surprising.

- **A retried attempt keeps the base timeout; the engine escalates it.** `GaugedFetcher` raises a
  fetch's per-attempt budget on consecutive attempt timeouts (base → 20 s → 40 s, via
  `TransientRetryFetcher.escalationLevel`), so a real retry gets longer to answer than the attempt it
  is retrying. Here every attempt in a chain is bounded by the same declared
  `probeAttemptTimeoutNanos` / `workerAttemptTimeoutNanos`. The effect is confined to the storm
  regime — where the engine would ride out a slow store that this executor gives up on, so a modelled
  run in that regime **under**-states how much a real fleet recovers. The retry *count* is faithful:
  the declared cap is spent in full on both the first attempt's timeout and every retried one.
- **A rejected durable split records futility against its victim; the engine does not.** The
  `split_aborted` branch here calls `recordFutileSteal`, where `Thief.commit` restores the bound and
  leaves the victim's futility tally alone (only the two earlier losers record it). Reachable at most
  once in the runs measured so far — see the guard note above — so it has never moved a pacing
  decision, and it is written down rather than quietly fixed because the fix is a behaviour change to
  an executor whose whole claim is that it does what the engine does.

## What is deliberately not modelled

- **Durability.** Writing a checkpoint row costs time, and that time is charged by the client-cost
  model's checkpoint stage. The ledger only decides outcomes. Nothing is persisted, so nothing can be
  resumed: a simulated run is one process's lifetime by construction.
- **Threads and permits.** The concurrency target bounds how many page fetches may be outstanding; a
  worker denied a slot waits for one to be released. There is no permit to leak and no thread to park.
- **Cancellation and watchdogs.** What a page fetch does when its transient retries run out is a
  **declared input**, because the shipped client has two dispositions and they end a run differently.
  Under the default the client rides the storm out — a watchdog owns storm death — so the fetch keeps
  retrying and the run ends on the ceilings it declared. Under the bounded disposition (no watchdog
  armed) the retry ceiling ends the run as *stuck*. A scenario that picks the bounded one and then
  models a store that times out will therefore end stuck exactly where a real ride-out run would have
  kept making progress; that is the disposition's own meaning, not a simulator artefact, and it is
  stated in the scenario rather than assumed.
- **The retry backoff's jitter.** The backoff between retries is one declared interval rather than the
  engine's full-jitter draw, because jitter's live purpose is desynchronising separate processes and a
  single-threaded kernel has no analogue of that.
- **Permit hand-off in detail.** The concurrency target bounds outstanding page fetches; slots are
  handed to waiting workers in the order they began waiting, and a growth step hands out as many as it
  released rather than one per completion. There is still no permit to leak and no thread to park.
