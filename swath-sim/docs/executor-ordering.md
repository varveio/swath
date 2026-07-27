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

## Timeouts, and what a cancelled timer costs

The kernel has no cancellation. So:

- **When the completion instant is known at issue** (the ordinary store, which answers independently),
  the executor schedules the response *or* the timeout — never both. A timeout costs no extra event.
- **When it is not** (a modelled store with a queue, whose answer depends on what else is in flight),
  both are armed and whichever fires second finds its subject already retired and returns. Those
  second firings are dispatched events: they count against the run's event budget, and they are
  counted (`events.stale`) so a budget can be sized including them rather than despite them.

The same mechanism retires a park timer whose worker was woken early by a signal — a child being
published, or a range completing.

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
