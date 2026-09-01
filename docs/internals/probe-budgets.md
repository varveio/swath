# swath — probe budgets (contributor reference)

> **You don't need this to use swath.** This is the contributor-tier reference for how swath sizes
> the per-attempt SDK timeout of each **call class**. The user-facing timeout/retry knobs are in
> [`docs/configuration.md`](../configuration.md). The field data behind the current sizing is in
> [`docs/ops/dev/field-investigations.md`](../ops/dev/field-investigations.md).

---

## 1. Call classes are sized by cost shape, not by role

`S3PageFetcher#callClass` classifies every outgoing `ListObjectsV2` purely from the request shape
into one of three call classes. The per-attempt budget each one gets is chosen by **how its cost
scales**, not by whether it is "a probe":

| call class | shape | cost shape | per-attempt budget |
|---|---|---|---|
| `worker_page` | no delimiter, configured page size | **scan** — S3 walks the range | `S3Config.DEFAULT_ATTEMPT_TIMEOUT` (10 s, client-level, no override) |
| `structure_probe` | `delimiter=/` | **scan** — S3 walks forward rolling keys into `CommonPrefixes` | `S3Config.DEFAULT_ATTEMPT_TIMEOUT` (10 s, client-level, no override) |
| `pivot_probe` | `max_keys<=1`, no delimiter | **point** — answered from the first key at/after the cursor | `S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT` (3 s, per-request override) |

The load-bearing distinction is **point vs scan**. A pivot probe asks S3 for the first key at or
after a cursor; S3 answers it from one position and the latency is near-constant regardless of how
much data lies beyond. A `delimiter=/` structure probe asks S3 to scan forward, rolling every key it
crosses up into a `CommonPrefix`, until it has filled `max_keys` prefixes — so its latency tracks the
**keyspace it crosses**, exactly like a worker page. Two probes, two different cost shapes, two
different budgets.

`S3PageFetcher#usesShortProbeBudget` is the single place that encodes this. It returns true for
`pivot_probe` only.

## 2. Why the split exists

Structure probes originally shared the 3 s point-probe budget, on the reasoning that "a probe carries
no backpressure signal, so abandon it quickly." That reasoning is right for a point probe and wrong
for a scan, and on a large deep bucket it produced a self-sustaining **probe-timeout storm**:

- A structure probe on a deep keyspace measured ~1.15 s standalone but ~5.4 s at 64-way concurrency —
  so the 3 s budget sat *below the call's real cost at the concurrency the run was using*, and about
  half of all structure-probe attempts timed out. Pivot probes, on the same client and the same
  budget, never timed out once.
- Each timeout aborts its connection, forcing a fresh TLS handshake — the storm pays for its own
  reconnection churn.
- Structure probes are the thief's **pivot source**, so the real cost was not the wasted calls but
  **work starvation**: splits collapsed, steal success fell to a few percent, workers parked, and
  in-flight concurrency decayed to a small fraction of its target while throughput fell ~20×.

The timeouts were the visible symptom; work starvation was the actual cost. Correcting the budget
split raised splits ~7×, roughly doubled throughput, and turned a run that never finished into one
that completes. Full before/after numbers, and the bucket to reproduce on, are in
[`field-investigations.md`](../ops/dev/field-investigations.md).

**A note on what this is not.** The storm was invisible to AIMD, correctly — and the reason is
**actuator mismatch**, not just the documented freeze-without-shed deadlock. AIMD's only actuator is
`T`, which gates slot-holding worker fetches; probes hold no slot, so no value of `T` acts on probe
traffic. In this storm workers were *starved*, not congested, so reducing `T` would have been between
useless and harmful. A controller with no actuator over the disturbance should not be fed the
disturbance. Probe traffic has its own control stack — the per-attempt budget, `PROBE_TRANSIENT_RETRY_CAP`
fail-fast, and `IdleStealBackoff`'s fleet-wide pacing — and every rung of it behaved correctly given a
deadline set below the call's intrinsic cost. That is a configuration no controller can rescue, which
is why the fix is budget sizing and not concurrency.

## 3. Escalation is a level; the store decides what a level costs

`GaugedFetcher` escalates a logical fetch's per-attempt budget on consecutive `ATTEMPT_TIMEOUT`
faults. What it publishes is a **level** on `PageRequest.attemptTimeoutEscalationLevel` — never a
duration. The store maps level to wall-clock in `S3PageFetcher#attemptTimeoutForLevel`:

```
budget = base(callClass) × 2^level
```

| call class | base (level 0) | level 1 | level 2 |
|---|---|---|---|
| scan (`worker_page`, `structure_probe`) | 10 s | 20 s | 40 s |
| point (`pivot_probe`) | 3 s | 6 s | 12 s |

**Why the split of ownership.** Retry *policy* — how many rungs exist and when to climb one — is the
engine's; it is about failure behavior. What a rung is *worth* is the store's, because only the
store knows each call class's base budget, and (per §1) call classes differ by more than a constant
factor. `TransientRetryFetcher` therefore holds only `MAX_ATTEMPT_TIMEOUT_ESCALATION_LEVEL`.

**This used to be the other way round, and it bit.** The engine held absolute durations
(`{20 s, 40 s}`) authored against the scan base of 10 s. Applied to a 3 s point probe, the first
escalation was a **6.7× jump straight to 20 s** — with `PROBE_TRANSIENT_RETRY_CAP=1`, a failing pivot
probe burned 3 s + 20 s and still returned nothing. The interim fix divided the engine's ask back out
to recover the multiple and re-applied it to the class's own base; that worked, but it also shipped a
latent bug, because the pass-through branch for scan-class calls was un-floored and a scan base
configured *above* a ladder rung let an "escalation" **shrink** the budget below its own base.

Doubling from the class's own base makes that unrepresentable rather than guarded: the result is
monotone in `level` for any configured base, so escalation can only ever buy room. If you need a
different ladder shape, change `MAX_ATTEMPT_TIMEOUT_ESCALATION_LEVEL` (rung count) in the engine or
the mapping in the store — but keep durations out of the engine.

## 4. Guards

| guard | what it pins |
|---|---|
| `S3PageFetcherProbeAttemptTimeoutTest#structureProbeKeepsTheScanClassTimeout_noShortProbeFuse` | a `delimiter=/` probe carries **no** per-request override (scan-class budget) |
| `S3PageFetcherProbeAttemptTimeoutTest#pivotProbeGetsTheShortProbeAttemptTimeout` | a `max_keys<=1` probe still gets the 3 s point budget |
| `S3PageFetcherEscalationBudgetTest` | each class climbs its own ladder (scan 10/20/40 s, point 3/6/12 s); escalation never shrinks a budget at any configured base |
| `GaugedFetcherAttemptTimeoutEscalationTest` | the engine-side rung policy (level 0 → 1 → 2, capped) |

## 5. If you change a budget

Re-derive it from a measurement on a real bucket **at the concurrency the run will actually use**,
not from a single-request timing — a structure probe measured 4.7× slower at 64-way than standalone,
and that gap is precisely what the original 3 s budget missed. `probe_latency[]` in the JSON run
summary already decomposes `connect_acquire`/`ttfb`/`total` per call class over the whole run, so a
single run tells you whether a budget is sized correctly. Write the result up in
[`field-investigations.md`](../ops/dev/field-investigations.md).

A scan-class budget cannot be sized to cover every keyspace: a `delimiter=/` probe crossing a very
large flat directory can exceed any fixed budget, which is what the escalation ladder is for. Judge a
budget by its **p50 and the run completing**, not by driving tail timeouts to zero.

**The tail is handled by not re-asking, not by a bigger budget.** `Thief#structureProbesEnabled`
suppresses structure probing per-victim on two independent streaks: consecutive zero-fan-out probes
(the region answered, and it is flat) and consecutive TIMED-OUT probes (the region could not answer
at all). The timeout streak has a much lower threshold — `STRUCTURE_TIMEOUT_SUPPRESS_THRESHOLD = 2`
vs 8 — because the two cost differently by an order of magnitude: a zero-fan-out probe answered
cheaply, a timed-out one burned its whole escalated budget and aborted a connection to say nothing.
Both suppressions share the same 1-in-N random re-probe escape, so a victim is never locked out
permanently.

That feedback loop was missing at first: a probe that times out throws past the fan-out accounting,
so it left every counter untouched — *the timeout destroyed the very evidence that would have stopped
the next probe*, and the thief re-probed regions that had already proved they could not answer.
`Thief#probeStructure` is the chokepoint that closes it.

**What resets the timeout streak, precisely.** Only a probe that comes back with a genuine structure
answer resets it — `probeStructure`'s success path unconditionally calls
`victim.resetTimedOutStructureProbes()`. A `SLOWDOWN`/5xx or a `NETWORK` fault does neither: it is
store backpressure or a client-side blip, not evidence about this keyspace's shape, so it leaves an
existing streak exactly where it was — neither incremented (only `Kind.ATTEMPT_TIMEOUT` counts) nor
cleared (reset is SUCCESS-only). See `StructureProbeTimeoutSuppressionTest`, which pins this from a
pre-existing streak so "left untouched" cannot be confused with "reset".

**Don't make the budgets adaptive.** Deriving a budget from an observed latency EWMA/percentile looks
attractive but is the wrong shape here. The structure-probe distribution is *bimodal*, not drifting
(p50 43 ms against p90 10.2 s): a budget sized off the healthy mode shrinks and still times out on
flat-directory crossings, while one sized off the tail converges to "no budget". Adaptive timeouts
answer latency *drift*; per-call escalation already answers the second mode, and answers it with
evidence about that specific call. An adaptive budget would also put a second feedback loop on the
same latency signal the AIMD freeze rung reads — two controllers, one signal, different time
constants, and no post-hoc explanation for the oscillation. Both budgets are `S3Config` knobs, which
is the right escape hatch for an unusual endpoint.
