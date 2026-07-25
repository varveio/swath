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

## 3. Escalation is re-expressed against each class's own base

`GaugedFetcher` escalates a logical fetch's per-attempt budget on consecutive `ATTEMPT_TIMEOUT`
faults via `TransientRetryFetcher.ATTEMPT_TIMEOUT_ESCALATION_LEVELS` — 20 s then 40 s. Those are
**absolute durations authored against the scan base of 10 s**: the ladder is really "2× base, then
4× base". The engine cannot know any better — escalation level is all it has, and only the store
layer knows what each call class's base budget actually is.

Applied unclamped, a 3 s point probe's first escalation is a **6.7× jump straight to 20 s**. With
`PROBE_TRANSIENT_RETRY_CAP=1` (one retry, then fail fast), a failing pivot probe would burn
3 s + 20 s and still return nothing.

`S3PageFetcher#escalatedAttemptTimeoutFor` therefore converts the engine's ask to the **multiple** it
represents over the scan base and re-applies that multiple to the call class's own base — preserving
the ladder's progression rather than flattening it to a single ceiling:

| call class | base | level 1 | level 2 |
|---|---|---|---|
| scan (`worker_page`, `structure_probe`) | 10 s | 20 s | 40 s (untouched — the ladder as authored) |
| point (`pivot_probe`) | 3 s | 6 s | 12 s |

The multiple is **derived** from the two base durations rather than hardcoded, so the ladder stays
single-sourced in the engine: re-tune it there and the rescale follows.

## 4. Guards

| guard | what it pins |
|---|---|
| `S3PageFetcherProbeAttemptTimeoutTest#structureProbeKeepsTheScanClassTimeout_noShortProbeFuse` | a `delimiter=/` probe carries **no** per-request override (scan-class budget) |
| `S3PageFetcherProbeAttemptTimeoutTest#pivotProbeGetsTheShortProbeAttemptTimeout` | a `max_keys<=1` probe still gets the 3 s point budget |
| `S3PageFetcherEscalationRescaleTest` | escalation is re-expressed on the class's own base; the scan-class 20 s/40 s ladder passes through untouched; the multiple is derived, not hardcoded |
| `GaugedFetcherAttemptTimeoutEscalationTest` | the engine-side escalation ladder itself (base → 20 s → 40 s) |

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

**Don't make the budgets adaptive.** Deriving a budget from an observed latency EWMA/percentile looks
attractive but is the wrong shape here. The structure-probe distribution is *bimodal*, not drifting
(p50 43 ms against p90 10.2 s): a budget sized off the healthy mode shrinks and still times out on
flat-directory crossings, while one sized off the tail converges to "no budget". Adaptive timeouts
answer latency *drift*; per-call escalation already answers the second mode, and answers it with
evidence about that specific call. An adaptive budget would also put a second feedback loop on the
same latency signal the AIMD freeze rung reads — two controllers, one signal, different time
constants, and no post-hoc explanation for the oscillation. Both budgets are `S3Config` knobs, which
is the right escape hatch for an unusual endpoint.
