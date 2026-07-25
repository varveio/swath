# swath — probe budgets (contributor reference)

> **You don't need this to use swath.** This is the contributor-tier reference for how swath sizes
> the per-attempt SDK timeout of each **call class**, and for the failure mode that motivated the
> current split. The user-facing timeout/retry knobs are in
> [`docs/configuration.md`](../configuration.md).

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

## 2. Why: the genomeark probe-timeout storm

Structure probes originally shared the 3 s point-probe budget, on the reasoning that "a probe carries
no backpressure signal, so abandon it quickly." That reasoning is right for a point probe and wrong
for a scan.

A `swath list s3://genomeark/` run (6.6 M keys, `T=64`) produced this:

| class | calls | p50 total | p99 total | attempt timeouts |
|---|---|---|---|---|
| `pivot_probe` | 3169 | 103 ms | 300 ms | **0** |
| `structure_probe` | 2612 | 10,196 ms | 20,397 ms | **~1308** |
| `worker_page` | 6819 | 172 ms | 482 ms | 0 |

Same client, same 3 s budget, same concurrency — the point-probe class never tripped the fuse once,
and the scan-probe class tripped it on roughly half of all attempts. Measured directly against the
bucket, a single structure probe on that keyspace costs ~1.15 s standalone, ~1.68 s at 16-way, and
~5.4 s at the run's own 64-way concurrency. The 3 s budget sat **below the call's real cost at the
concurrency the run itself was using**.

The consequences compounded well past the wasted calls:

- Every attempt timeout aborts its connection (1308 `swath.s3.pool.connection_aborted`, 1:1 with the
  timeouts), forcing a fresh TLS handshake (1392 `swath.s3.pool.handshakes`) — so the storm paid for
  its own re-connection churn.
- Structure probes are the thief's pivot source. With half of them failing, the split machinery
  starved: **55 splits and 39 stolen children for 6.6 M keys**, a 2.5 % steal success rate, 2126
  `STEAL/futility_paced` and 1005 `OWNER_SPLIT/floor_reflected_blocked`.
- With no work to steal, workers parked (31762 `swath.idle_backoff.slot_denied`). Concurrency
  collapsed from 18.5 in-flight to **2.0** against a target of 64, and throughput decayed from 147 k
  to 7.3 k keys/s over the run.

The timeouts were the visible symptom; **work starvation was the actual cost**.

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

Re-derive it from a measurement on a real bucket at the concurrency the run will actually use, not
from a single-request timing — the genomeark structure probe is 4.7× slower at 64-way than
standalone, and that gap is precisely what the original 3 s budget missed. `probe_latency[]` in the
JSON run summary already decomposes `connect_acquire`/`ttfb`/`total` per call class, so a single run
tells you whether a budget is sized correctly.
