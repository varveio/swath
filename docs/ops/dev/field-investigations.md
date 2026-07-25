# swath — field investigations (contributor reference)

> **You don't need this to use swath.** This is where investigations against *specific real buckets*
> are written up: the raw evidence, the bucket, the numbers, and what changed as a result. Design
> docs and code comments stay bucket-neutral and cite an entry here rather than naming a bucket
> inline — a tuning decision should be legible from the mechanism, with the field data one link away.

Add an entry when a real-bucket run drives a code change. Keep the bucket name, the flags, and the
before/after numbers: the point of this file is that someone can re-run it.

---

## 2026-07-25 — probe-timeout storm on a deep genomics bucket

**Bucket:** `s3://genomeark/` (public, `us-east-1`, ~8.8 M objects, `mass_skew_gini` 0.81, very deep
`working/…` tree with several enormous flat directories).

**Command:**

```bash
swath list s3://genomeark/ --region us-east-1 --no-sign-request --format parquet -o <dir>
```

### Symptom

Throughput decayed ~20× over the run (147 k → 7.3 k keys/s), `in_flight` collapsed from 18.5 to 2.0
against a target of 64, and the log filled with ~1300 `ApiCallAttemptTimeoutException` warnings. The
run had listed 6.6 M objects in 280 s and was still going when it was interrupted.

### Diagnosis

Every one of the 1308 attempt timeouts was a **probe**, and within probes, only the
`delimiter=/` structure class. The per-call-class histograms were unambiguous — same client, same 3 s
budget, same concurrency:

| class | calls | timeouts |
|---|---|---|
| `pivot_probe` | 3169 | **0** |
| `structure_probe` | 2612 | **~1308** |
| `worker_page` | 6819 | **0** |

Measured directly against the bucket with the AWS CLI, one structure probe on that keyspace costs
~1.15 s standalone, ~1.68 s at 16-way and ~5.4 s at 64-way — i.e. the shared 3 s point-probe budget
sat *below the call's real cost at the concurrency the run itself was using*. Structure probes are
the thief's pivot source, so the fallout was work starvation rather than merely wasted calls: 55
splits and 39 stolen children for 6.6 M keys, 2.5 % steal success, 31762
`swath.idle_backoff.slot_denied`.

Two aggravating factors, both fixed:

- Each timeout aborted its connection (1308 `connection_aborted`, 1:1) forcing a fresh TLS handshake
  (1392 `handshakes`) — the storm paid for its own reconnection churn.
- The engine's escalation ladder (20 s → 40 s) is authored against the 10 s scan base, so a 3 s probe
  escalated 6.7× straight to 20 s. With `PROBE_TRANSIENT_RETRY_CAP=1`, 556 probes burned 3 s + 20 s
  and returned nothing.

**AIMD correctly did nothing.** Only 4 genuine 5xx occurred in the whole run — S3 was never unhappy.
Attempt timeouts deliberately don't vote `T` down, and *probe* timeouts are excluded from every AIMD
down path (`ConcurrencyGauge#onTransientTimeout(false)` early-returns; the run recorded 1308
`GROWTH/probe_timeout_excluded`). The sustained-timeout shed gates on worker-class timeouts, of which
there were none. This was swath hurting itself in a traffic class AIMD does not govern, so no
concurrency setting would have fixed it — lowering `--concurrency` would only have masked it by
shrinking probe fan-out.

### Changes it drove

- Per-attempt budgets sized by **call-class cost shape** (point vs scan) rather than by "is it a
  probe" — `docs/internals/probe-budgets.md` §1.
- Escalation **re-expressed against each call class's own base** — `probe-budgets.md` §3.
- Retryable fault log lines carry `call_class`/`prefix`/`start_after`, so a storm is attributable
  from the log alone (previously it could only be reconstructed from the JSON summary).
- Run-summary distribution statistics made **run-scoped** — see below, found while investigating.

### Before / after

Cumulative counters are directly comparable. **Percentiles are not**: before the run-scoped-window
fix, `max` and all percentiles were Micrometer's rolling 2-minute window while `count`/`total` were
cumulative, so the "before" percentiles described only that run's final ~2 minutes.

| | before | after |
|---|---|---|
| run | 6.6 M objects in 280 s, interrupted, still going | **8.8 M objects, complete, 173 s** |
| throughput | 23.8 k keys/s | **50.8 k keys/s** |
| attempt timeouts | 1308 | **216** |
| connections aborted / handshakes | 1308 / 1392 | **216 / 441** |
| splits | 55 | **380** |
| `latency_inflation` freezes | 1176 | **32** |

Run-scoped per-class latency after the fix:

| class | calls | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| `worker_page` | 9229 | 107 ms | 149 ms | 258 ms | 321 ms |
| `pivot_probe` | 15363 | 34.6 ms | 45.1 ms | 74.4 ms | 119 ms |
| `structure_probe` | 4169 | 43.0 ms | 10,200 ms | 15,568 ms | 17,372 ms |

### Residual, and the right fix for it

`structure_probe` keeps a heavy **tail** (p90 10.2 s) and 216 attempt timeouts remain. These
concentrate in a few enormous flat directories — `working/staging/all_vs_all_alignments/FastGA/10k/`
is the reproducible one — where a `delimiter=/` scan must cross a very large number of keys before it
can return a single `CommonPrefix`. Even the 10 s scan budget is not enough there, and the ladder
correctly escalates those to 20 s/40 s. This is inherent to the call shape on that keyspace, not a
budget error: p50 is 43 ms, the run completes, and the thief gets pivots again.

**Fixed** (see "Structure-probe timeout suppression" below). **No budget can fix it, but a feedback gap can.** `Thief#structureProbesEnabled` already suppresses
structure probes per-victim after `STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD` consecutive zero-fanout
probes — precisely the enormous-flat-directory defense. But a structure probe that **times out** never
returns its zero-fanout answer, so `consecutiveZeroFanoutProbes` never increments and the suppressor
never engages: *the timeout destroys the very evidence that would stop the next probe*. Folding a
structure-probe timeout streak into that same per-victim suppression counter turns the residual from
"pay this cost repeatedly per flat region" into "pay once or twice, then fall back to bisection
pivots". That is per-victim-scoped like the existing futility pacing, trivially instrumentable per the
§5 engagement-counter doctrine, and it generalizes to flat buckets far better than any budget policy —
it stops issuing the wrong probe *shape* instead of re-sizing its deadline. This, not budget tuning
and not the probe-pacing latch, is the right home for the residual.

**Related open follow-up — a leaked pacing latch, not missing pacing.** Probes are `slotGated=false`,
so they bypass the AIMD slot gate, but they are *not* unbounded by design: every thief steal attempt
must pass `IdleStealBackoff#tryAcquireAttemptSlot` (`WorkStealingScan.java:507`), whose stated
contract is **at most one speculative steal attempt in flight fleet-wide**. The bound leaks:
`IdleStealBackoff#reset()` clears `attemptInFlight` unconditionally, and it is called on every
ordinary claim (`WorkStealingScan.java:496`) and every non-empty page commit
(`WorkStealingScan.java:723`) — i.e. by unrelated workers, while an attempt is still running. So
effective probe concurrency is roughly `1 + (reset rate × probe duration)`: with multi-second probes
and tens of commits/sec it reaches tens in flight. The class javadoc concedes "a progress reset can
briefly let a second through"; under this load both "briefly" and "a second" understate it.

The follow-up is therefore to make `IdleStealBackoff` honor its own invariant — e.g. generation-stamp
the in-flight attempt so `reset()` clears backoff state without releasing a genuinely running
attempt's slot — **not** to add a second probe-concurrency controller. Still touches steal pacing, so
it needs its own design + adversarial review. Its acceptance gate should be split rate: strict
one-in-flight pacing risks capping splits near ~0.3–0.5/s when probes are slow, where the healthy run
needed 380 splits in 173 s (~2.2/s). Fixing starvation from the other side would be no better.

(An earlier revision of this note claimed "64 workers can fire ~64 simultaneous probes". That
overstates the mechanism — the design intends one, and the real number is set by the leak rate, not
by worker count. Corrected here so the follow-up targets the leak.)

### Found while investigating

`swath.rate_limit.wait` reported `count=6819, total_ms=143045, max_ms=0.001117` — 21 ms of average
slot wait against a sub-microsecond max. Not a unit bug: Micrometer's default
`DistributionStatisticConfig` is a rolling window (`expiry=2m`, `bufferLength=3`), so `max` and every
published percentile decay while `count`/`totalTime` stay cumulative. Slot contention stopped once
concurrency collapsed, and the rolling max had decayed to nothing by the time the summary was
written. Every `probe_latency[]` percentile and `shape.regime.api_latency_p*` shared the defect on
any run longer than two minutes. Fixed in `RunMetrics#DISTRIBUTION_WINDOW`.

---

## 2026-07-25 — structure-probe timeout suppression (follow-up to the above)

Closed the feedback gap identified above: a structure probe that times out reports nothing, so it
threw past the fan-out accounting and left every per-victim suppression counter untouched — the
timeout destroyed the evidence that would have stopped the next probe. `Thief#probeStructure` now
attributes an `ATTEMPT_TIMEOUT` to the victim (and only that kind — a 503 is store backpressure, not
a statement about the keyspace) before rethrowing, and `structureProbesEnabled` suppresses on a
timeout streak of 2 as well as the existing zero-fan-out streak of 8.

Interleaved A/B against the immediately preceding commit, same bucket, alternating runs:

| round | build | result | keys/s | timeouts | splits | `structure_probe` p90 |
|---|---|---|---|---|---|---|
| 1 (unbounded) | before | **did not finish** in 260 s | 34,669 | 631 | 302 | 10,736 ms |
| 1 (unbounded) | after | **complete in 163 s** | 53,965 | 177 | 204 | 2,683 ms |
| 2 (110 s cap) | before | 4.64 M objects | 46,875 | 301 | 42 | 10,199 ms |
| 2 (110 s cap) | after | 6.56 M objects | 66,750 | 176 | 146 | 5,636 ms |
| 3 (110 s cap) | before | 4.45 M objects | 44,594 | 301 | 42 | 10,199 ms |
| 3 (110 s cap) | after | 5.65 M objects | 58,773 | 204 | 154 | 8,589 ms |

Throughput +32–42 % in every round, timeouts roughly halved, and the structure-probe p90 tail cut by
1.2–4×.

**On split count — read it per unit time.** The unbounded round shows *fewer* absolute splits after
the change (204 vs 302), which looks like a regression and was initially recorded as one. It is an
artifact of comparing runs of different length: the "before" run was still going at 260 s, so it had
far longer to accumulate splits. The equal-duration rounds are the honest comparison, and there
splits went **up 3.5×** (146/154 vs 42/42 in the same 110 s). Suppressing hopeless probes does not
cost parallelism — it buys it, because the thief stops spending its steal attempts on regions that
cannot answer.
