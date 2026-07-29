# swath — metrics internals (contributor reference)

> **You don't need this to use swath.** This is the deep, contributor-tier companion to
> [`docs/metrics-and-observability.md`](../metrics-and-observability.md) — the JSON run-summary
> **forensics fields**, the **instrumentation-discipline** essay and **steal-reason counter
> registry**, the **run-trace format** spec, and the **replay-server meters**. The user-facing
> meter glossary, `list_run_summary` line, `-v` progress line, exit codes, and the core JSON
> summary shape all live in that file.

**Section references.** §1 / §1.1 (meters), §2 (`list_run_summary`), §3-core (the JSON summary
shape + efficiency ratios + `stop_source`/`error_class`), §4 / §4a (progress line), §6 (exit codes),
and the §7 `--trace` user intro are in [`docs/metrics-and-observability.md`](../metrics-and-observability.md).
The §3 forensics fields, §5, §5a, §7 trace format, and §8 below are here.

---

## 3. JSON run-summary — post-hoc forensics fields

**Every distribution statistic is run-scoped.** `max` and all published percentiles
(`probe_latency[]`, `shape.regime.api_latency_p*`, and every `swath.*` timer's `max_ms`) cover the
whole run, exactly like the `count`/`total_ms` beside them. This is deliberate and non-default:
Micrometer's stock `DistributionStatisticConfig` is a *rolling* window (`expiry=2m`,
`bufferLength=3`), under which `max`/percentiles decay while `count`/`totalTime` stay cumulative — so
a single summary row would mix two time bases, and any run longer than two minutes would report
percentiles describing only its final window. `RunMetrics#DISTRIBUTION_WINDOW` pins one
non-rotating bucket instead; `RunMetricsDistributionWindowTest` is the guard. If you add a `Timer` or
`DistributionSummary` to `RunMetrics`, build it through `runScopedTimer`/`runScopedSummary` or it
will silently reintroduce the rolling window.

**Consequence for any MID-RUN reader.** These percentiles are now run-cumulative, not recent-window,
so a consumer that samples them *while the run is going* (a progress line, a live dashboard, an OTLP
scrape) sees the run so far rather than "the last two minutes". That is the correct semantics for the
end-of-run summary these feed today, but a future live-monitoring consumer that genuinely wants a
recent window must derive it (successive scrapes) rather than assume decay. Memory is unaffected:
expiry governs histogram rotation, not bucket count, and `bufferLength=1` holds fewer rings than the
default 3.

These are the deep, post-hoc forensics fields of the JSON run-summary artifact (the artifact itself,
its write/atomicity semantics, and the `stop_source`/`error_class` terminal facts are in
[`docs/metrics-and-observability.md`](../metrics-and-observability.md) §3). Each reconstructs some
aspect of a run's shape or trajectory from one artifact, without a separate metrics backend or a log
join. All are omitted (not null-valued) on the construction paths where they never computed.

**`seed`** (optional): `SeedStep`'s already-computed shape for a fresh run that actually
seeded (`mode` is `none`/`shallow`/`hints`; `probes`/`cut_points`/`synthesized_cuts`/`ranges` are
its exact worklist-tiling accounting) — promoted here from what was previously only the
`seed_shallow`/`list_seed` log lines, so post-hoc analysis reads them without scraping logs. Omitted
entirely (not a null-valued object) on a resumed run — seeding never re-runs on resume — or a
sequential/no-checkpoint run that never seeds. The companion classification counter is
`SEED.<shape>` in `swath.steal_reason` (§1/§5): `flat_trivial`, `dense_root_radix_banded`,
`tiny_leaf_explosion`, and the generic `delimiter_seeded` (plain `delimiter=/` cut-point tiling,
none of the other subtypes applied). (`scatter_scout` — the opt-in scout-placed variant of
`dense_root_radix_banded` — is no longer emitted: it had zero engagement in practice.)

**`seed.cuts_discovered`** (issue #83): how many DISTINCT cut points the descent actually found via
probing, BEFORE any over-cap subsample reduced that set toward `cut_points` (equal to `cut_points`
whenever `cuts_discovered <= targetSeeds` and no subsample ran). Exists because `cut_points` alone
cannot distinguish "the descent found little" from "the descent found plenty and a subsample
discarded most of it" — the second shape hid a real collapse bug in `massWeightedSubsample` (a
handful of heavy-weighted samples could starve the whole walk down to a small fraction of its
budget even with `cut_points`/`SEED.mass_weighted_subsample` looking unremarkable) until this field
made `cuts_discovered ≫ cut_points` directly readable post-hoc. A healthy subsampled run still shows
`cuts_discovered > cut_points` (that is the subsample doing its intended job); the pathology is
`cut_points` sitting far BELOW `targetSeeds` despite `cuts_discovered` clearing it comfortably.

**`seed.decisions[]`**: the per-probed-level seed decision trace — one entry per `delimiter=/`
structure probe `SeedStep` issued while building the tiling above (bounded by the same probe cap the
`probes` field already reports, ≤ ~256; index `0` is always the top-level probe). Each entry is
`{prefix, fanout, truncated, classification, cuts_kept, cuts_discarded, depth, quota_cut_off}`:
`prefix` is the probed directory (display-escaped/truncated, the same rendering every other byte-key
field in this document gets); `fanout` is that level's raw `CommonPrefixes` count; `cuts_kept`/
`cuts_discarded` are this level's OWN contribution to the global cut-point set (new distinct cut byte
strings added vs. duplicates of a cut already present) — `0`/`0` for a level whose prefixes were never
tiled. `depth` is this probed prefix's delimiter-count depth (`0` for the top-level/
`top_probe_paginated` entries, which precede the depth-ordered descent frontier). `quota_cut_off`
(issue #15's per-depth yield quota) is whether this entry's own depth had ALREADY been marked cut
off — its recent probes stopped averaging at least one new cut per probe — by the time this probe was
issued, i.e. this decision only happened because the frontier fell back to a starved depth once
nothing better remained; always `false` for the top-level entries and for a `mass_aware_seed=off` run
(the plain FIFO frontier has no notion of depth). `classification` is one of:

| value | meaning |
|---|---|
| `narrow` | not truncated — kept descending (or, for a small/trivial flat top, nothing more to probe). |
| `tiny_leaf_explosion` | truncated WITH common prefixes. Two distinct sub-cases share this label (a known, intended overlap): **(a) the TOP-level probe (index 0)** — ANY truncated top with common prefixes gets this classification regardless of whether the names are `key=value/`-shaped, but it is NOT left whole: the top level's cuts were already captured (`addCutsCounted` runs before the truncation check, unconditionally), so it still tiles via the GENERIC top-level cap (`cuts_kept > 0`, up to `min(1000, 4*W)` ranges — `SeedStepRootFanoutBudgetTest` pins this). **(b) a descended SUB-level (index ≥ 1)** whose common prefixes are plain (non-`key=value/`) directory names — a true 1:1 directory explosion genuinely left whole for work-stealing (`cuts_kept == 0`), never enumerated further. |
| `fanout_tiled` | a DESCENDED SUB-LEVEL ONLY (never the top-level probe, index 0 — see the `tiny_leaf_explosion` row) that is truncated WITH common prefixes shaped like Hive/Spark `key=value/` partition directories — tiled at seed time along a `W`-capped subset of those already-probed prefixes (`cuts_kept > 0`), instead of the earlier break-and-discard. A root-level (`key=value/`-named or not) truncated fan-out never reaches this classification or its `W` cap; it is bounded by the generic top-level `4*W` cap instead (see the `tiny_leaf_explosion` row (a)). |
| `flat_wide` | truncated, NO common prefixes, only direct objects — a dense-flat-region radix-banding CANDIDATE. |
| `dense_root_radix_banded` | a `flat_wide` region that was ACTUALLY radix-banded (only known after the whole seed collection is in hand — a run-level disposition promoted onto that one level's entry). The promotion matches the level by prefix equality, so the top-level entry of a whole-bucket scan records the NORMALIZED empty prefix rather than the raw `null` the API also accepts — before issue #29/#33 was fixed a dense flat ROOT reached by a literal-`null` prefix kept the `flat_wide` label while the run-level `SEED.dense_root_radix_banded` counter fired, the two artifacts disagreeing. Unreachable from the CLI, which normalizes in `S3Uri`. |
| `delimiter_seeded` | the TOP level only, when the run's overall shape is the generic plain-tiled case (no dense-root/tiny-leaf subtype applied anywhere) — mirrors the run-level `SEED.delimiter_seeded` classification above. |

Reconstructs a bucket's seed-time structure-probe trajectory (which levels were probed, what each
one looked like, and what the seed step decided to do about it) without re-deriving it from logs.
Absent (an empty array, never omitted) on `--tune seed.mode=none` (no probe ever runs) and — like the rest of
`seed` — the whole `seed` block is omitted on `swath resume` (seeding never re-runs).

**`trajectory`**: the bounded time-bin rollup of in-flight concurrency + progress rate over
the run, plus four derived scalars — reconstructs a dense-shape bucket's fan-out trajectory (did it
ramp up and stay parallel, or collapse to a near-serial tail, and when) without a separate metrics
backend. `in_flight[]`/`progress_rate[]` are parallel arrays, one entry per **time bin actually
used** (never zero-padded to a fixed length): the average in-flight listing count and the keys/sec
observed in that bin. Bounded memory regardless of run length — a fixed **30-bin** rollup that
halves its live bin count (merging adjacent pairs) and doubles its bin width whenever the run's
elapsed time would otherwise need more bins, the classic ring/doubling-bucket downsample; a window
between two in-flight transitions is attributed to the bin containing the window's END instant (a
documented approximation — this is a diagnostic rollup, not a correctness measurement). Folded on
the SAME "sample on every transition" seam `swath.in_flight.avg` (§1) already uses, so it costs
nothing extra on the healthy path beyond a few array writes per LIST call.

| field | meaning |
|---|---|
| `serial_frac` | fraction of total wall-time spent at `<= 2` in-flight (time-weighted over the bins actually used). |
| `collapse_at_frac` | the fractional bin index where a TRAILING run of `<= 2` in-flight began (e.g. `0.5` = the back half of the run ran serial) — `null` when the run never permanently collapsed (its last bin is still `> 2`, the good outcome). |
| `peak_workers` | the existing `peak_in_flight` counter, reused here (not a second measurement). |
| `final_workers` | the last bin's average in-flight, rounded — how parallel the run was in its final moments. |

Omitted entirely (not a null-valued object) on the same construction paths `shape` is (no page was
ever fetched — a pre-run early-exit summary).

**`slow_ranges[]`**: the top-10 slowest/remaining live ranges at the INSTANT this summary was
built (by estimated remaining span, descending), each with its own per-range `steal_reasons` tally —
a terminal (or mid-run periodic) snapshot of exactly which ranges are dragging and why, without
joining the per-run `swath.steal_reason` totals back to a specific range. Each entry is `{lo, hi,
cursor, est_remaining, drain_rate, steal_reasons}`: `lo`/`hi`/`cursor` are display-escaped/truncated
range bounds (never raw, unbounded key bytes); `est_remaining` is that range's own `StealMath`
remaining-span estimate (the same one victim selection uses); `drain_rate` is its observed keys/sec
since the range was created. **`est_remaining` is JSON `null` for an OPEN-FRONTIER range** (`hi ==
null` — `StealMath#estRemaining` returns `Double.POSITIVE_INFINITY`, "always scores highest"):
`null` here means "unbounded / open frontier", not "unavailable" (a distinct sentinel meaning from
every other `null` in this document). The ranking that produces this top-10 list still treats
`+Infinity` as the largest value (an open-frontier range still sorts first) — only the JSON
rendering is `null`, never the non-finite value's Jackson default (which would otherwise render the
literal string `"Infinity"`, silently turning a numeric field into a string and breaking the
additive-schema claim; this path is exercised on every periodic mid-run flush, not just the
terminal write, since an open frontier is a normal live-range shape mid-run). `steal_reasons`
carries four plain per-range counters — bumped
alongside the existing GLOBAL `swath.steal_reason` counters at the same decision points (never a new
hot-path check, never a per-range map): `cursor_passed_pivot` (a thief lost the race — the drainer's
cursor had already passed the pivot), `no_pivot` (this range hit a genuine dead end — no room left to
split), `structure_suppressed` (this victim's `delimiter=/` structure probes were suppressed, either
because it proved zero-fan-out or because its probes kept timing out — see
`STRUCTURE.suppressed_zero_fanout` / `STRUCTURE.suppressed_probe_timeout`, §5; this one per-range
tally covers both, though the global counters distinguish them), `demand_gated` (this range's OWN proactive
owner-split was suppressed by the saturation/demand gate — see `OWNER_SPLIT.demand_gated`, §5).
Always present as an array (possibly empty — an empty array is itself informative: nothing is left
in flight, e.g. a genuinely COMPLETED run has already drained every range by the time its terminal
summary is built).

**`probe_latency[]`**: the per-call-class latency-phase percentile breakdown — the dedicated
readback of `swath.fetch.latency.phase` (§1) since the generic `meters[]` array below carries a
Timer's `count`/`total_ms`/`max_ms` only, not its percentiles (the same reason `shape.regime.
api_latency_p50_ms`/`_p99_ms` needed one). Each entry is `{call_class, phase, count, p50_ms, p90_ms,
p99_ms, max_ms}` — `call_class` is `worker_page`/`pivot_probe`/`structure_probe`, `phase` is
`connect_acquire`/`ttfb`/`sdk_unmarshal`/`total`/`response_parse` (see §1's meter row for exactly
what each phase does and does NOT capture — they are NOT guaranteed additive to `total`). Always
present as an array (possibly empty — never omitted); a `call_class`/`phase` pair with zero
observations is omitted from the array, never a fabricated all-zero row.

**The two response-side phases, and the residual they narrow.** `total` minus `ttfb` is everything a
page costs the client after the store started answering, and for a long time it was readable ONLY as
that subtraction — a single number with no distribution, no call-class attribution and no
decomposition. Two of the five phases now sit inside it:

- **`sdk_unmarshal`** — the SDK's own response-handling window: first response byte through the SDK's
  protocol response handler returning. Bridged in by `S3CallClassLatencyPublisher` from the SDK's own
  per-attempt stamps, not measured by swath. On the sync `ApacheHttpClient` path the body is still a
  live socket stream when that handler runs, so it spans draining the remaining response bytes off the
  wire **plus** the XML parse and POJO construction. It is the DOMINANT term of the residual on a full
  1000-key page.

  **Why it is derived rather than read.** `CoreMetric.UNMARSHALLING_DURATION` is the exact boundary and
  was this phase's first implementation — but it is never published for an S3 `ListObjectsV2`, and that
  is true **by construction, not by accident**: `DefaultS3Client` holds an `AwsS3ProtocolFactory`, which
  overrides `createCombinedResponseHandler` to route through its own
  `createErrorCouldBeInBodyResponseHandler` (S3 can return an error document under a `200`), and that
  path never goes through `AwsXmlProtocolFactory.timeUnmarshalling` — the decorator that reports the
  metric. So no S3 operation reports it and no SDK upgrade within that design will start. The phase was
  simply absent from real runs while every unit test (which hand-builds the metric tree) stayed green;
  `S3SdkUnmarshalPhaseLocalStackIT` is what caught that, and it is now the standing guard.

  What the SDK *does* publish is `TIME_TO_LAST_BYTE`, stamped in `HandleResponseStage` **after** the
  response handler returned — so the publisher derives `TimeToLastByte - TimeToFirstByte`, taking both
  stamps from the SAME `MetricCollection` so a retried call can never cross attempts, and letting a
  collection that reports a first byte supersede any earlier attempt's window (a final attempt that
  read-timed-out mid-body has no window of its own, and an earlier attempt's must not stand in for it).
  A negative difference is rejected rather than clamped; an exact-zero window is admissible and records
  as a real sample. Treat the result as a close UPPER bound on the SDK's client-side response cost,
  never as pure CPU.
- **`response_parse`** — swath's own conversion of the returned response object into `ListEntry`/
  `KeyBytes`, after the call returned. Cheap by comparison.

**What is still only-a-residual after subtracting both.** `total - ttfb - sdk_unmarshal -
response_parse` is NOT zero. It still contains, at minimum:

- **The SDK's response-INTERCEPTOR chain**, which runs after the response handler and is therefore
  outside `sdk_unmarshal` — for S3 that includes the percent-decode of the `encoding-type=url`
  response **swath itself requests** (`S3PageFetcher` sets `EncodingType.URL` on every request; the
  SDK's decode interceptor is always registered and engages because of that choice), which walks every
  key and rebuilds the response object.
- **Everything request-side, because `ttfb`'s zero is the ATTEMPT start, not the API call's.** The SDK
  reports `TimeToFirstByte` equal to `ServiceCallDuration` — i.e. measured from the HTTP execute —
  so marshalling, endpoint resolution, credential fetch and SigV4 signing all sit in the residual, in
  front of `ttfb`.
- **Retry backoff sleep and every superseded attempt.** `total` is the fetcher's wall-clock around the
  whole API call including all attempts, while `ttfb`/`sdk_unmarshal` describe the LAST attempt only.
  On a retried call (a 503 ridden out transiently) the residual therefore absorbs the earlier attempts
  and their backoff delays outright — material, and the reason a per-page cost read should be taken on
  a low-throttle run or cross-checked against `swath.throttle.events`.

So a per-page cost analysis must still report the residual rather than assume the decomposition is
complete. The subtraction itself is legitimate for these phases (they follow one another in
wall-clock); it is `connect_acquire` and `ttfb` that must never be summed — see §1's meter row.

**`client_cost[]`**: the per-page **client-service-cost** decomposition — what one page costs the
CLIENT once the store has answered, split into the spans that can contend independently. Each entry
is `{span, count, p50_ms, p90_ms, p99_ms, max_ms}`, the same shape and the same dedicated-readback
reason as `probe_latency[]` above (the generic `meters[]` array carries a Timer's
`count`/`total_ms`/`max_ms` only, never its percentiles). Always present as an array (possibly empty);
a span with zero observations is omitted, never a fabricated all-zero row.

| `span` | source meter | what it measures |
|---|---|---|
| `checkpoint_commit_wait` | `swath.checkpoint.commit.wait` | the FETCH WORKER's own blocking wait for its page commit to become durable (the I1 commit-before-emit await) — **one observation per page**. This is what a page actually paid; the two writer-thread spans below decompose the same work as the single writer sees it. |
| `checkpoint_queue_wait` | `swath.checkpoint.queue.wait` | per checkpoint TASK: enqueue → the batch drain that picked it up. **Not page-scoped**: the queue also carries the run's lifecycle tasks (`openRun`/`insertNode`/`completeNode`), so its count is `>=` the page count, never exactly it. |
| `checkpoint_commit` | `swath.checkpoint.commit.latency` | per writer-thread BATCH: op execution + `conn.commit()` (the WAL-fsync critical path). |
| `emit` | `swath.emit.latency` | per page: the consumer stage's whole sink write (format+write for text, pool dispatch for Parquet, lane admission for `--sort`), including that stage's own row tally. |
| `writer_backpressure` | `swath.queue.wait` | per page: the fetch worker blocked handing the page onto a full downstream channel. |
| `parquet_write` | `swath.parquet.write.latency` | per stretch of Parquet WRITER-LANE work: the encode+write of a batch's rows into the open part, plus any finalize it triggered (footer fsync, part MD5, manifest rewrite), timed on the lane's own thread between two waits on its queue. **Not page-scoped** and **not on the page's critical path**: one observation per batch written, plus one per idle-cadence rotation and one per lane's drain-time finalize/discard, so its count is `>=` the page count on a clean run (an aborted/failed run drains its queued batches without writing them, and those record nothing). Parquet-sink runs only. |

**Reading it.** The spans are percentile-bearing precisely because a per-page cost read as a MEAN
cannot distinguish an iid per-page cost from a queue behind a shared single writer — whose tail grows
with worker count while its mean may not. `checkpoint_commit_wait` ≫ `checkpoint_commit` with
`checkpoint_queue_wait` climbing as `T` climbs is the contended-writer signature; the three moving
together and flat in `T` is the iid one. The two **response-side** members of the same decomposition
are deliberately NOT here: both are attributable per call class, so they live in `probe_latency[]` as
`phase=sdk_unmarshal` and `phase=response_parse` rather than being flattened into call-class-blind
rows — and `sdk_unmarshal` is by far the larger of the two, so a client-cost read that consults only
`client_cost[]` will understate per-page client cost by roughly an order of magnitude. Note also that
`checkpoint_commit_wait` is near-zero (but still recorded) on a run with no checkpoint — there is
nothing to wait for, which is a real client cost of zero, not a missing measurement.

**Why `parquet_write` exists.** For the text sinks `emit` is the whole sink write, so summing the
spans of a run brackets its process CPU. For the Parquet sink it is not: `emit` ends at the handoff
to the writer pool (dispatch — a rounding error per page), and the encode/compress/write that
follows happens on the pool's own lane threads. Without a span there, that work shows up in the
process's CPU with nothing attributing it, and a CPU-accounting cross-check (summed spans against
`cpu_seconds`) under-accounts on Parquet runs while closing on jsonl ones — measured as roughly
2 ms/page of pool CPU at low concurrency, of which `swath.parquet.finalize.latency` caught only the
footer-fsync sliver. `parquet_write` is that missing term, measured around the lane's own work.

**Not additive — same non-additivity discipline as `probe_latency[]` above.** THREE overlaps live
here, not one: two pairs of spans that measure the same work from both ends, plus `parquet_write`,
which overlaps everything because it is measured on other threads. Summing spans (or cross-checking
their sum against a run's wall-clock/page count) must account for all three: (1)
`checkpoint_commit_wait` is the SAME durability work as that page's share of
`checkpoint_queue_wait` + `checkpoint_commit` — one is the fetch worker's own observed wait, the
other two are the writer thread's per-task/per-batch view of the identical commit, not additional
cost on top of it; and (2) `emit` and `writer_backpressure`
overlap in wall-clock by construction — the worker blocks handing a page onto the downstream channel
precisely because the consumer stage is still inside that page's (or an earlier page's) `emit` span,
so the two are two ends of the same handoff, not sequential costs. And (3) `parquet_write` is
measured on the pool's lane threads, which run concurrently with the fetch workers and the consumer
stage, so it overlaps *every* span above in wall-clock and is never part of a page's serial
latency — while for CPU accounting it is genuinely additive to them
(different threads, disjoint CPU). It also strictly CONTAINS the `swath.parquet.finalize.latency`
sample of any rotation that fired inside the stretch, so those two must never be added together.

**`demand_gate`**: the `OWNER_SPLIT.demand_gated` fixed-threshold/effective-T snapshot — `{events, last_t, min_t,
t_max}`. `events` is the total count of demand-gate suppressions this run; `last_t`/`min_t` are the
effective concurrency target observed at the most recent / lowest-T gate event (the companion
`swath.owner_split.demand_gated_t`/`_t_min` gauges, §1, carry the same two numbers live); `t_max` is
the run's configured ceiling, repeated here so the block is self-contained without cross-referencing
`config.max_parallel_listings`. The gate itself always compares live-node
`outstanding` (queued plus active) with fixed `t_max`; `last_t`/`min_t` only show
whether adaptive concurrency happened to be shed when those fixed-threshold gate
events occurred. Shrinking effective `T` does not close the gate.
Omitted entirely (not a null-valued block) when the demand gate never fired this run — same idiom as
`seed`/`shape`/`trajectory`.

**`shape`**: one flat, log-join-free **feature-vector** of post-hoc classification
signals — the substrate for post-hoc shape classification. It is a
run-level **AGGREGATE**, and on a `--max-duration`/interrupted run it describes only the **listed span**,
not the whole bucket (read `partial` — it mirrors `!completed` — alongside the top-level `stop_reason`).
Fields:

| field | source | how to read it |
|---|---|---|
| `alphabet_cardinality[]` | `AlphabetDigest` per-position printable-ASCII masks, UNION-ed across every completed node | distinct scalars observed at each of 8 relative code-point positions **past each range's divergence point**. Low (≤16) = hex/UUID/base64; high = base58/natural-language. A single vector washes out per-range drift — a **v1 corpus signal, said honestly**. **`entropy` is NOT computed** (only cardinality is tracked). |
| `alphabet_positions_observed` | derived | how many of those 8 positions saw any signal. |
| `divergence_depth_histogram[]` | the LCP depth where each split's pivot diverges from its range's cursor (the byte the split turns on), from `recordPivotByteRegion` | the depth distribution of splits; the last bucket is depth `>= 15`. Shallow = splitting near the root; deep = splitting inside long shared prefixes. |
| `mass_skew_gini` | Gini over the `CHILD_MASS.{empty,tiny,small,large}` distribution | inequality of per-child emitted mass; a **coarse 4-bucket approximation** (raw per-child masses are never retained). High = bimodal empty/large = the zero-transfer-split fingerprint. |
| `delimiter_fanout.{max,total,probes}` | `commonPrefixes().size()` at each seed + thief `delimiter=/` structure probe | the widest / total delimiter-child count observed and how many probes saw it — the bucket's branching factor. **Thief contributions saturate at `ThiefPolicy.STRUCTURE_PROBE_MAX_KEYS`**, so on a bucket whose structure is discovered at steal time rather than at seed time this under-reads the true branching factor; `STRUCTURE.fanout_capped` says how often that happened. Seed probes are unaffected (they use the full `PROBE_PAGE`). |
| `regime.api_latency_p50_ms` / `_p99_ms` | client-side percentiles of `swath.api.latency` (now `publishPercentiles(0.5, 0.99)`) | the RTT confound; `null` when no call was timed. |
| `regime.worker_count` / `region` | the run config (`--concurrency`, `--region`) | the two other regime confounds (`T`, region). |
| `regime.throttle_includes_attempt_timeout` | constant `false` (since client attempt-timeouts were reclassified as transients) | client `attempt_timeout`s no longer fold into `THROTTLE.*` / the AIMD signal — they are `TRANSIENT.*` (retried, no AIMD vote). `throttle_events` counts real 503/5xx only; see also `transient_events` / `aimd_votes`. |
| `fingerprint.{binary_sha256,git_sha,started_at,finished_at}` | process/build (best-effort; `null` when unavailable — e.g. a dev exploded-classes run has no `binary_sha256`) | run identity for reproducibility. `argv` is the **top-level** array (referenced, not duplicated). |

The block is **omitted entirely** (not written half-null) when no shape was computed. `partial:true` marks
a shape vector that covers only the listed span of a timeboxed/interrupted run.

---

## 5. Instrumentation discipline & post-hoc classification (why swath emits so much)

**Why.** swath runs **one** engine (`WorkStealingScan`) across varied supported
general-purpose S3 key distributions — flat, deep,
fan-out, dense-tail, hex/UUID, natural-language. We deliberately do **not** bake bucket-type detection or
per-shape routing into the engine: a router that guesses the shape mis-routes a meaningful fraction (the
very reason swath replaced the four-strategy router with one engine). Instead the engine emits **rich raw per-path
signals** and we classify **externally, post-hoc**, over the emitted counters. That buys three things:
(1) a faithful record of *how swath actually behaved* on each real bucket; (2) the evidence to find which
shapes are slow/fast and **why** — every one of these steal-path behaviors was diagnosed from these signals, not a debugger;
(3) a **data-driven bucket taxonomy** built from real runs rather than guessed up front.

**What we instrument (one coherent taxonomy).** Every steal/split/seed decision reports through the same
taxonomy so the signal set stays comparable:
- **Engagement counters** — `metrics.recordStealReason(category, reason)`: *did this path fire, and how
  often.* Live set: `NO_VICTIM.no_splittable_victim`, `RETRY.{cursor_passed_pivot, bound_moved,
  retry_pivot_adjacent}`, `CHILD_CREATED.split_committed`, `UNSPLITTABLE.no_pivot`,
  `OWNER_SPLIT.{self_published, self_aborted}`. The **ratio** between paths is the diagnostic
  (`cursor_passed_pivot ≫ split_committed` = the cursor-passed-pivot race; `OWNER_SPLIT.self_published` dominating
  = the owner-side split carrying the tail).

  **Pivot-mechanism attribution.** A winning `CHILD_CREATED` split also records which of the seven
  pivot mechanisms produced it, via a second `recordStealReason("PIVOT", <mechanism>)` at the same
  hand-off:

  | mechanism | tag | fires when |
  |---|---|---|
  | byte-midpoint | `PIVOT.midpoint` | plain code-point midpoint of `(cursor, hi]`, or the far-ahead step-back |
  | far-ahead | `PIVOT.far_ahead` | the far-ahead fraction (density-informed, `>0.5`) |
  | open-frontier extrapolation | `PIVOT.extrapolate` | `hi == null`; density-reflected pivot toward the prefix ceiling |
  | plain structure probe | `PIVOT.{structure_probe, structure_capped}` | the empty-upper funnel's `delimiter=/` discovery — `structure_probe` when the probe page was complete (true-median pivot), `structure_capped` when it truncated at `STRUCTURE_PROBE_MAX_KEYS` (furthest proved boundary) |
  | adaptive structure back-out | `PIVOT.{adaptive_structure, adaptive_structure_capped}` | the parent-empty sliver's coarse→fine `delimiter=/` back-out — kept distinct from `structure_probe` above so post-hoc analysis can tell which durability mechanism carried a split; the `_capped` variant marks the same truncated-page regime as `structure_capped` |
  | empty-upper bisection fallback | `PIVOT.bisect` | structure probe found nothing; retries nearer the cursor |
  | flat-leaf density reflection | `PIVOT.flat_leaf` | parent-empty sliver, no sub-directory structure — density extrapolation inside the leaf |
  | owner self-split | `OWNER_SPLIT.{self_published, self_aborted, demand_gated}` | the owner-side proactive split (a distinct category, not `PIVOT`, since it never goes through `Thief.steal`). `demand_gated` = the split was *suppressed* by the saturation/demand gate below because the worklist already held at least the fixed configured worker count (`Tmax`) in live nodes |
  | rank-space (alphabet-aware) synthesis | `ALPHABET.{alphabet_chosen, alphabet_fallback}` | an orthogonal modifier — did the observed-alphabet chooser deflect the pivot from the plain code-point value, for *any* of the mechanisms above (or an owner-split) that call `interpolate(..., alphabetDigest())` |
  | alphabet consult fallback reason | `ALPHABET.{fallback_out_of_window, fallback_no_room, window_gap}` | the per-consult version of `alphabet_fallback`: *why* an `AlphabetDigest.chooseScalar` consult produced no scalar — the position fell outside the tracked/clean window (`fallback_out_of_window`), there was no room for any scalar strictly between the bounds (`fallback_no_room`), or the observed alphabet had no value populating the gap (`window_gap`). Diagnoses the rank-space synthesis's dead-zone residual: `window_gap` dominating means the observed alphabet is too sparse to deflect the pivot. |

  **Over-fetch caps.** Two waste fingerprints: (B) owner-split *confetti* on
  large saturated buckets and (A) thief *probe*-storm on big skewed buckets, where probes can dominate
  the API-call budget. Each cap carries an engagement counter:
  - **Owner-split demand gate (fingerprint B).** On a saturated bucket the worklist already holds
    enough live nodes to keep every worker busy, so a proactive owner self-split buys ZERO extra
    parallelism and only over-fetches its bounded child's terminal page. `maybeOwnerSelfSplit` therefore
    suppresses the carve once `outstanding ≥ Tmax`, the fixed configured worker count
    (using the existing lock-free live-node `AtomicLong`, which counts queued plus active nodes),
    recording `OWNER_SPLIT.demand_gated` per suppressed split; a below-gate `(1−f)·est > 2·maxKeys`
    child-mass floor additionally forbids sub-two-page "confetti" children. The gate uses `Tmax`, not
    `2·Tmax`: on a saturated bucket idle thieves drain owner-split children as fast as they're created, so
    `outstanding` plateaus at ~Tmax and never climbs to `2·Tmax`, making that larger gate a no-op; `Tmax` engages
    because once every worker already has claimable work, an extra split still buys nothing. The gate
    stays dormant during ramp (`outstanding < Tmax`, still climbing to fill workers). The
    adaptive gauge's effective `T` is recorded as context but does not drive the gate, so
    load shedding cannot lower its threshold. It is also skipped entirely
    when `workerCount == 1` — with no second worker (and no thief) "buys zero parallelism" is moot, not
    true, and gating there would only shrink the durable checkpoint granularity for a lone worker,
    never save an S3 call. Post-hoc, `demand_gated` ≫ 0
    with a low `overfetch_ratio` is the fingerprint-B win; `demand_gated ≈ 0` means the run never
    saturated to `Tmax` live nodes and the gate was a no-op.
  - **Confetti realized-mass feedback gate (fingerprint B residual on skewed keyspaces).**
    The demand gate and the observed-mass child-tail floor both reason from UPSTREAM estimates
    (`est`/`densityRatio`, extrapolated from the in-cluster density already emitted). On a keyspace
    whose code-point tail thins out hard past the drained cluster (high mass-skew), those
    estimates still pass carves whose REALIZED emitted mass turns out confetti-sized — the estimate has
    no way to see the mostly-empty tail it is extrapolating across. `maybeOwnerSelfSplit` closes the
    loop with GROUND
    TRUTH instead: every owner-split child is tagged at creation, and on completion is classified
    `OWNER_SPLIT_CHILD.{confetti, substantial}` via `isConfettiChild` — confetti requires **BOTH**
    (1) `keysEmitted <= 2·maxKeys` (the same threshold the child-tail floor uses) **AND** (2) the
    child never itself split during its lifetime (no owner self-split, no successful thief steal,
    ever carved a child off it in turn — `WorkerState#hasSplit`). Condition (2) is load-bearing, not
    an edge case:
    on a dense/uniform range (this mechanism's own target shape) the owner-side split recurses
    deliberately deep, so a healthy intermediate node routinely finishes with a small own tally purely
    because it shed its own further tail(s) onward — that is proof the carve was worthwhile, not
    evidence of a thinning tail, and must never count against the observed rate. Only a node that
    never split AND still finished tiny is a genuine terminal confetti leaf — exactly the
    thinning-tail pathology (there the 1-page children never split further; they just
    end), restoring zero-regression-by-construction on non-skewed shapes. **Resume semantics:** the
    tag set and `hasSplit` are process-local, run-scoped state (never persisted to the checkpoint —
    the same "in-memory only, re-learned on resume" treatment as the per-worker density EWMA and the AIMD
    concurrency target). After a crash/resume, a child tagged before the crash completes UNTAGGED in
    the resumed process (no double-count — it simply contributes no classification, not a wrong one)
    and the gate's `MIN_SAMPLE` warmup restarts from zero in the new process; this is intentional, not
    a durability gap. Once at least `MIN_SAMPLE=8` tagged children have completed this run
    (a warmup — too few samples is not evidence), an observed confetti rate strictly above
    `SUPPRESS_THRESHOLD=0.5` suppresses further carving, recording `OWNER_SPLIT.confetti_suppressed`
    per suppressed attempt. Every `PROBE_K=16`-th would-be-suppressed attempt is let through anyway
    (`OWNER_SPLIT.confetti_probe`) so the gate can never starve its own feedback signal — and exactly
    **one** carve per probe slot, even when several owners consult the run-scoped gate concurrently and
    all decide the same slot is theirs: the executor resolves the winner with a `compareAndSet` on the
    sequence each of them snapshotted, and the losers record `confetti_suppressed` (issue #31; before
    that fix all of them carved) — a keyspace
    that later turns genuinely dense again recovers on its own once enough probes/completions pull the
    rate back at/under the threshold. `MIN_SAMPLE`/`SUPPRESS_THRESHOLD`/`PROBE_K` are hand-picked
    constants, not yet backed by a tuning sweep. Post-hoc, the
    `OWNER_SPLIT_CHILD.confetti / (OWNER_SPLIT_CHILD.confetti + OWNER_SPLIT_CHILD.substantial)` ratio
    IS the observed confetti rate the gate itself reads — a high ratio alongside a low
    `confetti_suppressed` count means the run never accumulated `MIN_SAMPLE` evidence (too few
    owner-splits fired at all); a high ratio alongside a high `confetti_suppressed` count is the gate
    engaging as designed. Diagnostic-tier `--engine-toggle confetti_feedback=off` (default **on**)
    restores exact pre-gate behavior (only the demand gate and the child-tail floor bound owner-split
    carving) — see the `--engine-toggle` ablation marks subsection below.
  - **Open-frontier serial-tail attribution (issue #76).** `SeedStep` closes every seed plan with one
    final open range `(lastCut, null]`, and `OwnerSplitGovernor.decide` early-outs on it before any
    other gate — an owner carve interpolates a midpoint between `lo` and `hi`, and a `null` upper
    bound has no midpoint, so the range structurally cannot be owner-split (thieves are not blocked
    the same way: `ThiefPolicy`'s `OPEN_FRONTIER_BAND_WIDTH_FALLBACK` still splits it). That
    interpolation argument is sound, but the corollary once drawn from it — "so there is nothing for
    post-hoc analysis to learn from a rate here" — is not: `(lastCut, null]` can hold an arbitrary
    share of a bucket's mass, and on a keyspace whose mass sits past the last seed cut it holds most
    of it, draining as a serial tail no thief happens to find. Left uncounted, the metrics cannot tell
    that shape apart from an ordinary gate-blocked tail. Two signals close the gap: the skip now
    records `OWNER_SPLIT.open_frontier` like every other gate (its own population — qualifying page
    commits against an unbounded range — is the diagnostic, not a ratio against a sibling reason), and
    `swath.open_frontier.keys_emitted` counts the keys actually committed while `hi` was `null` —
    folded at the same already-serialized page-commit site that reads `hi` to trim the batch anyway
    (O(1), no extra read, no per-key work). Divide it by `swath.entries.emitted` for the share of the
    run's mass the open frontier carried; a large share alongside a slow, low-parallelism tail is the
    fingerprint this instrument exists to surface.
  - **Reflect-lift, the zero-page-per-carve fix (root cause of the fingerprint-B residual on skewed
    keyspaces; the confetti gate above is demoted to a backstop).** On an adjacent-scalar range
    `ByteMidpoint.between`'s NONE branch appends
    `MIN_SAFE` (U+0020) to the cursor verbatim, ignoring the requested fraction — the owner's kept
    share `(cursorTo, m]` is then realistically empty (no key extends another with a byte `< 0x21`),
    so the owner's very next page-fetch comes back with **zero keys**, just to *prove* the carve was
    empty, before the range completes. That one wasted fetch is the ENTIRE waste mechanism: on an
    adjacent-scalar keyspace nearly every carve produces one zero-key final page, and the resulting
    over-fetch is predicted almost exactly by "one wasted page per carve"
    alone. Two earlier approaches do not work: (a) an owner-kept MASS floor
    (`est * spanIn(cursorTo, m, cursorTo, H) <= maxKeys`) blocks a genuinely healthy carve, because a
    re-scoped `[cursorTo, H]` span is measured in a different frame than `est`'s own `[ws.lo(), H]`
    window; (b) a STRUCTURAL relay-pivot guard that unconditionally blocked any carve shaped
    exactly `cursorTo·MIN_SAFE` collapses the owner-side split's own healthy count, because its relay carves
    are themselves NET-POSITIVE (free idle-worker feed at zero pivot-synthesis cost), so *suppressing*
    them removes healthy splits instead of just fixing the waste. The shipped fix keeps every carve
    (never suppresses) and instead LIFTS the pivot: when the FINAL post-clamp pivot `m` would
    leave the owner a sub-one-page kept share — measured honestly in `est`'s own `[ws.lo(), H]` frame
    (`fKeptLo`, never a re-scoped span — the exact bug attempt (a) had) — `maybeOwnerSelfSplit` lifts
    `m` up to the density-reflected pivot (`StealMath.extrapolate`, the same machinery the reflect
    clamp already uses) so the owner keeps roughly one page of REAL mass instead of a degenerate near-empty
    sliver: its final page partial-trims rather than coming back empty, and the child still gets the
    far tail. The lift only ever moves `m` UP (the reflect clamp above already owns the down direction) and
    only when the lifted child tail still clears the child-tail floor (`childTailBelowObservedMassFloor`, the
    same fraction-in-`[ws.lo(), H]`-frame math the clamp already uses); any condition failing falls through
    to today's unchanged, owner-split-safe carve. A winning lift records `OWNER_SPLIT.pivot_reflect_lifted`
    (the twin of `OWNER_SPLIT.pivot_reflect_clamped` — lift and clamp are the same
    reflected-pivot mechanism pulling in opposite directions).
    **Regime change, by design.** Because a lifted carve leaves the owner REAL mass to drain, the
    owner no longer completes instantly after the carve the way a pre-lift degenerate relay did — it
    re-engages the existing progress-gate (`SELF_SPLIT_MIN_PAGES_BETWEEN=32`) before it is eligible
    for another self-split. On relay-prone shapes this collapses the raw split COUNT by roughly 4×
    while the engine's own contracts still hold (API budget,
    `structure_probes < owner_splits`) — fewer, larger, REAL splits replacing many free zero-page
    relay hops, exactly the diagnosis's prediction. Post-hoc, `OWNER_SPLIT.pivot_reflect_lifted`
    alongside `ALPHABET.fallback_no_room` and the `CHILD_MASS.*` distribution jointly identify an
    adjacent-scalar-gap keyspace (hex/UUID/base64-style alphabets with narrow gaps between populated
    scalars) from the metrics alone. **Toggle hierarchy**: the lift is itself
    a density-reflection application (it calls the same `StealMath.extrapolate` the empty-upper
    reflection and the clamp already use), so
    `maybeOwnerSelfSplit` gates it on `toggles.reflect() && toggles.reflectLift()`, never
    `reflectLift()` alone — `--engine-toggle reflect=off` disables the reflected empty-upper pivot,
    the clamp, and the lift together (full
    reflection ablation, matching the "`reflect=off` restores exact pre-reflection placement" claim
    below), while `--engine-toggle reflect_lift=off` (default **on**) disables ONLY the lift, leaving
    the reflected pivot and its clamp active. Either path back to `reflect_lift` effectively off
    restores exact pre-lift behavior (every carve publishes at cursorTo's degenerate successor unchanged; the
    confetti feedback gate above remains the sole backstop for whatever degenerate shape the lift
    doesn't cover).
  - **Structure-probe zero-fan-out suppression (PER-VICTIM).** After `K=8`
    consecutive `delimiter=/` structure probes against **one victim** return zero fan-out (that
    victim's region has no sub-directory structure at the probed level), the thief stops issuing them
    against **that victim** (falling to the byte-midpoint/sliver fallback) and records
    `STRUCTURE.fanout_capped` per structure probe whose page truncated — the boundaries are only a
    prefix of the directory's children, so the committed pivot is the furthest one proved
    (`PIVOT.structure_capped`, or `PIVOT.adaptive_structure_capped` on the parent-empty sliver's
    back-out) rather than the true median (`PIVOT.structure_probe` / `PIVOT.adaptive_structure`). A
    high `fanout_capped` with few `*_capped` pivots means probes are truncating without producing
    splits; a high ratio of `*_capped` to uncapped structure pivots means the cap is shaping most
    carves and is the signal to weigh raising it against probe latency.

    `STRUCTURE.suppressed_zero_fanout` per suppressed probe; a 1-in-64 recovery probe still fires so
    late-appearing structure re-enables probing on that victim (any non-zero fan-out resets its
    counter). The counter and threshold live on `WorkerState`, not the shared `Thief`: a flat/suppressed
    victim can never suppress probing on a DIFFERENT (e.g. structured, parent-empty-sliver) victim —
    the earlier global-scope form was the same starvation anti-pattern as the reverted global-futility
    pacing. `swath.probe.structure_fetches` counts the structure-probe I/O this suppresses, and is
    folded into `wasted_probe_ratio`'s denominator.
  - **Empty-upper bisection budget.** The retry-nearer-cursor bisection is capped at a
    **log-scaled** per-attempt budget — `B = ceil(log2(bandWidthBytes)) + MARGIN(6)`, where
    `bandWidthBytes` is a coarse byte-distance estimate of the `(cursor, hi]` gap (a fixed fallback
    width when `hi` is the open frontier) — rather than a blunt fixed cap, so legitimate
    `O(log band width)` wide-gap convergence (a band wider than its content) always completes; on
    exhaustion (a gap whose true convergence depth exceeds even this generous estimate) the attempt
    records `RETRY.bisect_budget_exhausted` and bails rather than committing a doomed near-cursor
    pivot. Restores the "never unbounded LISTs per attempt" budget as a closed-form ceiling
    without narrowing the convergence contract in contracts.md §2.

  **Retry-reason attribution.** Transient page-fetch retries also tag *why* the
  engine backed off, via `recordStealReason(<outcome>, <reason>)` at the `S3PageFetcher`
  classification sites. **Service-side backpressure** votes AIMD down and is tagged `THROTTLE.*`:
  `THROTTLE.slowdown` (a 503 `SlowDown`/throttle **service** response) and `THROTTLE.server5xx` (an
  S3-side or intermediary 5xx server error, e.g. 500 InternalError, after retry exhaustion).
  **Client-side transients** are self-inflicted, retried by the swath-owned bounded retry, and do
  **not** vote AIMD down (counting client attempt-timeouts as throttling collapsed AIMD
  to serial with zero real `slowdown`); they are tagged `TRANSIENT.*`: `TRANSIENT.attempt_timeout` (an
  SDK per-attempt/per-call **client** timeout — `ApiCallAttemptTimeoutException` /
  `ApiCallTimeoutException`) and `TRANSIENT.network` (an exhausted network-class `SdkClientException` —
  connection reset, read-timeout, DNS/TLS failure, precisely discriminated as an `IOException`
  somewhere in the cause chain — that survived the SDK's own retry budget; deliberately narrower
  than "any non-service `SdkClientException`" so a deterministic credential/config/signing
  `SdkClientException` — no `IOException` cause — stays fatal instead of retrying). Only the two
  `THROTTLE.*` (service) reasons increment `swath.errors{type=throttle}` and cast an AIMD vote; the
  `steal_reason` split (plus `swath.throttle.events{type}` and the
  `throttle_events`/`transient_events`/`aimd_votes` diagnostics) lets post-hoc analysis distinguish a
  server-side throttle from a client-side latency stall from a network fault from a server-side 5xx
  (they imply different remedies — back off vs. raise the per-attempt timeout / lower `T` vs check
  network health vs treat as a transient S3-side incident).

  **`--engine-toggle` ablation marks.** `swath` exposes a diagnostic-tier `--engine-toggle
  NAME=VALUE` namespace (`docs/usage.md` §Diagnostic-tier ablation; NOT a supported configuration —
  the defaults are) for per-mechanism A/B measurement of the engine. Turning a mechanism off mostly
  proves itself *inherently* — its own engagement counters above go quiet (e.g. `structure_probes=off`
  silences `swath.probe.structure_fetches` and every
  `PIVOT.{structure_probe,structure_capped,adaptive_structure,adaptive_structure_capped}`;
  `alphabet_pivots=off` drives `ALPHABET.alphabet_chosen` to zero while `ALPHABET.alphabet_fallback`
  keeps firing; `radix_bands=off` silences `SEED.dense_root_radix_banded`). But two of them
  (`density_ewma`, `far_ahead`) only change the *inputs* to the existing far-ahead pivot arithmetic —
  they have no distinct code path of their own to go quiet — so their OFF state would otherwise be
  invisible from the counters alone. Every disabled toggle therefore ALSO fires an explicit,
  once-per-run `TOGGLE.<name>_off` mark (`recordStealReason("TOGGLE", "<name>_off")`) so post-hoc
  analysis never has to infer an ablation from absence:

  | toggle | where marked | fires |
  |---|---|---|
  | `owner_split`, `confetti_feedback`, `reflect_lift` | `WorkStealingScan`'s constructor | once per engine instance (all three are this engine's OWN mechanisms — never `Thief`'s) |
  | `density_ewma`, `structure_probes`, `far_ahead`, `alphabet_pivots`, `reflect` | `Thief`'s constructor | once per engine instance (`Thief` is constructed exactly once per `WorkStealingScan`, so this is still exactly once per run) |
  | `radix_bands`, `fanout_tiling` | `SeedStep`'s constructor | once per fresh-run seed step (resume never re-seeds, so a resumed run never re-fires it — consistent with both mechanisms only ever running at seed time) |

  The two default-divergent new-mechanism toggles use INVERTED once-per-run engagement marks
  instead: `TOGGLE.readahead_on` (opt-in) and `TOGGLE.mass_aware_seed_on` (fires on
  every default run since mass-aware seed descent flipped default-ON) — see their registry rows below.
  `TOGGLE.decision_rng_seeded` shares that polarity without being an `--engine-toggle` key at all: it
  marks the opt-in seeded-`DecisionRng` construction branch (`EngineContext#decisionRngSeed`), which
  is selected per run through the engine context rather than through the toggle set.

  A companion per-attempt tag on the mechanisms that ARE otherwise silenced distinguishes a genuine
  toggle-caused suppression from the pre-existing runtime suppression it would otherwise be confused
  with: `STRUCTURE.toggle_disabled` (vs. the per-victim `STRUCTURE.suppressed_zero_fanout` that
  suppression already emits) and `SEED.radix_bands_toggle_disabled` (a per-attempt mark recorded only when the
  toggle actually suppressed a banding that would otherwise have fired, i.e. `collected.flatWideRegion
  != null` — distinguishing "this run hit the dense-flat-region shape but the toggle turned banding
  off" from "this run never would have banded anyway"). The CLI additionally logs one startup INFO
  line (`engine_toggles_effective owner_split=... density_ewma=... ...`, all 13 toggles) whenever
  any toggle is non-default, and the JSON run-summary's `engine_flags` block (§3) echoes all 13
  toggle names every run.

  Mechanically, `recordStealReason`/`recordSeedBands`
  back a single real Micrometer counter, `swath.steal_reason{outcome, reason}` (lazily registered per
  `category.reason` pair via the same `computeIfAbsent` idiom as `apiCalls`/`errors`/`steals`; there
  used to be a second, parallel hand-rolled `ConcurrentHashMap<String,LongAdder>` doing the same
  counting, now removed). `list_run_diagnostics`' `steal_reasons` map is rebuilt by reading those meters
  back from the registry (`registry.find("swath.steal_reason").counters()`), so it is unchanged for
  today's consumers; the counter is also a first-class meter now, so it will export for free once
  `--metrics-port` (Prometheus) ships, and it is a real target for a future collect/discard `MeterFilter`
  gated on a debug flag (deferred) without inventing a second gate mechanism.
- **Aggregate efficiency** — the JSON summary's `api_calls_per_1k_objects` (over-fetch), `splits`,
  `steals`, `pages`, `peak_in_flight`, `cpu_efficiency`, and the derived `efficiency.*` ratios
  (`overfetch_ratio`, `page_fill_ratio`, `empty_split_ratio`, `wasted_probe_ratio`,
  `steal_success_rate` — §3).
- **Classification signals** — cheap facts about the *keyspace* a path observed (observed per-position
  **alphabet cardinality** — low = hex/UUID/base64 sparse, high = natural-language; prefix depth; local
  density). These are what let post-analysis **label** a bucket, not just measure it. The run-level
  aggregates of these are promoted into the JSON summary's flat **`shape`** feature-vector (§3) — alphabet
  cardinality, the divergence-depth histogram, the `CHILD_MASS` Gini, and delimiter fan-out — so
  post-hoc classification trains on them without log joins (satisfying the new-feature checklist: each rides an
  existing engagement counter — `ALPHABET.*`, `PIVOT_BYTE.*`, `CHILD_MASS.*`, the structure probe — and
  adds a discriminating summary field).

**The discipline — how to keep adding it for every new feature.** When you add a new algorithm path (a new
pivot strategy, split trigger, seed mode, backoff, router branch), you MUST:
1. **Emit an engagement counter** via `recordStealReason(category, reason)` — a distinct `category.reason`
   naming your path, so a post-hoc reader can see it fired, how often, and compare it against sibling
   paths. (The owner-side split's `OWNER_SPLIT.self_published` is *what proves* it carried the vast
   majority of a bucket's splits — one counter.)
2. **Emit any cheap classification signal your path observes** — if it learns something about the keyspace
   (alphabet cardinality, density, depth, entropy), surface it. Prefer facts that **discriminate shapes**.
3. **Surface it** in `list_run_diagnostics` and the JSON run-summary (counters flow through
   `steals{result}` / the meter registry automatically; a genuinely new dimension gets a summary field).
4. **Keep it zero-cost** — counters and end-of-run aggregates, never high-volume per-key logging;
   instrumentation must not change engine behavior or dominate the hot path.

**Why a test sees no checkpoint/Parquet meters.** They are **null-safe by construction** — a
`SqliteCheckpointStore`/`ParquetWriterPool` built without a `RunMetrics` (nearly every unit test)
registers none of them; only the production `ListCommand`/`ListRunner` path wires a live one through.

> **New-feature checklist (enforced in review):** *Does it add a distinct engagement counter? Can
> post-analysis tell from the metrics alone whether this path engaged on a given bucket, and whether it
> helped?* If not, it's under-instrumented — add the counter before the PR lands. **Reviewers must check
> this** on any new algorithm path, alongside the correctness and design-conformance gates — a missing counter is a
> review finding, not a nice-to-have.

**Post-hoc classification — the use, and how.** The signals feed external, post-hoc analysis
— **no bucket-type logic lives in the engine.** The base
discriminator is `splits`-trend × `in_flight`:

| pattern | class |
|---|---|
| splits stable+low, `in_flight` high | **fan-out** — work-stealing parallelizes (the design win) |
| splits climbing, `in_flight` ~1–2 | **deep/clustered** — near-serial regardless of size |
| splits 0, `in_flight` 1 | trivial single-page |

New signals **extend** this map: **alphabet cardinality** separates hex/UUID (sparse) from
natural-language keyspaces; the **`OWNER_SPLIT` vs `cursor_passed_pivot` vs structure-probe** ratio shows
*which mechanism carried* a bucket (proactive owner-split, reactive steal, or seed banding). To add a
discriminator: emit the signal (per the discipline), run it across a corpus of real buckets, and add the
signal→class mapping to the post-hoc classifier.

**Forward path.** These same counters are the substrate for an **online** classifier: the planned
`inspect` subcommand / a live shape-classifier can compute the bucket class *during* a run and surface
"this is a Class-2 deep-clustered bucket, here's why" — turning the post-hoc taxonomy into an
operator-visible feature. That is only possible because the raw signals are emitted comprehensively today.

### 5a. Steal-reason counter registry (CI-enforced)

Every `recordStealReason(category, reason)` / `recordSeedBands`-style `swath.steal_reason{category,
reason}` pair emitted anywhere in `swath-model`/`swath-core`/`swath-s3`/`swath-cli`'s
`src/main/java` MUST have a row below. A row is only exempt from
having a live emitter if its `status` column says `REMOVED <date>` (the counter was intentionally
retired — its emitter was deleted in the same change that added the annotation).

<!-- ci:steal-reason-table:start -->
| category | reason | meaning | status |
|---|---|---|---|
| `NO_VICTIM` | `no_splittable_victim` | no live victim had a splittable range this steal attempt. **The aggregate** — exactly one of the five discriminators below co-fires with it, so `sum(discriminators) == no_splittable_victim` and either series can be read alone | |
| `NO_VICTIM` | `pool_empty` | the steal-eligible pool was EMPTY: every live worker is awaiting its next non-empty page commit (`WorkerState#stealEligible`), which is the progress gate doing its job, not a failure. Expected to dominate on a healthy high-concurrency run and to be near-zero on a collapsed one | |
| `NO_VICTIM` | `all_no_remaining_span` | every candidate scored `estRemaining <= 0`. ⚠️ On a deep shared prefix this can be a MEASUREMENT artefact rather than a fact about the keyspace: `StealMath.fracIn` reads only `K = 12` bytes past the longest common prefix of `[lo, hi]`, so a cursor that agrees with `hi` across those 12 bytes yields a span that underflows to exactly 0.0 in double precision even with millions of keys left. A run where this dominates while the range is demonstrably not exhausted is the signature of that artefact and should be investigated before the range is believed | |
| `NO_VICTIM` | `all_futility_paced` | every candidate was in its per-victim futility cooldown (`WorkerState#stealPaced`, armed by consecutive `cursor_passed_pivot`/`bound_moved`/`bisect_budget_exhausted`). Means the thieves are correctly backing off racing drainers; co-read with `STEAL.futility_paced` for the skip volume | |
| `NO_VICTIM` | `all_unsplittable` | every candidate was cached `unsplittable` (a genuine no-pivot terminal — bounds UTF-8-adjacent, or a frontier cursor at its ceiling). The cache is PERMANENT for a worker, so a rising count means ranges are genuinely atomic, not transiently unlucky | |
| `NO_VICTIM` | `mixed_skips` | candidates were rejected for more than one reason in the same attempt, so no single discriminator explains it. A high share means the pool is heterogeneous and the aggregate should be read per-worker (slow-range dump) rather than fleet-wide | |
| `RETRY` | `unchanged_nonproductive_snapshot` | the victim's cursor/hi snapshot hadn't advanced since the last look | |
| `RETRY` | `cursor_at_or_past_hi` | the victim's cursor had already reached/passed `hi`; nothing left to steal | |
| `RETRY` | `unstarted_frontier` | the range hasn't started far enough in to slice yet | |
| `RETRY` | `retry_pivot_adjacent` | `c` and the live head are byte-adjacent now; re-steal later | |
| `RETRY` | `bisect_budget_exhausted` | the log-scaled empty-upper bisection budget ran out | |
| `RETRY` | `bound_moved` | the victim's bound moved during the attempt | |
| `RETRY` | `cursor_passed_pivot` | the victim's cursor raced past the computed pivot before the split committed | |
| `RETRY` | `split_aborted` | the CAS-guarded split commit was rejected by the store | |
| `UNSPLITTABLE` | `no_pivot` | no valid pivot exists in `(cursor, hi]` | |
| `CHILD_CREATED` | `split_committed` | a thief steal split off a child range | |
| `PIVOT` | `step_back` | the far-ahead step-back fired (stepped back to the plain code-point midpoint) | |
| `PIVOT` | `extrapolate` | winning mechanism: open-frontier density-reflected pivot (`hi == null`) | |
| `PIVOT` | `far_ahead` | winning mechanism: the density-informed far-ahead fraction (`>0.5`) | |
| `PIVOT` | `midpoint` | winning mechanism: the plain code-point midpoint of `(cursor, hi]` | |
| `PIVOT` | `structure_probe` | winning mechanism: the empty-upper funnel's `delimiter=/` discovery (complete probe page — true-median pivot) | |
| `PIVOT` | `structure_capped` | winning mechanism: as `structure_probe`, but the probe page truncated at `STRUCTURE_PROBE_MAX_KEYS` — the pivot is the furthest proved boundary, not the median | |
| `PIVOT` | `adaptive_structure` | winning mechanism: the parent-empty sliver's coarse→fine `delimiter=/` back-out (complete probe page) | |
| `PIVOT` | `adaptive_structure_capped` | winning mechanism: as `adaptive_structure`, but on a truncated probe page (furthest proved boundary) | |
| `PIVOT` | `bisect` | winning mechanism: structure probe found nothing; retried nearer the cursor | |
| `PIVOT` | `flat_leaf` | winning mechanism: parent-empty sliver, no sub-directory structure; density extrapolation inside the leaf | |
| `PIVOT` | `reflect` | winning mechanism: the density-reflected empty-upper pivot (`extrapolate(lo, c, H)` landed on populated mass) | |
| `PIVOT` | `reflect_hit` | the reflected empty-upper pivot probed NON-empty (seeded a commit at m_r, skipping the blind bisection) | |
| `PIVOT` | `reflect_empty` | the reflected empty-upper pivot probed empty; bisection re-seeded at the shorter `(c, m_r]` interval | |
| `STRUCTURE` | `suppressed_zero_fanout` | per-victim structure-probe suppression after K consecutive zero-fan-out probes | |
| `STRUCTURE` | `probe_timed_out` | a `delimiter=/` structure probe hit its attempt-timeout budget; recorded against the victim so the timeout streak can suppress further probing there (a timeout otherwise reports NOTHING, destroying the evidence that would stop the next probe) | |
| `STRUCTURE` | `suppressed_probe_timeout` | per-victim structure-probe suppression driven by the TIMEOUT streak rather than zero fan-out — the region could not answer at all, vs. answered "flat" | |
| `STRUCTURE` | `fanout_capped` | a structure probe's page truncated at `STRUCTURE_PROBE_MAX_KEYS` — its CommonPrefixes are a prefix of the directory's children, so any committed pivot is the furthest proved boundary (`PIVOT.{structure,adaptive_structure}_capped`), not the true median | |
| `OWNER_SPLIT` | `open_frontier` | the range is unbounded (`hi == null`) — the owner can never self-split it (no midpoint to interpolate), so this is not a suppressed carve like its siblings but the diagnostic itself: paired with `swath.open_frontier.keys_emitted`, its own population (qualifying page commits against an unbounded range) tells whether a run's tail sat on the un-carvable open frontier (issue #76) | |
| `OWNER_SPLIT` | `remaining_est_floor` | a proactive owner self-split was suppressed because the estimated remaining work does not clear `SELF_SPLIT_MIN_REMAINING_PAGES * maxKeys` — the range is too close to finishing to be worth a proactive carve (issue #16) | |
| `OWNER_SPLIT` | `rate_limited` | a proactive owner self-split was suppressed by the page-spacing rate limit — fewer than `SELF_SPLIT_MIN_PAGES_BETWEEN` committed pages have passed since this range's last published self-split | |
| `OWNER_SPLIT` | `demand_gated` | a proactive owner self-split was suppressed by the saturation/demand gate | |
| `OWNER_SPLIT` | `floor_reflected_blocked` | a proactive owner self-split was blocked by the observed-mass child-tail floor (reflected estimate) | |
| `OWNER_SPLIT` | `pivot_reflect_clamped` | the owner-split pivot was clamped down to the density-reflected pivot m_r (interpolate overshot the observed mass) | |
| `OWNER_SPLIT` | `pivot_reflect_lifted` | the owner-split pivot was lifted UP to the density-reflected pivot (degenerate pivot left the owner a sub-one-page kept share) | |
| `OWNER_SPLIT` | `self_aborted` | the owner's proactive self-split CAS was rejected | |
| `OWNER_SPLIT` | `self_published` | the owner's proactive self-split committed | |
| `OWNER_SPLIT` | `confetti_suppressed` | a carve was suppressed by the realized-child-mass confetti feedback gate | |
| `OWNER_SPLIT` | `confetti_probe` | a would-be-suppressed carve was let through as the periodic probe (every `PROBE_K`-th) | |
| `OWNER_SPLIT` | `unsplittable_pivot` | the synthesized owner-split pivot was `null`, or not strictly inside `(cursorTo, hi]`, THIS page-commit — transient and per-attempt (the range is reconsidered at its next qualifying commit), NOT the thief's permanently-cached `UNSPLITTABLE.no_pivot` | |
| `OWNER_SPLIT_CHILD` | `confetti` | a tagged owner-split child completed with realized mass `<= 2*maxKeys` AND never itself split (no owner self-split, no successful thief steal) | |
| `OWNER_SPLIT_CHILD` | `substantial` | a tagged owner-split child completed with realized mass `> 2*maxKeys`, OR it did itself split (regardless of its own final tally) | |
| `ALPHABET` | `alphabet_chosen` | the observed-alphabet chooser deflected the pivot from the plain code-point value | |
| `ALPHABET` | `alphabet_fallback` | the alphabet chooser landed on the same value as the plain code-point pivot | |
| `ALPHABET` | `fallback_out_of_window` | an `AlphabetDigest` consult fell outside the tracked/clean window | |
| `ALPHABET` | `fallback_no_room` | no room for any scalar strictly between the consult's bounds | |
| `ALPHABET` | `window_gap` | the observed alphabet had no value populating the consult's gap | |
| `SEED` | `flat_trivial` | flat top (no common prefixes) that is not a dense leaf; falls back to one `(⊥, null]` range | |
| `SEED` | `dense_root_radix_banded` | the dense flat root was pre-cut into leading-byte radix bands | |
| `SEED` | `tiny_leaf_explosion` | the seed collected a tiny-leaf-explosion shape — fires for BOTH a truncated-with-CommonPrefixes TOP level (any name shape; still tiled via the generic `4*W` cap, `SeedStepRootFanoutBudgetTest`) and a truncated, plain-named (non-`key=value/`) descended SUB-level (genuinely left whole) — see the `seed.decisions[]` `tiny_leaf_explosion` row for the (a)/(b) distinction | |
| `SEED` | `fanout_tiled` | a truncated `delimiter=/` DESCENDED SUB-LEVEL ONLY (never the top-level probe) whose common prefixes are Hive/Spark `key=value/` partition dirs was tiled (W-capped) along the already-probed prefixes, instead of the earlier break-and-discard — a root-level truncated fan-out is bounded by the generic top-level `4*W` cap instead, never this classification | |
| `SEED` | `delimiter_seeded` | the generic/base seed shape: plain `delimiter=/` structure tiled into cut-points | |
| `SEED` | `top_truncated` | the top-level LIST used to collect cut-points was truncated | |
| `SEED` | `top_complete` | the top-level LIST used to collect cut-points completed in one page | |
| `SEED` | `radix_bands` | incremented by the leading-byte band count (not just 1) whenever banding fires | |
| `SEED` | `heavy_cut_descended` | mass-aware seed descent (`mass_aware_seed`, default ON; opt-out via `mass_aware_seed=off`): the bounded second-level sample (§2, `SAMPLE_WIDTH` children within the `SAMPLE_BUDGET`≤32 sub-budget carved out of `maxProbes`) fired on an ambiguous truncated-with-CommonPrefixes cut to disambiguate heavy-subtree vs. 1:1 explosion — fires once per sampled cut regardless of the verdict | |
| `SEED` | `explosion_confirmed` | the second-level sample proved a 1:1 tiny-leaf explosion (a majority of sampled children hold ≪ `SAMPLE_DENSE_MIN_OBJECTS` objects, no sub-dirs) — not-heavy, so no banding fires; the cut falls through to its existing not-heavy classification, which is `tiny_leaf_explosion` (left whole) ONLY for plain (non-`key=value/`) names — a `key=value/` explosion still routes to `fanout_tiled` (see that row), so do not expect `tiny_leaf_explosion` on `pid=`/`date=`-shaped cuts that fire this counter | |
| `SEED` | `heavy_cut_banded` | the sample proved a heavy deep subtree, so the cut was BANDED — tiled along the child prefixes already in its probed page (co-fires with `heavy_cut_descended`) so its mass parallelizes into many seed ranges instead of one serial tail | |
| `SEED` | `frontier_level_ordered` | the mass-aware descent frontier held cuts at MORE THAN ONE depth, so its level-first key (depth ascending, `spanScore` descending within a level) actually decided a poll — no cut at depth `D+1` is probed while an unprobed depth-`D` cut remains. Fires at most once per run, and only on a genuinely multi-depth frontier: on a uniform-depth frontier the poll order is byte-identical to the pure-span order and this would be reporting a no-op. Span alone is uncorrelated with mass on a name-keyed keyspace (measured Spearman `rho = -0.153` over `wis2globalcache`'s 111 top-level cuts), because it rewards a cut whose next sibling diverges early in the NAME — the level key bounds what that costs to one level instead of letting it strand whole subtrees | |
| `SEED` | `heavy_prior_applied` | an ambiguous truncated-with-CommonPrefixes cut was reached AFTER the `SAMPLE_BUDGET`/`maxProbes` sub-budget was spent, so no sample could run — instead of silently defaulting to not-heavy (leave whole, the least-parallel outcome, decided by descent ORDER rather than by mass) the verdict is carried from the majority of siblings already sampled in this same descent. Zero extra probes: no page is fetched on this path. Fires once per unsampled ambiguous cut; the companion `heavy_prior_banded`/`heavy_prior_left_whole` row records which way it went. A run with `heavy_prior_applied` ≫ `heavy_cut_descended` means the sub-budget is undersized for that bucket's shape | |
| `SEED` | `heavy_prior_banded` | the carried prior came out heavy, so the unsampled cut was BANDED exactly as a sampled-heavy cut would be (co-fires with `heavy_prior_applied`; does NOT fire `heavy_cut_banded`, which stays reserved for cuts an actual sample proved). This is the counter that shows the fix earning its keep — on `nara-1950-census` it is the difference between New_York/California being tiled and being one serial range | |
| `SEED` | `heavy_prior_left_whole` | the carried prior came out not-heavy (a majority of sampled siblings were confirmed 1:1 explosions), so the unsampled cut was left whole — byte-identical to the pre-prior behaviour. On a genuinely INT-8-shaped keyspace this is the row that should fire, and `heavy_prior_banded` should stay at zero | |
| `SEED` | `descent_cuts_subsampled` | the descent's accumulated cut set exceeded `targetSeeds` and was actually reduced (weight-proportionally or, absent a sample budget, positionally) before tiling. The descent itself is no longer bounded by `targetSeeds` (only the probe budget and frontier exhaustion are), so this is the point the cut-point cap now actually bites — fires whenever it does, regardless of which reduction method ran; `mass_weighted_subsample` (below) fires only on the weighted branch specifically | |
| `SEED` | `mass_weighted_subsample` | an over-cap cut set (more cut-points than `targetSeeds` — a wide top, or a descent that accumulated interior cuts from several heavy regions) was subsampled weight-proportionally (heaviness estimated by a bounded sample within the same sub-budget) instead of positionally, so a heavy region that is a small slice of the cut-index range keeps more interior cuts | |
| `SEED` | `top_probe_paginated` | a truncated TOP-level structure probe was paginated by one extra page (TOP level only, never a sub-level) to recover its overflow common prefixes (1001..2000) so mass-weighting/tiling can see them | |
| `SEED` | `frontier_reordered` | the mass-aware best-first descent frontier (a span-scored priority order maintained across every insertion, not a one-shot pre-pass) actually had a real choice to make — mass-aware ON, a non-truncated top, AND more than one expandable frontier entry at ANY poll during the descent (not just before the loop starts — a frontier that starts with a single entry but expands mid-descent still engages the priority order the first time a real choice exists; a <=1-entry frontier throughout gives the priority order nothing to rank and does not fire this) | |
| `SEED` | `frontier_continued_past_explosion` | a truncated (exploding) sub-level was classified and disposed of on its OWN — the descent then kept probing the REST of the frontier instead of abandoning it, AND the frontier still had an entry the descent's own remaining caps (probe budget, `maxProbes`) allow it to actually reach. Fires exactly where an unconditional stop-at-first-explosion descent would have stranded an unrelated, still-splittable sibling (e.g. a uniform child tree sitting next to an exploding one) at one giant near-serial range | |
| `SEED_SCHED` | `distinct_seed_worker` | fired once the first time each distinct worker claims a SEED (initial worklist) node in `WorkStealingScan`, so the counter's total is the COUNT of distinct workers that consumed a seed range at runtime. The deterministic, structure-level replacement for the removed CI-flaky `avg_in_flight` uplift guard: `SeedMassAwareDescentTest.distinctSeedWorkers_concurrentHeavyTailSpreadsAcrossManyWorkers` and `distinctSeedWorkers_serialCollapseAblationDropsToOneAndFailsFloor` now guard the signal. A forced-serial run (`workerCount == 1`) reads exactly 1; a concurrent run that actually spreads a correctly-placed banded heavy tail's seed ranges across the pool reads many. Excludes thief/owner-split children (only initial seed ids are tracked), so a collapsed single-heavy-seed shape that only parallelizes via later child splits cannot inflate it. Pure observation — zero effect on scheduling/stealing | |
| `SEED` | `banding_deferred_to_fanout` | the fanout-tiling-precedence rule: on a truncated sub-level cut that is BOTH a `key=value/` partition fan-out (`fanout_tiling` on) AND WOULD have been sampled by mass-aware banding (`mass_aware_seed` on, sample budget available) — the sharp, zero-probe partition signal took precedence and the sample was short-circuited entirely (no `heavy_cut_descended`/`explosion_confirmed`/`heavy_cut_banded` on this cut). Measures how often the precedence rule actually engages on a both-eligible cut, distinct from an ordinary `fanout_tiled` cut where `mass_aware_seed` was off or the sample budget was already exhausted | |
| `SEED` | `yield_quota_cutoff` | issue #15's per-depth yield quota: a depth's last `YIELD_WINDOW` (4) descent-loop probes collectively produced at most `YIELD_WINDOW_MIN_CUTS` (2) new cuts — a floor on the WHOLE window's total, not a strict per-probe break-even, so one unlucky/lucky probe cannot tip the verdict on its own — so `SpanPriorityFrontier#poll` stops offering that depth's remaining entries ahead of any other depth that still has queued work — level order is still the starvation bound: if every depth with queued entries is cut off, `poll`'s fallback pass resumes strict shallowest-first exactly as before the quota existed. One increment per depth the FIRST time it crosses into cut-off (a sticky, one-way gate — never re-evaluated once tripped). Fires zero times on a bottomless narrow chain (one queued entry per depth is never enough probes to fill the judging window) — see `SeedDescentRightmostChainDoesNotStarveWideNonLastSiblingTest`, unaffected by this change | |
| `SEED` | `yield_quota_cutoff_shallow` | co-fires with `yield_quota_cutoff` whenever the cut-off depth is `<= 2` — the cheap keyspace-classification signal distinguishing which half of the issue's pathology tripped the quota: a wide SHALLOW level (top level or one level in) that stops yielding cuts | |
| `SEED` | `yield_quota_cutoff_deep` | as `yield_quota_cutoff_shallow`, for a cut-off depth `> 2` — a DEEP narrow chain that stops yielding cuts | |
| `RESUME` | `nodes_reopened` | count of checkpointed nodes reopened on a genuine `swath resume` | |
| `RESUME` | `durable_cursor_lag` | count of reopened nodes whose `durable_cursor` lagged `cursor` (a non-durable tail re-listed) | |
| `RESUME` | `args_hash_refused` | a `swath resume` was refused because `args_hash` changed since the checkpointed run | |
| `PIVOT_BYTE` | `hex_digit` | a committed split's pivot diverges at a hex-digit byte (`0x30`-`0x39`) | |
| `PIVOT_BYTE` | `dead_zone` | ...diverges in the `0x3A`-`0x60` dead zone between hex digits and hex letters | |
| `PIVOT_BYTE` | `hex_alpha` | ...diverges at a hex-letter byte (`0x61`-`0x66`) | |
| `PIVOT_BYTE` | `other` | ...diverges at any other byte | |
| `CHILD_MASS` | `empty` | a completed node (split child or seed) emitted 0 keys | |
| `CHILD_MASS` | `tiny` | emitted 1-100 keys | |
| `CHILD_MASS` | `small` | emitted 101-10,000 keys | |
| `CHILD_MASS` | `large` | emitted more than 10,000 keys | |
| `STEAL` | `attempted` | one real thief steal attempt (after the idle-backoff slot is acquired), regardless of outcome | |
| `STEAL` | `futility_paced` | a victim was skipped this steal attempt because it is in a per-victim futility cooldown | |
| `IDLE_SLOT` | `in_flight` | an idle worker was refused a steal attempt because **another worker owns the sole in-flight slot** — the fleet-wide one-attempt bound holding. The counter records the refusal only; what the worker does next is decided from the state it re-observes under the ledger gate, and may be any of claiming a child that became ready meanwhile, parking on the seconds-scale in-flight backstop because the slot is still held, or parking on whatever pacing window the owner's own outcome left behind. A high count against few `STEAL.attempted` is the expected shape of a slow probe, not a fault. Splits the aggregate `swath.idle_backoff.slot_denied`, which cannot tell this from `paced` | |
| `IDLE_SLOT` | `paced` | an idle worker was refused a steal attempt because the **fleet is in exponential idle backoff** after consecutive non-productive outcomes — the slot itself is FREE. The opposite situation from `in_flight`: there is no release to wait for, only the pacing window, which either elapses or is cleared early by a claim or a non-empty page commit. Read with `swath.idle_backoff.level`: `paced` denials rising with a level pinned high means the fleet has stopped finding splittable victims | |
| `THROTTLE` | `slowdown` | a 503 `SlowDown`/throttle **service** response (votes AIMD down) | |
| `THROTTLE` | `server5xx` | an S3-side (or intermediary) 5xx server error — e.g. 500 InternalError — after retry exhaustion (votes AIMD down) | |
| `THROTTLE` | `attempt_timeout` | REMOVED 2026-07-07 — reclassified to `TRANSIENT.attempt_timeout`: a client attempt-timeout is self-inflicted, not S3 backpressure, and must not vote AIMD down | removed |
| `AIMD` | `slow_start_double` | a slow-start growth step doubled the effective concurrency target `T` (paced, fires only while `!congestionSeen`) — one per doubling step (4→8→16→32→64) on a healthy endpoint ramping out of the `SLOW_START_INITIAL_T=4` floor | |
| `AIMD` | `slow_start_exit_congestion` | the FIRST congestion signal of the run (a WORKER, i.e. `slotGated=true`, attempt-timeout, an AIMD 503 down-vote, or a sustained-timeout shed) latched `congestionSeen`, ending slow-start's multiplicative doubling for the rest of the run (growth reverts to the cautious additive `+1`) — fires at most once per run. **Probe-class transients are excluded:** a probe-class transient (attempt-timeout or network fault) does not count — only a WORKER attempt-timeout, an AIMD 503 down-vote, or a shed can end slow-start | |
| `AIMD` | `decrease_at_floor` | a `multiplicativeDecrease` (THROTTLE or TIMEOUT_SHED, kind-agnostic) fired with `T <= 2` read at entry (before any CAS) — the near-serial floor band. One of four bounded T-band engagement counters (`decrease_at_floor`/`decrease_low_t`/`decrease_mid_t`/`decrease_high_t`) fired once per decrease event, so post-hoc can attribute decreases to the `T` regime they hit without re-deriving it from the `swath.workers.active` gauge trace | |
| `AIMD` | `decrease_low_t` | as `decrease_at_floor`, for `3 <= T <= 8` read at entry | |
| `AIMD` | `decrease_mid_t` | as `decrease_at_floor`, for `9 <= T <= 32` read at entry | |
| `AIMD` | `decrease_high_t` | as `decrease_at_floor`, for `T > 32` read at entry | |
| `AIMD` | `floor_noop_rearm` | a `multiplicativeDecrease` produced NO numeric change (the CAS loop's `next >= cur` break — already at the floor, or rounding collapsed the step) yet `markCongestion()` and the vote/shed counter and `stealingAllowed=false` all still fired unconditionally upstream — the "floor 5xx" observable. The cool-down re-arm (`lastThrottleNs`) is NOT one of the unconditional side effects this event co-occurs with — it is suppressed at exactly this break (see `floor_rearm_suppressed` below); `floor_noop_rearm`'s own counting rule fires once per no-op decrease event, read once at entry, emitted once at the break, never per CAS retry iteration | |
| `AIMD` | `floor_rearm_suppressed` | fires at the SAME no-op-decrease break as `floor_noop_rearm` above, count-identical to it on any run — the engagement signal for the floor-rearm suppression: a `multiplicativeDecrease` (THROTTLE or TIMEOUT_SHED, kind-agnostic) that removed zero concurrency (`next >= cur`) no longer re-arms the 10s `CLEAN_WINDOW` cool-down (`lastThrottleNs`), which now re-arms ONLY on the CAS-success branch of a REAL reduction (`next < cur`). Lets post-hoc confirm the change actually engaged on a given bucket (`floor_rearm_suppressed > 0`) and cross-check it against `floor_noop_rearm` (should be `==`) | |
| `AIMD` | `growth_blocked_cooldown` | `onSuccess()`'s clean-window cool-down gate (`lastThrottleNs != 0 && now - lastThrottleNs < CLEAN_WINDOW_NANOS`) suppressed a success's growth opportunity (the ordinary `+1` recovery AND the latency-freeze valve both sit downstream of this gate) — measures how much growth the re-armed cool-down actually eats, fired once per suppressed success. Because floor no-op decreases no longer re-arm the cool-down, this counter reads materially lower than a naive re-arm-on-every-decrease design would produce (fewer cool-down bail-outs at the floor) — the counting rule itself is unchanged | |
| `SHED` | `timeout_storm` | the sustained-timeout SHED engaged — a starved attempt-timeout storm shed the concurrency target `T` (multiplicative ×0.5, ≤1 per jittered ~30s window; paired with `swath.aimd.timeout_shed`, NOT an AIMD vote). Since probe timeouts stopped feeding the shed gate, ONLY `slotGated=true` WORKER-fetch timeouts can trip this — a probe (pivot/structure) timeout never gates it (see `timeout_storm_probe_fed` below) | |
| `SHED` | `timeout_storm_worker_fed` | at the SAME shed fire as `timeout_storm` above, magnitude-incremented by the number of `slotGated=true` WORKER-fetch timeouts that fed the tripped window (`ConcurrencyGauge#onTransientTimeout(boolean)`, read at fire time) — the call-class mix that fed the storm, the client-vs-server falsifier signal. Since probe timeouts stopped feeding the shed gate this is the ENTIRE gate: it equals the window's `shedWindowTimeouts` total that actually tripped the shed | |
| `SHED` | `timeout_storm_probe_fed` | the probe-fetch (`slotGated=false`) half of the call-class split — same shed fire, same window, magnitude-incremented by the probe-timeout count. **VISIBILITY-ONLY.** A probe timeout carries no S3-backpressure signal, so it no longer feeds `shedWindowTimeouts` and can never gate/trip `timeout_storm` on its own — a pure probe-timeout storm now sheds nothing (before probe timeouts were excluded it could, the client-vs-server false positive). This counter still publishes at whatever shed a WORKER storm fires, showing the probe pressure that coexisted with it, purely as a diagnostic | |
| `FREEZE` | `latency_inflation` | the latency-freeze rung engaged — the successful-attempt latency EWMA inflated past `LATENCY_FREEZE_FACTOR`× the rolling-min baseline, freezing the +1 growth (paired with `swath.aimd.latency_freeze`; a growth GATE, never a decrease) | |
| `FREEZE` | `transient_timeouts` | the transient-timeout growth-freeze rung engaged — the 10s transient-timeout window (fed ONLY by `slotGated=true` WORKER-fetch timeouts, since probe transients were excluded) reached `TRANSIENT_FREEZE_THRESHOLD`, freezing the +1/doubling growth step (paired with `swath.aimd.growth_freeze`; a growth GATE, never a decrease; distinct from `latency_inflation` above so post-hoc can tell which rung suppressed a given step) | |
| `GROWTH` | `probe_timeout_excluded` | a probe-class transient (attempt-timeout or network fault) excluded from congestion/growth-freeze — the caller does not distinguish fault kind at this call site (it mirrors the shed-side classification, which excludes both kinds identically), so this fires for either; carries no S3-backpressure signal, so it no longer ends slow-start or feeds `FREEZE.transient_timeouts` on its own; it still increments `shedWindowProbeTimeouts` for `timeout_storm_probe_fed` visibility. Name kept as `probe_timeout_excluded` for continuity with the pre-existing metric even though network faults share the path — attempt-timeouts dominate on probes | |
| `GROWTH` | `frozen_growth_valve` | the latency-inflation freeze VALVE engaged — while `latencyFrozen()` (and NOT `growthFrozen()`) with progress (successes above the starvation gate) and the ~30 s valve cool-down elapsed, one paced additive +1 was admitted, demoting the latency latch to a damper. Fires per admitted step; `FREEZE/latency_inflation` still fires on the same step (semantics unchanged, pre/post comparable). A growth path, never a decrease. | |
| `THROTTLE` | `network` | REMOVED 2026-07-07 — reclassified to `TRANSIENT.network`: a client-side network fault is self-inflicted, not S3 backpressure, and must not vote AIMD down | removed |
| `TRANSIENT` | `attempt_timeout` | an SDK per-attempt/per-call **client** timeout (`ApiCallAttemptTimeoutException`/`ApiCallTimeoutException`) — retried by the swath-owned bounded retry; NOT S3 backpressure, casts no AIMD vote | |
| `TRANSIENT` | `network` | an exhausted network-class `SdkClientException` (connection reset, read-timeout, DNS/TLS failure; `IOException` in the cause chain) that survived the SDK's own retries — retried, no AIMD vote | |
| `TRANSIENT` | `socket_closure` | a client-local socket-closure / `IOException`-wrapper fault that escaped the SDK call as a **non-`SdkException` RuntimeException** (e.g. `UncheckedIOException(SocketException("Socket closed"))` from a transient S3 500 burst) — same client-local network class as `TRANSIENT.network` above, but the wrapper is not an `SdkException` so the modeled `isNetworkExhaustion` arm never saw it. Reclassified `Kind.NETWORK` (retried, no AIMD vote) instead of escaping raw as an exit-1 / `error_class=unknown` crash; also counts `swath.s3.socket_closure_recovered` (its own engagement counter, so post-hoc can tell it apart from the `SdkClientException` NETWORK path) | |
| `TRANSIENT` | `aborted` | an SDK-side `AbortedException` (request abort under connection churn / call-timeout abort) that arrived WITHOUT a thread interrupt — a transient client-side abort, distinct from a genuine attempt timeout; classified `Kind.ATTEMPT_TIMEOUT` (non-AIMD-voting, feeds the `stuck_api_timeouts` fingerprint), retried by the swath-owned bounded retry, no AIMD vote | |
| `TRANSIENT` | `attempt_timeout_escalated_` | a consecutive `ATTEMPT_TIMEOUT` retry of the SAME logical fetch escalated its per-attempt timeout override for the NEXT attempt — the emitted reason is suffixed with the 1-based escalation level reached (`attempt_timeout_escalated_1` = 20s, `attempt_timeout_escalated_2` = 40s, the `ATTEMPT_TIMEOUT_ESCALATION_LEVELS` cap) | |
| `TRANSIENT` | `page_completed_at_` | a page fetch SUCCEEDED only after its per-attempt timeout was escalated — the reason is suffixed with the escalation level it succeeded at (`page_completed_at_1`/`_2`) — the post-hoc §4 discriminator proving a tail page genuinely needed longer than the base 10s budget, not just a flaky retry | |
| `TRANSIENT` | `retry_cap_stuck` | under `RetryPolicy.BOUNDED` (both `LivenessWatchdog` windows disabled by flags, so nothing else could ever stop an unbounded ride-out), a transient-retry loop's `MAX_TRANSIENT_RETRIES` cap exhausted and cancelled the run `STUCK` (`CancelSource.TRANSIENT_RETRY_CAP`) — the legacy resumable-bound disposition, kept only when no watchdog backstop is armed | |
| `TRANSIENT` | `storm_ride_out` | under `RetryPolicy.RIDE_OUT` (a real `LivenessWatchdog` armed, the default), a transient-retry loop crossed `MAX_TRANSIENT_RETRIES` and kept retrying instead of cancelling the run — the cap becomes a backoff-shaping threshold only (raised full-jitter ceiling, `STORM_BACKOFF_CAP_MILLIS` 5s→15s), and the watchdog alone owns liveness death. WORKER-only since the probe fail-fast cap below — a probe can never reach this threshold | |
| `TRANSIENT` | `attempt_timeout_worker` | the §4 discriminator: a `GaugedFetcher` `ATTEMPT_TIMEOUT` retry attributed to a `slotGated=true` WORKER range fetch — fired on EVERY consecutive attempt-timeout retry (raw cadence, not gated on escalation level), so a corpus run can tell what fraction of tail-storm timeouts came from workers hitting a genuinely slow tail page (§4.1) vs the thief's probe loop (§4.2, `attempt_timeout_probe`) | |
| `TRANSIENT` | `attempt_timeout_probe` | the same attribution as `attempt_timeout_worker`, for a `GaugedFetcher` `ATTEMPT_TIMEOUT` retry on the thief's `slotGated=false` steal-probe fetch (structure probe / empty-upper probe / flat-leaf floor probe) — bounded to at most `PROBE_TRANSIENT_RETRY_CAP + 1 = 2` per probe since the probe fail-fast cap | |
| `TRANSIENT` | `storm_ride_out_worker` | same probe-vs-worker attribution as `attempt_timeout_worker`, but for the deeper `storm_ride_out` engagement (a transient retry past `MAX_TRANSIENT_RETRIES`, still riding out under `RetryPolicy.RIDE_OUT`) — always WORKER-attributed since the probe fail-fast cap (a probe never reaches this depth) | |
| `TRANSIENT` | `storm_ride_out_probe` | REMOVED 2026-07-08 — a probe can no longer reach ride-out at all: it fails fast on its own `PROBE_TRANSIENT_RETRY_CAP` first (an unbounded camping probe consumed a large fraction of total storm request volume). Superseded by `probe_retry_cap_failfast` below | removed |
| `TRANSIENT` | `probe_retry_cap_failfast` | the probe fail-fast cap (`WorkStealingScan.GaugedFetcher`): a probe fetch (`slotGated=false`) exhausted its own small, `RetryPolicy`-independent retry cap (`PROBE_TRANSIENT_RETRY_CAP = 1`, one grace retry) and failed fast with the ORIGINAL `ThrottleException` — never rides out, never touches the `CancellationToken`; see the `RETRY.probe_retry_cap_failfast` twin below for how `Thief.steal` re-enters its ordinary non-productive-steal flow | |
| `RETRY` | `probe_retry_cap_failfast` | the probe fail-fast cap's steal-side twin (`Thief.steal`): the `TRANSIENT.probe_retry_cap_failfast` `ThrottleException` above, caught and folded into the SAME non-productive-steal `RETRY` outcome an ordinary retry takes — frees the sole `IdleStealBackoff` in-flight slot immediately instead of camping on it | |
| `PROBE` | `slow_` | a probe fetch (never a worker page) was slow (>1 s) or failed — the reason is suffixed with the request's `call_class` (`slow_pivot_probe`/`slow_structure_probe`). Fired on EVERY such probe, where the `slow_probe_exemplar` DEBUG line beside it is rate-limited (first 20, then powers of two), so the count is the honest engagement figure and the line is only a reproducible sample of it. The exemplar's request identity (prefix/`start_after`) is deliberately NOT a tag — an object key is unbounded cardinality — and stays log-only | |
| `REDIRECT` | `region` | a 301 `PermanentRedirect` — the bucket lives in a region other than the passed `--region` — surfaced as a typed `RegionRedirectException` naming the correct region (never an untyped crash) | |
| `FATAL` | `access_denied` | a seed/fetch 403 `AccessDenied` — a permanent permissions/config failure (e.g. an anonymous or under-privileged LIST of a bucket that denies it) — surfaced as a typed `AccessDeniedException` with `error_class=access_denied`, exit 1, `stop_reason=seed_failure`; terminal, never retried/AIMD-fed (a transient 403-coded throttle is caught by `isThrottle` first) | |
| `FATAL` | `no_such_bucket` | a seed/fetch 404 `NoSuchBucket` — the bucket does not exist — typed `NoSuchBucketException`, `error_class=no_such_bucket`, exit 1, terminal (a sibling 404 `NoSuchKey` is NOT reclassified) | |
| `FATAL` | `unauthorized` | a seed/fetch HTTP 401 — missing/invalid credentials — typed `UnauthorizedException`, `error_class=unauthorized`, exit 1, terminal | |
| `FATAL` | `oversized_page` | a page whose entry count (objects + rolled-up common prefixes) exceeded the `MaxKeys` the request asked for — a response no conforming store may produce, so `S3PageFetcher` refuses it before materialising the entries (a hostile/broken endpoint that ignores `MaxKeys` would otherwise size swath's lists off the wire) — typed `ProtocolViolationException`, `error_class=oversized_page`, exit 1, terminal; never truncated (that would silently drop keys) and never retried (the endpoint would answer the retry the same way) | |
| `TOGGLE` | `<name>_off` | ablation-mark family, one mark per disabled ablation mechanism: fired once per run from `EngineToggles.recordOffMarks` for each key in `EngineToggles.NAMES` (`owner_split`, `density_ewma`, `radix_bands`, `structure_probes`, `far_ahead`, `alphabet_pivots`, `reflect`, `confetti_feedback`, `reflect_lift`, `fanout_tiling`) that resolved off for the run, so post-hoc analysis reads an ablation directly from the mark instead of inferring it from a mechanism's silence. `<name>` is the toggle key; the per-mechanism effect of each ablation and the constructor its mark fires from are tabulated in the `--engine-toggle` ablation-marks section above (the opt-in `readahead_on` / `mass_aware_seed_on` engagement marks are inverted and listed separately below) | |
| `TOGGLE` | `readahead_on` | engagement mark (opt-in, inverted from the `*_off` ablation marks): intra-range speculative readahead was ENABLED for this run (`--tune engine.readahead=on`) — fired once at scan start so post-hoc analysis sees the request even on a bucket where no range collapsed enough to engage it | |
| `TOGGLE` | `mass_aware_seed_on` | engagement mark, same wiring/polarity as `readahead_on` (fires whenever the toggle resolves ON, never inverted to match its own default): mass-aware seed descent was ENABLED for this run — fired once at seed step so post-hoc analysis sees the request even on a bucket whose shape never triggered a second-level sample. Since the default-ON flip this now fires on every DEFAULT run, not just an explicit `--engine-toggle mass_aware_seed=on`; `mass_aware_seed=off` is the now-notable opt-out case (absence of this mark on a run means it was explicitly disabled) | |
| `TOGGLE` | `decision_rng_seeded` | engagement mark, same polarity as `readahead_on` (fires only on the opt-in branch): the run drew its policy-level randomness from a seeded `SeededDecisionRng` (`EngineContext#decisionRngSeed`, per-worker SplitMix64 derivation) rather than the ambient `ThreadLocalRandom` default — fired once at engine construction. It is the only observable that separates the two: a seeded run and an ambient one make the SAME draws at the same decision points and their decision bytes are identical, so nothing downstream could otherwise tell a replay-reproducible run from a live one. The ambient default is deliberately unmarked (marking it would add a counter to every run that has ever existed, including the baselines this seam promises to leave byte-identical) | |
| `TOGGLE` | `rate_anchored_sensing_on` | engagement mark, same polarity as `readahead_on` (fires only on the opt-in branch): the run steered victim choice and the owner-split gates on the PORTED position sensor (`RateAnchoredEstimator` — proven mass, banded by cursor-anchored geometry at the promoted quarter floor) instead of the shipped `StealMath.estRemaining` window reading (`--engine-toggle rate_anchored_sensing=on`). Fired once at engine construction, so a real-bucket A/B reads which arm a run was on from the run itself; the `SENSING_OWNER.*` / `SENSING_STEAL.*` rows below are that sensor's own per-reading classification and are silent on a default run | |
| `SENSING_OWNER` | `geometry_capped` | the ported sensor's cursor-anchored geometric factor read ABOVE `RateAnchoredEstimator.GEOMETRY_BAND` at an OWNER-SPLIT gate consult, so the lift it applied to the range's proven mass was capped at the band. One of five mutually-exclusive readings recorded per classified estimate; **denominator: one classification per qualifying page commit** — the same population the `OWNER_SPLIT.*` skip reasons are counted over, so these rows read directly against them. Fires only under `rate_anchored_sensing=on`. The `SENSING_STEAL` rows below are the SAME five readings at the other decision site, kept in a separate namespace because their denominator is not this one: pooling them would make every ratio drawn from either uninterpretable | |
| `SENSING_OWNER` | `geometry_lift` | ...the factor read above 1 and inside the band, so measured position lifted the range's proven mass by exactly that factor. On a deep-nested keyspace this is the reading the shipped window cannot take at all (its consumed span underflows to zero and the emitted keys drop out of the estimate) | |
| `SENSING_OWNER` | `geometry_neutral` | ...the factor read exactly 1: either there is no consumed evidence to anchor (cursor at or before `lo`, or a consumed span that underflowed) or the frame is exactly balanced, so the estimate is the range's proven mass unmodified | |
| `SENSING_OWNER` | `geometry_cut` | ...the factor read below 1 but at or above the run's floor, so geometry cut the proven mass by the amount the floor still allows — the measured shortfall the floor deliberately keeps | |
| `SENSING_OWNER` | `geometry_floored` | ...the factor read BELOW the floor (`QUARTER_MIN_GEOMETRY`) and the floor refused the rest of the cut. This is the counter the promotion exists for: read against `OWNER_SPLIT.remaining_est_floor` (same denominator, same call), it is where the shipped reading would have scored a straggler's proven mass low enough to refuse its carve | |
| `SENSING_OWNER` | `page_floor` | the classified estimate's magnitude was the no-evidence page floor rather than proven mass (the range has emitted fewer keys than one page), so the reading is an assumption about a barely-started range and not a measurement of it. Co-fires with whichever geometry row above applies | |
| `SENSING_STEAL` | `geometry_capped` | the same reading as `SENSING_OWNER.geometry_capped`, taken at VICTIM SELECTION instead: the factor read above the band on the candidate the attempt chose. **Denominator: steal attempts that found a winner** — one classification per attempt, NOT per scored candidate (a pool member that lost is never classified) and NOT per page commit, so these rows are read against the steal-attempt outcome families (`CHILD_CREATED.*` / `RETRY.*` / `NO_VICTIM.*`), never against `OWNER_SPLIT.*`. Fires only under `rate_anchored_sensing=on` | |
| `SENSING_STEAL` | `geometry_lift` | ...the factor read above 1 and inside the band on the winning candidate, so measured position lifted the range's proven mass by exactly that factor — the reading that promotes a deep-nested range the shipped window scores as a bare width | |
| `SENSING_STEAL` | `geometry_neutral` | ...the factor read exactly 1 on the winning candidate: no consumed evidence to anchor, or an exactly balanced frame, so the estimate is its proven mass unmodified | |
| `SENSING_STEAL` | `geometry_cut` | ...the factor read below 1 but at or above the run's floor on the winning candidate — the measured shortfall the floor deliberately keeps | |
| `SENSING_STEAL` | `geometry_floored` | ...the factor read BELOW the floor (`QUARTER_MIN_GEOMETRY`) on the winning candidate and the floor refused the rest of the cut. Read against `NO_VICTIM.all_no_remaining_span`, this is where the floor kept a proven-mass range in victim selection that the shipped reading would have scored out of it | |
| `SENSING_STEAL` | `page_floor` | the winning candidate's magnitude was the no-evidence page floor rather than proven mass (it has emitted fewer keys than one page), so the attempt chose on an assumption about a barely-started range. Co-fires with whichever geometry row above applies | |
| `TOGGLE` | `tail_floor_est_direct_on` | engagement mark, same polarity as `readahead_on`, with the SELECTED mode in the mark's own name (`tail_floor_est_direct_on` / `tail_floor_reach_floored_on`): the run read the owner-split child-tail floor through a cure arm instead of the shipped arithmetic (`--engine-toggle tail_floor=MODE`, algorithms.md §3.3). Fired once at engine construction. The mode is otherwise invisible in the counters — every mode refuses under the same `OWNER_SPLIT.floor_reflected_blocked` code — so without this mark an A/B could not tell its arms apart; the default `current` mode is deliberately unmarked (marking it would add a counter to every run that has ever existed) | |
| `TOGGLE` | `tail_floor_reach_floored_on` | engagement mark for the `reach_floored` arm — see `tail_floor_est_direct_on`; one literal row per arm because the drift guard matches emitted names statically | |
| `TAIL_FLOOR` | `gate_admit_current_blocks` | a `tail_floor` arm CHANGED the child-tail floor's verdict at the owner-split GATE, in the cure's intended direction: the arm admitted a carve the shipped floor refuses. This is the counter a live A/B is read on — it attributes a split-rate difference to the toggle rather than to the run. **Denominator: qualifying page commits** (the same population the `OWNER_SPLIT.*` skip reasons are counted over, so it reads directly against `OWNER_SPLIT.floor_reflected_blocked`). Fires only under a non-default mode: agreement is silent, and the shipped mode never computes a second verdict to compare against | |
| `TAIL_FLOOR` | `gate_would_block_current_admits` | the opposite direction at the same site — the arm refused a carve the shipped floor admits. Both shipped arms are monotonically more permissive than `current`, so this row is EXPECTED to stay at zero; it exists so that claim is falsifiable from a run instead of asserted in a javadoc. A non-zero value means a future arm broke the monotonicity the arms were promoted on | |
| `TAIL_FLOOR` | `clamp_admit_current_blocks` | the same divergence at the owner-split REFLECTION CLAMP (the floor's second consult: whether an overshooting interpolated pivot may be clamped down onto the reflected mass horizon). **Denominator: carves that reached pivot synthesis**, NOT page commits — a different population from the `gate_*` rows above, which is why the two are not pooled | |
| `TAIL_FLOOR` | `clamp_would_block_current_admits` | ...and its opposite direction, expected zero for the same monotonicity reason | |
| `TAIL_FLOOR` | `lift_admit_current_blocks` | the same divergence at the REFLECT-LIFT (the floor's third consult: whether a degenerate pivot may be lifted up to the reflected pivot). **Denominator: post-clamp carves** — again its own population, again deliberately unpooled | |
| `TAIL_FLOOR` | `lift_would_block_current_admits` | ...and its opposite direction, expected zero for the same monotonicity reason | |
| `READAHEAD` | `engaged` | intra-range speculative readahead engaged on a range that collapsed to a serial owner drain (fired once per range at engage) | |
| `READAHEAD` | `guess_placed` | one speculative guessed-`start-after` fetch was launched ahead of the cursor — the speculative call-volume (cost) denominator against `adopted_page`. **Off-gauge by design:** this fetch runs through a DEDICATED fail-soft fetcher (mirrors the `Thief`'s own probe fetcher — off the concurrency gauge, no AIMD vote on the happy path, its own small transient-retry cap), never the worker's own slot-gated fetcher — `K` bounds the added raw HTTP concurrency, but it does not count against the adaptive concurrency target `T` or `swath.in_flight.avg`/`peak_in_flight` | |
| `READAHEAD` | `adopted_page` | a buffered/in-flight speculative page was adopted (exact or overlap-trimmed) as the next contiguous page, hiding one RTT of the serial chain | |
| `READAHEAD` | `discarded_overlap` | a completed speculative page was wholly redundant with already-emitted keys (cursor advanced past its whole content) and discarded — placement-error waste | |
| `READAHEAD` | `cancelled_split` | a speculative fetch/page was discarded because a thief/owner-split narrowed `hi` at/below its guess (its keys are now the child's) | |
| `READAHEAD` | `guess_gap` | the scanner fell back to a serial fetch while readahead was engaged because no buffered page was an adoptable continuation (a pipeline gap/miss) | |
| `READAHEAD` | `speculative_fault` | a speculative guess-ahead fetch faulted (throttle, a genuine listing fault, or cancellation) — fail-soft, dropped, and the scanner falls back to an ordinary serial fetch for that page. The one fault that is not absorbed is a protocol violation: an endpoint that over-serves a bounded page may answer the serial refetch conformingly, so it propagates and refuses the endpoint rather than being counted here | |
| `READAHEAD` | `disengaged_low_adoption` | disengage-on-low-adoption: an engaged range's adopted fraction over a tumbling window of `disengageWindow` scanner pages fell at/below `disengageMinAdoption` (the density-reflected guesses were not paying off — the transient-dense-stretch signature: low adoption plus heavy `discarded_overlap`), so it disengaged speculation and reverted to plain serial. Fires once per disengagement; pairs against `engaged`/`re_engaged` to see how often engagement was a false positive on a bucket | |
| `READAHEAD` | `re_engaged` | a range that had previously `disengaged_low_adoption` re-engaged speculation after a fresh `engageAfterFullPages` sustained-drain streak (a genuinely sustained drain resumed after a transient stretch). Fired IN ADDITION to `engaged` (so `engaged` remains the total engagement count and `re_engaged` isolates the recoveries); its presence means the low-adoption disengage was not permanently starving a range that later warranted readahead | |
| `READAHEAD` | `engage_deferred_est_remaining` | a range cleared the `engageAfterFullPages` streak but its density-extrapolated estimated-remaining-pages (`StealMath#estRemaining`, scoped to the current streak's own start key — see `ReadaheadConfig#minEngageRemainingPages`) was below the floor, so engagement was deferred (never engaged for this streak). The streak is NOT reset, so a short remaining tail keeps deferring on every subsequent full page until the range ends. **This floor is DISABLED by default (`DEFAULT_MIN_ENGAGE_REMAINING_PAGES=0`)** — in practice the page floor bought nothing on dense buckets and could collapse engagement into demand-gate/AIMD near-serialization on others, so it is disabled and this counter/mechanism kept only for explicit-config use, reading 0 under the shipped default (the disengage window was shrunk 32→16 instead; the engage streak was NOT raised, as raising it collapses engagement for all buckets under work-stealing) | |
| `STRUCTURE` | `toggle_disabled` | structure probe skipped because `structure_probes=off` (distinct from runtime zero-fanout suppression) | |
| `SEED` | `radix_bands_toggle_disabled` | radix banding skipped because `radix_bands=off` (distinct from shape-based non-engagement) | |
| `SORT` | `segment_flushed` | a sealed sort buffer was flushed as one sorted staging segment (`--sort`) | |
| `SORT` | `buffer_byte_gated` | the byte gate (not the entry cap) forced the segment flush — the 1 KB-key signal | |
| `SORT` | `merge_fastpath` | same-reader fast-path emissions in a merge/seal pass (how much the disjoint-range structure was exploited) | |
| `SORT` | `buffer_sort_fallback` | a sealed buffer failed the per-page order check and was TimSort-sorted instead of page-merged (expected 0) | |
| `SORT` | `merge_pass_cascaded` | a cascaded merge pass beyond the first (segments > fan-in) fired — the multi-pass visibility | |
| `SORT` | `merge_disjoint_copyable` | one merge-pass input segment was emitted as a single uninterrupted run — range-disjoint from every other input that pass assuming distinct keys across segments (exact for unique S3 object keys; a segment sharing a boundary key with another can be counted copyable under the §0.5 fixture duplicate-data path only), so byte-copy (not decode) would have sufficed — a prerequisite measurement for a future copy-based merge fast path | |
| `SORT` | `merge_interleaved_segment` | the complement of `merge_disjoint_copyable`: one merge-pass input segment shared its run with at least one other input (more than one run in the merged output) | |
| `SORT` | `merge_cascade_predicted` | at merge kickoff, `segments > effectiveFanIn` was already known to force a cascade — engagement counter for the up-front `sort_merge_cascade_predicted` warn log | |
| `SORT` | `fixture_dup_check_inline` | sort-fixture's inline cross-row-type duplicate-key guard (§0.5) was armed for a final file — once per file created via the checking writer factory (never fires on the live `--sort` path) | |
| `SORT` | `resume_reattached` | `swath resume` re-attached a sorted run to existing durable staging segments | |
| `RESUME` | `context_mismatch_` | a resumed run's re-passed CLI value overrode the connection/output context its checkpoint recorded; the reason is suffixed with the field (`no_sign_request`, `profile`, `region`, `fetch_owner`, `raw_output`, `output`, `output_type`, `request_payer`). The counter is how a post-hoc consumer holding only the run report learns that a resume silently changed context, and which field it changed — the overridden checkpoint VALUE stays in the `list_resume_context_mismatch` DEBUG line (a region/profile/path is unbounded text, never a tag), and the report already carries the effective value | |
| `SORT` | `merge_redone` | a crash-interrupted merge was re-run from staging (listing complete, manifest absent) | |
| `SORT` | `merge_range_parallel` | an off-by-default experiment (`swath.sort.merge-parallelism>1`): one contiguous key range of the final merge was merged concurrently on its own thread into a separate ordered part file (`ParallelRangeMerge`). Fires once per range engaged, so the run total equals the range count `R` — the cheap keyspace-partition signal (was the keyspace split, into how many ranges). Absent on the default serial merge (`merge-parallelism=1`) and on a degenerate keyspace that could not be split (fell back to serial) | |
| `SORT` | `merge_range_rowgroup_skipped` | row-group skip (off-by-default, parallel range-merge path): a range PRUNED ≥1 Parquet row group — it decoded only the physical row groups whose actual-first-key span overlaps its `[lo, hi)` (`ParallelRangeMerge#selectRowGroups`, keys from `SortedFileIndex#rowGroupSpans`, never footer stats §9.1), the decode-parallelism win over reading every segment whole. Fires once per range that skipped ≥1 group, so the run total is "how many ranges the skip engaged on"; the skip fraction (row-groups read vs skipped, the did-it-help signal) is in the per-range `sort_merge_range` log line. Absent when a range covers every group (nothing to skip — e.g. a single-row-group segment, or a range spanning the whole keyspace) and on the serial path | |
| `SORT` | `finalize_progress_tick` | the finalize/publish tail emitted a liveness-progress tick — at each final part's footer boundary (`ProgressMarkingSortedFileWriter` ticks pre-fsync in both `markFinal()` and `close()`) and once per 64 MiB streamed while computing that part's manifest MD5 (`md5HexWithLivenessProgress`). Byte-keyed, not timer-keyed, so it keeps the watchdog honest: proves the multi-GB finalize window was advancing (a true stall emits none). Count is proportional to published bytes / part count | |
| `SORT` | `page_whole_emitted` | the page-run merge fast path: the final page-aware merge (`PageAwareMerger`, engaged when every survivor is a page-run segment) emitted a whole page decode-free-planned — its current page's `maxKey` was strictly `<` (unsigned) the `minKey` of every other segment's current page AND of its OWN next page, so the page was globally next with no interleaving and was streamed in order without a heap merge. Fires once per page so emitted; the count is "how many pages the disjoint fast path carried" — the page-oriented analogue of `merge_fastpath` (which stays the entry-level `StreamingMerger` same-reader signal). High on a well-formed OBJECTS run (work-stealing nodes own disjoint key ranges ⇒ range-disjoint pages) | |
| `SORT` | `page_overlap_keymerge` | the page-run merge overlap guard (INVARIANT ALARM): the page-whole fast path did NOT hold for the minimum page — some other segment's current page (cross-segment guard) OR the same segment's own next page (intra-segment monotonicity guard: `page[i+1].minKey <= page[i].maxKey`, unsigned) began at/inside its `[minKey, maxKey]` range — so the involved pages were decoded and merged at the KEY level (always correct) instead of emitted whole. Fires once per such overlap-resolution event. **On a well-formed OBJECTS run this is 0** (pages are range-disjoint by the work-stealing ownership invariant): a nonzero value is a loud alarm that pages interleaved (mis-ordered/overlapping staging), never a silent misorder — the merge stays correct, but the disjoint-page assumption was violated | |
| `SORT` | `page_run_entry_whole_page` | the entry-typed page-run READ path (fast path): the `PageRunSegmentReader` — the stream a `.pageseg` is opened as on the **mixed/`StreamingMerger` route** (a merge group that also holds a columnar Parquet segment) — streamed one page whole because it does not overlap the segment's own next page. This is the read-level analogue of `page_whole_emitted` (which stays the MERGE-level signal, `PageAwareMerger` only), kept under its own reason so the two routes are distinguishable post-hoc. Fires once per page so streamed. **0 on the all-page-run production path** (that route never opens this reader): a nonzero value simply means a mixed merge group existed, e.g. a `CaptureSorter` or parallel-range-merge fixture segment | |
| `SORT` | `page_run_entry_overlap_keymerge` | the entry-typed page-run READ path (INTRA-SEGMENT overlap resolution): within ONE `.pageseg`, adjacent pages' RANGES overlapped (`page[i+1].minKey <= page[i].maxKey` — perfectly legal: `PageRunSegmentWriter#flush` orders pages by first key, and two interleaved node runs then produce pages like `[a..m]`,`[c..z]`), so `PageRunSegmentReader` decoded them and merged their entries at the KEY level under the §0.3 comparator instead of concatenating them in file order. This is what makes the reader's `EntryStream` a genuinely SORTED run — the one thing the entry-typed `StreamingMerger` (a k-way entry heap) assumes of every input. Before this resolution the reader just concatenated pages, so such a segment merged alongside a Parquet segment published silently misordered rows with no exception and no counter; the page-min regression guard stayed (correctly) silent because the page MINs never regressed. Read-level twin of `page_overlap_keymerge`; not an alarm — overlap is contract-legal output, this counter says the resolution engaged. **0 on the all-page-run production path** (this reader is not opened there) and 0 on a mixed group whose page-run segments happen to have range-disjoint pages | |
| `SORT` | `page_run_min_regression` | the page-run READ-TIME format guard (HARD INVARIANT VIOLATION): a page-run segment stored a page whose `minKey` went BACKWARDS (unsigned) relative to the previous page in the same file — the read caught it, incremented this counter, and failed the run with `SegmentCorruptionException` / `error_class=page_run_min_regression` (treated as segment corruption). The check lives in `PageRunSegmentIo#nextPage()`, the ONE page-advance primitive **both** page-run readers use, so it fires on **every** page-run read path: the decode-free `PageFrontierReader` (→ `PageAwareMerger`) *and* the entry-typed `PageRunSegmentReader` (→ `StreamingMerger`, the fallback taken whenever a merge group also holds a columnar Parquet segment). This placement is required because `StreamingMerger` assumes each input run is sorted; guarding only the frontier route would leave a mixed-segment route able to misorder output silently. Non-decreasing page mins are the precondition that makes the `PageAwareMerger`'s frontier a valid lower bound on a segment's remaining keys AND that makes a page-run segment a sorted run at all; `PageRunSegmentWriter#flush()` establishes it (pages sorted by first key) and this guard VERIFIES it, so a writer regression can never silently misorder billion-scale output (the `page_overlap_keymerge` alarm fires only AFTER such damage). Page-range OVERLAP is legal and does NOT fire this (that is `page_overlap_keymerge`'s key-merge fallback) — only a min regression does. The aborting run's default `_swath_summary.json` carries BOTH this counter (in `meters[]`) and `error_class=page_run_min_regression` (see the `error_class`-on-crash note in §3). **Always 0 on any run produced by the shipped writer**; nonzero ⇒ corrupt/hand-built staging bytes or a writer bug, and the run aborted rather than emitting misordered data | |
| `SORT` | `backpressure_engaged` | instrumentation-only engagement counter (no new backpressure BEHAVIOR): fired whenever a seal found the off-thread semaphore already at 0 available permits the instant it reached the acquire (`SortLane#seal`, a cheap non-blocking `availablePermits()==0` snapshot) — i.e. the configured `buffers()-1` off-thread bound was already saturated. Distinct from (not a duplicate of) `swath.sort.backpressure.wait`'s latency timer: this is the discrete "did admission hit the bound" engagement signal, that is the continuous wait-time distribution. Read together with the `swath.sort.staging.bytes.peak`/`swath.sort.off_thread.buffers.peak` gauges (§1) for a future billion-scale repro's bounded-vs-unbounded verdict | |
| `SORT` | `merge_fanin_clamped` | the single-pass fan-in safety clamp: the RUNTIME merge-entry clamp (`SortTransform#clampedMergeFanIn`) reduced the merge fan-in BELOW the static `SortConfig#effectiveFanIn()` — the raised `fan-in` default (10000, so a billion-scale run stays single-pass) was trimmed by the real process fd limit and/or the exact per-segment page size before opening any reader. Fires once per merge that was clamped. **On a healthy single-pass run (ample fd headroom, budget ≥ segment memory) this is 0**; a nonzero value means the launcher's fd/heap budget could not admit the full requested fan-in, so the merge fell back toward the cascade backstop. Paired with `merge_cascade_predicted`/`merge_pass_cascaded`: if the clamp pushed fan-in below the segment count the cascade actually engages | |
| `SORT` | `merge_fanin_fd_clamped` | the fd sub-reason of `merge_fanin_clamped`: the process SOFT open-file limit minus the fd headroom (`MergeFdBudget`, reserving descriptors for checkpoint SQLite / output part writers / logs / JVM) was itself below the static `effectiveFanIn()` — i.e. the fd budget, not memory, is what forced the clamp. Fires alongside `merge_fanin_clamped` whenever the fd bound bound. A misconfigured/low `ulimit -n` is the loud signal here: it degrades a single-pass merge to a multi-pass cascade rather than crashing hours in with EMFILE | |
| `SORT` | `merge_fanin_mem_clamped` | the exact-memory sub-reason of `merge_fanin_clamped`: the EXACT per-open-stream memory bound (`mergeBudgetBytes / max(maxRecordLen)` read O(1) from each page-run segment's trailer) was below the static `effectiveFanIn()` — i.e. the real packed-page sizes exceeded the ~64 KiB `merge-per-stream-bytes` estimate enough that the budget admits fewer streams than the static config bound assumed. Fires alongside `merge_fanin_clamped` whenever the exact-memory bound bound. Only computable for page-run staging (columnar Parquet fixtures fall back to the static estimate and never fire this) | |
| `SORT` | `pack_on_fetch` | the pack-on-fetch engagement counter: a page was packed into its compact `PageBlock` on the FETCH worker that built it (`WorkStealingScan`, right after `filters.apply`) instead of on the single sort drain thread (`SortBuffer.admit`). Fires once per non-empty page emitted while `--sort` pack-on-fetch is enabled (gated on `pagePacker != null`), so the run total equals the number of pages the sort listing shipped — the "did packing parallelize across fetch threads / did the channel carry a packed page instead of a `List<ListEntry>`" signal. **Absent (0) on every non-sort run** (text/parquet-direct pipelines carry raw entry lists, never pack on the fetch thread) and on any sort producer that still hands the lane a raw list | |
<!-- ci:steal-reason-table:end -->

This table is **CI-enforced**: `scripts/ci/check-instrumentation-drift.py` (run in the fast CI gate, see
`docs/ops/dev/TESTING.md`) parses every `recordStealReason`/`stealReasonCounter` call site in
those four modules' `src/main/java` (resolving ternaries, local variables, and pass-through wrapper parameters
structurally, not via a hardcoded list) and fails the build on either an **undocumented counter**
(code emits a `category.reason` pair missing from this table) or a **ghost row** (a table row with no
live emitter, not marked `REMOVED <date>`) — so this table can't silently drift from the code the way
the prose above already had (`STEAL.attempted` and the `RESUME.*` counters existed in code with no
row here until this guard was added). Run it locally with
`python3 scripts/ci/check-instrumentation-drift.py`; `--self-test` runs its own synthetic
undocumented-counter / ghost-row smoke test.

---

## 7. Run trace format (`--trace`)

The user-facing `--trace` intro (what it does, opt-in, checkpointed-path-only, the sensitivity note)
is in [`docs/metrics-and-observability.md`](../metrics-and-observability.md) §7. This is the format
spec — the event schema, its zero-cost seam, durability, and the visualization renderers.

**Zero cost when off.** The seam is `io.varve.swath.observability.TraceSink`: an interface with an
always-on no-op default (`TraceSink.NONE`) and a JSONL implementation, threaded through
`WorkStealingScan`/`Thief`'s constructors the same way `EngineToggles` is. Every call site checks
`sink.enabled()` **before** building the event object — `TraceSink.NONE.enabled()` is a
constant `false`, so a run without `--trace` never allocates an event, never touches
`ControlCharEscaper`, never does I/O; it costs exactly one interface-dispatched boolean check per
would-be event.

**Schema (`"v":1`).** One JSON object per line: `v`, `ts_ns` (monotonic, via the same
`LongSupplier nanoClock` seam added for `avg_in_flight`), `ts_ms` (wall clock), `event`,
`worker_id`, and `node_id` where applicable. Key fields (`lo`/`cursor`/`hi`/`pivot`) are
`ControlCharEscaper`-escaped exactly like text-sink output.

| `event` | Fields (beyond the common envelope) | Fires |
| --- | --- | --- |
| `seeded` | `node_id`, `lo`, `hi` | once per initial worklist range, at scan start |
| `claimed` | `worker_id`, `node_id`, `lo`, `cursor`, `hi` | a worker pulls a range off the ready queue |
| `page_committed` | `worker_id`, `node_id`, `keys`, `cursor`, `completed` | after every durable page commit (dominates volume, ~1/page) |
| `steal_attempt` | `worker_id`, `outcome`, `reason` | every `Thief.steal` attempt (piggybacks the `recordStealReason` call site) |
| `victim_scan` | `worker_id`, `seen`, `skipped_unsplittable`, `skipped_paced`, `skipped_no_span`, `chosen_node_id`, `best_est`, `reason` | every `ThiefPolicy.selectVictim` pass — one per steal attempt, immediately before that attempt's `steal_attempt`. Aggregate over the pool, never per candidate (the scan runs constantly). `chosen_node_id` is `-1` and `reason` the `NoVictimReason` discriminator when the scan refused; on a hit `reason` is `null` |
| `owner_split_decision` | `worker_id`, `node_id`, `reason`, `est`, `pages_since_last_self_split`, `outstanding`, `worker_count`, `far_ahead_fraction`, `density_ratio`, `keys_emitted` | every `OwnerSplitGovernor.decide` past the open-frontier early-out — one per qualifying page commit, blocked OR carved (so ~1/page on a bounded range, `page_committed`'s order of volume). `reason` is the GATE CHAIN's terminal reason: an `OwnerSplitSkipReason` code, or `confetti_probe`/`pivot_reflect_clamped`/`pivot_reflect_lifted`/`self_published` on the carve side |
| `split` | `worker_id`, `node_id` (parent), `child_node_id`, `mechanism`, `pivot`, `hi` | a thief commits a split — `mechanism` is the same pivot-attribution tag as the `PIVOT.<mechanism>` counter (§5) |
| `owner_split` | same shape as `split` | an owner-side proactive self-split — `mechanism` is always `self_published` |
| `completed` | `worker_id`, `node_id` | a range drains to its `hi` bound |
| `failed` | `worker_id`, `node_id`, `reason` | a range's scan ends without completing (exception; excludes the expected broken-pipe/cancel teardown) |

**Numbers, and the two "missing value" conventions.** JSONL has no `NaN`/`Infinity` literal, so a
non-finite numeric field is written as JSON **`null`** — the line stays strictly parseable. Two
distinct things produce it, disambiguated by the event's own `reason`:

- **not computed** — the owner-split gate chain short-circuits, so a gate that terminated ABOVE
  where `far_ahead_fraction`/`density_ratio` are read reports them as `null` rather than a
  plausible-looking `0` (`OwnerSplitGateInputs.NOT_COMPUTED`, i.e. `NaN`, in the engine). Today that
  is exactly `reason ∈ {remaining_est_floor, rate_limited, demand_gated}`; every other input is read
  before the first gate. An integral input under the same convention would carry `-1`.
- **a genuinely non-finite reading** — `density_ratio` is `+∞` with `density_ewma=off` (the floor's
  own "no signal" fallback), `est`/`best_est` is `+∞` for an open frontier, and `best_est` is `-∞`
  when a `victim_scan` refused (the argmax's own seed, nothing ever scored).

**Gate decision vs executor outcome.** `owner_split_decision.reason` reports what the *gate chain*
decided. A carve the chain admitted can still fail to publish executor-side (a lost confetti
probe-slot claim, a rejected split CAS) — that shows as the absence of a following `owner_split`
event for the same `node_id`, and in the `OWNER_SPLIT.confetti_suppressed`/`self_aborted` counters,
never as a different `reason` here.

**Durability (JFR framing).** Written stream-append, never atomic-rename — a real process kill
leaves a readable prefix, doubling as a black box. Buffered; explicitly flushed only on
`completed`/`failed` (not every page, which would make tracing dominate I/O cost), so a kill loses
only events buffered since the last node's completion/failure. **Consumers must tolerate a torn
final line after a hard kill:** the `BufferedWriter` auto-flushes on buffer-full at an arbitrary byte
boundary (possibly mid-line), so the last on-disk line may be a partial fragment. Every earlier line
is intact — parse line-by-line and drop a trailing unparseable fragment (we do not claim per-line
write atomicity against buffer-full auto-flush).

**Versioning.** The `"v":1` field is the compatibility contract for any downstream consumer
(the planned V2 Perfetto exporter, V3 keyspace×time renderer). A breaking schema change bumps
`v`; this table is the source of truth for what `v:1` contains — update it in the same change
that adds/removes/renames an event field. Adding a whole new **event kind** (as `victim_scan` and
`owner_split_decision` were) is additive, not breaking, and does **not** bump `v`: every existing
kind keeps its exact fields, and a consumer is required to ignore event kinds it does not know.

**Not covered by the §5 drift guard.** `scripts/ci/check-instrumentation-drift.py` enforces the
`recordStealReason` counter table above; it does **not** parse trace event call sites (trace
events aren't counters — they don't have a `category.reason` shape and can carry arbitrary
per-event fields). This table is versioned by hand.

### Reading a trace, visually

Two committed renderers turn a `--trace` file into pictures:
`trace-to-perfetto.py` emits Chrome Trace Event Format for https://ui.perfetto.dev (zoomable
worker timeline: range drains as slices, steals/splits as instant events); `trace-viz.py` emits
a self-contained HTML page adding the keyspace×time carving map. Reading signatures: a healthy
run is confetti (many short drains, everyone busy) and a map of wide/short rectangles; a dense
serial tail is a single tall thin column outliving the run; a thief probe storm is a blizzard of
steal-attempt markers; owner-split confetti is a stack of hairline rectangles in one keyspace
region. The HTML page embeds this guide so every generated trace self-explains.

---

## 8. Replay-server meters (`swath.replay.*`, module-local)

The `swath-replay-server` module is a separate Gradle module from `swath-core`, where
`WorkStealingScan`/`RunMetrics` run — its meters share the same Micrometer idiom (§1) but are
**outside the §5a drift guard**: `check-instrumentation-drift.py` only parses
`swath-model`/`swath-core`/`swath-s3`/`swath-cli`'s `src/main/java` (the modules that replaced the
old root `src/`), so this section (not a `SORT.*`-table row) is where they're documented, updated
in the same commit as whichever unit adds a meter. Full usage/tuning walkthroughs live in
`docs/swath-replay-server.md`; this is the meter reference.

`ReplayMetrics` (`io.varve.swath.replay.server`) — HTTP/fixture/DuckDB-query tiers:

| Meter | Type | Meaning |
| --- | --- | --- |
| `swath.replay.http.requests` / `.http.errors` | counter | Requests served / 5xx responses. |
| `swath.replay.http.request.latency` | timer | End-to-end HTTP request latency (p50/p99 published). |
| `swath.replay.fixture.list.latency` | timer | One `ListingFixture#list` call (p50/p99 published). |
| `swath.replay.serving.path{mode}` | counter | Per-`list` engagement signal: `mode=sorted` (role-2 `SortedParquetStore`) or `mode=duckdb` (role-1). The mode is fixed once at startup by the resolved `--serving-mode`. |
| `swath.replay.page.read.latency` | timer | One store-level range read (`ListingStore#rows`), started **after** the connection is borrowed so it excludes pool-wait — the decode cost B5 reports, distinct from the query/fixture/http tiers (p50/p99 published). When the sorted-path prefetch decorator (`WindowedListingStore`) is active it owns this timer for the **outer** per-page call (so the corridor number stays the honest amortized per-page cost — a hit sub-ms, a miss ≈ one window fill), and the wrapped `SortedParquetStore` suppresses its own copy so a miss isn't double-counted; the fill itself is `prefetch.window.fill`. |
| `swath.replay.parquet.query.latency` / `.query.errors` | timer / counter | One DuckDB query against the Parquet fixture / failures (p50/p99 published). Both stores emit it (materialised role-1 query and bounded `read_parquet` role-2 query). |
| `swath.replay.parquet.query.rows` | distribution | Rows returned per DuckDB query. |
| `swath.replay.parquet.queries.in_flight` | gauge | Concurrent DuckDB queries in flight. |
| `swath.replay.prefetch.window.hit` / `.window.miss` | counter | Sequential-window prefetch (role-2 `WindowedListingStore`, sorted path, on by default) engagement: a `rows()` call served from a buffered window (`hit`) vs. one that required a delegate window fill (`miss`). A sequential walk should be miss-then-many-hits; a persistently high miss rate means windows aren't being reused (mismatched `(toExclusive, projection)` keys, or `window-rows` too small for the page size). |
| `swath.replay.prefetch.window.fill` | timer | The delegate read on a prefetch miss (one `LIMIT window-rows` `SortedParquetStore` read, p50/p99 published). When prefetch is on, this — not `page.read.latency` — carries the ~50k-row window-fill cost; `page.read.latency` measures the outer amortized per-page cost (a hit is sub-ms, a miss ≈ one fill). |

`FixtureMetrics` (`io.varve.swath.replay.fixture`, a deliberate **sibling** to `ReplayMetrics` —
`.fixture` must not depend back on `.server`, which already depends on it) — the `sort-fixture`
legacy-transform path and the sorted-serving index derive/sanity-check path:

| Meter | Type | Meaning |
| --- | --- | --- |
| `swath.replay.sortfixture.build.latency` | timer | Wall time of one `sort-fixture` run. |
| `swath.replay.sortfixture.output.bytes` | distribution | Sorted output file size(s) for that run. |
| `swath.replay.sort.steal_reason{outcome,reason}` | counter | Registry-backed `io.varve.swath.sort.SortMetrics` adapter `sort-fixture` wires in (instead of `SortMetrics.NO_OP`) so the root sort library's engagement counters (`outcome=SORT`: `segment_flushed`, `merge_pass_cascaded`, `merge_fastpath`, `buffer_sort_fallback`, `buffer_byte_gated`) are observable for a real run; the printed summary line also carries `segments`/`merge_passes`/`cascaded_passes`. |
| `swath.replay.index.load.latency{source=derived}` | timer | Time to derive the in-memory row-group routing index (the `source` tag anticipates a future `footer` value once an embedded routing blob lets the server skip deriving). |
| `swath.replay.index.entries` | distribution | Row-group index entries produced by one derive pass. |
| `swath.replay.serving.fallback{reason}` | counter | Auto serving-mode declined sorted serving: `no_stamp` (a resolved file carries no recognized sortedness stamp), `unsupported_mode` (a resolved file is stamped a `versions` file, unsupported for serving in v1), `unknown_format_version` (a resolved file's stamp carries a `format_version` this reader doesn't recognize), `incomplete_multifile` (the resolved file set's `file_index`/`file_final` stamps don't prove multi-file completeness — e.g. a crash left only a stamped prefix of a multi-file publish on disk), `mixed_row_types` (a row group's `row_type` footer stats do not prove every row is `OBJECT` — a sorted file may legitimately mix row types, e.g. a legacy delimiter'd capture re-sorted by `sort-fixture`, which stamps `mode=objects` unconditionally), `sanity_failed` (the derived row-group first-key array was not strictly ascending). Every resolved file is checked, not just the first. `mixed_row_types`/`sanity_failed` are recorded directly by the index-derive step; the other four reasons by the `--serving-mode auto` decision that consumes it. `--serving-mode sorted` never bumps this counter (a hard fail is not a fallback). |
| `swath.replay.serving.refused{reason}` | counter | A request the sorted path had to refuse outright, tagged with the typed reason from `io.varve.swath.sort.RowGroupOrderException`: `row_group_disorder` (a row group's own rows are not in strictly ascending key order, seen by the `delimiter=/` skip-scan's key cursor as it steps over them). NOT a `serving.fallback` reason: eligibility already passed (it proves the ascent of row-group *first* keys only), the serving path was chosen at startup, and nothing can take the request over — it fails `500`. Bumped BEFORE the throw, so the exclusion survives into a sweep's metrics; a corpus sweep classifies a disordered capture from this counter, never from the error body. |

Post-hoc test, same discipline as §5: from these meters alone — did `sort-fixture` run, how big
was the output, did the derive index build cleanly, and if sorted serving was ever declined, why?
