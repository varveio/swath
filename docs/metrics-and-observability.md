# swath — metrics & observability reference

User-facing reference for everything swath emits: the Micrometer **meters**, the `-v` **progress**
**line**, the **`list_run_summary`** line (incl. the efficiency/resource fields), and the core shape
of the JSON run-summary artifact. The deep, contributor-tier companion — the JSON forensics fields,
the instrumentation-discipline essay and steal-reason counter registry, the run-trace format spec,
and the replay-server meters — is [`docs/internals/metrics-internals.md`](internals/metrics-internals.md).

Logs go to **stderr** (stdout is data). Verbosity is a global flag, accepted **before or after** the
subcommand: `swath -v list …` and `swath list -v …` both work (INFO), `-vv` (DEBUG), `-vvv` (TRACE).
`-q` lowers the level instead — `-q` (ERROR) or `-qq` (off), winning over `-v` if both are given.
One reporter covers the whole run — the seed step through listing, merging and writing — at a 30 s
default cadence, configurable with `--progress-interval` (floor: `1s`; a faster value is rejected,
not clamped). Its tick takes exactly one of two forms. When an operator display is wanted (§4's
gate, or `--progress`), it is the plain human line documented in
[`usage.md`](usage.md#progress); otherwise it is the structured `progress` **log** record below, at
INFO — so it needs `-v`. A display REPLACES that record rather than adding to it: one tick renders
once, never twice on the same stderr. The first record lands a couple of seconds into the run rather
than a whole cadence in, so a short run is not silent.

Verbosity does **not** gate the end-of-run summary block: it is written to stderr at the default
level for any run that earns one (over 1.5 s, durable output produced, or an early stop), whether
stderr is a terminal or a file — see [`usage.md`](usage.md#end-of-run-summary) for the block, its
`--stats`/`--no-stats` control, and how it relates to the `list_run_summary` line and `--report`.

---

## 1. Micrometer meters

| meter | type | tags | meaning |
|---|---|---|---|
| `swath.api.calls` | counter | `{strategy}` | actual S3 list attempts (incl. engine throttle-retries) — the real HTTP-request count. Tagged with the run's strategy at call time, and the only meter tagged this way. If the strategy isn't known before a run's very first call (including the seed probe), that run's calls fragment across two series — `strategy="unknown"` for whatever ran first, then the real strategy — and each series undercounts the run. The JSON summary's `cost.api_calls` sums every series and stays correct either way |
| `swath.api.latency` | timer | `{op}` | per-call network latency (e.g. `listObjectsV2`); publishes p50/p90/p99 plus `_sum`/`_count` for windowed means |
| `swath.fetch.latency.phase` | timer | `{call_class, phase}` | per-call-class latency-phase decomposition — what tells a probe-timeout storm's client-side pool starvation apart from server-side slowness. `call_class` is `worker_page`\|`pivot_probe`\|`structure_probe`, classified purely from the request shape (`delimiter=/` = structure probe, `delimiter`-less `max_keys<=1` = pivot probe, everything else = a worker's range page fetch); `phase` is `connect_acquire`\|`ttfb`\|`total`. `connect_acquire` (the connection-pool checkout wait) and `ttfb` come from the AWS SDK's own per-attempt metrics and are **best-effort** — either may be absent on a given attempt, and is then skipped, never fabricated as a `0` sample; `total` is swath's own measured wall-clock elapsed. Bounded cardinality (3×3 = 9 series max). Publishes p50/p90/p99; read them back from the JSON summary's `probe_latency[]` (§3), since the generic `meters[]` readback carries a Timer's count/total_ms/max_ms only, not percentiles. **The phases are NOT additive:** the SDK does not say whether time-to-first-byte is measured from request-issue or from after the connection is already leased, so `connect_acquire` and `ttfb` may partially overlap — read each as its own independent signal, never as phase shares that sum to `total`. **Records on the FAILURE path too**, not just success, so a storm's ~10 s failed probes land in the percentiles instead of being survivorship-filtered out of them (same exception-inclusive behaviour as `swath.api.latency`). `total` is recorded for every attempt, success or failure; `connect_acquire`/`ttfb` are recorded from whatever the SDK captured before the fault and are commonly unavailable on a fast-failing attempt (a hung read that never got a service response has no time-to-first-byte to report). **Caveats:** (1) `structure_probe` is NOT thief-exclusive — the seed step's own `delimiter=/` probes at run start classify identically, because the classifier sees only the request shape, never who issued it; (2) `pivot_probe` assumes the run's configured page size is `> 1` — a (never-recommended) `--max-keys=1` run misclassifies every ordinary worker page fetch as `pivot_probe`, since the classifier only checks `max_keys<=1` on the request |
| `swath.entries.emitted` | counter | — | object entries emitted downstream |
| `swath.bytes.estimated` | counter | — | estimated output bytes |
| `swath.workers.active` | gauge | — | live concurrency target `T` (the AIMD value) |
| `swath.in_flight.avg` | gauge | — | time-weighted average in-flight listing count since run start, folded on every in-flight transition (sample-on-change, no polling thread). Also carried end-of-run as `avg_in_flight` in the JSON summary `engine` block (§3), next to `peak_in_flight`: `peak_in_flight` saturates and blinds you once the concurrency ceiling is hit, while `avg_in_flight` shows whether the run *sustained* parallelism or merely spiked to it |
| `swath.steals` | counter | `{result}` | steal outcomes (e.g. `CHILD_CREATED`) |
| `swath.errors` | counter | `{type}` | errors by type (e.g. `throttle`) |
| `swath.steal_reason` | counter | `{outcome, reason}` | engagement counters for every steal/split/seed decision path (§5); bounded enum (~30-50 `outcome.reason` combinations, same low-cardinality shape as `swath.steals{result}`) |
| `swath.probe.fetches` | counter | — | 1-key `start_after` steal probes issued (the speculative `max_keys=1` LIST) |
| `swath.probe.structure_fetches` | counter | — | `delimiter=/` structure-probe LIST fetches issued — a distinct probe-I/O class, so `wasted_probe_ratio` can attribute structure-probe waste |
| `swath.probe.empty_upper_bisections` | counter | — | probe bisections that found an empty upper half |
| `swath.split.unsplittable_victims` | counter | — | steal attempts that found a victim with no valid pivot |
| `swath.split.guard_aborts` | counter | — | in-flight splits aborted by the split guard |
| `swath.page.raw_count` | counter | — | raw S3 pages fetched (pre-any page-shape normalization) |
| `swath.page.raw_keys` | counter | — | raw keys observed across fetched pages |
| `swath.page.short_truncated` | counter | — | truncated pages that returned fewer than `max-keys` |
| `swath.throttle.events` | counter | `{type}` | throttle events, typed `attempt_timeout\|slowdown\|server5xx\|network` — separates a real S3 `503 SlowDown`/5xx/network fault from a **self-inflicted** client-side attempt timeout. The distinction is load-bearing: counting swath's own 30 s timeouts as "throttle" collapses AIMD to serial with zero real `slowdown` |
| `swath.aimd.target_reductions` | counter | — | AIMD concurrency-target reductions |
| `swath.owner_split.demand_gated_t` | gauge | — | the effective concurrency target `T` observed at the INSTANT the most recent `OWNER_SPLIT.demand_gated` suppression fired — so a shed-shrunken `T` closing the owner-split demand gate is visible without cross-referencing `swath.workers.active`'s history against a log timestamp. `NaN` until the gate has fired at least once |
| `swath.owner_split.demand_gated_t_min` | gauge | — | the LOWEST effective `T` observed across every demand-gate firing this run (the running min, mirroring `swath.aimd.target_low_water`'s idiom) — how far the shed/AIMD brake had pulled `T` down at the gate's worst observed moment. `NaN` until the gate has fired at least once |
| `swath.queue.wait` | timer | — | producer backpressure (bounded output queue full) |
| `swath.rate_limit.wait` | timer | — | the always-on **reactive AIMD** concurrency-slot wait — the gate every worker page fetch waits on, regardless of `--request-rate`. See §1.1 |
| `swath.rate_limit.api_wait` | timer | — | the opt-in **proactive** `--request-rate` client-side cap, on its own dedicated meter so it is never conflated with `swath.rate_limit.wait` above. When `--request-rate` is unset (default) or an explicit `0`, the limiter is never wired in, so this meter is **genuinely zero** — no approximation. See §1.1 |
| `swath.process.memory.rss.bytes` | gauge | `{kind=current\|peak}` | resident set size — current (`/proc/self/status` `VmRSS`) and high-water (`VmHWM`) |
| `swath.process.memory.heap.bytes` | gauge | `{kind=current\|peak}` | JVM heap usage — current and peak, summed across HEAP pools |
| `swath.process.cpu.time` | function counter | — | cumulative process CPU seconds (`getProcessCpuTime()`); monotonic, hence a counter not a gauge |
| `swath.idle_backoff.level` | gauge | — | current shared idle-steal backoff level (the backoff guard's `consecutiveNonProductive`) — live, not a per-run max |
| `swath.idle_backoff.resets` | counter | — | the backoff recovered from a non-productive streak (a productive claim/split after the level was `>0`) |
| `swath.idle_backoff.slot_denied` | counter | — | a worker denied an idle-steal attempt slot by the backoff guard itself (not the AIMD gauge) |
| `swath.idle_backoff.park_time` | timer | — | actual time a worker spent parked on the idle-wait, not the requested budget |
| `swath.checkpoint.commit.latency` | timer | — | the checkpoint writer thread's per-batch op execution + commit (the WAL-fsync critical path) |
| `swath.checkpoint.commit_batch_size` | distribution summary | — | tasks per committed writer-thread batch (up to `MAX_BATCH`=256) |
| `swath.checkpoint.queue.wait` | timer | — | time a checkpoint task waited on the writer queue before its batch was drained |
| `swath.checkpoint.queue.depth` | gauge | — | live depth of the checkpoint writer's task queue |
| `swath.parquet.rotation` | counter | `{trigger}` | a lane rotation fired, by which cadence trigger (`size`\|`rows`\|`time`) |
| `swath.parquet.parts` | counter | `{outcome}` | a lane's open part reached a terminal outcome — `finalized`\|`discarded`\|`finalize_failed` (the part's footer write/fsync threw, so the part is deleted and NOT in the manifest) |
| `swath.parquet.finalize.latency` | timer | — | footer-write + fsync latency — the durability point for a Parquet part |
| `swath.sort.entries` | counter | — | entries accepted by the `--sort` lane |
| `swath.sort.segments.written` | counter | — | sorted staging segments flushed |
| `swath.sort.segment.bytes` | counter | — | compressed staging-segment bytes written |
| `swath.sort.merge.passes` | counter | — | k-way merge passes executed (1 = single pass; >1 = cascade) |
| `swath.sort.merge.latency` | timer | — | per-run merge wall time (staging segments → published sorted file) |
| `swath.sort.merge.range.latency` | timer | — | per-RANGE merge wall time on the off-by-default parallel range-merge path (`swath.sort.merge-parallelism>1`) — recorded once per concurrent range, distinct from `swath.sort.merge.latency` (the whole-run wall), so an A/B can read per-range cost with vs. without row-group skip. Untouched (zero samples) on the default serial merge |
| `swath.sort.backpressure.wait` | timer | — | time the listing waited to hand a sealed buffer to the (busy) encoder — the accepted cost of off-thread encoding |
| `swath.sort.page_runs_per_buffer` | distribution summary | — | per-node page runs per sealed buffer (JSON `sort.page_runs_per_buffer` classification signal) |
| `swath.sort.staging.bytes.peak` | gauge | — | high-water mark of total live (admitted-but-not-yet-durable) `--sort` staging bytes — the fill buffer's current byte estimate plus every sealed-but-unfinalized buffer's estimate (captured at seal time). **The number to read after a sort-staging OOM:** if it tracks roughly `T × segmentBytes` and stays bounded as `T` stabilizes, the OOM is linear-in-`T` tuning (give the run more heap); if it climbs unboundedly under a retry storm while `T` is stable, that is an unbounded-leak signature. Instrumentation only — it never gates admission itself |
| `swath.sort.handoff.queue.depth.peak` | gauge | — | high-water mark of the sort lane's handoff-queue depth, sampled right after a seal hands a buffer off. A semaphore already gates entry to the queue, so this should never exceed the configured `buffers()-1` bound — a peak above that bound is itself a bug signal, not tuning variance |
| `swath.sort.off_thread.buffers.peak` | gauge | — | high-water mark of concurrently-live off-thread (queued + actively encoding) sealed buffers, against that same configured `buffers()-1` bound — shows whether the bound is actually holding under load |
| `swath.disk.free_bytes` | gauge | — | free space (`FileStore.getUsableSpace()`) on the scratch/output volume — sort-staging segments + Parquet parts (the volume that `--sort` staging can exhaust); pull-based, sampled on each scrape like the `swath.process.*` gauges, `NaN` when unavailable or no output directory is known for this run (emitted at OTLP export, dropped downstream — see the NaN caveat below) |
| `swath.s3.pool.leased` | gauge | — | S3 `ApacheHttpClient` connection pool: connections currently leased (in use) — whole-client count, not per-connection |
| `swath.s3.pool.idle_available` | gauge | — | S3 `ApacheHttpClient` connection pool: idle connections available for reuse |
| `swath.s3.pool.pending_acquisition` | gauge | — | S3 `ApacheHttpClient` connection pool: requests blocked waiting for a connection — `>0` is live, unambiguous **acquisition-starvation** (pool exhausted) |
| `swath.s3.pool.max` | gauge | — | S3 `ApacheHttpClient` connection pool: configured max connections |
| `swath.s3.pool.connection_aborted` | counter | — | a connection was forced-destroyed by a client-side abort — incremented once per classified `attempt_timeout`/`network` `swath.throttle.events` fault, never on the reusable-release path. A distinct series from `swath.throttle.events{type}`: that counts the fault classification, this counts the connection churn it drives |
| `swath.s3.pool.handshakes` | counter | — | one new pooled connection completed its TCP+TLS handshake (including TLS layered over an HTTPS-proxy tunnel) — fires only when the pool opens a genuinely NEW connection, and only after that handshake completes successfully; never on lease/reuse of an existing one, and never on a failed connect (see `connection_aborted` above for that). The rate this climbs to under an attempt-timeout storm is the handshake churn `swath.s3.pool.connection_aborted` predicts; the `swath.s3.pool.*` gauges above cannot see that rate at all (they are event-driven snapshots, not churn counters). swath's short connection idle time (5 s) and time-to-live (1 min) force frequent reconnects by design, so a nonzero handshake baseline in an otherwise-healthy run is expected, not pathology |
| `swath.s3.socket_closure_recovered` | counter | — | a client-local socket-closure fault that escaped the SDK as a plain runtime exception rather than an `SdkException` (e.g. `UncheckedIOException(SocketException("Socket closed"))` surfacing from a transient S3 500 burst) was reclassified as a transient network fault and ridden out, instead of crashing the run unclassified at exit `1` / `error_class=unknown`. Incremented once per such recovery, alongside `swath.throttle.events{type=network}`, `swath.s3.pool.connection_aborted`, and `TRANSIENT.socket_closure`. A distinct series from the modeled network path, so you can tell this recovery engaged and how often; surfaced in the `recovered_errors` JSON rollup as `socket_closure` |
| `swath.progress.units` | counter | — | **the stuck signal.** One monotonic counter that advances in **every** phase by construction — entries emitted during listing/writing, rows merged during the `--sort` k-way merge (batched, via the merge's progress callback). `rate(progress.units)==0` is therefore a phase-independent "stuck" test with no phase-gating logic and no boundary race — unlike `swath.entries.emitted` alone, which legitimately flatlines during the sort merge. It advances during intermediate cascade merge passes too, not just the final pass; one accepted seconds-scale window remains (the final Parquet-part flush) — see the caveats below |
| `swath.phase` | gauge | — | live run phase for dashboard readability (and the self-throttle/tail boards) only — **not** the stuck-detection gate (a gauge is sampled at push time and can miss a phase shorter than the export interval). `0`=listing, `1`=merging, `2`=writing, `3`=complete, `4`=seeding (added later, hence the out-of-order code: the published codes are explicit and stable and can never be silently renumbered). `NaN` before the first phase is set (emitted at OTLP export, dropped downstream — see the NaN caveat below); live progress reports that pre-phase state as `starting` rather than fabricating a phase. A fresh run sets `seeding` before its first seed probe, and so does a resume whose checkpoint holds zero nodes — the state an interrupted seed leaves behind, which must be re-seeded rather than resumed as complete. Non-sort runs stay at `listing` until `complete` (their writing is concurrent with listing); the sort path sets `merging` for the cascade passes, `writing` once the final streaming pass + publish begin, then `complete` |
| `swath.output.files` | counter | `{format, outcome}` | output files/parts successfully written, per sink — `format` is one of `jsonl\|tsv\|table\|parquet` (a text sink is always 1 file; `--sort`'s final published output is itself Parquet, so it also reports `format=parquet` — the `swath.sort.*` meters are what distinguish the code path that produced it); `outcome` is `written`, or `truncated` for a text sink whose downstream pipe broke mid-run or at close (paired with `swath.output.broken_pipe`) — Parquet/sort have no broken-pipe concept and always report `written`. The same counts are carried in the JSON summary's `output_files`/`compressed_size_bytes`, so the two never disagree |
| `swath.output.bytes` | counter | `{format}` | output bytes written INTO the sink, per sink/format — bytes offered to the sink's buffer, not bytes confirmed delivered downstream (see the broken-pipe row below). For text sinks (JSONL/TSV/TABLE) this is a REAL count of UTF-8 encoded bytes, not an estimate |
| `swath.output.broken_pipe` | counter | — | a text sink (stdout or a file) was truncated by a downstream reader closing the pipe mid-run or at final flush — the run still exits 0. `swath.output.bytes` counts bytes offered INTO the sink, not bytes actually delivered downstream: on a mid-run break it undercounts the full listing, but on a close-time break it can equal the full logical output even though delivery was truncated — this counter is the truncation signal, not the byte count |
| `swath.run.duration` | timer | — | end-of-run wall-clock duration, recorded exactly ONCE per completed run (alongside `Phase.COMPLETE`) — NOT the same thing as the live `-v` progress cadence; a post-hoc "how long did runs take" aggregate |
| `swath.run.throughput` | gauge | — | end-of-run **lifetime average** keys/sec (the JSON summary's `keys_per_sec`), set exactly ONCE at completion — an end-of-run aggregate for post-hoc analysis only. A lifetime average is a misleading live/continuously-scraped signal, so never read this as a live dashboard number; use `swath.in_flight.avg` or the live progress snapshot's windowed rate for that |

Three resume-engagement signals ride the existing `swath.steal_reason{outcome=RESUME, reason}` counter
(§5) rather than a meter of their own: `nodes_reopened` (nodes reverted from `IN_PROGRESS`/
non-durable-`COMPLETED` back to `PENDING` — always zero on a fresh run, since a freshly-seeded node is
already `PENDING`), `durable_cursor_lag` (a **node-count** proxy for the checkpoint RPO bound — how
many reopened nodes had a non-durable tail; NOT an exact key/page count), and `args_hash_refused`
(a `swath resume` rejected because `args_hash` changed).

The **distinct timers** (`api.latency`, `queue.wait`, `rate_limit.wait`, `rate_limit.api_wait`) keep a
network slowdown, a downstream stall, a reactive AIMD pause, and a proactive client-side cap wait from
ever being conflated. `bucket` is **not** a default tag
(CloudWatch cardinality). The five `swath.process.*` meters are **pull-based** — Micrometer evaluates
their supplier on each read/scrape (no background sampler thread), so they're live in-run, not an
exit-only snapshot. An unavailable source (non-Linux `/proc`, no `com.sun` OS bean) makes a gauge
supplier return `NaN`, which is emitted as a `NaN` datapoint at OTLP export and dropped downstream by
the collector/backend (see the NaN caveat below); `swath.process.cpu.time` is only registered at all
when the CPU-time bean is available at construction (a counter can't sanely be `NaN`).
`--metrics-port` (Prometheus) is v1.1; today metrics are observed via the logs and the end-of-run
summary's peak RSS/heap (the `-v` progress record does not sample memory per tick, see §4).

The four `swath.s3.pool.*` gauges come from the AWS SDK's own `ApacheHttpClient` metrics
(`HttpMetric.{LEASED_CONCURRENCY, AVAILABLE_CONCURRENCY, PENDING_CONCURRENCY_ACQUIRES,
MAX_CONCURRENCY}`), delivered by the SDK's own metric-publisher hook — not sampled by swath itself.
They are **whole-client** counts (the Apache pool is shared across every request on that client), not
per-connection, and unobserved (`NaN`, dropped downstream) until the SDK reports the first attempt.
Read `pending_acquisition > 0` as live **acquisition-starvation** (the pool is exhausted); `leased ≪
max` while `in_flight.avg`/`workers.active` are low is **AIMD self-throttle**, not starvation — the
two are indistinguishable from `workers.active`/`in_flight.avg` alone. **Freeze caveat:** these gauges
are **event-driven** (updated once per completed API attempt), so during a zero-API window (the whole
sort merge, or a full AIMD stall) they **hold their last observed value** rather than decaying to
zero — read them together with `swath.api.calls` rate, and don't diagnose a *frozen* `pending>0`
from a pre-merge burst as live starvation. Concurrent SDK-thread publishes are last-writer-wins per
field (the four fields may come from different attempts); acceptable at scrape granularity.

**Caveats for the OTLP-exported operational meters (accepted windows, not bugs):**
- **`swath.progress.units` writing window:** progress folds in *entries emitted* for listing/writing
  (not bytes flushed), so a non-sort run whose listing has drained while the writer pool finalizes
  its last Parquet parts (footer+fsync) shows a brief `rate==0` — bounded by the output-queue depth
  and the final-part flush, seconds not minutes. The sort path's final streaming pass IS counted
  (via the merge callback), so the large merge window is covered. A watchdog keyed to this counter
  should use a stall threshold well above that final-flush window — **≥5 min**, not seconds.
- **`swath.disk.free_bytes` coverage:** one gauge on the output/sort-staging volume (they share a
  filesystem — staging lives under the output dir). It does **not** cover the checkpoint-SQLite
  volume or a text/stdout run with no output dir; a checkpoint-DB volume filling is still invisible.
- **`NaN` gauges reach the wire.** swath does **not** rely on Micrometer's OTLP registry omitting a
  `NaN` gauge at export — it doesn't: an unavailable-source gauge is emitted as a literal `NaN`
  datapoint. The `NaN → no stored series` behaviour these gauges rely on happens **downstream** —
  the collector/backend drops the `NaN` datapoint and still accepts the rest of the batch. swath
  deliberately does not add a `NaN`-dropping `MeterFilter` to hide this at the client; every gauge
  uses the same idiom, emitting `NaN` when its source is unavailable and letting the backend drop it.
- **Histogram flavour is pinned.** swath pins the OTLP histogram flavour to explicit-bucket
  histograms rather than letting the ambient
  `OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION` env var decide. This changes **nothing
  on today's wire**: swath configures only client-side published percentiles (no percentile
  histograms, SLOs, or meter filters), so no current series reaches the flavour-sensitive branch, and
  every run's payload carries `SUMMARY`/`GAUGE` datapoints under any value of that env var. The pin
  is forward-looking — it guarantees that any future histogram-publishing series gets a wire shape
  decided by swath, not by the ambient environment.

### 1.1 Throttle coverage: proactive cap vs. reactive AIMD backoff

swath has **two independent throttle mechanisms**, and each has its own meter, so they never have to
be told apart from context after the fact:

- **`--request-rate` (proactive, client-driven) → `swath.rate_limit.api_wait`.** A user-configured
  ceiling on the run's aggregate S3 API request rate, enforced *before* any request is sent,
  regardless of whether the store is under load. One limiter is shared by every worker thread and the
  steal-probe path, so the cap applies to the whole run, never per-thread. Blocked time accrues into
  `swath.rate_limit.api_wait`. Unset (default) or an explicit `0` both mean "no cap" — the limiter is
  then never wired in at all, so it adds zero overhead and the meter is **genuinely zero**, not just
  "no additional accrual".
- **AIMD 503/SlowDown backoff (reactive, server-driven) → `swath.rate_limit.wait`.** swath
  multiplicatively lowers the live concurrency target `T` only *after* the store itself returns a
  `503 SlowDown` (algorithms.md §5); it never engages if the store never throttles. That is the
  `swath.throttle.events` / `swath.aimd.target_reductions` path. Separately, the concurrency *gate*
  every worker page fetch waits on is always active — regardless of `--request-rate`, and regardless
  of whether AIMD has ever reduced `T` — and its wait accrues into `swath.rate_limit.wait` too. So
  `swath.rate_limit.wait` is **not** a pure "AIMD actually throttled" signal even though it is named
  after the AIMD path: it is zero only on a run with essentially no concurrency-slot contention at all.

Read together: a nonzero `swath.rate_limit.api_wait` is proof `--request-rate` was set (it is zero
otherwise, by construction), and `swath.rate_limit.wait` is the AIMD/gate-wait reading with no
proactive-cap contribution mixed into it.

---

## 2. `list_run_summary` (one line at run end)

Core: `run_id, objects, duration_ms, strategy, api_calls, cost_usd, output_files,
compressed_size_bytes, keys, pages, peak_in_flight, steals, splits, errors, keys_per_sec`.

`objects` describes the **dataset the run published**, so on a resume it includes the rows a
previous attempt already made durable (managed-Parquet parts, `--sort` staging segments) — the same
rows the published `manifest.json` counts. Everything measured against this process's own clock or
its own API calls (`keys_per_sec`, `api_calls_per_1k_objects`, `overfetch_ratio`) excludes them: the
recovered rows cost this run neither a second nor a LIST call.

**`duration_ms` is the LISTING clock, not the whole session.** A fresh run's seed step (probing the
bucket's shape to tile the initial worklist) runs BEFORE this clock's zero point, so `duration_ms` —
and everything divided by it, `keys_per_sec` included — excludes seeding entirely; it is the honest
throughput denominator, since seeding fetches no object. The JSON report's top-level
`session_duration_ms` is the OTHER clock: the whole CLI invocation, seeding included — the same span
the live progress line's `elapsed` already reports. The two agree exactly on a resumed run (seeding
never re-runs on a normal resume) or any run whose seed step was cheap; a fresh run against a
deeply-nested or hinted bucket is where they diverge, sometimes by tens of seconds. Neither figure is
wrong — read `duration_ms` for throughput, `session_duration_ms` for "how long did the operator
actually wait" — see the end-of-run summary block below, which prints both, clearly labeled, exactly
when they diverge materially.

The JSON report's `engine` block additionally carries the two ramp-up timings the
`list_run_diagnostics` line prints — `time_to_first_steal_ms` and `time_to_peak_in_flight_ms`
(milliseconds from run start; `null` when the event never happened) — and `cost.basis` names the
rate `cost_usd` was derived from (`rate_per_1k_usd`, `source`), so no consumer has to assume it.

**Efficiency / resource fields** (summary/log fields — NOT Micrometer meters; sampled once at end;
`-1` where unavailable, e.g. off-Linux):

| field | meaning | how to read it |
|---|---|---|
| `api_calls_per_1k_objects` | actual S3 requests per 1000 objects listed **by this run** (a resume's recovered rows are excluded — they cost this process no LIST call) | **efficiency.** Healthy flat/deep listing ≈ 1 (1 page = 1000 keys). **High** (tens–thousands) = wasted probes — over-splitting / idle-worker steal churn. The single best "are we being efficient" number. |
| `peak_rss_bytes` | peak resident memory (`/proc/self/status` VmHWM) | the real memory footprint — use to size containers / pick a heap |
| `peak_heap_bytes` | peak JVM heap (sum of heap-pool peaks) | JVM heap demand (Parquet path is the heaviest) |
| `cpu_seconds` | process CPU time consumed by the run | total compute spent |
| `cpu_efficiency` | `cpu_seconds / wall_seconds` ≈ mean cores used | **≈ 1** is healthy for an I/O-bound listing. **≫ 1 on a small/fast listing** = idle workers spin-stealing (lower `--concurrency`). |

### Interpreting them together
- **Ideal small/flat bucket at any concurrency:** `api_calls_per_1k_objects ≈ 1`, `cpu_efficiency ≈ 1`,
  `splits` small.
- **Probe waste / over-concurrency:** `api_calls_per_1k_objects` and `cpu_efficiency` both climb while
  `keys_per_sec` does *not* — you're paying CPU + S3 requests for no throughput. Reduce concurrency.
- **Deep-prefix buckets** were the worst case (the latency livelock and the idle-steal probe storm — both fixed);
  these metrics are exactly what surface a regression of either.

---

## 3. JSON run-summary artifact

A machine-readable, versioned sidecar carrying the complete end-of-run state — both the
`list_run_summary` fields **and** a `meters[]` array generated by iterating the `MeterRegistry`
(so it self-syncs with §1's meter set, including the `swath.process.*` resource meters).

**When it's written:** by default, every non-stdout Parquet destination gets a
sidecar at `<output>/_swath_summary.json` (leading `_` keeps it out of a bare
`*.parquet` glob). This includes FILE-kind `-o capture.parquet`, whose current
physical representation is a one-writer dataset directory at that path. A
stdout or FILE-kind text run writes nothing unless `--report <path>` is given
explicitly — **stdout stays data**, the JSON never goes there.
It is written **atomically** (serialize to `<path>.tmp`, then rename over `<path>`) every
`--tune summary.interval` (default: `--progress-interval`) with `completed: false, exit_code:
null`, plus a final write at `close()` time (run end, including a killed/aborted run) — a consumer
always finds a file and knows from `completed` whether it is done. `exit_code` is `0` on a clean
`completed: true` finish and `null` on most `completed: false` records — the writer runs inside
the process and, on a generic partial, has no way to learn the CLI's actual process exit code, so
it asserts "unknown" rather than guessing. The exceptions are the **fatal early exits**, which
report the **real resolved code** rather than a fixed one: *every* seed failure threads whatever
the terminal exception resolves to (swath walks the cause chain to the throwable's own code — `1`
for the listing failures this path sees in practice, not `1` by definition), and a fatal
sort-segment corruption reports the `1` it maps to.
`error_class` is a separate axis: it is filled in only when the failure classified itself
(`access_denied`, `unauthorized`, `no_such_bucket`, `region_redirect` on a seed;
`page_run_min_regression` on a corrupt sort segment), so a generic seed failure still carries a
real `exit_code` with `error_class: null`. `oversized_page` (a page response that exceeds the
requested `max_keys` bound) can fire on any page fetch or steal probe, not only the seed — but the
sidecar only surfaces it as `error_class` when the refusal happens during the seed probe's own
catch; the identical refusal later in the run still lands as an unattributed crash with
`error_class: null`. Between the two a supervisor can tell a
permissions/config refusal from an unattributed crash out of the sidecar alone. Read `exit_code` as "the true code
when swath knows it, `null` when it doesn't", not as "always null unless complete".

**The final write is NOT simply "the last periodic flush left in place".** `close()` always
re-reads a FRESH snapshot at close time, then retries the full write (including `meters[]`) up to
a bounded number of attempts; if every full attempt fails, it degrades to a **`meters[]`-dropped**
write that still carries every terminal fact a reader needs (`completed`, `stop_reason`,
`stop_source`, `error_class`, `as_of`, `objects`/`duration_ms`, the recovered-error rollup, …); if
even that fails, the run still exits cleanly — a sidecar failure never crashes the run — but logs a
grep-able `summary_json_final_write_failed` marker on stderr. So **`meters[]` is
NOT guaranteed present on every write** — a degraded terminal write omits it — and `as_of` (stamped
on every write, periodic or terminal) plus the `summary_json_final_write_recovered` /
`summary_json_final_write_degraded` / `summary_json_final_write_failed` /
`summary_json_final_snapshot_failed` log markers are the freshness/failure signals a consumer or
downstream parser should key off, not "does `meters[]` exist".

```json
{
  "schema_version": 2,
  "run_id": 42,
  "args_hash": "…",
  "argv": ["list", "s3://my-bucket", "-o", "out/", "--no-sign-request"],
  "strategy": "work_stealing",
  "completed": true,
  "exit_code": 0,
  "stop_reason": "completed",
  "stop_source": null,
  "error_class": null,
  "started_at": "2026-07-01T00:00:00Z",
  "as_of": "2026-07-01T00:00:25Z",
  "duration_ms": 25731,
  "session_duration_ms": 25731,
  "objects": 109858,
  "config": {
    "target": "s3://my-bucket", "region": "us-east-2", "format": "parquet",
    "max_parallel_listings": 64, "no_sign_request": true, "rate_limit_api": null,
    "progress_interval_ms": 30000, "filters": []
  },
  "engine_flags": { "owner_split": true, "density_ewma": true, "radix_bands": true,
    "structure_probes": true, "far_ahead": true, "alphabet_pivots": true, "reflect": true,
    "confetti_feedback": true, "reflect_lift": true, "fanout_tiling": true,
    "mass_aware_seed": true, "readahead": false, "max_duration_ms": null },
  "output": { "format": "parquet", "files": 4, "compressed_size_bytes": 1234567 },
  "cost": { "api_calls": 118, "cost_usd": 0.00059,
    "basis": { "rate_per_1k_usd": 0.005, "source": "aws-list-reference-rate" } },
  "efficiency": {
    "keys_per_sec": 4269.5, "api_calls_per_1k_objects": 1.07,
    "peak_rss_bytes": 268435456, "peak_heap_bytes": 134217728,
    "cpu_seconds": 26.4, "cpu_efficiency": 1.03,
    "overfetch_ratio": 1.02, "page_fill_ratio": 0.94,
    "empty_split_ratio": 0.03, "wasted_probe_ratio": 0.11, "steal_success_rate": 0.62,
    "compression_ratio": 1.9
  },
  "engine": { "pages": 112, "peak_in_flight": 61, "avg_in_flight": 16.4,
    "time_to_first_steal_ms": 180, "time_to_peak_in_flight_ms": 4200,
    "steals": 232602, "splits": 98, "errors": 0 },
  "seed": { "mode": "shallow", "probes": 3, "cut_points": 41, "synthesized_cuts": 0, "ranges": 42,
    "decisions": [
      { "prefix": "", "fanout": 3, "truncated": false, "classification": "delimiter_seeded",
        "cuts_kept": 3, "cuts_discarded": 0 }
    ] },
  "trajectory": {
    "in_flight": [4.0, 22.5, 58.0, 60.9, 12.0],
    "progress_rate": [820.0, 5100.0, 6300.0, 6100.0, 900.0],
    "serial_frac": 0.08, "collapse_at_frac": null, "peak_workers": 61, "final_workers": 3
  },
  "slow_ranges": [
    { "lo": "logs/2024/12/31/", "hi": "<none>", "cursor": "logs/2024/12/31/f8a2…", "est_remaining": null,
      "drain_rate": 210.4,
      "steal_reasons": { "cursor_passed_pivot": 4, "no_pivot": 0, "structure_suppressed": 2, "demand_gated": 0 } }
  ],
  "probe_latency": [
    { "call_class": "worker_page", "phase": "total", "count": 112, "p50_ms": 38.2, "p90_ms": 61.0, "p99_ms": 205.4, "max_ms": 412.0 },
    { "call_class": "pivot_probe", "phase": "connect_acquire", "count": 44, "p50_ms": 0.4, "p90_ms": 1.1, "p99_ms": 3.9, "max_ms": 9.8 },
    { "call_class": "pivot_probe", "phase": "ttfb", "count": 44, "p50_ms": 9800.1, "p90_ms": 10000.0, "p99_ms": 10000.0, "max_ms": 10000.0 }
  ],
  "demand_gate": { "events": 214, "last_t": 8, "min_t": 4, "t_max": 64 },
  "shape": {
    "partial": false,
    "alphabet_cardinality": [16, 16, 15, 12, 0, 0, 0, 0],
    "alphabet_positions_observed": 4,
    "alphabet_cardinality_note": "run-level aggregate over per-range positions past divergence; entropy not computed",
    "divergence_depth_histogram": [0, 4, 61, 22, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "mass_skew_gini": 0.71,
    "delimiter_fanout": { "max": 37, "total": 210, "probes": 12 },
    "regime": {
      "api_latency_p50_ms": 41.2, "api_latency_p99_ms": 210.7,
      "worker_count": 64, "region": "us-east-2",
      "throttle_includes_attempt_timeout": false
    },
    "fingerprint": {
      "binary_sha256": "…", "git_sha": "…",
      "started_at": "2026-07-01T00:00:00Z", "finished_at": "2026-07-01T00:00:25Z"
    }
  },
  "meters": [
    { "name": "swath.api.calls", "type": "counter", "tags": {"strategy":"WORK_STEALING"}, "value": 118 },
    { "name": "swath.api.latency", "type": "timer", "tags": {"op":"listObjectsV2"},
      "count": 118, "total_ms": 8123.4, "max_ms": 412.0 }
  ]
}
```

**`sort` block, `merge_progress_units`:** for a `--sort` run, the `sort` block
(`{ "enabled": true, "segments": …, "passes": …, "segment_bytes": …, "merge_ms": …,
"page_runs_per_buffer": …, "buffer_sort_fallbacks": …, "effective_fan_in": …,
"merge_progress_units": … }`) additionally carries `merge_progress_units`, a live read of
`swath.progress.units`. That tally is **phase-agnostic, not merge-only**: the same counter advances
during listing and writing, so on a `--sort` run `merge_progress_units`' value at merge start already
includes every object emitted during listing. It is a monotone liveness signal, not a merge-only
processed-row count starting at `0` — track that it keeps **advancing** once the merge begins, never
its absolute magnitude. `segments` and `passes` are populated exactly ONCE, after the whole merge
finishes, so both stay flat/zero for the entire merge; `merge_progress_units` is the field that
genuinely advances across successive periodic flushes mid-merge, which is what lets an external
`_swath_summary.json` poller distinguish alive-and-merging from wedged.

**Merge-phase progress log line:** the merge/publish phase is covered by the run's single progress
reporter, which spans seeding, listing, merging and writing alike — so it emits the SAME `progress`
record as every other phase, with a merge-shaped tail:
```
progress run_id=<n> phase=merging … rows_merged=<n> staged_rows=<n> segments=<n> merge_passes=<n>
```
at the `--progress-interval` cadence (default 30 s), so a short merge under a tight external
watchdog still gets periodic ticks. `rows_merged` — rows of merge work done so far — is the field
that advances on every tick; `merge_passes` reads 0 for the whole merge (the pass counter is bumped
exactly once, at merge end, mirroring the `sort` block's `passes` field). A `completed=…` percentage
appears only once the phase reaches `writing`: a cascading merge rewrites every staged row once per
pass and does not know its pass count in advance, so `rows_merged` legitimately runs past
`staged_rows` mid-cascade, and only the final pass — which drains exactly the staged rows once — has
an honest denominator. There is no extra final line at merge end:
one tick renders one record, and the terminal disposition is the run summary's job.

**`stop_source`/`error_class`** (top-level, alongside `stop_reason`): post-hoc forensics
fields for a `stop_reason: "stuck"` terminal — `stop_source` names WHICH mechanism attributed the
cooperative cancel (`liveness_watchdog`, `transient_retry_cap`, `seed_interrupt`, `timebox`), and
`error_class` is the source-routed proximate-cause classification
(`stuck_api_timeouts` / `stuck_throttle` / `stuck_unknown`) — a `liveness_watchdog` stop reads the
run-wide since-last-real-progress window, while a `transient_retry_cap` stop reads the ONE
cap-tripping fetch's own local fault history (an unrelated healthy worker's progress would
otherwise erase the wedged fetch's history from a run-wide window). Both are `null` for every
non-`stuck` terminal (including a clean `completed` run, as above), and both are derived by the same
code path the `list_stuck_stop` stderr marker uses, so the two observability surfaces can never
disagree. Present (as JSON `null` when not applicable) on every
write, including a degraded meters-dropped terminal write — they are terminal FACTS, not meters.

**`error_class` on a `stop_reason: "crash"` terminal.** `error_class` is NOT stuck-only: a
run that unwinds on a **classified fatal** in-process failure names it here too. Today the one
such class is `page_run_min_regression` — a staged page-run segment whose page `minKey`s regress
(read-time format corruption). So a corrupt-segment abort is greppable in `_swath_summary.json`:
`stop_reason: "crash"`, `error_class: "page_run_min_regression"`, `exit_code: 1` — and its
engagement counter rides along in the same file's `meters[]`, since the terminal write serializes
the registry. An UNclassified crash keeps `error_class: null`.

**`error_class=sort_disk_exhausted` is a SEPARATE, stderr-only classification — not
a value of this JSON field.** The `--sort` disk-headroom guard's two refusal paths (the periodic
in-run halt and the startup pre-check) each log a terminal marker carrying
`error_class=sort_disk_exhausted stop_reason=sort_disk_exhausted resumable=true`, but neither path
ever reaches a JSON run-summary write (the halt bypasses the finalizer; the pre-check refuses
before a run begins), so this classification never appears in `error_class` above — grep for it on
stderr instead. It is stderr-only for a MECHANICAL reason (no summary
write happens at all), not because the JSON field is closed to sort-side failures: the
`page_run_min_regression` path above does unwind through a real terminal write, so it is carried in
the JSON.

`config` echoes the effective/resolved flags (self-describing for reproducibility) alongside
`args_hash` (the two agree). Unavailable resource values (`peak_rss_bytes`, `peak_heap_bytes`,
`cpu_seconds`, `cpu_efficiency`) render as JSON `null` — not the `-1` log-line sentinel — as does a
`NaN` gauge in `meters[]`.

`argv` (top-level array) and `engine_flags` (all 12 `--engine-toggle` names — `owner_split`,
`density_ewma`, `radix_bands`, `structure_probes`, `far_ahead`, `alphabet_pivots`, `reflect`,
`confetti_feedback`, `reflect_lift`, `fanout_tiling`, `mass_aware_seed`, `readahead` — plus
`max_duration_ms`) round out
identifiability: `args_hash` is deliberately the narrow
resume-safety fingerprint (bucket/prefix/recursive/all_versions/strategy/hints/inventory — it
excludes progress/diagnostic-only flags like `--max-duration` and ablation toggles like
`--engine-toggle owner_split=off`), so a run wouldn't otherwise be distinguishable from these fields alone.
`argv` is the raw invocation (picocli's parsed original args) — ground truth for corpus
reproducibility; `engine_flags` carries the genuine engine on/off toggles.

**`efficiency.*` derived ratios:** computed once at end-of-run from counters already
maintained elsewhere (no hot-path cost); a zero denominator renders `0.0`, not `null` (these are
always computable, just possibly vacuous on a tiny/degenerate run — unlike the resource fields
above, which are genuinely platform-unavailable).

| field | numerator / denominator | how to read it |
|---|---|---|
| `overfetch_ratio` | `swath.page.raw_keys` / this session's emitted objects (`swath.entries.emitted` less a resume's recovered rows, which this process never fetched) | **the classifier's key number.** Keys actually fetched from the store per key emitted downstream; `1.0` is perfectly efficient (every fetched key was kept and emitted), higher means wasted fetch volume — over-splitting, probe/steal churn, or heavy filtering. |
| `page_fill_ratio` | mean keys per fetched page / configured `max-keys` | how full pages come back; near `1.0` is healthy, low means many short/truncated pages. |
| `empty_split_ratio` | `UNSPLITTABLE` steal outcomes / total steal attempts | the fraction of steals that hit a victim with no valid pivot. |
| `wasted_probe_ratio` | `swath.probe.empty_upper_bisections` / (`swath.probe.fetches` + `swath.probe.structure_fetches`) | the fraction of probe I/O (1-key steal probes and `delimiter=/` structure probes) that found an empty upper half — i.e. produced no usable split on that probe. |
| `steal_success_rate` | `CHILD_CREATED` steal outcomes / total steal attempts | the fraction of steal attempts that actually committed a split. |
| `compression_ratio` | `swath.bytes.estimated` (raw, S3-reported) / `compressed_size_bytes` | raw estimated bytes per actually-written output byte, higher is better compression. `0.0` only if `compressed_size_bytes` is `0` (a genuinely empty output). `compressed_size_bytes` is a real byte count for text sinks too, not just Parquet, so this ratio is meaningful for JSONL/TSV/TABLE output as well. |

**Post-hoc forensics fields.** The JSON summary also carries deeper, contributor-tier forensics
blocks — `seed` / `seed.decisions[]`, `trajectory`, `slow_ranges[]`, `probe_latency[]`,
`demand_gate`, and the `shape` feature-vector (all shown in the example above). Their field-by-field
reference is in [`docs/internals/metrics-internals.md`](internals/metrics-internals.md) §3.

---

## 4. `-v` progress record (30 s default; `--progress-interval`)

ONE reporter covers the whole run — seeding, listing, merging, writing — and emits one `progress`
record per tick. This is the log form of the tick, the surface an external supervisor tailing the
log reads; it is what ticks whenever the operator display is not installed (§the gate in
[`usage.md`](usage.md#progress) — a non-terminal stderr, or `-v`, which turns this record on in the
first place). `--no-progress` suppresses progress outright: no display AND no record. Every record
carries `run_id, phase, strategy, elapsed_ms, phase_elapsed_ms, api_calls, retries`, plus `cost_usd`
where the provider's LIST pricing is knowable — under `--endpoint-url` the field is absent rather
than quoting an AWS-derived guess, exactly as the end-of-run block withholds the `$`. `elapsed_ms`
is always the whole session (a phase transition never resets the clock) and `phase_elapsed_ms` sits
alongside it. The tail is phase-shaped:

- **`phase=seeding`** — `probes`, `probe_budget`, `last_probe_age_ms`. A seeding run emits no
  objects, fetches no pages and holds no workers, so a listing-shaped line would read as all zeros
  whether it was healthy or hung; the probe count and the age of the last completed probe are what
  tell those apart.
- **`phase=listing`** — `objects` (this session's own work), `recovered_objects` (rows a resume
  carried over, counted separately so a resumed run neither displays a zero it did not earn nor
  jumps by the whole pre-crash total when the backfill lands), `live_rate` (**windowed** objects/sec
  since the previous tick, so a stall shows immediately) vs `avg_rate` (cumulative), `pages`,
  `in_flight` vs `target_workers` (how much of the AIMD concurrency `T` is actually busy; the
  Micrometer gauge for the target is `swath.workers.active`), `steals`, `splits`.
- **`phase=merging`/`writing`** — `rows_merged`, `staged_rows`, `segments`, `merge_passes`.
- **`phase=starting`** — the run has set no phase yet (checkpoint open, resume reconciliation): a
  run-level line only, and an honest one rather than a fabricated `listing`.

A `completed=<done>/<total> <unit> percent=<n>` field appears only where BOTH sides have exact,
documented semantics: seeding against its bounded probe budget, and the merge's FINAL pass
(`phase=writing`) against the staged row total. Listing has neither — no total object count exists mid-scan — so it carries no percentage,
no bar and no ETA, rather than a placeholder pretending otherwise. Live resident/heap memory is not
sampled per tick (it was, at a full `/proc` + JVM-bean probe per record); peak RSS and heap are in
the end-of-run summary, and the `swath.process.memory.*{kind=current}` gauges from §1 remain the
live surface for it.

### 4a. Start-of-run and first-request/first-page markers

Silence is the worst diagnostic: a run that hangs before making any progress (e.g. the seed probe
itself wedges) would otherwise produce **zero** log lines — indistinguishable from "not started yet"
or "logging broken." Three markers close that:

- **`list_command_start`** (once per run): `bucket`, `prefix`, `args_hash`, `checkpoint_path`,
  `strategy`, `max_parallel_listings` (`W`), and `engine_toggles_default` (`true` unless at least one
  `--engine-toggle` is non-default — the full per-toggle breakdown still logs separately, right
  after, as `engine_toggles_effective ...`, and stays silent on an all-default run by design).
- **`list_first_request_issued`** / **`list_first_page_returned`** (each with `elapsed_ms` since the
  checkpoint dispatch began): these fire around the very FIRST page fetch the run makes — the seed
  probe or the engine, whichever goes first. A run wedged on its very first LIST/region-resolve logs
  `list_first_request_issued` immediately and never logs `list_first_page_returned` — an unambiguous
  "stuck on request #1" signal, distinct from a run that never started at all.
- **Seed-phase progress:** the §4 progress tick fires unconditionally at `--progress-interval`,
  whether or not anything changed, and the reporter is started around the WHOLE run — the seed-probe
  window included — so a hung seed gets a `progress … phase=seeding probes=0 …` record at the
  configured cadence instead of nothing, with the first one a couple of seconds in rather than a
  whole cadence later. On the common fast-seed path it never ticks at all (seeding finishes in
  milliseconds); it only fires when seeding is genuinely slow or wedged.

---

## 5. Instrumentation discipline & the steal-reason counter registry

Why swath emits so much, the one-taxonomy engagement-counter discipline, and the CI-enforced
steal-reason counter registry are contributor-tier — they live in
[`docs/internals/metrics-internals.md`](internals/metrics-internals.md) §5 / §5a.

---

## 6. Exit codes

`0` success (empty result / already-complete resume / broken-pipe included), `1`
listing/output/checkpoint error, `2` bad input/config/flags (and guarded refusals), `75` retryable
stuck state (`EX_TEMPFAIL`), `124` stopped by `--max-duration`, `130` SIGINT,
`143` SIGTERM. A partial is resumable only when the run used a managed
directory-dataset checkpoint; stdout and `--checkpoint none` runs have no
durable state to resume.

---

## 7. Run trace (`--trace`)

**V1**, diagnostic tier, opt-in. `--trace <file>`
writes a JSONL "flight recorder": one event object per line describing how
the work-stealing engine carved and drained the keyspace over time — the picture the aggregate
summary JSON and the DEBUG log narrative can't give (no time axis, no per-range story).
`--checkpoint none` still uses this same work-stealing engine through an
ephemeral checkpoint store, so its ready-queue, thief, commit, and split events
are traced too; only durable resume is absent.

**Sensitivity (OSS note).** A trace is the worst case: it carries real key names —
`lo`/`cursor`/`hi`/`pivot` on nearly every event. But **the summary JSON is not key-free
either**. It records the raw `argv` and `config.target` (bucket, prefix, endpoint, and the
output path you wrote to), `config.filters` (your `--include`/`--exclude` regexes and size/
mtime bounds verbatim), the `seed.decisions[].prefix` cut points, and `slow_ranges[]`
`lo`/`hi`/`cursor` — real key material from the bucket. Treat both files with the same care as
the output listing itself: skim them before attaching either to a public bug report, and redact
the bucket/prefix, endpoint, filters, and key samples if the names themselves are sensitive.

The trace event schema, its zero-cost seam, the durability/versioning contract, and the two
visualization renderers are in [`docs/internals/metrics-internals.md`](internals/metrics-internals.md) §7.

---

## 8. Replay-server meters

The `swath-replay-server` module's module-local `swath.replay.*` meters are a contributor-tier
reference in [`docs/internals/metrics-internals.md`](internals/metrics-internals.md) §8 (usage and
tuning are in [`docs/swath-replay-server.md`](swath-replay-server.md)).
