# Watch and tune a run

What a running listing shows on the terminal, in its progress file and in its report, the signals that mean
trouble, and the flags to change when they do. How much disk and memory a run needs is in
[Size a run](size-a-run.md).

## Watching a run

**On a terminal.** swath draws one progress line on stderr, redrawn every second: phase, objects (with rows a
resume recovered), their bytes so far, elapsed time, live and average keys/s (while every request fails, the
cause retried instead), parts, then requests in flight, API calls and a cost estimate when a tariff is known. Off a terminal it is off unless asked: `--progress` appends the same record as a plain line every 30 s
(`--progress-interval` sets it, at least 1 s); a log never receives control sequences. `-q` turns the
automatic progress and the warnings off, `--no-progress` turns progress off everywhere. While `swath list`'s
rows go to the same terminal there is no line, and `--progress` is refused; `swath du` draws it while it lists
and erases it before its table. At the end, runs over
1.5 s, runs that wrote output and runs that did not complete print their closing lines on stderr: the verdict
(`complete`, or with its cause `partial: 1 unreadable entry`, `stopped: --max-duration reached`,
`interrupted`), the objects and bytes listed, the time and (from 2 s) the rate, and the output written, then
`next:` with `swath resume DIR` for a resumable stop, or `swath show` and `swath du --from-dataset` for a
committed dataset ([closing lines](commands/swath.md#stdout-and-stderr)). `--stats` prints them always and adds
the statistics (API calls, mean and peak in flight, throttles and retries when any, the estimate, peak RSS),
also for a supervisor's log; `--no-stats` drops them. Warnings come before the lines.
stdout carries only rows, totals or `--report -`. Color: `--color auto|always|never`, auto honoring `NO_COLOR`.

`DIR/.swath/progress.json` is rewritten every 2 s (atomic rename) while the run holds the output: the live
counts, the request account (`http_attempts`, `retries`, `inflight`, the limiter's `limit`, `cost_usd`), the order
stage's resources and the run's `state`. The report written at the end keeps the curve (`diagnostics.history`, up to 300 samples), the phase times
(`diagnostics.seed_ms`, `listing_ms`, `drain_ms`, `publication_ms`), the request account (`requests`), the
resources and the provider's counters (`diagnostics.source.stats.counters.values`: attempt latency percentiles, the
limiter's steps, the split counters). Both schemas are in
[Run report and progress file](reference/run-report.md); the counters in [Counters (internals)](internals/counters.md). The signals to read:

**The drain after listing.** On a sorted run the provider can finish long before the last row is handed on:
parallel listing finishes ranges in roughly key-uniform order, so most rows wait ahead of the frontier (spilled)
until the leftmost range ends, and are then replayed in order. `timings.provider_finished_at` is when the provider
finished; `listing_finished_at` when the order stage ended; `diagnostics.resources.drain.tail_ns` the interval between, the
part of the run in which only the drain was left. In it idle workers read and decode spilled frames ahead of
the drainer (`diagnostics.resources.drain.read_ahead`) and more encoders are admitted ([encoders](size-a-run.md#resources-and-their-flags)). What the drainer did:
`replay_read_ns` and `ahead_wait_ns` (the spill), `order_check_ns`, `sink_write_ns` (the encoders, with
`encoder_wait_ns`), over the whole run, not the tail alone; and in the tail, the workers'
`worker_idle_after_provider_finished_ns`. A tail that is most of the run with `sink_write_ns` most of the drainer's
time is encoder-bound; with `replay_read_ns` or `ahead_wait_ns`, spill-bound.

| Signal | Healthy | Act when |
|---|---|---|
| `part_age_ms` | under a minute | several minutes: no part has been published for that long; a crash would list again at least what was listed since |
| `order.spill_bytes` | the spill held now: on an unseeded sorted run of a large list it grows toward most of the rows listed, since such a run spills nearly every row once (a median 96 % of rows on lists of 20M keys or more, [measurements](internals/measurements.md#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)); seeded from a counted prior, it stays near the frontier window's rows (`diagnostics.resources.window`) | near `diagnostics.resources.spill.max_bytes` or the filesystem's free space: the next frames are refused and listing slows to the drain |
| `order.nodes`, `order.markers` | tens to hundreds | tens of thousands and rising: a deep speculative tree behind a slow frontier |
| `order.ahead_rows`, `diagnostics.resources.peak_ahead_rows` (report) | anywhere up to the rows listed: ranges finish in roughly key-uniform order, so most rows of a large sorted run wait ahead of the frontier until listing nearly ends (a median 71 % at peak on lists of 20M keys or more). The set is bounded by `--spill-budget`, not by `--sort-buffer`; a count holds none | not by itself; read it with `diagnostics.resources.spill.refused`, `budget_wait_ns` and `max_frontier_stuck_ns` |
| `diagnostics.resources.spill.refused` (report) | 0 | above 0: the spill cap or the disk's reserve refused frames and emitters waited; with a large `budget_wait_ns` the listing ran at the drain's rate (with plain spill frames, before the delta coding, a cap of 2 × the row budget left 56 of 64 large lists unfinished after 900 s) |
| `diagnostics.resources.max_frontier_stuck_ns` (report) | a few attempt round trips: a median 0.28 s and at most 1 s on measured sorted runs of 20M keys or more at 50 ms pages, spill-capped ones included | seconds to minutes: the range at the frontier stopped handing rows on (a slow, retried or hung request); an interrupted run reports its last stall too. A spill-capped run that crawls shows in `budget_wait_ns`, not here |
| `diagnostics.resources.peak_heap_live_bytes` against `invocation.sort_buffer_bytes` (report) | the budget plus the encoders' allowance and the pages in flight | far beyond: memory the budget does not see |
| `diagnostics.resources.spill.fs_type` (report) | a disk filesystem | `tmpfs`: spill is memory; write the output (for stdout, point `TMPDIR`) to a disk |
| `output.text.sink_wait_ns` against `timings.listing_ms` (report) | a fraction | near it: the order stage spends the run handing rows on (one formatter: formatting them; more: their queue full); rows wait behind it, spill grows and workers idle. More `--encoders`, if the budget carries them; `output_wait_ns` near the listing time instead: the compressor or the disk is the limit |
| `order.spill_pressure_since` | absent | present: spill has been refused, with workers waiting on it (in publish or at admission), since then: the spill cap is full (with the default cap, the disk: grow it and listing continues), or free space is down to the reserve; at 2 minutes the run says so on stderr (`spill_pressure`), and `--spill-wait-timeout` stops it ([three bad patterns](#three-bad-patterns)) |
| `rows` against `encoded`, both per second | similar | listed far ahead of encoded: work fetched that cannot be published yet |
| `advance` | rising | flat while `answered` rises: alive but not progressing; `empty_chain_pages` rising: the service answers only empty pages, and the watchdog stops the run (75, `stuck_empty_chain`) after `--stall-timeout` |
| `throttles` | flat | rising steadily: see [Throttling](#throttling) |
| `inflight` against `limit` | `limit` near `--workers`, `inflight` near `limit` | `limit` falling: the service throttles; `inflight` far below `limit`: the engine has too little work in parallel |
| `diagnostics.resources.budget_wait_ns` (report) | near 0 with spill on | large: workers blocked on a full sort buffer |
| `mean_active` against mean in flight (report) | both near `--workers` | active high, in flight low: requests held by the limiter or admission |
| `inflight.p99_ns` against `inflight.p50_ns` (report) | a few times p50 | p99 tens of times p50: a tail of slow attempts (timeouts, a throttled route) |
| `inflight.latency_overflow` (report) | 0 | above 0: attempts of 18 min 20 s or longer; a percentile among them reads 9223372036854775807, open-ended, not a measured bound |
| `idle_demand_ns` against `busy_ns` (report) | small | comparable: workers sat idle with work wanted; the split policy could not hand out work (see the split counters) |
| exit 75 on the same output | never twice | a second 75 in a row needs a person |

## A healthy run report

Five fields to check in the `--report` file of a sorted listing of a million keys or more:

| Field | Normal |
|---|---|
| `outcome.class` | `ok` (exit 0), with `outcome.complete` true; for the whole source, also no filters in the dataset's coverage ([check](results.md#coverage-and-unreadable-entries)) |
| `diagnostics.efficiency.http_attempts_per_1k_rows` | 1.0 to 1.4. On lists under a million keys, up to about 2 (the seed and first splits are a large share of a few hundred requests) |
| `diagnostics.resources.spill.refused` | 0 |
| `diagnostics.resources.budget_wait_ns` | 0 or close to it |
| `diagnostics.resources.max_frontier_stuck_ns` | a few request round trips: a median 0.28 s and at most 1 s on sorted runs of the 64 lists of 20M keys or more through a 50 ms fake (candidate 4, run `c4big`: its resumed and spill-capped arms) |

Spill is normal. A sorted run of a large bucket spills almost every row once (a median 96 % of rows on the 64
lists of 20M keys or more: candidate 4, run `c4big`, plain frames), because ranges finish in roughly
key-uniform order and the output waits for the leftmost one.

## Three bad patterns

**Spill refused, workers waiting.** `diagnostics.resources.spill.refused` is above 0 and `diagnostics.resources.budget_wait_ns` is
large. `diagnostics.resources.free_disk_min_bytes` is close to `diagnostics.resources.spill.reserve_bytes`, or the spill reached
`diagnostics.resources.spill.max_bytes`. Listing then runs at the output's pace, and on a very large bucket that pace
can be a small fraction of the listing's: three listings of 8 to 30 billion objects on a 100 GiB disk at 2,048
workers fell from about 7,000 requests a second to about 10 once the spill cap filled, and were stopped after
four hours ([measurements](internals/measurements.md#spill-at-billion-scale-and-a-spill-cap-too-small-2026-11-13)).
`diagnostics.resources.spill.refused_ns` is how long spill stood refused; `publish_wait_ns`, `admission_wait_ns` and
`idle_wait_ns`, with their peaks, say where the workers waited; and the range engine's `awaited.*` counters say
why the range the output waited for was or was not split
([counters](internals/counters.md#order-stage-and-encoders-run-summary-top-level)).
The run does not fail by itself. Once spill has stood refused with workers waiting on it for 2 minutes, swath says so
on stderr, an error, and again every 30 minutes while it lasts: how long listing has waited, spill and its
cap, the output's rate, and the remedy; `progress.json` has `order.spill_pressure_since` meanwhile. Left
alone, the run finishes at the output's pace. On an unattended run, `--spill-wait-timeout 30m` stops it
instead once the wait lasts that long: exit 74, `cause` `spill_budget`, resumable. Give it disk. With the default
cap, grow the filesystem under the running process (a cloud persistent disk resized, then its filesystem grown
online): within seconds the run rereads the free space, the cap rises and listing continues;
`diagnostics.resources.spill.cap_raises` counts the rises, `max_bytes` is the cap reached and `start_max_bytes` the one the
run started with. A `--spill-budget` is fixed: stop the run and resume it with a larger one on a disk that has
the room, `swath resume DIR --spill-budget 200GiB`; a plain `swath resume DIR` keeps the recorded cap, and with
the default derives it from the free space it finds (the certified spill the stopped run kept counted as
free). A repeat listing avoids most of this by seeding from the previous dataset ([warm start](resume-and-refresh.md#warm-start-for-repeat-listings)).

**A stuck frontier.** `diagnostics.resources.max_frontier_stuck_ns` is seconds to minutes. In `progress.json`, `rows_fetched`
rises while `rows_encoded` and `parts_written` stay flat and `part_age_ms` grows. One range at the front of the output is
slow: a slow, retried or hung request. Check `inflight.p99_ns` against `inflight.p50_ns` and the retries in
the report's `diagnostics.source.stats.counters.values`. After `--stall-timeout` (10 min) of no progress the run stops
with exit 75 and writes every goroutine's stack to `DIR/.swath/stacks-stuck.txt`. Resume it, and keep that file
if it happens again.

**Throttling.** `requests.throttles` rises, the counter `limiter.decreases` is above 0, and `progress.json`'s
`limit` falls below `--workers`. Throttle answers cause no cut while they are under 15 % of the limiter's
window, the last answers since its last cut (four per worker, at least 256 and at most 4,096); the counter
`limiter.cuts_suppressed.tolerated` counts those answers. For a quota shared with other clients, set
`--request-rate N` below it. If a run crawls with the limit near 1 under sustained throttling, use `--no-adaptive-limit --request-rate N` ([Throttling](#throttling)).

## Throttling

By default, S3, GCS and Azure Blob share an adaptive request limit that starts at `--workers`,
reduces concurrency under sustained throttling and recovers as answers improve.
[Request admission](internals/clients.md#request-admission) owns its window, growth rules,
timeout and latency brakes, and counters.

- `--no-adaptive-limit` removes that limit. Seed listings and structure probes can run beside the
  workers, so more than `--workers` requests can be in flight.
- `swath diag list --adaptive-limit-slow-start` enables an experimental ramp from a small initial limit;
  it is off by default.
- `--request-rate N` adds an aggregate rate cap for a quota shared across a fleet. Reservations
  are spaced; scheduling delays can bunch actual sends.
- Exhausted engine listing calls requeue while the run lives; admission failures are described
  under [object-store requests](sources/README.md#object-store-requests). A stall watchdog or deadline can stop
  a run that keeps failing. An endpoint that has never answered, every connection refused or its host not
  found, with nothing in the run answered, stops it as stuck after 30 s instead (exit 75, cause `unreachable`;
  [troubleshooting](troubleshooting.md#the-endpoint-cannot-be-reached)).

Measured on the synthetic S3 fake ([measurements](internals/measurements.md#red-team-fixes-2026-11-07)): 1 % and 5 % random
throttle answers do not move the limit, and a 300 requests/s cap holds it at the cap. Storms and sustained
throttling are not measured under this rule.
If a run crawls with `throttles` rising and the report's
`limiter.decreases` high (`limiter.peak_depth` near `--workers`: the limit fell to about 1), use
`--no-adaptive-limit` with `--request-rate` for a fixed-rate alternative. These measurements do not establish
fit for live provider throttling; [request admission](internals/clients.md#request-admission) describes the tuning limits.

## Three flags on a small runner

On a 1-vCPU machine with 1 to 2 GB of memory the defaults are the place to start. Change these three only
when the report says so.

- **`--workers`** (default 64 for an object store; for a local path 4 per CPU, so 4 here). Listing waits on request round trips, not the CPU. On one core the sorted
  output (ordering, spill and one Parquet encoder) is the limit, so more workers do not help there. Raise it
  (128, 256) only on a machine with spare cores and a service that does not throttle. Lower it if the service
  throttles at 64.
- **`--sort-buffer`** (default 1/16 of memory, at most 1 GiB). The bytes of rows held in memory between listing
  and encoding. Memory is the least of physical memory, the cgroup's limit and `GOMEMLIMIT`, and 128 MiB stands
  in when none is found: a 2 GiB container gets 128 MiB, an 8 GiB host 512 MiB, 16 GiB or more 1 GiB
  (`diagnostics.sort_buffer`: `reason`, `bound_by` and the limits found). Rows listed ahead of the output past it
  spill to disk: a listing whose waiting rows fit in the budget never spills. It is
  not a memory cap, and swath sets none: pages in flight, writers, the tail and the runtime come on top. The
  writers are admitted against their own allowance, the same 1/16 of memory whatever `--sort-buffer` says
  (`diagnostics.resources.drain.encoder_allowance_bytes`), so a smaller sort buffer does not mean fewer encoders. The measurements
  that follow were taken at a fixed 128 MiB budget (`--sort-buffer 128MiB`): sorted Parquet over the 251 production key lists through a 50 ms fake peaked at a median 0.46 GB and at most
  0.78 GB RSS (`head1`, `7b05605`) and 0.90 GB (`e2b`, `7fb447f8`, on ooni-data, 15.5M keys); six giants of
  267M to 523M keys peaked at 0.6 to 0.7 GB (`huge2`, `bf5e6d52`). Those runs were on machines of 16 to 32
  vCPU, and no run has been measured on a 1.8 GB machine; the 1 GiB default on such hosts has not been measured
  over those lists. If `process.peak_rss_bytes` comes near the machine's memory, lower the budget. If spill is
  heavy (`diagnostics.resources.spill.rows` near the rows listed) or `diagnostics.resources.budget_wait_ns` is large and memory is free,
  raise it: past what a listing holds ahead of its output, more buys only tail workers.
- **`--spill-budget`** and the disk. Rows that are listed ahead of the point the output has reached go to
  disposable spill files on the output's filesystem. By default spill may use that filesystem's free space
  after a reserve of at least 1 GiB: half of it when the parts are written there too, all of it for a
  dataset in a bucket. The default follows the disk: a frame it would refuse re-reads the free space, so a
  filesystem grown under the running process is used without a restart. Set `--spill-budget`, a fixed cap,
  when the filesystem is shared with other work. Size the disk as [disk sizing](size-a-run.md#disk-sizing) describes.
