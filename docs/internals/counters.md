# Counters

Every algorithm path counts its engagements. A provider declares its counters once, as a table of
`counters.Def{Key, Kind, Unit, Help, Feature}` indexed by a typed `counters.ID` and backed by one
`atomic.Int64` each (package `internal/counters`). That table is the one definition of every name, kind and
meaning: the provider reads it out as one immutable `counters.Snapshot` (code reads `Get(id)`), and the run
report renders the snapshot by iterating the table (`run.json` →
`diagnostics.source.stats.counters`: `features` and `values`, flat names), and the table below is
generated from the same definitions (`UPDATE=1 go test ./internal/source/s3 -run TestDocsCounters`). One
adapter sits beside it: the HTTP client's counters (`objhttp.Counters`, the limiter's and the rate cap's) are
kept in the client, which a provider may share or replace, and copied into the table when the provider
snapshots, each by its named ID (`objhttp.ClientCounter`, end sentinel `NumCounters`; `Counts.Store`), so
no position carries a meaning and `TestCounterTable` refuses an unfilled or doubly filled entry. A new
path costs one row and its increment. Kinds: `count` is a monotonic sum, `max` a high-water mark, `flag`
a boolean, `gauge` a value derived when the provider snapshots (the attempt latency percentiles). Names: a dotted group, a snake_case leaf, durations ending in `_ns`. An extreme says which kind it is: `peak_`
is a high-water mark over time, the most held at once (`peak_active`, `inflight.peak_active`); `max_` on a
configured value is a limit (`spill.max_bytes`, `drain.read_ahead.max_reads`); `max_` on a measured quantity is
its largest value over the items it measures, as SQL's `max()` (`split.max_depth` over ranges,
`mutex.max_hold_ns` over holds, `groups.max_rows` over groups). The `max` kind records either kind of extreme.

**Every name on this page is a diagnostic.** The counters, and every member of the run report's
`diagnostics` they sit in, follow the algorithm: a release renames, adds and removes them as the algorithm
changes, without a new `report_version` ([run report](../reference/run-report.md#diagnostics)). They are for a
person explaining a run, and for tests and benchmarks pinned to a build, not for a program that must keep working
across releases.

**Feature state is separate from engagement; off, absent and zero are different.** A counter that measures
a policy names it (`Feature`). `features` maps every such policy to whether this run had it on; the values of
an off policy are left out (`TestSummaryDistinguishesOffFromZero`):
- absent, no feature: this build or provider has no such counter;
- absent, feature `false`: the policy was off for this run (for S3: `seed` with `swath diag list --no-seed` or `--sequential`,
  `split` with `--sequential`, `structure` with `--structure=off` (`--sequential`
  and `--structure` are `swath diag list` flags; a library caller's zero `Options` has the seed and
  the probes off), `resume` on a fresh run, `max_keys`
  without `--max-keys`, `trace` without `--trace` ([trace.md](trace.md)), `limiter` with `--no-adaptive-limit`, a library caller's zero `Admission` or a supplied client, `slow_start` without
  `--adaptive-limit-slow-start` or without the limiter, `request_rate` without `--request-rate`, `end_check` for a
  versions listing, and always for GCS and Azure, whose requests carry an end bound);
- present and zero: the policy was on and never fired.

The report's `diagnostics.resources` follow the same rule: `features.spill` is false and the `spill` object absent with
`swath diag list --no-spill` or a count, `features.order` is false for `swath du` and `swath diag count` (rows counted, not
ordered), and `encoder_wait_ns` is absent without output. `spill.durable` (the durable spill's account: captures, seals,
abandons by reason, retirements, retained bytes, its limits with what they derive from (`slots`,
`history_cap`, `limit_bytes`, `limited_by`), and on a resume `resume` with what it accepted, rejected and
replayed; [ordering.md](ordering.md#durable-spill)) is absent when durable spill was off, and
`spill.durable_off` then says why when it could have run (a spill budget or free space below 16 MiB, a
provider without fences, a directory that could not be set up).

Request accounting uses `attempts` (HTTP requests, retries included) as the denominator; `requests` counts
logical calls.

Attempt latency is kept in the client as a fixed log-spaced histogram (four buckets per power of two of
nanoseconds, up to 2^40 ns; one atomic per bucket) and reported as three derived values, `inflight.p50_ns`,
`inflight.p90_ns` and `inflight.p99_ns`: each the upper edge of the bucket holding the percentile, at most 25 %
above it. Attempts of 2^40 ns (about 18 min 20 s) or longer land in one open-ended bucket past the 156 finite
ones, counted by `inflight.latency_overflow`; a percentile that falls there reads 9223372036854775807 (the
largest gauge value), never the last finite edge, which would read as a bound (`TestLatencySaturation`). The
buckets themselves are not counters: 157 rows would swamp the report for the four numbers a reader uses. The built client's [`objhttp.Meter`](clients.md#retry-policy-and-accounting) owns these readings.
Attempt timing starts after credential readiness and covers the SDK call, including Guard body
handling and any decoding the SDK performs, on success or failure. It excludes S3's subsequent
scan of the captured listing body and GCS's row mapping and validation after SDK decoding.
A delivered status 200 alone does not certify a valid page; see
[capture timing](clients.md#capture-timing). Admission feedback measures the attempt callback, including credential waits, and excludes
limiter/rate waits ([request admission](clients.md#request-admission)).

Derived readings of the range engine's counters: the mean leaf depth is `split.leaf_depth_sum` /
`split.leaves`; the work a frontier split hands out is `split.frontier_handout_keys` /
`split.accepted_frontier`; the `split.pivot.*` counters sum to the accepted splits, and the
`child_empty.by.*` counters (and separately `child_empty.hi.pivot` + `child_empty.hi.bound`) to
`child_empty.lower` + `child_empty.upper`. The `awaited.*` counters (sorted output only: the order stage's
emitter says whether the drain is at the range, `source.Awaiter`) take the split decision after each continuing
page of a range the output waits for; every one but `awaited.open_pages` sums to `awaited.pages`. They are read
for the report only: no decision reads whether a range is awaited.

## Reading a run report

How to explain a slow or unhealthy run from its `progress.json` and its report. The user page,
[Watch and tune a run](../watch-a-run.md), keeps the few values a user acts on; this section is the full reading.
`diagnostics.resources` holds the order stage's counters (the table below), the provider's counters are
`diagnostics.source.stats.counters.values`; `progress.json` carries the live values under `order`.

**The drain after listing.** On a sorted run the provider can finish long before the last row is handed on:
parallel listing finishes ranges in roughly key-uniform order, so most rows wait ahead of the frontier (spilled)
until the leftmost range ends, and are then replayed in order. `timings.provider_finished_at` is when the provider
finished; `listing_finished_at` when the order stage ended; `drain.tail_ns` the interval between, the part of the
run in which only the drain was left. In it idle workers read and decode spilled frames ahead of the drainer
(`drain.read_ahead`) and more encoders are admitted ([encoders](../size-a-run.md#encoders)). What
the drainer did: `replay_read_ns` and `ahead_wait_ns` (the spill), `order_check_ns`, `sink_write_ns` (the encoders,
with `encoder_wait_ns`), over the whole run, not the tail alone; and in the tail, the workers'
`worker_idle_after_provider_finished_ns`. A tail that is most of the run with `sink_write_ns` most of the drainer's
time is encoder-bound; with `replay_read_ns` or `ahead_wait_ns`, spill-bound.

| Signal | Healthy | Act when |
|---|---|---|
| `part_age_ms` | under a minute | several minutes: no part has been published for that long |
| `order.spill_bytes` | the spill held now: on an unseeded sorted run of a large list it grows toward most of the rows listed, since such a run spills nearly every row once (a median 96 % of rows on lists of 20M keys or more, [measurements](measurements.md#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)); seeded from a counted prior, it stays near the frontier window's rows (`window`) | near `spill.max_bytes` or the filesystem's free space: the next frames are refused and listing slows to the drain |
| `order.nodes`, `order.markers` | tens to hundreds | tens of thousands and rising: a deep speculative tree behind a slow frontier |
| `order.ahead_rows`, `peak_ahead_rows` (report) | anywhere up to the rows listed: ranges finish in roughly key-uniform order, so most rows of a large sorted run wait ahead of the frontier until listing nearly ends (a median 71 % at peak on lists of 20M keys or more, same measurements). The set is bounded by `--spill-budget`, not by `--sort-buffer`; a count holds none | not by itself; read it with `spill.refused`, `budget_wait_ns` and `max_frontier_stuck_ns` |
| `spill.refused` (report) | 0 | above 0: the spill cap or the disk's reserve refused frames and emitters waited; with a large `budget_wait_ns` the listing ran at the drain's rate (a cap of 2 × the row budget left 56 of 64 large lists unfinished after 900 s: [measurements](measurements.md#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)) |
| `max_frontier_stuck_ns` (report) | a few attempt round trips: a median 0.28 s and at most 1 s on measured sorted runs of 20M keys or more at 50 ms pages, spill-capped ones included (same measurements) | seconds to minutes: the range at the frontier stopped handing rows on (a slow, retried or hung request); an interrupted run reports its last stall too. A spill-capped run that crawls shows in `budget_wait_ns`, not here |
| `peak_heap_live_bytes` against `invocation.sort_buffer_bytes` (report) | the budget plus the encoders' allowance and the pages in flight | far beyond: memory the budget does not see |
| `spill.fs_type` (report) | a disk filesystem | `tmpfs`: spill is memory; write the output (for stdout, point `TMPDIR`) to a disk |
| `output.text.sink_wait_ns` against `timings.listing_ms` (report) | a fraction | near it: the order stage spends the run handing rows on (one formatter: formatting them; more: their queue full); rows wait behind it, spill grows and workers idle. More `--encoders`, if the budget carries them; `output_wait_ns` near the listing time instead: the compressor or the disk is the limit |
| `order.spill_pressure_since` | absent | present: spill has been refused, with workers waiting on it (in publish or at admission), since then: the spill cap is full (with the default cap, the disk: grow it and listing continues), or free space is down to the reserve; at 2 minutes the run says so on stderr (`spill_pressure`), and `--spill-wait-timeout` stops it |
| `rows_fetched` against `rows_encoded`, both per second | similar | listed far ahead of encoded: work fetched that cannot be published yet |
| `advance` | rising | flat while `answered` rises: alive but not progressing; `empty_chain_pages` rising: the service answers only empty pages, and the watchdog stops the run (75, `stuck_empty_chain`) after `--stall-timeout` |
| `throttles` | flat | rising steadily: the limiter's counters below |
| `inflight` against `limit` | `limit` near `--workers`, `inflight` near `limit` | `limit` falling: the service throttles; `inflight` far below `limit`: the engine has too little work in parallel |
| `budget_wait_ns` (report) | near 0 with spill on | large: workers blocked on a full sort buffer |
| `mean_active` against mean in flight (report) | both near `--workers` | active high, in flight low: requests held by the limiter or admission |
| `inflight.p99_ns` against `inflight.p50_ns` (report) | a few times p50 | p99 tens of times p50: a tail of slow attempts (timeouts, a throttled route) |
| `inflight.latency_overflow` (report) | 0 | above 0: attempts of 18 min 20 s or longer; a percentile among them reads 9223372036854775807, open-ended, not a measured bound |
| `idle_demand_ns` against `busy_ns` (report) | small | comparable: workers sat idle with work wanted; the split policy could not hand out work (see the split counters) |
| exit 75 on the same output | never twice | a second 75 in a row needs a person |

**A healthy report** of a sorted listing of a million keys or more:

| Field | Normal |
|---|---|
| `outcome.class` | `ok` (exit 0), with `outcome.complete` true; for the whole source, also no filters in the dataset's coverage ([check](../results.md#coverage-and-unreadable-entries)) |
| `diagnostics.efficiency.http_attempts_per_1k_rows` | 1.0 to 1.4. On lists under a million keys, up to about 2 (the seed and first splits are a large share of a few hundred requests) |
| `spill.refused` | 0 |
| `budget_wait_ns` | 0 or close to it |
| `max_frontier_stuck_ns` | a few request round trips (the measured median and bound in the table above) |

Spill is normal: a sorted run of a large bucket spills almost every row once, because ranges finish in roughly
key-uniform order and the output waits for the leftmost one.

**Spill refused, workers waiting.** `spill.refused` is above 0 and `budget_wait_ns` is large;
`free_disk_min_bytes` is close to `spill.reserve_bytes`, or the spill reached `spill.max_bytes`. Listing then
runs at the output's pace, and on a very large bucket that pace can be a small fraction of the listing's
([billion-scale spill](measurements.md#spill-at-billion-scale-and-a-spill-cap-too-small-2026-11-13)).
`spill.refused_ns` is how long spill stood refused; `publish_wait_ns`, `admission_wait_ns` and `idle_wait_ns`,
with their peaks, say where the workers waited; and the range engine's `awaited.*` counters say why the range the
output waited for was or was not split. With the default cap, `spill.cap_raises` counts the rises of a cap that
followed a disk grown under the run, `max_bytes` is the cap reached and `start_max_bytes` the one the run started
with. A plain `swath resume DIR` keeps a recorded `--spill-budget`, and with the default derives the cap from the
free space it finds, the certified spill the stopped run kept counted as free
([durable spill](ordering.md#durable-spill)).

**A stuck frontier.** `max_frontier_stuck_ns` is seconds to minutes. In `progress.json`, `rows_fetched` rises
while `rows_encoded` and `parts_written` stay flat and `part_age_ms` grows: one range at the front of the output
is slow, a slow, retried or hung request. Read `inflight.p99_ns` against `inflight.p50_ns` and the retries in the
provider's counters.

**Throttling.** `requests.throttles` rises, `limiter.decreases` is above 0, and `progress.json`'s `limit` falls
below `--workers`; `limiter.peak_depth` near `--workers` means the limit fell to about 1, and `limiter.limit` is
where it ended. Throttle answers under 15 % of the limiter's window cause no cut; `limiter.cuts_suppressed.tolerated` counts
them ([request admission](clients.md#request-admission) has the rule).

## Order stage and encoders (run summary, top level)

| Key | Unit | Meaning |
|---|---|---|
| `frontier_wait_ns` | ns | the drainer waited for the range it needs next while a worker was still listing it (the slow-frontier cost) |
| `budget_wait_ns` | ns | emitters, and workers with ranges queued, waited on a full reorder budget (the reorder cost); summed over goroutines |
| `encoder_wait_ns` | ns | the order stage waited for an encoder: a full part queue, or every encoder busy when a part opened |
| `publish_wait_ns`, `peak_publish_waiters` | ns, goroutines | emitters waiting in publish on a full budget (each a worker still in its range's `Expand`, so the range engine counts it active), summed over them, and the most at once (live: `progress.json` `order.publish_waiters`) |
| `admission_wait_ns`, `peak_admission_waiters` | ns, goroutines | workers waiting to claim a queued range while the budget and spill were used; with `publish_wait_ns`, `budget_wait_ns` (live: `order.admission_waiters`) |
| `idle_wait_ns`, `peak_idle_workers` | ns, goroutines | workers with nothing queued to list, read-ahead waits included, over the whole run (`drain.worker_idle_after_provider_finished_ns` is its part after listing; live: `order.idle_workers`) |
| `window.tiles`, `window.forced`, `window.grown` | tiles | a run planned from a counted prior ([the frontier window](ordering.md#the-frontier-window); absent otherwise): tiles admitted; those the drainer admitted past a refusal, having drained every other (a window smaller than a tile admits only these); tiles whose rows reached 4 times the prior's (each stopped admission until the frontier passed it) |
| `window.rows`, `window.bound_by` | rows, name | the window's rows when last evaluated and what set them: `rows` (the target, 8,000,000), `set` (`swath diag list --seed-from-window-rows`), each only under what the storage gives: `spill_cap` (half the spill cap over a spilled row's bytes, with what the row budget holds), `budget` (spill off: what the row budget holds alone) |
| `window.waits.rows`, `.spill`, `.growth` | waits | workers' waits at the window, by the refusal that held them: the tiles' charges and the next tile past the window's rows; live spill, less the drained certificates waiting for K, past half its cap, or a frame refused; a grown tile held |
| `window.spill_rows`, `window.peak_behind_bytes`, `window.evicted` | rows, bytes, certificates | the spill gate ([spill behind the output](ordering.md#the-frontier-window)): the mean, over the admission probes it refused (a forced admission's probe included, so a crawl of forced admissions counts every one), of the rows charged to the tiles held then (0: it never refused); the most live spill seen behind the output (drained durable files not yet unlinked), sampled at every check, a full spill's included; the drained certificates it evicted while live spill, less the retired files whose unlink is pending, was past half the cap (also in `spill.durable.retired.evicted`) |
| `window.wakes.spill`, `.read_ahead`, `.read_ahead_share` | signals | wakes of a worker held at the window on the paths that engage only with durable spill (0 without it): spill refused a tile and no longer refuses; frames planned for the read-ahead, one per read it can start; the read-ahead back under half its memory share, one per read |
| `window.wait_ns`, `window.peak_waiters`, `window.plan_wait_ns` | ns, goroutines, ns | workers' time at the window summed, the most at once; the drainer's time waiting for the prior to be read (every admitted tile drained) |
| `window.peak_tiles`, `window.peak_credit_rows`, `window.peak_retained_bytes` | tiles, rows, bytes | the most admitted tiles held at once, the most rows charged to them (each the larger of its expected rows and its rows received and not yet handed to the sink), and the most bytes the order stage held at an admission (row budget units in memory and live spill): what the window held, not a bound |
| `spill.rows`, `spill.bytes`, `spill.ranges` | rows, bytes, ranges | rows written to spill and replayed, bytes written, ranges that spilled |
| `spill.frames`, `spill.opens` | frames, opens | row frames written (a file's header, capture and trailer frames excluded); spill file opens attempted, failed ones included: one per frame written, one per read handle miss (a frame read at an offset, a trailer read or a reader whose file had no open handle), seals and recovered certificates' replays (a resume's validation of the recorded certificates is not counted). Against `spill.ranges`, the per-file and per-frame cost of many small spills |
| `spill.read_handles.hits`, `.misses`, `.raced` | leases | reads of a spill file (a frame at an offset, a trailer, the drainer's reader; not a recovered certificate's replay) served by the file's open descriptor, and those that opened it (one open each, two when the first is refused with EMFILE/ENFILE); `raced`: misses whose open lost to a concurrent one for the same file (closed at once). `hits / (hits + misses)` is the reuse rate; a range that spills one frame can at best share its frame read with its trailer read |
| `spill.read_handles.evictions.capacity`, `.retired`, `.truncated`, `.low_fd`, `.closed` | handles | handles closed out of the cache: idle past the cap (`order.MaxSpillReaders` descriptors, least recently used first), the file drained (removed, or handed back to the durable cache), the file truncated (an ENOSPC rollback), idle when an open failed for want of descriptors (the cache then keeps none for the rest of the run), cached when the run stopped or its output's identity check failed (a leased one stays usable until its release; retirement does not revoke reads) |
| `spill.read_handles.peak_open` | descriptors | the most spill read descriptors the cache held open at once, leased or idle (opens under way not counted; `spill.peak_readers` adds certificate replays). `order.MaxSpillReaders` is the idle-retention target: with every handle leased (more segment workers than 16) it is exceeded |
| `spill.frame_bytes`, `spill.frame_rows` | bytes, rows | row frames written by size (`p50`, `p90`, `p99`: the bound of the power-of-two bucket holding the quantile, so the quantile is at most it and more than half of it; `max`: exact). `spill.rows / spill.frames` is the mean |
| `spill.logical_bytes`, `spill.format` | bytes, name | the spilled rows' logical size (key and column values, as the part router counts them; `spill.bytes` against it is what the frame coding saved) and the frames written: `delta+zstd` (the default), `delta` (`--no-spill-compress`), `plain+zstd` or `plain` (`--spill-plain`) |
| `spill.decode_refused` | frames | frames refused at read for decoding past their declared storage (a malformed frame; the run fails with it) |
| `spill.peak_bytes`, `spill.refused` | bytes, frames | peak live spill bytes; frames the disk budget refused (the emitter waited instead) |
| `spill.refused_ns`, `spill.refused_spells` | ns, intervals | the time spill stood refused, from a refused frame until a spill file's credit next returned (an interval open at the end included), and the intervals that ended: many short spells are a budget that frees and refills, one long one a budget that never frees |
| `spill.max_by`, `spill.start_max_bytes`, `spill.max_bytes`, `spill.cap_raises`, `spill.cap_rechecks`, `spill.cap_recheck_raises` | name, bytes, bytes, rises, reads, rises | what set the spill cap: `spill_budget` (`--spill-budget`, fixed), `disk` (the default for a dataset in a bucket: the spill filesystem's free space after the reserve), `disk_half` (the default otherwise, the filesystem shared with the output or, for a stream, other programs' temporary files: half of it), `none`; the cap the run started with; the cap in effect at the end; the times the cap rose (the filesystem grew, or other files on it went), each ending a standing refusal and waking the waiters; the follower's rereads of the free space every 2 s while the cap held the run back (a frame refused, workers at the frontier window, in publish or at admission), and those that raised the cap (the rest rose at a refused frame). `max_bytes` above `start_max_bytes` is a disk grown under the live run; 0 rises with `refused` above 0 is a cap that bound on a disk that stayed as it was ([ordering.md](ordering.md#spill)) |
| `spill.dir`, `spill.fs_type` | path, name | the spill directory and its filesystem (Linux: `tmpfs`, `ext4`, `xfs`, ..., or the statfs magic in hex; absent elsewhere): on `tmpfs` or `ramfs` spill is memory outside `peak_rss_bytes`, and a run that spills there warns at its first spill |
| `peak_ahead_rows` | rows | the most rows listed and not yet handed on at once: ahead of the publish frontier, in memory or spilled (live: `progress.json` `order.ahead_rows`) |
| `max_frontier_stuck_ns` | ns | the longest interval in which the order stage handed nothing on: from the drain's start to the first row, between two hand-ons, or from the last row to the drain's end, whether it ends in success, cancellation or an error |
| `timings.provider_finished_at` | time | the provider finished: every range expanded, none in flight (a resume's reused certificates are not listed: it is when the gaps around them are); absent if it never did; `listing_finished_at` is the order stage's end, after the drain |
| `drain.tail_ns` | ns | from `provider_finished_at` to the drain's last row: the part of the run in which only the drain was left |
| `drain.worker_idle_after_provider_finished_ns` | ns | the workers' idle time after `provider_finished_at` (nothing queued to list, read-ahead waits included), summed over workers |
| `drain.replay_read_ns`, `drain.replay_decode_ns` | ns | the drainer reading spilled frames itself, and the part of it spent decoding (trailers included); the drainer's times accumulate over the whole run, not the tail alone |
| `drain.ahead_wait_ns`, `drain.order_check_ns`, `drain.sink_write_ns` | ns | the drainer waiting for a frame the read-ahead was reading; the stream-wide order guard; handing batches to the sink (`encoder_wait_ns` included) |
| `drain.writer_before_provider_finished.parts`, `.rows` | parts, rows | Parquet parts published before `provider_finished_at`, and their rows (absent when listing never finished). Approximate at the boundary: a part published as listing ends may fall on either side |
| `drain.writer_before_provider_finished.encode_ns`, `.output_ns` | ns | the encoders' time on the batches written before `provider_finished_at`: building and compressing pages and hashing (`encode_ns`), and writing to the part's file or upload with the sync and close that complete it (`output_ns`) |
| `drain.read_ahead.frames`, `drain.read_ahead.rows` | frames, rows | spilled frames idle workers read and decoded ahead of the drainer (feature: `spill`) |
| `drain.read_ahead.drainer_frames`, `drain.read_ahead.unplanned_frames` | frames | frames planned and taken over unread by the drainer; frames it reached with nothing planned (both read by the drainer; with `frames`, every frame spilled, each once) |
| `drain.read_ahead.drainer_waits`, `drain.read_ahead.capped_waits` | waits | the drainer waiting for a frame being read; workers waiting at the read-ahead's share of the row budget |
| `drain.read_ahead.read_ns`, `drain.read_ahead.decode_ns` | ns | the workers' read-ahead time, and the part of it spent decoding |
| `drain.read_ahead.max_reads` | reads | reads allowed at once: 8, at most `GOMAXPROCS` - 1; 0 is off (one processor: the drainer replays alone) |
| `drain.segments.reason` | reason | the segment-parallel replay after listing: `engaged`, or why not: `one_processor` (GOMAXPROCS 2 or less), `one_encoder`, `single_part`, `not_planned` (the router's rule not stated before the drain ended), `row_budget` (the row budget holds one segment worker only), `listing_active` (the drain ended before listing did: a stopped run) (absent without a dataset) |
| `drain.segments.segments`, `.workers`, `.rows`, `.max_rows`, `.seam_checks` | parts, workers, rows, rows, seams | segments written (one part each), the workers writing them, their rows and the largest segment's, the seams checked |
| `drain.segments.time_ns`, `.max_time_ns`, `.plan_ns` | ns | segments' time summed (open to close) and the longest; cutting the rest into segments |
| `drain.segments.read_ns`, `.decode_ns`, `.write_ns`, `.max_workers` | ns, workers | of `time_ns`: spill frames read (and decoded), batches handed to the parts (encoder queue waits included); the workers' cap (`inventory.segmentWorkers`) |
| `drain.segments.durable_files_read`, `.durable_boundaries` | files, boundaries | durable spill files (`order.Cache`) whose frames the segments read, a resume's reused certificates included, and the segment boundaries that fall inside one of their frames |
| `drain.segments.certificates_read`, `.certified_rows`, `.certificate_scan_ns` | certificates, rows, ns | on a resume: the reused certificates the segments replayed (those the drainer had not reached when listing ended; `spill.durable.resume.replayed_certificates` counts the drainer's too), their rows above K (in `rows`), and the plan's time reading them before the cut (in `plan_ns`) |
| `drain.segments.deferred_rows` | rows | versions schemas: rows a planned segment boundary moved past because they continued the group of the key before it (no part ends inside a key) |
| `drain.segments.tree_bytes` | bytes | what the tree held when the segments were planned: not the segments' budget, which they take beside it (two budgets in all, at most) |
| `drain.segments.workers_by`, `.per_worker_bytes`, `.free_bytes` | reason, bytes, bytes | what set the workers: `encoders`, `processors`, `descriptors`, `row_budget` or `segments`; the budget one worker holds at most (its largest unit, a gathered and an encoding batch, its part's queue, at the heaviest run of consecutive rows), and the budget the segments could take when planned (the row budget less what the encoders' queues held) |
| `drain.segments.budget_waits`, `.budget_wait_ns`, `.peak_held_bytes` | waits, ns, bytes | workers waiting for the row budget before reading or gathering a unit (a resume's certificate scans too, before each certificate), and their time; the most the segments held at once beside the tree (the encoders' queues, the units being read or gathered, the frames being scanned) |
| `drain.encoders_at_start`, `drain.encoders_after_listing`, `drain.encoder_admission` | encoders, reason | Parquet encoders admitted at the start and once listing ended (`inventory.admitAfterListing`); why: `listing_done`, `explicit` (`--encoders` given), `at_cap`, `not_reached` (absent without a dataset) |
| `drain.encoder_allowance_bytes`, `.encoder_writer_bytes`, `.encoder_upload_bytes` | bytes | the memory encoders are admitted against (1/16 of memory, whatever `--sort-buffer` says) and one encoder's prices: its writer, and a dataset in a bucket's upload buffer (a GCS chunk, an S3 part or an Azure block) |
| `drain.encoders_at_start_bound_by`, `.encoders_after_listing_bound_by` | name | what bound each count: `memory` (the allowance), `cpu` (GOMAXPROCS), `descriptors`, `minimum` (one), `explicit`; after listing, the start's when listing did not raise the count |
| `peak_heap_live_bytes` | bytes | the peak live heap while listing (`/gc/heap/live:bytes`, sampled every 100 ms); against `invocation.sort_buffer_bytes` it is the memory the budget does not see, the garbage collector's headroom aside |

A text run's sink (run summary, `output.text`):

| Key | Unit | Meaning |
|---|---|---|
| `sink_wait_ns` | ns | the order stage was in the text sink: rows waiting on the writer (one formatter: formatting them, and `output_wait_ns`; more: handing batches to the formatters' full queue) |
| `output_wait_ns` | ns | handing bytes on: to the compressor (a synchronous encoder compressing, or every parallel job busy), or to the file or stream uncompressed; by the order stage with one formatter, by the sequencer with more |
| `formatters` | goroutines | rows formatted at once: `--encoders` (as admitted) for a file, 1 (the order stage) for stdout |
| `compressor.bytes_in`, `compressor.bytes_out` | bytes | formatted rows into the compressor, compressed bytes delivered (absent uncompressed) |
| `compressor.jobs`, `compressor.jobs_reason` | jobs, name | zstd jobs compressed at once and why: `cores` (GOMAXPROCS), `encoder_allowance` (16 MiB each of the encoder allowance carries fewer), `encoders` (asked), `gzip` (one stream) |

A count (`swath du`, `swath diag count`) does not order (`features.order` false): `peak_buffered_bytes`, `peak_ahead_rows`
and `max_frontier_stuck_ns` are 0 and `spill` is absent.

Discarded speculative work is `overfetch_keys` and `child_empty.*` below.

## S3 provider (`internal/source/s3.Counters`)

<!-- s3:begin -->
| Key | Kind | Unit | Feature | Meaning |
|---|---|---|---|---|
| `objects` | count | objects |  | objects listed (emitted rows) |
| `bytes` | count | bytes |  | sum of listed object sizes |
| `pages` | count | pages |  | listing pages processed (and the seed's probes) |
| `requests` | count | requests |  | logical listing requests (a call the client retried or the engine requeued is one); attempts counts HTTP requests |
| `overfetch_keys` | count | keys |  | keys listed past a range's hi (or at or below a resume key) and dropped |
| `truncated_at_max_keys` | flag |  | max_keys | -max-keys stopped the listing; the dataset is incomplete |
| `seed.prefixes` | count | prefixes | seed | common prefixes on the top level's seed pages |
| `seed.cuts` | count | cuts | seed | cut points the seed tiled the scope at (every level, after the subsample) |
| `seed.unsafe` | count | prefixes | seed | prefixes dropped as unusable start-after values |
| `seed.subsampled` | flag |  | seed | more cut points than min(1000, 4×workers): cuts were subsampled |
| `seed.truncated` | flag |  | seed | the top level had more pages than the seed followed (4) |
| `seed.sentinel` | flag |  | seed | the closing sentinel (pk, prefixCeil(pk)] was added |
| `seed.pages` | count | pages | seed | the top level's delimiter pages the seed followed (at most 4) |
| `seed.requests` | count | requests | seed | delimiter requests the seed sent, every level, the empty continuing pages followed included (at most 64; also in requests) |
| `seed.round_trips` | count | waves | seed | round trips of seed requests before any range lists: a wave counts its longest chain of requests, the empty continuing pages followed included (at most 6) |
| `seed.levels` | count | levels | seed | levels the seed descended below the top (at most 4) |
| `seed.level.0.cuts` | count | cuts | seed | usable folders found on the top level (before the subsample) |
| `seed.level.1.cuts` | count | cuts | seed | usable folders found one level below the top |
| `seed.level.2.cuts` | count | cuts | seed | usable folders found two levels below the top |
| `seed.level.3.cuts` | count | cuts | seed | usable folders found three levels below the top |
| `seed.level.4.cuts` | count | cuts | seed | usable folders found four levels below the top |
| `seed.stop.wide` | count | seeds | seed | the seed stopped at a level with at least workers folders |
| `seed.stop.leaf` | count | seeds | seed | the seed stopped at a level with no folder |
| `seed.stop.levels` | count | seeds | seed | the seed stopped after descending 4 levels |
| `seed.stop.requests` | count | seeds | seed | the seed stopped: the next level, or the next empty page a chain follows, would pass 64 requests |
| `seed.stop.round_trips` | count | seeds | seed | the seed stopped: 6 round trips spent (a wave costs its longest chain of requests, empty pages followed included) |
| `seed.stop.failed` | count | seeds | seed | the seed stopped: a listing failed or the run was canceled (the seed counters are what it spent until then) |
| `seed.stop.one_page` | count | seeds | seed | the seed stopped: its first data page, sent with the top level's first delimiter request, was not truncated (the scope fits in one page; no tile) |
| `seed.stop.flat` | count | seeds | seed | the seed stopped at a level with no folder, after a chain whose page held no folder was not followed |
| `seed.first_page_keys` | count | objects | seed | keys the seed's first data page emitted: the whole scope, in one page |
| `seed.first_page_discarded` | flag |  | seed | the seed's first data page was truncated: discarded, the tiles list the scope from its start (one request more) |
| `seed.first_page_skipped_reuse` | flag |  | seed | a resume with reused certificates sent no first data page from K: it could list (and, fitting one page, emit) keys a certificate replays |
| `seed.flat_chains` | count | chains | seed | delimiter chains not followed past a page that held no folder (objects only) |
| `seed.resumed` | flag |  | seed | the seed tiled a resumed scope (K, ⊤] |
| `seed.from.parts` | count | parts | seed_from | parts of the --seed-from dataset (0 for a cut file) |
| `seed.from.indexed_parts` | count | parts | seed_from | parts whose key page index was read for finer cuts |
| `seed.from.index_failed` | count | parts | seed_from | parts whose key page index could not be read: their part bounds were cut at instead |
| `seed.from.cuts` | count | cuts | seed_from | cut keys the prior offered (before the merge into tiles) |
| `seed.from.unsafe` | count | cuts | seed_from | cuts the provider does not accept as a bound: cut at their longest accepted prefix, or merged into the next |
| `seed.from.tiles` | count | ranges | seed_from | ranges the prior tiled the scope into, the open frontier after its last cut included (at most min(1000, 4×workers), except a counted prior's plan, made as the run goes) |
| `seed.from.expected_keys` | count | keys | seed_from | keys the prior expected in its bounded tiles (0 without counts): against objects, the scope's growth |
| `seed.from.error.within_2x` | count | ranges | seed_from | prior tiles whose ranges listed within 2x of the keys the prior expected (a tile's descendants included) |
| `seed.from.error.grown` | count | ranges | seed_from | prior tiles whose ranges listed more than 2x the keys the prior expected: the segment grew (past its count the gauge estimates its remainder, and splits share it out) |
| `seed.from.error.shrunk` | count | ranges | seed_from | prior tiles whose ranges listed less than half the keys the prior expected: the segment shrank or emptied |
| `seed.from.error.max_grown` | max | x | seed_from | the largest ratio of a prior tile's listed keys to the prior's expectation, rounded |
| `seed.from.page_size` | max | keys | seed_from | the page size a counted prior set (the provider's largest; 0: not engaged) |
| `seed.from.tile_pages` | max | pages | seed_from | the target pages per counted-prior tile (a planned run: TilePages, or --seed-from-tile-pages); a coarse first cut can exceed the target (0: not engaged) |
| `seed.from.held_pages` | count | pages | seed_from | range pages after which the prior's count (its expected keys less those listed) stayed the range's remainder estimate instead of the page's gauge |
| `seed.from.outrun` | count | ranges | seed_from | ranges that listed their prior's count and went on: the page's gauge estimates their remainder from there |
| `seed.from.merged` | count | cuts | seed_from | cuts that repeated the previous cut's key (a version group across key pages) and were merged into it |
| `seed.from.plan_errors` | count | errors | seed_from | a counted prior's plan failed while the run went on (an invalid cut, or the prior's reader): the run fails, it never ends the plan early |
| `seed.sentinel_declined.truncated` | count | decisions | seed | sentinel declined: the top level had more pages than the seed followed |
| `seed.sentinel_declined.object_last` | count | decisions | seed | sentinel declined: the top level's last page ended with an object |
| `seed.sentinel_declined.no_ceiling` | count | decisions | seed | sentinel declined: no prefix ceiling above the last cut (and a resume key) |
| `seed.sentinel_declined.unsafe` | count | decisions | seed | sentinel declined: the ceiling is not a safe key |
| `split.considered` | count | decisions | split | split decisions taken with demand and rank |
| `split.pivot.reflect` | count | splits | split | accepted splits whose pivot is the cursor reflected from its chain's anchor (forwardReflect at the scalar where they diverge) |
| `split.pivot.midpoint` | count | splits | split | accepted splits whose pivot is a safe scalar midpoint between the cursor and hi |
| `split.pivot.density.bounded` | count | splits | split | accepted splits of bounded ranges whose pivot is the galloped density step (max(4, hunger) << generation page spans, at most halfway to hi), further than the anchor reflection |
| `split.pivot.density.frontier` | count | splits | split | accepted frontier splits whose pivot is the galloped density step, further than the anchor reflection |
| `split.sliver_refused` | count | decisions | split | splits not taken because the only pivot was a sliver just above the cursor (no density step, no safe midpoint): the range keeps listing and decides again after its next page |
| `split.gallop.max_generation` | max | generations | split | highest gallop generation (consecutive upper children along a chain) of a range that split: its density stride is max(4, hunger) << generation page spans |
| `split.gallop.halvings` | count | halvings | split | density stride halvings within decisions, after a target past the representable keys, a hole or outside (cursor, hi) |
| `split.frontier.anchored` | count | splits | split | accepted frontier splits whose chain has an anchor (a seed or resume tile's lo, or the chain's first pivot): the reflection doubles the covered distance |
| `split.reflect.ceiling` | count | decisions | split | reflections at or past the range's ceiling (hi, or the scope's prefixCeil on the frontier): the pivot fell back to the midpoint between the cursor and the ceiling, and the upper child starts a new chain |
| `split.density.shrink_prevented` | count | decisions | split | decisions whose density step lay nearer the cursor than the anchor reflection: not taken (the density step may move a pivot further, never nearer) |
| `split.density.refused.short_page` | count | decisions | split | density step refused (after its halvings): fewer than 2 keys on the page |
| `split.density.refused.alphabet` | count | decisions | split | density step refused (after its halvings): a byte on the page or the cursor is non-ASCII or a control character ('%' is read as a digit) |
| `split.density.refused.zero_span` | count | decisions | split | density step refused (after its halvings): the page spans no distance |
| `split.density.refused.past_top` | count | decisions | split | density step refused (after its halvings): the target is past the representable keys |
| `split.density.refused.hole` | count | decisions | split | density step refused (after its halvings): the target is not a string (an end-of-string digit inside it) |
| `split.density.refused.outside` | count | decisions | split | density step refused (after its halvings): the target is not strictly inside (cursor, hi) |
| `split.accepted_bounded` | count | splits | split | splits of bounded ranges |
| `split.accepted_frontier` | count | splits | split | splits of the open frontier |
| `split.rejected.cursor` | count | decisions | split | guard: pivot not above the cursor |
| `split.rejected.bound` | count | decisions | split | guard: pivot not below hi (or the scope ceiling) |
| `split.rejected.no_pivot` | count | decisions | split | guard: no safe pivot exists |
| `split.rejected.unsafe_cursor` | count | decisions | split | guard: the cursor is not a usable start-after |
| `split.rejected.unsafe_pivot` | count | decisions | split | guard: the provider vetoes the pivot as a bound |
| `split.skipped.no_demand` | count | decisions | split | no idle worker (hunger <= 0) |
| `split.reason.slow_range` | count | splits | slow_split | --slow-split: splits taken without an idle worker: the range's pages took at least 4 times the run's mean page latency and its time left (keys left x its page latency) was at least twice the mean of the running ranges' |
| `split.slow.max_range_latency_ns` | max | ns | slow_split | --slow-split: the highest page latency (EWMA) of a range |
| `split.slow.run_latency_ns` | gauge | ns | slow_split | --slow-split: the run's mean range page latency |
| `split.skipped.outranked` | count | decisions | split | larger ranges took the demand |
| `split.skipped.remainder` | count | decisions | split | bounded range whose remainder (cursor, hi] is under 2 pages by its last page's density: not split, the owner lists it in at most two more requests (decided again after the next page) |
| `split.skipped.lower_tiny` | count | decisions | split | bounded range whose pivot would leave the lower child (cursor, pivot] under 1 page by its last page's density: not split, the owner's next request covers it (decided again after the next page) |
| `split.skipped.upper_tiny` | count | decisions | split | bounded range whose pivot would leave the upper child (pivot, hi] under 1 page by its last page's density (often: past the remainder's last key): not split (decided again after the next page) |
| `split.estimate_none` | count | decisions | split | bounded split decisions past the guard without a density estimate (a page of fewer than 2 keys or no span): the admission gates did not apply |
| `split.admission.overridden` | count | decisions | split | bounded split decisions the admission gates would refuse, taken because the range's previous 4 decisions (with demand) were refused: the policy kept proposing a pivot the gates refuse |
| `split.max_depth` | max | splits | split | longest chain of splits from a root or seed tile to a range: each step waits for a page, so at one page latency per step it bounds the wall time from below |
| `split.leaves` | count | ranges | split | ranges that listed through their bound without splitting (the tiling's leaves) |
| `split.leaf_depth_sum` | count | splits | split | sum of the leaves' depths: over split.leaves, the mean leaf depth |
| `split.unsplittable` | count | ranges | split | ranges proven unsplittable: no pivot exists above a safe cursor (no_pivot); an unsafe cursor declines one page only |
| `split.unsplittable_keys` | count | keys | split | keys listed by ranges after they became unsplittable (drained by one worker) |
| `idle_demand_ns` | count | ns | split | worker time idle with demand: max(0, workers - active - queued) integrated while a range is in Expand |
| `awaited.pages` | count | pages | split | continuing pages of a range the drain was at (the output waiting for its rows; sorted output only), each counted once below by its split decision |
| `awaited.open_pages` | count | pages | split | of awaited.pages, those of the open frontier (the range ranked first) |
| `awaited.split` | count | pages | split | awaited pages after which the range split |
| `awaited.skipped.no_demand` | count | pages | split | awaited pages not split: no idle worker (hunger <= 0; a worker waiting in the order stage still counts as active) |
| `awaited.skipped.outranked` | count | pages | split | awaited pages not split: larger ranges took the demand |
| `awaited.unsplittable` | count | pages | split | awaited pages not split: the range was proven unsplittable |
| `awaited.skipped.group_open` | count | pages | groups | awaited pages not split: a hot key's group open, or no complete key yet |
| `awaited.rejected.cursor` | count | pages | split | awaited pages not split, guard: pivot not above the cursor |
| `awaited.rejected.bound` | count | pages | split | awaited pages not split, guard: pivot not below hi (or the scope ceiling) |
| `awaited.rejected.no_pivot` | count | pages | split | awaited pages not split, guard: no safe pivot exists |
| `awaited.rejected.unsafe_cursor` | count | pages | split | awaited pages not split, guard: the cursor is not a usable start-after |
| `awaited.rejected.unsafe_pivot` | count | pages | split | awaited pages not split, guard: the provider vetoes the pivot as a bound |
| `awaited.sliver_refused` | count | pages | split | awaited pages not split: the only pivot was a sliver just above the cursor |
| `awaited.skipped.remainder` | count | pages | split | awaited pages not split: the bounded remainder is under 2 pages |
| `awaited.skipped.lower_tiny` | count | pages | split | awaited pages not split: the lower child would be under 1 page |
| `awaited.skipped.upper_tiny` | count | pages | split | awaited pages not split: the upper child would be under 1 page |
| `awaited.skipped.window` | count | pages | seed_from | awaited pages not split: idle workers, but the frontier window refusing tiles (split.skipped.window) |
| `split.skipped.window` | count | decisions | seed_from | idle workers, but the frontier window refusing tiles: they feed only a range whose remainder holds two tiles or more (a planned run) |
| `child_empty.lower` | count | ranges | split | lower split children that listed no key |
| `child_empty.upper` | count | ranges | split | upper split children that listed no key |
| `child_empty.by.reflect.lower` | count | ranges | split | empty lower children of a reflect pivot |
| `child_empty.by.reflect.upper` | count | ranges | split | empty upper children of a reflect pivot |
| `child_empty.by.midpoint.lower` | count | ranges | split | empty lower children of a midpoint pivot |
| `child_empty.by.midpoint.upper` | count | ranges | split | empty upper children of a midpoint pivot |
| `child_empty.by.density.lower` | count | ranges | split | empty lower children of a density pivot |
| `child_empty.by.density.upper` | count | ranges | split | empty upper children of a density pivot |
| `child_empty.hi.pivot` | count | ranges | split | empty children whose hi is a split's pivot |
| `child_empty.hi.bound` | count | ranges | split | empty children whose hi is a real bound: a seed cut, the scope's end or ⊤ |
| `child_small.lower` | count | ranges | split | lower split children that ended or split having listed fewer keys than one page (empty ones included) |
| `child_small.upper` | count | ranges | split | upper split children that ended or split having listed fewer keys than one page (empty ones included) |
| `split.frontier_handout_keys` | count | keys | split | keys listed inside the lower children of frontier splits (the child and its descendants): over split.accepted_frontier, the work a frontier split hands out |
| `resumed_ranges` | count | ranges | resume | ranges a resume started with: (K, ⊤], or the seed's tiling of it |
| `reuse.ranges` | count | ranges | resume | reused coverage certificates a resume replays from durable spill instead of listing them |
| `reuse.gaps` | count | ranges | resume | ranges a resume with reused certificates lists: the gaps between them, tiled |
| `reuse.boundary_overfetch_keys` | count | keys | resume | keys a gap listed past its end, a reused certificate's start, and dropped (also in overfetch_keys) |
| `peak_active` | max | ranges |  | peak ranges in Expand at once |
| `peak_queued` | max | ranges | split | peak ranges queued for a worker |
| `busy_ns` | count | ns |  | sum of Expand wall time |
| `sink_wait_ns` | count | ns |  | time Expand spent handing rows to the order stage (backpressure) |
| `requeues` | count | requests |  | pages asked again after the client's retries were exhausted (the run never fails on that) |
| `structure.requests` | count | requests | structure | delimiter probes (max-keys 32) after a cursor, one per folder level; also counted in requests |
| `structure.zero_fanout` | count | probes | structure | probes that found no folder at any level |
| `structure.no_boundary` | count | probes | structure | probes whose folders all lay outside the range |
| `structure.capped` | count | probes | structure | probes whose chosen level filled its page (the furthest folder is the pivot) |
| `structure.suppressed` | count | decisions | structure | probes not sent: 8 in a row found no folder along this range's ancestry |
| `structure.late` | count | decisions | structure | split decisions taken while the range's probe was still in flight |
| `structure.sent.frontier` | count | probes | structure | probes sent by the open frontier (the scope root and the two folders above the cursor's own) |
| `structure.sent.stall` | count | probes | structure | probes sent by a bounded range whose parent's pivot stayed in the cursor's own folder |
| `structure.sent.forced` | count | probes | structure | probes sent by a bounded range without that evidence (--structure=on) |
| `structure.boundaries` | count | folders | structure | folder boundaries the probes found inside their ranges |
| `structure.frontier_pivots` | count | splits | structure | frontier splits whose pivot is a folder boundary, further than the anchor reflection (also in split.pivot.structure) |
| `structure.suppressed_inherited` | count | decisions | structure | suppressed probes whose 8 empty probes were all an ancestor's: the range inherited the suppression |
| `split.pivot.structure` | count | splits | structure | accepted splits whose pivot is a folder boundary from a structure probe |
| `child_empty.by.structure.lower` | count | ranges | structure | empty lower children of a structure pivot |
| `child_empty.by.structure.upper` | count | ranges | structure | empty upper children of a structure pivot |
| `empty_pages` | count | pages |  | continuing pages with no entry, followed by their token (also counted in requests) |
| `empty_chain_pages` | count | pages |  | continuing pages past the first 1,024 without a greater key (empty, suppressed or same-key group pages): followed without watchdog credit |
| `prefixes_normalized` | count | pages |  | pages whose common prefixes came unsorted or repeated, sorted and deduplicated |
| `bound_drops` | count | keys |  | keys equal to an inclusive start bound and common prefixes at or below it, listed again and dropped (not in overfetch_keys) |
| `mutex.wait_ns` | count | ns |  | time goroutines spent waiting for the engine's decision mutex (summed over goroutines) |
| `mutex.max_hold_ns` | max | ns |  | the longest single hold of the engine's decision mutex |
| `groups.open_pages` | count | pages | groups | pages that were all one key's rows, emitted while that key's group may continue (a hot key: its range does not split) |
| `groups.fence_deferred` | count | pages | groups | page ends that declared no coverage fence because the hot key's group was still open (durable spill certifies only complete keys) |
| `groups.held_rows` | count | rows | groups | rows of a page's trailing key held back until the next page proved the key complete |
| `groups.split_relisted_rows` | count | rows | groups | held rows dropped at a split, listed again by the lower child |
| `groups.max_rows` | max | rows | groups | the largest observed key group in emitted or held rows (all emission paths, before filters) |
| `groups.overfetch_rows` | count | rows | groups | rows above a range end read to close its last key group (excludes resume suppression and split relisting) |
| `split.skipped.group_open` | count | decisions | groups | a range inside a hot key's group (its rows emitted across pages), or that had emitted no complete key yet, was not split |
| `trace.events` | count | events | trace | events the engine handed to the run trace (--trace) |
| `trace.dropped` | count | events | trace | trace events dropped because the trace writer was a full buffer behind: the engine never waits for the file |
| `end_check.claims` | count | pages | end_check | final pages (not truncated) of chains that started after a key: a range's, a resumed sequential chain's, a resumed seed's single first page; checked against the keys and folders the run had seen, else by a ladder |
| `end_check.contradicted` | count | pages | end_check | claims a key or folder past the page's last key contradicted (seen by the run, or found by a ladder): asked again |
| `end_check.witness_checks` | count | requests | end_check | requests that asked whether a contradicting key or folder still exists (prefix, no start-after; also counted in requests) |
| `end_check.asked_again` | count | requests | end_check | contradicted pages asked again after their last key (also counted in requests) |
| `end_check.raced_new_keys` | count | pages | end_check | contradicted pages asked again that listed keys after them: written meanwhile, or a service whose listing lags; the page was not final and the chain went on after it |
| `end_check.raced_gone` | count | pages | end_check | contradicting keys or folders that no longer existed when asked (deleted meanwhile, or written since the run started): the claim went to a ladder |
| `end_check.omitted` | count | pages | end_check | pages still contradicted after being asked again 3 times: the service omitted keys and the run failed (cause omitted_keys) |
| `end_check.ladders` | count | claims | end_check | claims nothing the run had seen contradicted, confirmed by asking whether any key follows them (one round trip of max-keys 1 requests, one per folder level and two per level above) |
| `end_check.ladder_requests` | count | requests | end_check | the ladders' requests (also counted in requests) |
| `end_check.ladder_found` | count | claims | end_check | claims a ladder found a key after: contradicted, and asked again (also in end_check.contradicted) |
| `end_check.floor` | count | claims | end_check | claims at or above one a ladder already confirmed: nothing follows them, at no request |
| `end_check.ladder_waits` | count | claims | end_check | claims that waited for a ladder in flight from at or below them instead of asking their own (then counted in floor, or contradicted by the key it found) |
| `end_check.new_writes` | count | pages | end_check | pages asked again that listed after the claim only keys modified since the run started, not the witness: no answer, asked again |
| `attempts` | count | requests |  | HTTP requests sent: every attempt, client retries and engine requeues included (the request economics) |
| `retries` | count | requests |  | attempts repeated after a retryable failure |
| `throttles` | count | requests |  | attempts answered 429/503/SlowDown, retried or not (the last of an exhausted call included) |
| `timeouts` | count | requests |  | attempts past -request-timeout, retried or not |
| `parse_failures` | count | responses |  | 200 answers whose body did not parse (retried) |
| `unreachable.attempts` | count | requests |  | attempts that failed to connect deterministically (refused, host not found, network or host unreachable) while nothing in the run had answered |
| `unreachable.stops` | count | calls |  | calls stopped as unreachable: nothing in the run had answered, and connections had failed so for 30s (at most half --stall-timeout; exit 75, cause unreachable) |
| `inflight.peak_active` | max | requests |  | most attempts in flight at once (admitted, send to body read) |
| `inflight.busy_ns` | count | ns |  | attempts' time in flight, summed: over the listing's wall time, the mean in flight |
| `inflight.p50_ns` | gauge | ns |  | attempt latency (send to body read), 50th percentile: the upper edge of its bucket in a log-spaced histogram, at most 25% above; 9223372036854775807 (open) when it falls past 2^40 ns |
| `inflight.p90_ns` | gauge | ns |  | attempt latency (send to body read), 90th percentile: the upper edge of its bucket in a log-spaced histogram, at most 25% above; 9223372036854775807 (open) when it falls past 2^40 ns |
| `inflight.p99_ns` | gauge | ns |  | attempt latency (send to body read), 99th percentile: the upper edge of its bucket in a log-spaced histogram, at most 25% above; 9223372036854775807 (open) when it falls past 2^40 ns |
| `inflight.latency_overflow` | count | attempts |  | attempts at or past 2^40 ns (about 18 min 20 s), the latency histogram's open last bucket |
| `limiter.decreases` | count | steps | limiter | cuts: limit multiplied by 0.7 on a 429/503/SlowDown answer, at most one per round trip, while 15 % or more of the window's answers were throttles |
| `limiter.floor_throttles` | count | answers | limiter | cuts due at a limit of 1: no decrease |
| `limiter.cuts_suppressed.round_trip` | count | answers | limiter | 429/503/SlowDown answers to attempts sent before the last cut: no further cut (one per round trip) |
| `limiter.cuts_suppressed.tolerated` | count | answers | limiter | 429/503/SlowDown answers while under 15 % of the window's answers were throttles: no cut (a background the retries absorb) |
| `limiter.throttle_permille` | gauge | permille | limiter | throttle answers per 1,000 of the window (the last 4 x workers answers since the last cut) at the end |
| `limiter.limit` | gauge | permits | limiter | the limit at the end: where it converged |
| `limiter.increases` | count | steps | limiter | growth steps: limit raised by one, at most one per second, while the window since the last cut is full with under 7.5 % throttles, or after 10 s without a throttle answer |
| `limiter.growth_cooldown` | count | steps | limiter | due growth steps held back by throttling: the window since a cut not yet full, or 7.5 % or more of it throttles, and a throttle answer in the last 10 s |
| `limiter.growth_latency_frozen` | count | steps | limiter | due growth steps held back: successful attempts' latency EWMA above twice the baseline |
| `limiter.growth_timeout_frozen` | count | steps | limiter | due growth steps held back: 3 or more attempt timeouts in the last 10 s |
| `limiter.valve` | count | steps | limiter | limit raised by one through the latency freeze (at most one per 30 s, with progress) |
| `limiter.waits` | count | attempts | limiter | attempts that waited for a permit |
| `limiter.wait_ns` | count | ns | limiter | time those attempts waited, summed |
| `limiter.peak_depth` | max | permits | limiter | most permits the limit stood below the worker count |
| `slow_start.doubles` | count | steps | slow_start | --adaptive-limit-slow-start: limit doubled (from 4, at most once per second, before the first cut or timeout) |
| `slow_start.ramp_ns` | max | ns | slow_start | --adaptive-limit-slow-start: time from the start to the ramp's end (the worker count reached, or the first cut or timeout) |
| `slow_start.end_limit` | max | permits | slow_start | --adaptive-limit-slow-start: the limit the ramp ended at (0: still ramping) |
| `rate.waits` | count | attempts | request_rate | attempts that waited for a --request-rate token |
| `rate.wait_ns` | count | ns | request_rate | time those attempts waited, summed |
| `credentials.refreshes` | count | refreshes |  | signing credentials fetched after the first: ahead of their expiry (by the smaller of 5 minutes and a quarter of their lifetime), or renewed; 0 for credentials that do not expire, --anonymous, a bearer token or a supplied client |
| `credentials.expired_refreshes` | count | refreshes |  | renewals fetched after an expired-token answer (ExpiredToken, ExpiredTokenException, TokenRefreshRequired) to credentials that can expire, its request retried once: one per generation of credentials, however many requests it refused |
| `b2.versions_reordered` | count | pages |  | Backblaze B2 ListObjectVersions pages whose delete markers came before their versions, put in S3's order (key, then newest first) |
| `b2.latest_cleared` | count | entries |  | Backblaze B2 entries marked IsLatest on a version-id-marker continuation, of the marker's key, where none can be the latest; cleared |
<!-- s3:end -->

Every object-store attempt is normalized once into an error carrying transient, throttle and timeout
signals, HTTP status, provider code or reason, and a safe cause. Admission feedback, retries and these
counters consume that same value; parse accounting uses its `parse` diagnostic. Exhaustion wraps it,
and the engine requeues from its transient signal rather than a report label. Credential failures can
supply timeout/retry signals before any listing request is sent, so those counters need not be bounded
by `attempts`.

The summary also carries `anonymous`, `credentials`, `region` and `mean_active` (time-weighted active
ranges), which are not counters.
