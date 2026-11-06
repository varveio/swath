# Counters

Every algorithm path counts its engagements. A provider declares its counters once, as a table of
`counters.Def{Key, Kind, Unit, Help, Feature}` indexed by a typed `counters.ID` and backed by one
`atomic.Int64` each (package `internal/counters`). That table is the one definition of every name, kind and
meaning: the provider reads it out as one immutable `counters.Snapshot` (code reads `Get(id)`), and the run
report renders the snapshot by iterating the table (`run.json` →
`source.stats.counters`: `features` and `values`, flat stable names), and the table below is
generated from the same definitions (`UPDATE=1 go test ./internal/source/s3 -run TestCountersDoc`). One
adapter sits beside it: the HTTP client's counters (`objhttp.Counters`, the limiter's and the rate cap's) are
kept in the client, which a provider may share or replace, and copied into the table when the provider
snapshots, each by its named ID (`objhttp.ClientCounter`, end sentinel `NumCounters`; `Counts.Store`), so
no position carries a meaning and `TestCounterTable` refuses an unfilled or doubly filled entry. A new
path costs one row and its increment. Kinds: `count` is a monotonic sum, `max` a high-water mark, `flag`
a boolean. Names: a dotted group, a snake_case leaf; high-water marks start with `max_`, durations end in `_ns`.

**Feature state is separate from engagement; off, absent and zero are different.** A counter that measures
a policy names it (`Feature`). `features` maps every such policy to whether this run had it on; the values of
an off policy are left out (`TestSummaryDistinguishesOffFromZero`):
- absent, no feature: this build or provider has no such counter;
- absent, feature `false`: the policy was off for this run (for S3: `seed` without `-seed` or on resume,
  `split` and `probe` with `--sequential`, `probe` without `--probe`, `floors` without `--floors`, `structure` without `--structure` (all `swath diag list` flags), `resume` on a fresh run, `max_keys`
  without `-max-keys`, `limiter` with `--limiter=false`, a library caller's zero `Admission` or a supplied client, `slow_start` without
  `--limiter-slow-start` or without the limiter, `request_rate` without `--request-rate`);
- present and zero: the policy was on and never fired.

The report's `resources` follow the same rule: `features.spill` is false and the `spill` object absent with
`swath diag list --no-spill`, and `encoder_wait_ns` is absent without output.

Request accounting uses `attempts` (HTTP requests, retries included) as the denominator; `requests` counts
logical calls.

## Order stage and encoders (run summary, top level)

| Key | Unit | Meaning |
|---|---|---|
| `frontier_wait_ns` | ns | the drainer waited for the range it needs next while a worker was still listing it (the slow-frontier cost) |
| `budget_wait_ns` | ns | emitters, and workers with ranges queued, waited on a full reorder budget (the reorder cost); summed over goroutines |
| `encoder_wait_ns` | ns | the order stage waited for an encoder: a full part queue, or every encoder busy when a part opened |
| `spill.rows`, `spill.bytes`, `spill.ranges` | rows, bytes, ranges | rows written to spill and replayed, bytes written, ranges that spilled |
| `spill.peak_bytes`, `spill.refused` | bytes, frames | peak live spill bytes; frames the disk budget refused (the emitter waited instead) |

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
| `seed.prefixes` | count | prefixes | seed | common prefixes on the top seed page |
| `seed.cuts` | count | cuts | seed | cut points the seed tiled the scope at |
| `seed.unsafe` | count | prefixes | seed | prefixes dropped as unusable start-after values |
| `seed.subsampled` | flag |  | seed | more cut points than min(1000, 4×workers): cuts were subsampled |
| `seed.truncated` | flag |  | seed | the seed page was truncated |
| `seed.sentinel` | flag |  | seed | the closing sentinel (pk, prefixCeil(pk)] was added |
| `seed.sentinel_declined.truncated` | count | decisions | seed | sentinel declined: the seed page was truncated |
| `seed.sentinel_declined.object_last` | count | decisions | seed | sentinel declined: the page ended with an object |
| `seed.sentinel_declined.no_ceiling` | count | decisions | seed | sentinel declined: no prefix ceiling |
| `seed.sentinel_declined.unsafe` | count | decisions | seed | sentinel declined: the ceiling is not a safe key |
| `split.considered` | count | decisions | split | split decisions taken with demand and rank |
| `split.pivot_extrapolated` | count | pivots | split | pivots from page density (else midpoint or reflection) |
| `split.accepted_bounded` | count | splits | split | splits of bounded ranges |
| `split.accepted_frontier` | count | splits | split | splits of the open frontier |
| `split.rejected.cursor` | count | decisions | split | guard: pivot not above the cursor |
| `split.rejected.bound` | count | decisions | split | guard: pivot not below hi (or the scope ceiling) |
| `split.rejected.done` | count | decisions | split | guard: the range had ended |
| `split.rejected.no_pivot` | count | decisions | split | guard: no safe pivot exists |
| `split.rejected.unsafe_cursor` | count | decisions | split | guard: the cursor is not a usable start-after |
| `split.rejected.unsafe_pivot` | count | decisions | split | guard: the provider vetoes the pivot as a bound |
| `split.skipped.no_demand` | count | decisions | split | no idle worker (hunger <= 0) |
| `split.skipped.outranked` | count | decisions | split | larger ranges took the demand |
| `split.skipped.not_continuing` | count | decisions | split | the range's last page did not continue |
| `child_empty.lower` | count | ranges | split | lower split children that listed no key |
| `child_empty.upper` | count | ranges | split | upper split children that listed no key |
| `resumed_ranges` | count | ranges | resume | ranges a resume started with: (K, ⊤] |
| `max_active` | max | ranges |  | peak ranges in Expand at once |
| `max_queued` | max | ranges | split | peak ranges queued for a worker |
| `busy_ns` | count | ns |  | sum of Expand wall time |
| `sink_wait_ns` | count | ns |  | time Expand spent handing rows to the order stage (backpressure) |
| `requeues` | count | requests |  | pages asked again after the client's retries were exhausted (the run never fails on that) |
| `split.skipped.remaining_floor` | count | decisions | floors | bounded range with at most 4 pages of keys left by its estimate: not split |
| `split.skipped.tail_floor` | count | decisions | floors | the child past the pivot would realize at most 2 pages of keys: not split |
| `structure.requests` | count | requests | structure | delimiter probes (max-keys 32) after a cursor, one per folder level; also counted in requests |
| `structure.zero_fanout` | count | probes | structure | probes that found no folder at any level |
| `structure.no_boundary` | count | probes | structure | probes whose folders all lay outside the range |
| `structure.capped` | count | probes | structure | probes whose chosen level filled its page (the furthest folder is the pivot) |
| `structure.suppressed` | count | decisions | structure | probes not sent: 8 in a row found no folder in this range |
| `structure.late` | count | decisions | structure | split decisions taken while the range's probe was still in flight |
| `split.pivot_structure` | count | pivots | structure | pivots on a folder boundary from a structure probe (else midpoint, density or reflection) |
| `empty_pages` | count | pages |  | continuing pages with no entry, followed by their token (also counted in requests) |
| `prefixes_normalized` | count | pages |  | pages whose common prefixes came unsorted or repeated, sorted and deduplicated |
| `bound_drops` | count | keys |  | keys equal to an inclusive start bound and common prefixes at or below it, listed again and dropped (not in overfetch_keys) |
| `mutex.wait_ns` | count | ns |  | time goroutines spent waiting for the engine's decision mutex (summed over goroutines) |
| `mutex.max_hold_ns` | max | ns |  | the longest single hold of the engine's decision mutex |
| `attempts` | count | requests |  | HTTP requests sent: every attempt, client retries and engine requeues included (the request economics) |
| `retries` | count | requests |  | attempts repeated after a retryable failure |
| `throttles` | count | requests |  | attempts answered 429/503/SlowDown, retried or not (the last of an exhausted call included) |
| `timeouts` | count | requests |  | attempts past -request-timeout, retried or not |
| `parse_failures` | count | responses |  | 200 answers whose body did not parse (retried) |
| `inflight.max_active` | max | requests |  | most attempts in flight at once (admitted, send to body read) |
| `inflight.busy_ns` | count | ns |  | attempts' time in flight, summed: over the listing's wall time, the mean in flight |
| `limiter.decreases` | count | steps | limiter | limit multiplied by 0.7 on a 429/503/SlowDown answer |
| `limiter.floor_throttles` | count | answers | limiter | 429/503/SlowDown answers at a limit of 1: no decrease and no fresh 10 s cool-down |
| `limiter.increases` | count | steps | limiter | limit raised by one (at most one per second, after 10 s without a throttle) |
| `limiter.growth_cooldown` | count | steps | limiter | due growth steps held back: a throttle answer in the last 10 s |
| `limiter.growth_latency_frozen` | count | steps | limiter | due growth steps held back: successful attempts' latency EWMA above twice the baseline |
| `limiter.growth_timeout_frozen` | count | steps | limiter | due growth steps held back: 3 or more attempt timeouts in the last 10 s |
| `limiter.valve` | count | steps | limiter | limit raised by one through the latency freeze (at most one per 30 s, with progress) |
| `limiter.waits` | count | attempts | limiter | attempts that waited for a permit |
| `limiter.wait_ns` | count | ns | limiter | time those attempts waited, summed |
| `limiter.max_depth` | max | permits | limiter | most permits the limit stood below the worker count |
| `slow_start.doubles` | count | steps | slow_start | --limiter-slow-start: limit doubled (from 4, at most once per second, before the first throttle or timeout) |
| `slow_start.ramp_ns` | max | ns | slow_start | --limiter-slow-start: time from the start to the ramp's end (the worker count reached, or the first throttle or timeout) |
| `slow_start.end_limit` | max | permits | slow_start | --limiter-slow-start: the limit the ramp ended at (0: still ramping) |
| `rate.waits` | count | attempts | request_rate | attempts that waited for a --request-rate token |
| `rate.wait_ns` | count | ns | request_rate | time those attempts waited, summed |
<!-- s3:end -->

The summary also carries `anonymous`, `credentials`, `region` and `mean_active` (time-weighted active
ranges), which are not counters.
