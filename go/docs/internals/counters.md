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
a boolean, `gauge` a value derived when the provider snapshots (the attempt latency percentiles). Names: a dotted group, a snake_case leaf; high-water marks start with `max_`, durations end in `_ns`.

**Feature state is separate from engagement; off, absent and zero are different.** A counter that measures
a policy names it (`Feature`). `features` maps every such policy to whether this run had it on; the values of
an off policy are left out (`TestSummaryDistinguishesOffFromZero`):
- absent, no feature: this build or provider has no such counter;
- absent, feature `false`: the policy was off for this run (for S3: `seed` with `--seed=false` or `--sequential`,
  `split` with `--sequential`, `floors` without `--floors`, `structure` with `--structure=off` (`--sequential`,
  `--floors` and `--structure` are `swath diag list` flags; a library caller's zero `Options` has the seed and
  the probes off), `resume` on a fresh run, `max_keys`
  without `-max-keys`, `limiter` with `--limiter=false`, a library caller's zero `Admission` or a supplied client, `slow_start` without
  `--limiter-slow-start` or without the limiter, `request_rate` without `--request-rate`);
- present and zero: the policy was on and never fired.

The report's `resources` follow the same rule: `features.spill` is false and the `spill` object absent with
`swath diag list --no-spill` or `--count-only`, `features.order` is false for `--count-only` (rows counted, not
ordered), and `encoder_wait_ns` is absent without output.

Request accounting uses `attempts` (HTTP requests, retries included) as the denominator; `requests` counts
logical calls.

Attempt latency is kept in the client as a fixed log-spaced histogram (four buckets per power of two of
nanoseconds, up to 2^40 ns; one atomic per bucket) and reported as three derived values, `inflight.p50_ns`,
`inflight.p90_ns` and `inflight.p99_ns`: each the upper edge of the bucket holding the percentile, at most 25 %
above it. The buckets themselves are not counters: 156 rows would swamp the report for three numbers a reader
uses. Every attempt is timed from send to body read, failed ones included (a timed-out attempt lands at
`--request-timeout`).

Derived readings of the range engine's counters: the mean leaf depth is `split.leaf_depth_sum` /
`split.leaves`; the work a frontier split hands out is `split.frontier_handout_keys` /
`split.accepted_frontier`; the `split.pivot.*` counters sum to the accepted splits, and the
`child_empty.by.*` counters (and separately `child_empty.hi.pivot` + `child_empty.hi.bound`) to
`child_empty.lower` + `child_empty.upper`.

## Order stage and encoders (run summary, top level)

| Key | Unit | Meaning |
|---|---|---|
| `frontier_wait_ns` | ns | the drainer waited for the range it needs next while a worker was still listing it (the slow-frontier cost) |
| `budget_wait_ns` | ns | emitters, and workers with ranges queued, waited on a full reorder budget (the reorder cost); summed over goroutines |
| `encoder_wait_ns` | ns | the order stage waited for an encoder: a full part queue, or every encoder busy when a part opened |
| `spill.rows`, `spill.bytes`, `spill.ranges` | rows, bytes, ranges | rows written to spill and replayed, bytes written, ranges that spilled |
| `spill.logical_bytes`, `spill.format` | bytes, name | the spilled rows' logical size (key and column values, as the part router counts them; `spill.bytes` against it is what the frame coding saved) and the frames written: `delta+zstd` (the default), `delta` (`swath diag list --spill-compress=false`), `plain+zstd` or `plain` (`--spill-plain`) |
| `spill.decode_refused` | frames | frames refused at read for decoding past their declared storage (a malformed frame; the run fails with it) |
| `spill.peak_bytes`, `spill.refused` | bytes, frames | peak live spill bytes; frames the disk budget refused (the emitter waited instead) |
| `spill.dir`, `spill.fs_type` | path, name | the spill directory and its filesystem (Linux: `tmpfs`, `ext4`, `xfs`, ..., or the statfs magic in hex; absent elsewhere): on `tmpfs` or `ramfs` spill is memory outside `peak_rss_bytes`, and the run warns at start |
| `peak_ahead_rows` | rows | the most rows listed and not yet handed on at once: ahead of the publish frontier, in memory or spilled (live: `progress.json` `order.ahead_rows`) |
| `frontier_stuck_max_ns` | ns | the longest interval in which the order stage handed nothing on: from the drain's start to the first row, between two hand-ons, or from the last row to the drain's end |
| `peak_heap_live_bytes` | bytes | the peak live heap while listing (`/gc/heap/live:bytes`, sampled every 100 ms); against `row_budget_bytes` it is the memory the budget does not see, the garbage collector's headroom aside |

A dry run (`--count-only`) does not order (`features.order` false): `max_buffered_bytes`, `peak_ahead_rows`
and `frontier_stuck_max_ns` are 0 and `spill` is absent.

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
| `seed.resumed` | flag |  | seed | the seed tiled a resumed scope (K, ⊤] |
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
| `split.density.refused.alphabet` | count | decisions | split | density step refused (after its halvings): a byte on the page or the cursor is non-ASCII or unsafe ('%', control) |
| `split.density.refused.zero_span` | count | decisions | split | density step refused (after its halvings): the page spans no distance |
| `split.density.refused.past_top` | count | decisions | split | density step refused (after its halvings): the target is past the representable keys |
| `split.density.refused.hole` | count | decisions | split | density step refused (after its halvings): the target is not a string (an end-of-string digit inside it) |
| `split.density.refused.outside` | count | decisions | split | density step refused (after its halvings): the target is not strictly inside (cursor, hi) |
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
| `empty_chain_pages` | count | pages |  | empty continuing pages past the 1,024th of their run: followed, but not progress for the watchdog (also in empty_pages) |
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
| `inflight.p50_ns` | gauge | ns |  | attempt latency (send to body read), 50th percentile: the upper edge of its bucket in a log-spaced histogram, at most 25% above |
| `inflight.p90_ns` | gauge | ns |  | attempt latency (send to body read), 90th percentile: the upper edge of its bucket in a log-spaced histogram, at most 25% above |
| `inflight.p99_ns` | gauge | ns |  | attempt latency (send to body read), 99th percentile: the upper edge of its bucket in a log-spaced histogram, at most 25% above |
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
