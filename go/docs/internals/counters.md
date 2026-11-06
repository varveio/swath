# Counters

Every algorithm path counts its engagements. A provider declares its counters once, as a table of
`counters.Def{Key, Kind, Unit, Help, Feature}` indexed by a typed `counters.ID` and backed by one
`atomic.Int64` each (package `internal/counters`). That table is the one representation: the provider reads
it out as one immutable `counters.Snapshot` (code reads `Get(id)`; there is no parallel struct of fields),
and the run report renders the snapshot by iterating the table (`run.json` →
`source.stats.counters`: `features` and `values`, flat stable names), and the table below is
generated from the same definitions (`UPDATE=1 go test ./internal/source/s3 -run TestCountersDoc`). A new
path costs one row and its increment. Kinds: `count` is a monotonic sum, `max` a high-water mark, `flag`
a boolean.

**Feature state is separate from engagement; off, absent and zero are different.** A counter that measures
a policy names it (`Feature`). `features` maps every such policy to whether this run had it on; the values of
an off policy are left out (`TestSummaryDistinguishesOffFromZero`):
- absent, no feature: this build or provider has no such counter;
- absent, feature `false`: the policy was off for this run (for S3: `seed` without `-seed` or on resume,
  `split` and `probe` with `--sequential`, `probe` without `--probe`, `floors` without `--floors`, `structure` without `--structure` (all `swath diag list` flags), `resume` on a fresh run, `max_keys`
  without `-max-keys`, `limiter` with `--limiter=false`, a library caller's zero `Policy` or a supplied client);
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
| `requests` | count | requests |  | listing requests, retries not counted |
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
| `child_empty.lower` | count | ranges | split | lower split children that listed no key (the cost of not probing) |
| `child_empty.upper` | count | ranges | split | upper split children that listed no key |
| `resumed_ranges` | count | ranges | resume | ranges a resume started with: (K, ⊤] |
| `max_active` | max | ranges |  | peak ranges in Expand at once |
| `max_queued` | max | ranges | split | peak ranges queued for a worker |
| `busy_ns` | count | ns |  | sum of Expand wall time |
| `sink_wait_ns` | count | ns |  | time Expand spent handing rows to the order stage (backpressure) |
| `probe.requests` | count | requests | probe | 1-key probes (max-keys=1 after a pivot), also counted in requests |
| `probe.upper_empty` | count | probes | probe | probes that found no key past the pivot: an empty upper child avoided |
| `probe.moved` | count | pivots | probe | pivots moved to the midpoint of (cursor, pivot] after an empty probe |
| `split.rejected.probe_empty` | count | decisions | probe | splits skipped: probes found no key past any pivot tried |
| `requeues` | count | requests |  | pages asked again after the client's retries were exhausted (the run never fails on that) |
| `split.skipped.remaining_floor` | count | decisions | floors | bounded range with at most 4 pages of keys left by its estimate: not split |
| `split.skipped.tail_floor` | count | decisions | floors | the child past the pivot would realize at most 2 pages of keys: not split |
| `seed.probes` | count | requests | seed | probes the seed descent asked for (a probe asked twice is fetched once; also in requests) |
| `seed.probes_unused` | count | requests | seed | probes fetched ahead that the descent did not ask for |
| `seed.radix_bands` | count | cuts | seed | cuts synthesized after a dense flat folder ('!'..'~' without '%') |
| `seed.top_paginated` | flag |  | seed | the top level was truncated and a second top page was read |
| `seed.mass_weighted` | flag |  | seed | cuts were subsampled by sampled mass, not evenly |
| `seed.sampled_levels` | count | levels | seed | ambiguous truncated folders sampled (up to 3 children each) |
| `seed.explosions_confirmed` | count | levels | seed | sampled folders found one object per subfolder: left whole |
| `seed.heavy_prior` | count | levels | seed | ambiguous folders classified from earlier samples (sample budget spent) |
| `seed.frontier_continued` | count | levels | seed | the walk went on past a truncated folder |
| `seed.flat_trivial` | flag |  | seed | no folders and a complete top page: one range |
| `seed.sentinel_declined.no_prefix` | count | decisions | seed | sentinel declined: the top level has no folder |
| `seed.sentinel_declined.no_greatest_item` | count | decisions | seed | sentinel declined: the top probe returned nothing |
| `seed.sentinel_declined.no_spare_slot` | count | decisions | seed | sentinel declined: the cuts already fill min(1000, 4×workers) |
| `seed.sentinel_declined.not_above_last_cut` | count | decisions | seed | sentinel declined: the ceiling is not above the last cut |
| `seed.levels.narrow` | count | levels | seed | folders listed whole in one page: their subfolders are cuts and are walked |
| `seed.levels.flat_wide` | count | levels | seed | truncated folders of objects only |
| `seed.levels.fanout_tiled` | count | levels | seed | truncated folders of key=value partitions: up to workers of them are cuts |
| `seed.levels.heavy_cut_banded` | count | levels | seed | truncated folders sampled heavy: up to workers subfolders are cuts |
| `seed.levels.tiny_leaf_explosion` | count | levels | seed | truncated folders of one-object subfolders: left whole |
| `seed.levels.dense_root_radix_banded` | count | levels | seed | the dense flat folder cut into radix bands |
| `seed.levels.delimiter_seeded` | count | levels | seed | a complete top level whose folders seeded the tiles (nothing else engaged) |
| `structure.requests` | count | requests | structure | delimiter probes (max-keys 32) after a cursor, one per folder level; also counted in requests |
| `structure.zero_fanout` | count | probes | structure | probes that found no folder at any level |
| `structure.no_boundary` | count | probes | structure | probes whose folders all lay outside the range |
| `structure.capped` | count | probes | structure | probes whose chosen level filled its page (the furthest folder is the pivot) |
| `structure.suppressed` | count | decisions | structure | probes not sent: 8 in a row found no folder in this range |
| `structure.late` | count | decisions | structure | split decisions taken while the range's probe was still in flight |
| `split.pivot_structure` | count | pivots | structure | pivots on a folder boundary from a structure probe (else midpoint, density or reflection) |
| `empty_pages` | count | pages |  | continuing pages with no entry, followed by their token (also counted in requests) |
| `seed.top_paginated_declined.unsafe` | count | probes | seed | top-up probes not sent: the top page's last entry is not a bound the provider accepts |
| `prefixes_normalized` | count | pages |  | pages whose common prefixes came unsorted or repeated, sorted and deduplicated |
| `bound_drops` | count | keys |  | keys equal to an inclusive start bound and common prefixes at or below it, listed again and dropped (not in overfetch_keys) |
| `seed.short_continuing_estimated` | count | samples | seed | continuing seed samples shorter than a nominal page, weighed (or classed dense) as a full page: an estimate |
| `attempts` | count | requests |  | HTTP requests, retries included |
| `retries` | count | requests |  | attempts repeated after a retryable failure |
| `throttles` | count | requests |  | of the retries, 429/503/SlowDown answers |
| `timeouts` | count | requests |  | of the retries, attempts past -request-timeout |
| `parse_failures` | count | responses |  | 200 answers whose body did not parse (retried) |
| `limiter.decreases` | count | steps | limiter | limit multiplied by 0.7 on a 429/503/SlowDown answer |
| `limiter.floor_throttles` | count | answers | limiter | 429/503/SlowDown answers at a limit of 1: no decrease and no fresh 10 s cool-down |
| `limiter.increases` | count | steps | limiter | limit raised by one (at most one per second, after 10 s without a throttle) |
| `limiter.growth_cooldown` | count | steps | limiter | due growth steps held back: a throttle answer in the last 10 s |
| `limiter.growth_latency_frozen` | count | steps | limiter | due growth steps held back: successful attempts' latency EWMA above twice the baseline |
| `limiter.growth_timeout_frozen` | count | steps | limiter | due growth steps held back: 3 or more attempt timeouts in the last 10 s |
| `limiter.valve` | count | steps | limiter | limit raised by one through the latency freeze (at most one per 30 s, with progress) |
| `limiter.waits` | count | attempts | limiter | attempts that waited for a permit |
| `limiter.wait_ns` | count | ns | limiter | time those attempts waited, summed |
| `limiter.depth` | max | permits | limiter | most permits the limit stood below the worker count |
<!-- s3:end -->

The summary also carries `anonymous`, `credentials`, `region` and `mean_active` (time-weighted active
ranges), which are not counters.
