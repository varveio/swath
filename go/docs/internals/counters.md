# Counters

Every algorithm path counts its engagements. A provider declares its counters once, as a table of
`counters.Def{Key, Kind, Unit, Help}` backed by one `atomic.Int64` each (package `internal/counters`). The run
summary renders the table by iterating it (`_swath_summary.json` → `go.s3`), and the table below is
generated from the same definitions (`UPDATE=1 go test ./internal/source/s3 -run TestCountersDoc`). A new
path costs one row and its increment. Kinds: `count` is a monotonic sum, `max` a high-water mark, `flag`
a boolean.

**Off, absent and zero are different.** A counter that measures a policy names it (`Feature`). When the run
turns that policy off, its counters are left out and the policy is listed under `disabled` in the same
object (`TestSummaryDistinguishesOffFromZero`):
- absent, and not listed: this build or provider has no such counter;
- absent, and listed under `disabled`: the policy was off for this run (for S3: `seed` without `-seed` or on
  resume, `split` and `probe` with `-sequential`, `probe` with `-no-probe`, `resume` on a fresh run,
  `max_keys` without `-max-keys`);
- present and zero: the policy was on and never fired.

The summary's top level follows the same rule: `spill` is an object that is absent with `-no-spill`
(listed under the top-level `disabled`), and `encoder_wait_ns` is absent without output.

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
| `pages` | count | pages |  | ListObjectsV2 pages processed (and the seed page) |
| `requests` | count | requests |  | ListObjectsV2 calls, retries not counted |
| `attempts` | count | requests |  | HTTP requests, retries included |
| `retries` | count | requests |  | attempts repeated after a retryable failure |
| `throttles` | count | requests |  | of the retries, 429/503/SlowDown answers |
| `timeouts` | count | requests |  | of the retries, attempts past -request-timeout |
| `parse_failures` | count | responses |  | 200 answers whose body did not parse (retried) |
| `overfetch_keys` | count | keys |  | keys listed past a range's hi (or at or below a resume key) and dropped |
| `truncated_at_max_keys` | flag |  | max_keys | -max-keys stopped the listing; the dataset is incomplete |
| `seed.prefixes` | count | prefixes | seed | common prefixes on the seed page |
| `seed.cuts` | count | cuts | seed | cut points the seed tiled the scope at |
| `seed.unsafe` | count | prefixes | seed | prefixes dropped as unusable start-after values |
| `seed.subsampled` | flag |  | seed | more than min(1000, 4×workers) usable prefixes: cuts were subsampled |
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
| `split.skipped.no_demand` | count | decisions | split | no idle worker (hunger <= 0) |
| `split.skipped.outranked` | count | decisions | split | larger ranges took the demand |
| `split.skipped.not_continuing` | count | decisions | split | the range's last page did not continue |
| `child_empty.lower` | count | ranges | split | lower split children that listed no key (the cost of not probing) |
| `child_empty.upper` | count | ranges | split | upper split children that listed no key |
| `resumed_ranges` | count | ranges | resume | tiles of (K, ⊤] a resume started with |
| `max_active` | max | ranges |  | peak ranges in Expand at once |
| `max_queued` | max | ranges | split | peak ranges queued for a worker |
| `busy_ns` | count | ns |  | sum of Expand wall time |
| `sink_wait_ns` | count | ns |  | time Expand spent handing rows to the order stage (backpressure) |
| `probe.requests` | count | requests | probe | 1-key probes (max-keys=1 after a pivot), also counted in requests |
| `probe.upper_empty` | count | probes | probe | probes that found no key past the pivot: an empty upper child avoided |
| `probe.moved` | count | pivots | probe | pivots moved to the midpoint of (cursor, pivot] after an empty probe |
| `split.rejected.probe_empty` | count | decisions | probe | splits skipped: probes found no key past any pivot tried |
<!-- s3:end -->

The summary also carries `anonymous`, `credentials`, `region` and `mean_active` (time-weighted active
ranges), which are not counters.
