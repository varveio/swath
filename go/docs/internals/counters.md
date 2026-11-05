# Counters

Every algorithm path counts its engagements. A provider declares its counters once, as a table of
`counters.Def{Key, Kind, Unit, Help}` backed by one `atomic.Int64` each (package `internal/counters`). The run
summary renders the table by iterating it (`_swath_summary.json` → `go.s3`), and the table below is
generated from the same definitions (`UPDATE=1 go test ./internal/source/s3 -run TestCountersDoc`). A new
path costs one row and its increment. Kinds: `count` is a monotonic sum, `max` a high-water mark, `flag`
a boolean.

## S3 provider (`internal/source/s3.Counters`)

<!-- s3:begin -->
| Key | Kind | Unit | Meaning |
|---|---|---|---|
| `objects` | count | objects | objects listed (emitted rows) |
| `bytes` | count | bytes | sum of listed object sizes |
| `pages` | count | pages | ListObjectsV2 pages processed (and the seed page) |
| `requests` | count | requests | ListObjectsV2 calls, retries not counted |
| `attempts` | count | requests | HTTP requests, retries included |
| `retries` | count | requests | attempts repeated after a retryable failure |
| `throttles` | count | requests | of the retries, 429/503/SlowDown answers |
| `timeouts` | count | requests | of the retries, attempts past -request-timeout |
| `parse_failures` | count | responses | 200 answers whose body did not parse (retried) |
| `overfetch_keys` | count | keys | keys listed past a range's hi (or at or below a resume key) and dropped |
| `truncated_at_max_keys` | flag |  | -max-keys stopped the listing; the dataset is incomplete |
| `seed.skipped` | flag |  | no seed: the root was (⊥, ⊤] |
| `seed.prefixes` | count | prefixes | common prefixes on the seed page |
| `seed.cuts` | count | cuts | cut points the seed tiled the scope at |
| `seed.unsafe` | count | prefixes | prefixes dropped as unusable start-after values |
| `seed.subsampled` | flag |  | more than min(1000, 4×workers) usable prefixes: cuts were subsampled |
| `seed.truncated` | flag |  | the seed page was truncated |
| `seed.sentinel` | flag |  | the closing sentinel (pk, prefixCeil(pk)] was added |
| `seed.sentinel_declined.truncated` | count | decisions | sentinel declined: the seed page was truncated |
| `seed.sentinel_declined.object_last` | count | decisions | sentinel declined: the page ended with an object |
| `seed.sentinel_declined.no_ceiling` | count | decisions | sentinel declined: no prefix ceiling |
| `seed.sentinel_declined.unsafe` | count | decisions | sentinel declined: the ceiling is not a safe key |
| `split.considered` | count | decisions | split decisions taken with demand and rank |
| `split.pivot_extrapolated` | count | pivots | pivots from page density (else midpoint or reflection) |
| `split.accepted_bounded` | count | splits | splits of bounded ranges |
| `split.accepted_frontier` | count | splits | splits of the open frontier |
| `split.rejected.cursor` | count | decisions | guard: pivot not above the cursor |
| `split.rejected.bound` | count | decisions | guard: pivot not below hi (or the scope ceiling) |
| `split.rejected.done` | count | decisions | guard: the range had ended |
| `split.rejected.no_pivot` | count | decisions | guard: no safe pivot exists |
| `split.rejected.unsafe_cursor` | count | decisions | guard: the cursor is not a usable start-after |
| `split.skipped.no_demand` | count | decisions | no idle worker (hunger <= 0) |
| `split.skipped.outranked` | count | decisions | larger ranges took the demand |
| `split.skipped.not_continuing` | count | decisions | the range's last page did not continue |
| `child_empty.lower` | count | ranges | lower split children that listed no key (the cost of not probing) |
| `child_empty.upper` | count | ranges | upper split children that listed no key |
| `resumed_ranges` | count | ranges | tiles of (K, ⊤] a resume started with |
| `max_active` | max | ranges | peak ranges in Expand at once |
| `max_queued` | max | ranges | peak ranges queued for a worker |
| `busy_ns` | count | ns | sum of Expand wall time |
| `sink_wait_ns` | count | ns | time Expand spent handing rows to the order stage (backpressure) |
<!-- s3:end -->

The summary also carries `anonymous`, `credentials`, `region` and `mean_active` (time-weighted active
ranges), which are not counters.
