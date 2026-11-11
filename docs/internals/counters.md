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
- absent, feature `false`: the policy was off for this run (for S3: `seed` with `swath diag list --no-seed` or `--sequential`,
  `split` with `--sequential`, `structure` with `--structure=off` (`--sequential`
  and `--structure` are `swath diag list` flags; a library caller's zero `Options` has the seed and
  the probes off), `resume` on a fresh run, `max_keys`
  without `--max-keys`, `limiter` with `--no-adaptive-limit`, a library caller's zero `Admission` or a supplied client, `slow_start` without
  `--adaptive-limit-slow-start` or without the limiter, `request_rate` without `--request-rate`);
- present and zero: the policy was on and never fired.

The report's `resources` follow the same rule: `features.spill` is false and the `spill` object absent with
`swath diag list --no-spill` or a count, `features.order` is false for `swath du` and `swath diag count` (rows counted, not
ordered), and `encoder_wait_ns` is absent without output. `spill.durable` (the durable spill's account: captures, seals,
abandons by reason, retirements, retained bytes, and on a resume `resume` with what it accepted, rejected and
replayed; [ordering.md](ordering.md#durable-spill)) is absent when durable spill was off, and
`spill.durable_off` then says why when it could have run (a version 7 journal, a spill budget below 16 MiB, a
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
`child_empty.lower` + `child_empty.upper`.

## Order stage and encoders (run summary, top level)

| Key | Unit | Meaning |
|---|---|---|
| `frontier_wait_ns` | ns | the drainer waited for the range it needs next while a worker was still listing it (the slow-frontier cost) |
| `budget_wait_ns` | ns | emitters, and workers with ranges queued, waited on a full reorder budget (the reorder cost); summed over goroutines |
| `encoder_wait_ns` | ns | the order stage waited for an encoder: a full part queue, or every encoder busy when a part opened |
| `spill.rows`, `spill.bytes`, `spill.ranges` | rows, bytes, ranges | rows written to spill and replayed, bytes written, ranges that spilled |
| `spill.frames`, `spill.opens` | frames, opens | row frames written (a file's header, capture and trailer frames excluded); spill file opens attempted, failed ones included: one per frame written, one per read handle miss (a frame read at an offset, a trailer read or a reader whose file had no open handle), seals and recovered certificates' replays (a resume's validation of the recorded certificates is not counted). Against `spill.ranges`, the per-file and per-frame cost of many small spills |
| `spill.read_handles.hits`, `.misses`, `.raced` | leases | reads of a spill file (a frame at an offset, a trailer, the drainer's reader; not a recovered certificate's replay) served by the file's open descriptor, and those that opened it (one open each, two when the first is refused with EMFILE/ENFILE); `raced`: misses whose open lost to a concurrent one for the same file (closed at once). `hits / (hits + misses)` is the reuse rate; a range that spills one frame can at best share its frame read with its trailer read |
| `spill.read_handles.evictions.capacity`, `.retired`, `.truncated`, `.low_fd`, `.closed` | handles | handles closed out of the cache: idle past the cap (`order.MaxSpillReaders` descriptors, least recently used first), the file drained (removed, or handed back to the durable cache), the file truncated (an ENOSPC rollback), idle when an open failed for want of descriptors (the cache then keeps none for the rest of the run), cached when the run stopped or its output's identity check failed (a leased one stays usable until its release; retirement does not revoke reads) |
| `spill.read_handles.peak_open` | descriptors | the most spill read descriptors the cache held open at once, leased or idle (opens under way not counted; `spill.peak_readers` adds certificate replays). `order.MaxSpillReaders` is the idle-retention target: with every handle leased (more segment workers than 16) it is exceeded |
| `spill.frame_bytes`, `spill.frame_rows` | bytes, rows | row frames written by size (`p50`, `p90`, `p99`: the bound of the power-of-two bucket holding the quantile, so the quantile is at most it and more than half of it; `max`: exact). `spill.rows / spill.frames` is the mean |
| `spill.logical_bytes`, `spill.format` | bytes, name | the spilled rows' logical size (key and column values, as the part router counts them; `spill.bytes` against it is what the frame coding saved) and the frames written: `delta+zstd` (the default), `delta` (`--no-spill-compress`), `plain+zstd` or `plain` (`--spill-plain`) |
| `spill.decode_refused` | frames | frames refused at read for decoding past their declared storage (a malformed frame; the run fails with it) |
| `spill.peak_bytes`, `spill.refused` | bytes, frames | peak live spill bytes; frames the disk budget refused (the emitter waited instead) |
| `spill.dir`, `spill.fs_type` | path, name | the spill directory and its filesystem (Linux: `tmpfs`, `ext4`, `xfs`, ..., or the statfs magic in hex; absent elsewhere): on `tmpfs` or `ramfs` spill is memory outside `peak_rss_bytes`, and the run warns at start |
| `peak_ahead_rows` | rows | the most rows listed and not yet handed on at once: ahead of the publish frontier, in memory or spilled (live: `progress.json` `order.ahead_rows`) |
| `frontier_stuck_max_ns` | ns | the longest interval in which the order stage handed nothing on: from the drain's start to the first row, between two hand-ons, or from the last row to the drain's end, whether it ends in success, cancellation or an error |
| `timings.listing_done_at` | time | the provider finished: every range expanded, none in flight (absent if it never did); `listing_finished_at` is the order stage's end, after the drain |
| `drain.tail_ns` | ns | from `listing_done_at` to the drain's last row: the part of the run in which only the drain was left |
| `drain.worker_idle_after_listing_done_ns` | ns | the workers' idle time after `listing_done_at` (nothing queued to list, read-ahead waits included), summed over workers |
| `drain.replay_read_ns`, `drain.replay_decode_ns` | ns | the drainer reading spilled frames itself, and the part of it spent decoding (trailers included); the drainer's times accumulate over the whole run, not the tail alone |
| `drain.ahead_wait_ns`, `drain.order_check_ns`, `drain.sink_write_ns` | ns | the drainer waiting for a frame the read-ahead was reading; the stream-wide order guard; handing batches to the sink (`encoder_wait_ns` included) |
| `drain.read_ahead.frames`, `drain.read_ahead.rows` | frames, rows | spilled frames idle workers read and decoded ahead of the drainer (feature: `spill`) |
| `drain.read_ahead.drainer_frames`, `drain.read_ahead.unplanned_frames` | frames | frames planned and taken over unread by the drainer; frames it reached with nothing planned (both read by the drainer; with `frames`, every frame spilled, each once) |
| `drain.read_ahead.drainer_waits`, `drain.read_ahead.capped_waits` | waits | the drainer waiting for a frame being read; workers waiting at the read-ahead's share of the row budget |
| `drain.read_ahead.read_ns`, `drain.read_ahead.decode_ns` | ns | the workers' read-ahead time, and the part of it spent decoding |
| `drain.read_ahead.max_reads` | reads | reads allowed at once: 8, at most `GOMAXPROCS` - 1; 0 is off (one processor: the drainer replays alone) |
| `drain.segments.reason` | reason | the segment-parallel replay after listing: `engaged`, or why not: `one_processor` (GOMAXPROCS 2 or less), `one_encoder`, `single_part`, `not_planned` (the router's rule not stated before the drain ended), `row_budget` (the row budget holds one segment worker only), `listing_active` (absent without a dataset) |
| `drain.segments.segments`, `.workers`, `.rows`, `.max_rows`, `.seam_checks` | parts, workers, rows, rows, seams | segments written (one part each), the workers writing them, their rows and the largest segment's, the seams checked |
| `drain.segments.time_ns`, `.max_time_ns`, `.plan_ns` | ns | segments' time summed (open to close) and the longest; cutting the rest into segments |
| `drain.segments.read_ns`, `.decode_ns`, `.write_ns`, `.max_workers` | ns, workers | of `time_ns`: spill frames read (and decoded), batches handed to the parts (encoder queue waits included); the workers' cap (`listing.segmentWorkers`) |
| `drain.segments.durable_files_read`, `.durable_boundaries` | files, boundaries | durable spill files (`order.Cache`) whose frames the segments read, and the segment boundaries that fall inside one of their frames |
| `drain.segments.deferred_rows` | rows | versions schemas: rows a planned segment boundary moved past because they continued the group of the key before it (no part ends inside a key) |
| `drain.segments.tree_bytes` | bytes | what the tree held when the segments were planned: not the segments' budget, which they take beside it (two budgets in all, at most) |
| `drain.segments.workers_by`, `.per_worker_bytes`, `.free_bytes` | reason, bytes, bytes | what set the workers: `encoders`, `processors`, `descriptors`, `row_budget` or `segments`; the budget one worker holds at most (its largest unit, a gathered and an encoding batch, its part's queue, at the heaviest run of consecutive rows), and the budget the segments could take when planned (the row budget less what the encoders' queues held) |
| `drain.segments.budget_waits`, `.budget_wait_ns`, `.max_held_bytes` | waits, ns, bytes | workers waiting for the row budget before reading or gathering a unit, and their time; the most the segments held at once beside the tree (the encoders' queues, the units being read or gathered) |
| `drain.encoders_at_start`, `drain.encoders_after_listing`, `drain.encoder_admission` | encoders, reason | Parquet encoders admitted at the start and once listing ended (`listing.admitAfterListing`); why: `listing_done`, `explicit` (`--encoders` given), `at_cap`, `not_reached` (absent without a dataset) |
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

A count (`swath du`, `swath diag count`) does not order (`features.order` false): `max_buffered_bytes`, `peak_ahead_rows`
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
| `seed.from.tiles` | count | ranges | seed_from | ranges the prior tiled the scope into, the open frontier after its last cut included (at most min(1000, 4×workers)) |
| `seed.from.expected_keys` | count | keys | seed_from | keys the prior expected in its bounded tiles (0 without counts): against objects, the scope's growth |
| `seed.from.error.within_2x` | count | ranges | seed_from | prior tiles whose ranges listed within 2x of the keys the prior expected (a tile's descendants included) |
| `seed.from.error.grown` | count | ranges | seed_from | prior tiles whose ranges listed more than 2x the keys the prior expected: the segment grew (past its count the gauge estimates its remainder, and splits share it out) |
| `seed.from.error.shrunk` | count | ranges | seed_from | prior tiles whose ranges listed less than half the keys the prior expected: the segment shrank or emptied |
| `seed.from.error.max_grown` | max | x | seed_from | the largest ratio of a prior tile's listed keys to the prior's expectation, rounded |
| `seed.from.page_size` | max | keys | seed_from | the page size a counted prior set (the provider's largest; 0: not engaged) |
| `seed.from.tile_pages` | max | pages | seed_from | the target pages per counted-prior tile; a coarse first cut can exceed the target (0: not engaged) |
| `seed.from.held_pages` | count | pages | seed_from | range pages after which the prior's count (its expected keys less those listed) stayed the range's remainder estimate instead of the page's gauge |
| `seed.from.outrun` | count | ranges | seed_from | ranges that listed their prior's count and went on: the page's gauge estimates their remainder from there |
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
| `max_active` | max | ranges |  | peak ranges in Expand at once |
| `max_queued` | max | ranges | split | peak ranges queued for a worker |
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
| `page.grown` | count | requests | page_policy | range requests that asked for more than the nominal page size (--page-growth or an oracle) |
| `page.trimmed` | count | requests | page_policy | range requests that asked for less than the nominal page size (an oracle) |
| `page.asked_keys` | count | keys | page_policy | page sizes the range requests asked for, summed |
| `page.growth_reset` | count | requests | page_policy | range requests back at the nominal size after a grown one: a short page, or too few keys estimated left |
| `attempts` | count | requests |  | HTTP requests sent: every attempt, client retries and engine requeues included (the request economics) |
| `retries` | count | requests |  | attempts repeated after a retryable failure |
| `throttles` | count | requests |  | attempts answered 429/503/SlowDown, retried or not (the last of an exhausted call included) |
| `timeouts` | count | requests |  | attempts past -request-timeout, retried or not |
| `parse_failures` | count | responses |  | 200 answers whose body did not parse (retried) |
| `inflight.max_active` | max | requests |  | most attempts in flight at once (admitted, send to body read) |
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
| `limiter.max_depth` | max | permits | limiter | most permits the limit stood below the worker count |
| `slow_start.doubles` | count | steps | slow_start | --adaptive-limit-slow-start: limit doubled (from 4, at most once per second, before the first cut or timeout) |
| `slow_start.ramp_ns` | max | ns | slow_start | --adaptive-limit-slow-start: time from the start to the ramp's end (the worker count reached, or the first cut or timeout) |
| `slow_start.end_limit` | max | permits | slow_start | --adaptive-limit-slow-start: the limit the ramp ended at (0: still ramping) |
| `rate.waits` | count | attempts | request_rate | attempts that waited for a --request-rate token |
| `rate.wait_ns` | count | ns | request_rate | time those attempts waited, summed |
<!-- s3:end -->

Every object-store attempt is normalized once into an error carrying transient, throttle and timeout
signals, HTTP status, provider code or reason, and a safe cause. Admission feedback, retries and these
counters consume that same value; parse accounting uses its `parse` diagnostic. Exhaustion wraps it,
and the engine requeues from its transient signal rather than a report label. Credential failures can
supply timeout/retry signals before any listing request is sent, so those counters need not be bounded
by `attempts`.

The summary also carries `anonymous`, `credentials`, `region` and `mean_active` (time-weighted active
ranges), which are not counters.
