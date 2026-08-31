# Metrics and observability

This is the operator reference for a run that needs monitoring or diagnosis. A first
listing needs no telemetry setup; use [Getting started](getting-started.md) and keep the
default terminal progress display.

swath exposes the same run through four progressively deeper surfaces:

| Need | Use |
| --- | --- |
| Watch a terminal run | the progress display, or `-v` structured progress logs |
| Compare completed runs | `_swath_summary.json` or `--report PATH` |
| Feed dashboards and alerts | OTLP metrics via `--metrics-endpoint` |
| Reproduce an engine decision | `--trace PATH`, then `swath-replay` |

Logs always go to stderr; listing data can therefore stream safely on stdout. Use `-v`,
`-vv`, or `-vvv` for INFO, DEBUG, or TRACE logs. `-q` selects ERROR and `-qq` disables
logging; quiet wins if both quiet and verbose flags are present.

## 1. Start with progress and the JSON report

Keep the default progress display for an ordinary run. If the run fails or appears slow,
open `_swath_summary.json` for managed output, or request the same report elsewhere with
`--report PATH`. Start with `complete`, `error_class`, `cost`, `output`, `duration_ms`,
`session_duration_ms`, and `listing_duration_ms` before moving into engine details.

This table provides a first diagnostic pass:

| Evidence | Likely bottleneck | Check next |
| --- | --- | --- |
| High API latency and many requests in flight | Object store or network | `fetch.latency.phase`, throttling, connection-pool waits, recovered errors |
| Low API rate and few requests in flight while listing | Not enough useful ranges, or one long tail | splits, probes, tail occupancy, open-frontier share |
| High `queue.wait` or `emit.latency` | Output formatting or storage | `dataset_writer`, format write/finalize time, output filesystem |
| High checkpoint queue or commit waits | Checkpoint storage | checkpoint latency and `checkpoint.queue.depth` |
| High sort backpressure or many merge passes | Sort memory, disk, or open-file limits | the `sort` block, staging peak, effective fan-in |
| `progress.units` unchanged for more than five minutes | Potential stall | phase, thread dump, last error, disk, and connection-pool state |

The sections below explain the report and logs in detail. Use the
[Micrometer meter reference](#5-metric-export-and-micrometer-reference) when you need
telemetry export or implementation-level diagnosis. For controlled
performance comparisons, continue to the [performance guide](performance.md).

<a id="2-list_run_summary-one-line-at-run-end"></a>

## 2. `list_run_summary` (one line at run end)

With INFO logging (`-v`), swath emits one structured `list_run_summary` record suitable
for log search. It contains the headline result and resource fields, including:

```text
run_id, objects, recovered_objects, duration_ms, listing_duration_ms,
session_duration_ms, strategy, api_calls, cost_usd, output_files,
compressed_size_bytes, keys, pages, peak_in_flight, steals, splits,
errors, keys_per_sec
```

The three clocks have different scopes:

- `session_duration_ms` covers the whole CLI invocation, including initial seeding.
- `duration_ms` starts after seeding and ends after any sorted merge and publication.
- `listing_duration_ms` ends at listing handoff; for sorted output it includes the final
  staging-lane drain, but not the subsequent merge.

`objects` is the complete published dataset. On resume, `recovered_objects` is the part
already durable before this process started. Per-process rates and costs use
`objects - recovered_objects`. For example, listing throughput is:

```text
(objects - recovered_objects) / (listing_duration_ms / 1000)
```

The report's efficiency block adds API calls per 1,000 objects, overfetch, CPU efficiency,
and idle/tail measurements. Use these as comparisons between similar runs, not universal
pass/fail thresholds.

## 3. JSON run-summary artifact

Managed output updates `_swath_summary.json` during the run. `--report PATH` writes the
same schema for other output modes. The file is atomic and machine-readable; its
`schema_version` is the compatibility boundary.

The stable top-level result is organized as follows:

| Fields or block | Meaning |
| --- | --- |
| `schema_version`, `run_id`, `args_hash`, `argv`, `started_at` | Identity and reproducibility. |
| `duration_ms`, `session_duration_ms`, `listing_duration_ms` | The clocks defined in §2. |
| `objects`, `recovered_objects`, `bytes`, `keys_per_sec` | Published size and throughput. |
| `complete`, `exit_code`, `stop_reason`, `stop_source`, `error_class` | Outcome and stop classification. |
| `config`, `engine_flags` | Effective user configuration and engine state. |
| `cost`, `output`, `efficiency`, `sort` | API-cost estimate, sink results, efficiency, and sorted-output details. |
| `recovered_errors` | Transient faults swath handled without failing the run. |
| `engine`, `seed`, `trajectory`, `slow_ranges`, `probe_latency`, `client_cost`, `dataset_writer`, `demand_gate`, `shape` | Deeper scheduling, keyspace, sink, and latency evidence. |
| `meters` | Final generic meter snapshot. |

`complete=true` means the requested dataset was published. On an unsuccessful run,
`stop_source` distinguishes a user stop, deadline, signal, output condition, and internal
failure; `error_class` groups the actionable cause. These fields drive automation more
reliably than parsing prose logs. `stuck` classification is based on the time since
`swath.progress.units` last advanced, with the final phase recorded alongside it.

The schema is additive within a major version: tolerate unknown fields and do not depend
on field order. Dedicated `*_ms` fields use milliseconds. Generic percentile gauges in
`meters` use Micrometer's base unit of seconds.

For page-run staging, the `sort` block exposes the header-scan, router, and encoder work directly.
Whole-page and overlap-component counters show routing shape; service timers show where the
pipeline waited; the decoded-page high-water mark shows retained merge state. Cascade activity is
reported separately by `passes` and the `SORT.merge_pass_cascaded`,
`SORT.cascade_page_whole_merge`, and `SORT.cascade_page_overlap_merge` reasons.

For an alternating sort campaign, `sort.pack_on_fetch_pages`, `segment_bytes`, and `segments` are
cumulative snapshots: their deltas across periodic reports describe packed arrival and segment
production. `backpressure_engaged`, `backpressure_wait_ms`, and
`backpressure_wait_max_ms` describe the sort-lane handoff only; `staging_bytes_peak`,
`handoff_queue_depth_peak`, and `off_thread_buffers_peak` are high-water marks. The report does
not currently echo the configured `sort.buffers` or `sort.segment-bytes` target, and it does not
label generic `swath.queue.wait` as an upstream sort queue, so those must be recorded in the
campaign command rather than inferred from this block.

`sort.arm` identifies the typed entry path: `LIVE_LIST_SORT` means this process performed the
listing, `MERGE_ONLY_PAGE_RUN` is a checkpoint-authorized zero-LIST merge re-entry, and
`PUBLISHED_REENTRY` is a checkpoint-authorized no-op resume that found this run already published
(so it ran neither listing nor merge). It is independent of the retained `merge_only_resume`
compatibility marker. `sort-fixture` has no run summary artifact; its existing stdout result line
is labelled `arm=SORT_FIXTURE` instead.

The `sort` block decomposes terminal work into `finalize_ms`, `finalize_close_ms`,
`local_publication_ms`, `finalize_parallelism`, and `manifest_*` fields. `finalize_ms` is wall
time from the first final close through publication, while `finalize_close_ms` sums per-part
close service across encoders and can exceed that wall span when closes overlap.
`finalize_parallelism` reports the admitted encoder count. Fresh final parts derive
their digest, byte count, row count, and exact bounds while writing; nonzero manifest bounds-read
metrics identify the compatibility path that reopened a carried part.
The same existing final-writer close timer is also exposed as `finalize_close_count`,
`finalize_close_max_ms`, and `finalize_close_p50_ms`/`_p90_ms`/`_p99_ms`; these are service-time
observations, not a second timing surface.

These `sort` fields describe finalization work:

| Fields | Meaning |
| --- | --- |
| `pipeline_pages_forwarded` | Whole pages whose stored maximum is below the next minimum, routed directly to an encoder. |
| `pipeline_cluster_pages`, `pipeline_cluster_rows` | Pages and rows processed through overlap-cluster row merging. |
| `pipeline_router_wait_ms` | Total time the single router waited for the next header reference or for space in the complete-plan queue. |
| `pipeline_header_scan_ms` | Header-read service summed across segment cursors; concurrent service means this can exceed merge wall time. |
| `pipeline_plan_queue_wait_ms` | The subset of router wait caused by a full complete-plan queue. |
| `pipeline_encoder_page_reads` | Positional full-frame page reads performed by encoders. |
| `pipeline_encoder_read_wait_ms` | Positional read and CRC service summed across encoders; concurrent service means this can exceed merge wall time. |
| `pipeline_decoded_page_bytes_peak` | Highest exact retained decoded-page residency observed by any encoder cluster. |
| `pipeline_parts_open` | Open pipeline output writers at the snapshot. |
| `pipeline_router_wait_share`, `pipeline_plan_queue_wait_share`, `pipeline_encoder_read_wait_share` | The corresponding cumulative millisecond value divided by `merge_ms`; zero when `merge_ms` is zero. These are dimensionless service-to-wall ratios, so a summed concurrent encoder ratio can exceed `1.0`. |

The report can contain the target URI, arguments, filter values, slow-range bounds, and
key samples. Treat it as operational data and redact it before sharing.

### Writer-pool details (advanced)

`dataset_writer` is present for unsorted Parquet and parallel directory TSV/JSONL output.
It is absent for stdout, text files, discard, and sorted output, which do not use this
writer pool.

| Fields | Meaning |
| --- | --- |
| `format`, `writer_count`, `total_queue_capacity` | Selected format, writer count, and aggregate queued-batch capacity. |
| `jvm_max_heap_bytes`, `row_group_target_bytes_per_writer`, `row_group_allowance_multiplier`, `planned_heap_bytes`, `heap_admission_applied` | Parquet memory inputs and the result of its safety check. These fields are null for text. |
| `part_rotation_interval_ms`, `part_rotation_max_rows`, `part_digest_*`, `manifest_write_*` | Part creation, checksum work, and the final manifest write. The manifest count is normally zero during listing and one after successful publication. |
| `submit_blocked_*` | Time spent waiting because the selected writer's queue was full. |
| `head_of_line_blocked_*` | The subset of waits that began while another writer had an empty queue and was waiting for work. |
| `lanes[]` | Per-writer queue use, rows, bytes, batches, active time, waits, parts, and finalization activity. |

Lane identifiers appear only in this structured block, not in Micrometer tags. Durations
are elapsed time, not CPU time. For CPU attribution, use the
[advanced JFR procedure](performance.md#retain-a-jfr-cpu-profile).

When `--writeback-size` is enabled, `swath.data_sync.residual.bytes` may be emitted when
a writeback-enabled part closes even if no periodic force was due. Only an actual
successful force emits `swath.data_sync.latency`, `swath.data_sync.bytes`,
`OUTPUT.data_sync`, and the corresponding format meter: `data_sync_text_uncompressed`,
`data_sync_text_compressed`, `data_sync_parquet`, or `data_sync_sorted_parquet`. These measure writeback shaping;
they do not indicate publication or an earlier crash-recovery boundary.

## 4. Progress and logs

One reporter spans seeding, listing, merging, and writing. Its default cadence is 30
seconds, controlled by `--progress-interval` (minimum `1s`). The first update is emitted
early so short runs are not silent.

On an interactive terminal, or with `--progress`, swath redraws a human progress display.
Otherwise the same tick becomes a structured INFO `progress` log record and requires
`-v`. A tick is rendered once, never both ways. The final human summary is separate and
is controlled by `--stats` / `--no-stats`; see
[Using swath](usage.md#progress-and-reports).

Useful markers include startup configuration, first request, first page, phase changes,
and the final result. DEBUG and TRACE logs add engine evidence; they are intended for
short investigations rather than routine high-volume runs.

## 5. Metric export and Micrometer reference

Configure OTLP with `--metrics-endpoint` or `SWATH_OTLP_ENDPOINT`; change its default `5s`
step with `SWATH_OTLP_INTERVAL`. `--no-metrics` disables export. Bucket names are not used
as default tags, avoiding an unbounded-cardinality series.

This is the public instrument index. Counters are cumulative; timers include count, total,
maximum, and configured percentiles; gauges are snapshots. The
[instrumentation reference](internals/metrics-internals.md) owns exact engagement-reason
values and implementation details.

| Area | Meters | What they answer |
| --- | --- | --- |
| Store requests | `swath.api.calls`, `swath.api.latency`, `swath.fetch.latency.phase` | How many LIST attempts ran, how long they took, and where client-visible latency accumulated. |
| Useful work | `swath.entries.emitted`, `swath.bytes.estimated`, `swath.progress.units` | How much output was accepted and whether any phase is still advancing. |
| Scheduling | `swath.workers.active`, `swath.in_flight.avg`, `swath.tail_occupancy.avg_in_flight`, `swath.tail_occupancy.wall_share`, `swath.open_frontier.keys_emitted` | Whether workers stayed supplied and whether an open-ended tail dominated the run. |
| Split and steal | `swath.steals`, `swath.errors`, `swath.steal_reason`, `swath.probe.fetches`, `swath.probe.structure_fetches`, `swath.probe.empty_upper_bisections`, `swath.split.unsplittable_victims`, `swath.split.guard_aborts` | Which work-discovery paths engaged, failed, or proved unproductive. |
| Page shape | `swath.page.raw_count`, `swath.page.raw_keys`, `swath.page.short_truncated` | Whether returned pages were full, sparse, or unexpectedly short. |
| Throttling | `swath.throttle.events`, `swath.aimd.target_reductions`, `swath.aimd.freeze_gate_checks`, `swath.owner_split.demand_gated_t`, `swath.owner_split.demand_gated_t_min` | Whether the store pushed back and how the adaptive target responded. |
| Waits | `swath.queue.wait`, `swath.rate_limit.wait`, `swath.rate_limit.api_wait` | Whether output admission, the concurrency gate, or a configured request-rate cap held work up. |
| Process | `swath.process.memory.rss.bytes`, `swath.process.memory.heap.bytes`, `swath.process.cpu.time` | Live process resource use when the platform exposes it. |
| Idle workers | `swath.idle_backoff.level`, `swath.idle_backoff.resets`, `swath.idle_backoff.slot_denied`, `swath.idle_backoff.park_time` | Whether idle workers repeatedly searched, backed off, or lacked probe slots. |
| Checkpoint | `swath.checkpoint.commit.latency`, `swath.checkpoint.commit_batch_size`, `swath.checkpoint.queue.wait`, `swath.checkpoint.commit.wait`, `swath.checkpoint.queue.depth` | Whether SQLite durability is the limiting stage. |
| Parquet | `swath.parquet.rotation`, `swath.parquet.parts`, `swath.parquet.finalize.latency`, `swath.parquet.write.latency` | How the managed Parquet sink rotated and finalized parts. |
| Sorted output | `swath.sort.entries`, `swath.sort.segments.written`, `swath.sort.segment.bytes`, `swath.sort.merge.passes`, `swath.sort.merge.latency`, `swath.sort.pipeline.pages_forwarded`, `swath.sort.pipeline.cluster_pages`, `swath.sort.pipeline.cluster_rows`, `swath.sort.pipeline.header_scan`, `swath.sort.pipeline.router_wait`, `swath.sort.pipeline.plan_queue_wait`, `swath.sort.pipeline.encoder_page_reads`, `swath.sort.pipeline.encoder_read_wait`, `swath.sort.pipeline.decoded_page_bytes.peak`, `swath.sort.pipeline.parts_open`, `swath.sort.finalize.close.latency`, `swath.sort.manifest.md5.bytes`, `swath.sort.manifest.md5.latency`, `swath.sort.manifest.bounds.rows`, `swath.sort.manifest.bounds.bytes`, `swath.sort.manifest.bounds.latency`, `swath.sort.publication.latency`, `swath.sort.finalize.latency`, `swath.sort.finalize.parallelism`, `swath.sort.backpressure.wait`, `swath.sort.page_runs_per_buffer`, `swath.sort.staging.bytes.peak`, `swath.sort.handoff.queue.depth.peak`, `swath.sort.off_thread.buffers.peak` | How much staging and merge work sorting required, and what constrained it. |
| Local resources | `swath.disk.free_bytes`, `swath.s3.pool.leased`, `swath.s3.pool.idle_available`, `swath.s3.pool.pending_acquisition`, `swath.s3.pool.max`, `swath.s3.pool.connection_aborted`, `swath.s3.pool.handshakes`, `swath.s3.socket_closure_recovered` | Whether disk, the connection pool, or connection churn constrained the client. |
| Phase and output | `swath.phase`, `swath.output.files`, `swath.output.bytes`, `swath.output.broken_pipe`, `swath.emit.latency` | What phase is active and how the selected sink behaves. |
| Run result | `swath.run.duration`, `swath.run.throughput` | Final wall time and lifetime-average throughput. These are end-of-run, not live, values. |

The discard profiling sink retains `swath.entries.emitted`, `swath.bytes.estimated`, progress,
and `swath.emit.latency`, but creates no `swath.output.files` or `swath.output.bytes` series
because it writes no output. Its engagement is visible as
`swath.steal_reason{outcome="OUTPUT",reason="discard"}`, and the report records zero output
files and bytes.

`swath.fetch.latency.phase` has bounded `call_class` values (`worker_page`,
`pivot_probe`, `structure_probe`) and `phase` values (`connect_acquire`, `ttfb`,
`sdk_unmarshal`, `total`, `response_parse`). Treat the phases as independent signals,
not additive slices: SDK boundaries can overlap, and `sdk_unmarshal` includes draining the
response body as well as SDK parsing. SDK-derived phases are best effort; absent samples
are omitted, not recorded as zero. `total` includes failures, while `response_parse` exists
only after a response was returned. The report's `probe_latency` block is the easiest way
to read their percentiles.

`swath.progress.units` is the phase-independent stuck signal. It advances for emitted rows
and during sorted merge passes. Allow at least five minutes before declaring a stall: a
final Parquet flush can legitimately leave it unchanged for a short window.

Process and SDK-pool gauges may be unavailable or may hold the last reported value. In
particular, pool gauges update on completed API attempts and freeze during a merge. Read
them with the API-call rate. A live `swath.s3.pool.pending_acquisition > 0` indicates pool
acquisition pressure; low in-flight work by itself does not.

### 5.1 Proactive rate cap vs. reactive adaptive backoff

Two independent mechanisms can delay a request:

- `--request-rate` is a proactive, run-wide cap. Its wait time is
  `swath.rate_limit.api_wait`; without a configured cap this stays zero.
- The additive-increase/multiplicative-decrease (AIMD) controller reacts to service
  throttling by lowering the live concurrency target. Throttle events and target reductions
  appear in `swath.throttle.events` and `swath.aimd.target_reductions`.
  `swath.rate_limit.wait` measures waits at the shared concurrency gate, so it can also be
  nonzero from ordinary slot contention.

Read all four signals together before attributing a slowdown to the store.

## 6. Exit codes

The canonical table is in [Using swath](usage.md#exit-codes). Automation should normally
interpret the process code together with `complete`, `stop_source`, and `error_class` in the
report. The code-1 sorted-output disk guard is the exception: inspect terminal or log output
for `sort_disk_precheck_refused` or `sort_disk_exhaustion_imminent`. A startup refusal may
create no report, and the emergency in-run halt can leave only a stale heartbeat report.

## 7. Run trace (`--trace`)

`--trace PATH` writes JSON Lines describing split, steal, and related engine decisions.
It is off by default and is independent of OTLP export. Traces can contain key bounds and
pivots, so handle them like bucket metadata.

Use a trace when aggregate counters show *that* a path engaged but you need to know *where*
or *why*. The format and event guarantees are specified in
[metrics internals](internals/metrics-internals.md#7-run-trace-format).

## 8. Replay meters

`swath-replay` has its own request, fixture, and fault-injection meters. They are
documented beside its configuration in the
[replay toolkit guide](swath-replay.md#metrics-and-tuning), because their meaning belongs to
the test server rather than a production listing run.
