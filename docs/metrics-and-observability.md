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
| Reproduce an engine decision | `--trace PATH`, then `swath-replay-server` |

Logs always go to stderr; listing data can therefore stream safely on stdout. Use `-v`,
`-vv`, or `-vvv` for INFO, DEBUG, or TRACE logs. `-q` selects ERROR and `-qq` disables
logging; quiet wins if both quiet and verbose flags are present.

For most investigations, start with the JSON report. It combines configuration, outcome,
cost, timing, and the final meter snapshot without requiring a telemetry backend.

## 1. Micrometer meters

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
| Sorted output | `swath.sort.entries`, `swath.sort.segments.written`, `swath.sort.segment.bytes`, `swath.sort.merge.passes`, `swath.sort.merge.latency`, `swath.sort.merge.range.latency`, `swath.sort.merge.boundaries.latency`, `swath.sort.merge.boundaries.embedded.entries`, `swath.sort.merge.boundaries.embedded.bytes`, `swath.sort.merge.boundaries.scan.bytes`, `swath.sort.finalize.close.latency`, `swath.sort.manifest.md5.bytes`, `swath.sort.manifest.md5.latency`, `swath.sort.manifest.bounds.rows`, `swath.sort.manifest.bounds.bytes`, `swath.sort.manifest.bounds.latency`, `swath.sort.publication.latency`, `swath.sort.finalize.latency`, `swath.sort.finalize.parallelism`, `swath.sort.backpressure.wait`, `swath.sort.page_runs_per_buffer`, `swath.sort.staging.bytes.peak`, `swath.sort.handoff.queue.depth.peak`, `swath.sort.off_thread.buffers.peak` | How much staging and merge work sorting required, and what constrained it. |
| Local resources | `swath.disk.free_bytes`, `swath.s3.pool.leased`, `swath.s3.pool.idle_available`, `swath.s3.pool.pending_acquisition`, `swath.s3.pool.max`, `swath.s3.pool.connection_aborted`, `swath.s3.pool.handshakes`, `swath.s3.socket_closure_recovered` | Whether disk, the connection pool, or connection churn constrained the client. |
| Phase and output | `swath.phase`, `swath.output.files`, `swath.output.bytes`, `swath.output.broken_pipe`, `swath.emit.latency` | What phase is active and how the selected sink behaves. |
| Run result | `swath.run.duration`, `swath.run.throughput` | Final wall time and lifetime-average throughput. These are end-of-run, not live, values. |

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

### 1.1 Proactive rate cap vs. reactive AIMD backoff

Two independent mechanisms can delay a request:

- `--request-rate` is a proactive, run-wide cap. Its wait time is
  `swath.rate_limit.api_wait`; without a configured cap this stays zero.
- AIMD reacts to service throttling by lowering the live concurrency target. Throttle
  events and target reductions appear in `swath.throttle.events` and
  `swath.aimd.target_reductions`. `swath.rate_limit.wait` measures waits at the shared
  concurrency gate, so it can also be nonzero from ordinary slot contention.

Read all four signals together before attributing a slowdown to the store.

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
| `engine`, `seed`, `trajectory`, `slow_ranges`, `probe_latency`, `client_cost`, `demand_gate`, `shape` | Deeper scheduling, keyspace, and latency evidence. |
| `meters` | Final generic meter snapshot. |

`complete=true` means the requested dataset was published. On an unsuccessful run,
`stop_source` distinguishes a user stop, deadline, signal, output condition, and internal
failure; `error_class` groups the actionable cause. These fields drive automation more
reliably than parsing prose logs. `stuck` classification is based on the time since
`swath.progress.units` last advanced, with the final phase recorded alongside it.

The schema is additive within a major version: tolerate unknown fields and do not depend
on field order. Dedicated `*_ms` fields use milliseconds. Generic percentile gauges in
`meters` use Micrometer's base unit of seconds.

For page-run staging, the `sort` block reports `merge_boundary_embedded_entries`,
`merge_boundary_embedded_bytes`, and `merge_boundary_scan_bytes` to distinguish validated
trailer samples from compatibility fallback scans. `merge_boundary_bytes` is the sum of
the two byte fields. Parquet boundary selection contributes zero to these page-run totals.

The `sort` block decomposes terminal work into `finalize_ms`, `finalize_close_ms`,
`local_publication_ms`, `finalize_parallelism`, and `manifest_*` fields. Fresh final parts
derive their digest, byte count, row count, and exact bounds while writing; nonzero manifest
bounds-read metrics identify the compatibility path that reopened a carried part.

The report can contain the target URI, arguments, filter values, slow-range bounds, and
key samples. Treat it as operational data and redact it before sharing.

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

## 5. Diagnosing a slow run

| Evidence | Likely limiting stage | Check next |
| --- | --- | --- |
| High API latency, high in-flight work | Store or network service time | `fetch.latency.phase`, throttles, pool pending, recovered errors |
| Low API rate and low in-flight work while listing | Work supply or a long tail | steal reasons, probes, tail occupancy, open-frontier share |
| High `queue.wait` or `emit.latency` | Output admission or sink | Parquet write/finalize latency, output filesystem |
| High checkpoint queue or commit waits | SQLite durability | checkpoint volume latency and queue depth |
| High sort backpressure or many merge passes | Sort memory/disk/FD limits | `sort` report block, staging peak, effective fan-in |
| `progress.units` flat for more than five minutes | Potential stall | phase, thread dump, last error, disk and pool state |

For repeatable performance experiments, use the
[performance guide](performance.md). For implementation-level interpretation, use the
[metrics internals](internals/metrics-internals.md).

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

## 8. Replay-server meters

`swath-replay-server` has its own request, fixture, and fault-injection meters. They are
documented beside its configuration in the
[replay-server guide](swath-replay-server.md#metrics-and-tuning), because their meaning belongs to
the test server rather than a production listing run.
