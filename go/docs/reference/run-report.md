# Run report and progress file

swath accounts for every run in one JSON document, the run report: `DIR/run.json` beside a committed dataset,
the file named by `--report FILE` (written when the attempt ends, on failure too), or stdout with
`--report -` where stdout carries no rows. While a dataset run goes, `DIR/.swath/progress.json` holds its live
counts.

```bash
swath list s3://bucket/logs/ -o logs.parquet --report attempt-1.json
jq '.outcome.class, .output.rows_committed, .requests.http_attempts' attempt-1.json
```

The fields most runs need:

| Field | What it says |
|---|---|
| `outcome.class`, `outcome.exit_code`, `outcome.cause` | how the attempt ended ([exit-codes.md](exit-codes.md)) |
| `outcome.complete` | the listing reached the end of its scope; it is true for a filtered run too, and entries that could not be read are not counted against it (`outcome.unreadable`, the dataset's `coverage.skipped_entries` and `coverage.filters`: [whole-source check](../getting-started.md#5-check-that-it-is-complete)) |
| `attempt.id` | this attempt; `error.json` carries the same |
| `output.rows_committed`, `output.object_bytes` | the dataset's rows, and the bytes of the objects this attempt listed |
| `output.totals` | `swath du`'s totals (the rows themselves are only on stdout) |
| `requests.http_attempts`, `cost.cost_usd` | requests made, and their cost at the provider's reference list rate |
| `resources.peak_rss_bytes` | the process's peak memory |
| `timings.wall_ms` | the run's duration |

`--report FILE` may not name a directory, a file in a directory that does not exist, the text output file,
one of the dataset directory's own entries (`manifest.json`, `data/`, `run.json`, `error.json`, `.swath/`)
or a path inside another swath dataset: those are refused before listing, with exit 2. A name of its own in
the output directory, such as `DIR/attempt-1.json`, is accepted. A report that cannot be written at the end
fails the command (exit 1). `--report -` where stdout carries rows is refused (exit 2) and prints nothing on
stdout.

## Progress file

While a dataset run holds its output, `DIR/.swath/progress.json` is rewritten every 2 seconds by an atomic
rename; the progress line on the terminal samples the same snapshot. A supervisor watches that file;
`DIR/run.json` follows the commit.

| Field | Meaning |
|---|---|
| `rows` | rows listed so far (a local tree's entries, directories included; with `--all-versions`, versions and delete markers) |
| `ordered`, `encoded` | rows handed to the encoders, rows written into parts |
| `parts` | parts published |
| `verified_bytes` | a resume's bytes of retained parts checked so far |
| `advance` | the sum of the above; rises in every phase |
| `answered` | requests the service answered usably (a sign the service is up, not progress) |
| `frontier` | the listing's pages: those that held an entry or ended a chain, and empty continuing pages within the first 1,024 of their run (the watchdog's liveness with `advance`) |
| `empty_chain_pages` | empty continuing pages past the first 1,024 of their run: answered, followed, not progress |
| `throttles`, `timeouts` | throttle failures; timeout failures, including credential waits before HTTP accounting starts |
| `part_age_ms` | time since the last part was published (or since the start): publication age, not the age of the rows a resume would list again |
| `mutex_wait_ms`, `mutex_max_hold_ms` | the range engine's decision mutex: summed wait, longest hold |
| `rows_retained` | rows a resume kept from the interrupted run (not in `rows`) |
| `requests`, `retries` | HTTP attempts; retry-loop repetitions, including credential-wait failures before HTTP accounting starts ([accounting](../internals/clients.md#retry-policy-and-accounting)) |
| `inflight`, `limit` | attempts in flight now; the adaptive limiter's current limit (0 without one) |
| `cost_usd`, `cost_rate_per_1k_usd` | `requests` at the service's reference list rate, and that rate (absent without a known tariff, including Azure Blob and custom endpoints) |
| `order.nodes`, `order.markers`, `order.spill_readers`, `order.spill_live_bytes`, `order.ahead_rows` | the order stage's live resources, each with a `peak_` twin (`peak_spill_live_bytes` for spill bytes); `ahead_rows` is rows listed and not yet handed on (in memory or spilled) |
| `started_at`, `as_of`, `duration_ms` | timing |

## Run report

<!-- report:begin -->
One schema (`report_version` 5) for `run.json` beside a committed dataset, `--report FILE`,
and what `--report -` prints. It is operational: a dataset is read by its manifest, never by
this report. A change to the schema bumps `report_version` and is listed in
[upgrade.md](../upgrade.md#run-report-and-counters). It accounts for this invocation (rows it fetched
and emitted, requests it made) separately from the dataset (rows retained from an interrupted run, durable,
committed). Durations are integers with the unit in the name; a measurement that was not taken is absent,
never zero.

| Field | Meaning |
|---|---|
| `report_version` | 5 |
| `build` (`tool`, `version`, `revision`) | the binary: `swath`, the version (the release's `X.Y.Z` that `build.sh` stamps from `$SWATH_VERSION`, `devel` for a binary built from a source checkout), the VCS revision (`-dirty` if modified) |
| `attempt` (`id`, `started`) | this invocation: 128 random bits in hex, and when the command began; `error.json` carries the same |
| `invocation` | what was asked: `argv` (the command in its canonical spelling, each flag `--name=value`; a secret flag's value `[redacted]`; a URL given to `-o` or `--endpoint-url`, and SOURCE, with its user information, query or fragment `[redacted]`, in every spelling of the flag; every other value, a filter's included, as given, so a resume's filters equal the run's), `source` (the root, narrowed by the includes' literal prefix on S3 and GCS), `narrowing` (with includes: `narrowed`, or why not: [filters.md](../filters.md#what-narrows-the-listing)), `out`, `resume`, `schema_id`, `fields` (the metadata tier), `workers`, `encoders`, `sort_buffer_bytes` (`--sort-buffer`, the bytes buffered for sorted output), `sort_buffer` (how it was chosen: `reason` `explicit`, `capacity_fraction` (1/16 of memory), `cap` (1 GiB), `minimum` or `unknown_fallback` (128 MiB, no memory limit found); `memory_bytes` and `bound_by` (`physical`, `cgroup` or `go_memory_limit`), the least of the limits found: `physical_bytes`, `cgroup_bytes`, `go_memory_limit_bytes`), `part_size_bytes`, `prior` (`--seed-from`: `path`, `source` `dataset` or `cuts`, `cuts` offered, `counted`) |
| `outcome` | `status` (`committed`, `written`, `listed`, `stopped`, `failed`), `error`, `complete`, `stop_reason`, `stop_source`, `entry_errors`, `unreadable` (some entries could not be read: the rows and totals leave them out, exit 0), `class`, `exit_code`, `resumable`, `cause`, `unrecorded` ([exit-codes.md](exit-codes.md)) |
| `args_hash` | SHA-256 over the run's frozen identity (tool, format, schema, key cap, writer, part size, row bound, provider, the source's identity fields such as bucket, prefix, route and region, and the filters): the same for a run and its resumes, never a credential |

**Timings** (`timings`). `started_at` is the run's start (after the command line was read), `finished_at` its
end, `wall_ms` between them; `session_ms` is the whole invocation, from `attempt.started` (credential lookup,
a resume's verification and publication included). The phases follow one another: `listing_ms` from
`started_at` to `listing_finished_at`, the end of the order stage (every row listed and handed on in order; it
includes the setup and the order stage's drain after the last request), then `drain_ms` until the encoders have
written and closed the last part, then `publication_ms` until the manifest is installed (`drain_ms` and
`publication_ms` absent without a committed dataset). `seed_ms` overlaps `listing_ms` at its start: the range
engine's seed, from its first delimiter request to its tiles (absent where none ran). `user_cpu_ms` and
`sys_cpu_ms` are the process's CPU times (absent where the platform does not measure them).

**Rows and bytes** (`output`). `rows_fetched` is what this invocation listed from the source, `rows_filtered`
what the filters dropped of it, `rows_emitted` what was handed on in order (swath du, swath diag count: counted). On a fresh run
that succeeded, `rows_fetched` = `rows_filtered` + `rows_emitted`. A resume also hands on the rows it replays
from certified spill files, which it did not list: on a resume that succeeded, `rows_fetched` − `rows_filtered`
+ `resources.spill.durable.resume.replayed_rows` = `rows_emitted`. A run that stopped has also fetched rows
ahead of the frontier that it never emitted. `rows_retained` is kept from an interrupted run, `rows_durable` in parts
that stay in the output (the committed dataset's, or after a failure the journaled parts'), `rows_committed` in
the committed dataset (`rows_retained` + `rows_emitted`). `parts_written` and `parts_retained` count parts.
`object_bytes` sums the sizes of the objects this invocation listed and the filters kept (the size column; absent for a schema without one; a local directory's own size is no object's, so a local tree's is the apparent bytes of the files kept). On a fresh committed run they are exactly those of `rows_emitted`; a resume does not count the rows it replays from spill, and a run that stopped also counts rows it listed and never handed on;
`output_bytes` is the dataset's part bytes. `text` is a text run's: `format`, `destination`, `compression`,
`rows`, `bytes` (delivered, compressed), `sink_wait_ns`, `output_wait_ns`, `formatters`, and `compressor`
(`bytes_in`, `bytes_out`, `jobs`, `jobs_reason`: `cores`, `row_budget`, `encoders`, `gzip`). `remote` is a
dataset written to a bucket's (`-o gs://...`, absent otherwise; [dataset.md](../dataset.md#a-dataset-in-a-bucket)):
`destination` (the root's URI, and ` at ` its endpoint when one is set), `work_dir`, `commit` (`created`;
`confirmed`: the creation's answer was lost or refused and the manifest read back is the run's candidate;
`resumed`: a resume found the commit made and finished its aftermath; `lost_race`: another run's manifest is
there; `unresolved`: neither could be established, and everything is kept; empty when the run did not reach its
commit), `manifest`, `manifest_identity` (its generation) and `manifest_sha256` once committed,
`uploads` (completed), `uploads_aborted`, `upload_bytes` (handed to uploads), `upload_wait_ns` (encoder time in
upload writes and completions: a write waits for the chunk before it), `retries` and `retry_classes` (attempts
the SDK retried, by `http_STATUS`, `timeout`, `connection`, `other`), `objects_verified` (retained parts a resume
read whole and checked), and the orphans (objects this run created, by the generation its upload recorded, that no committed
manifest names): `orphans_deleted`, `orphans_gone` (absent, or replaced since: not this run's),
`orphans_left` (a delete failed) and `orphans_unproven` (names allocated with no recorded creation, never
touched). `totals` is a
`swath du` run's (absent otherwise): `depth`, `prefixes` (the rows printed, the total included),
`max_prefixes` (the cap), `bytes_column` (the column summed: `size`, or `allocated_bytes` with `--allocated`),
`objects`, `bytes` and `dirs` (the total's; `objects` excludes directories), `not_objects` (rows counted as no object: directories, delete
markers), `unsized` (objects without a value in the bytes column: the sizes are then unknown), `links_once`
(`--count-links-once`), `linked_rows` (rows with more than one hard link), `inodes` (the distinct inodes among
them, each counted once) and `max_inodes` (their cap).

For versions schemas only, `versions`, `distinct_keys` and `live_objects` describe this attempt's
rows after filtering at the order stage's handoff (swath du, swath diag count: validated, counted rows).
`versions` excludes S3 delete markers; S3 also reports `delete_markers`, with
`rows_emitted` = `versions` + `delete_markers`. GCS reports `noncurrent` among its
`versions`, with `versions` = `noncurrent` + `live_objects`. A live S3 object has
`is_latest` true and `is_delete_marker` false; a live GCS generation has null
`time_deleted`. Zero counts are present. Retained resume rows, held tails, relisting,
and raw overfetch are excluded. `distinct_keys` excludes pending batches that never reached the sink, including on a failed run.

Object-store failure diagnostics retain `auth` (unusable credentials), `access_denied` (a listing
permission refusal), `throttle`, `timeout`, `parse` and `unsupported`. These labels describe the failure;
retry and engine requeue use its stored transient signal. Other service errors retain their HTTP status
and provider code or reason in the message without a separate report label. Invalid pages remain protocol
violations; caller cancellation keeps its stop disposition.

**Requests** (`requests`, `cost`, `efficiency`; absent for a source that makes no requests). `http_attempts`
is the one request count: every counted HTTP attempt (pages, probes, client retries, engine requeues).
`retries` counts retry-loop repetitions, `throttles` counts throttle failures and `timeouts` counts
timeout failures. Credential waits can fail before HTTP accounting starts, so `retries` and `timeouts`
can increase with zero `http_attempts`; they are not subsets of the HTTP count. `failure_warnings` counts the persistent-failure warnings printed on stderr ([swath(1)](../commands/swath.md#stdout-and-stderr)), `log_dropped` the
stderr lines dropped because stderr did not drain. See
[retry accounting](../internals/clients.md#retry-policy-and-accounting). `cost_usd` is `http_attempts` at `basis` `rate_per_1k_usd` (0.005, `basis` `source`
`aws-list-reference-rate` for AWS S3, `gcs-class-a-reference-rate` for GCS's own endpoint, however
`--endpoint-url` spells it); both `null` on a
custom endpoint, whose tariff is unknown, and for every Azure Blob run (no tariff estimate). `keys_per_sec` is `rows_emitted` over `listing_ms`;
`http_attempts_per_1k_rows` is `http_attempts` per 1,000 `rows_fetched` (`null` without a row). Latency
percentiles and in-flight time are the provider's counters (`source.stats.counters.values` `inflight.p50_ns`,
`inflight.p90_ns`, `inflight.p99_ns`, `inflight.busy_ns`).

**Resources** (`resources`). `peak_rss_bytes` (with `rusage_available`), `peak_heap_live_bytes` (sampled every
100 ms), `max_buffered_bytes` (peak reorder storage, against `invocation.sort_buffer_bytes`), `free_disk_min_bytes`
(the least free space seen, every second, on the filesystem the run writes to: the spill directory's, else the
output's; absent for swath du and swath diag count), `peak_ahead_rows`, `frontier_stuck_max_ns`, `frontier_wait_ns`,
`budget_wait_ns` and `encoder_wait_ns` (summed over goroutines: not elapsed time), `peak_nodes`, `peak_markers`,
`part_time_cuts`, `part_row_cuts`, `features` (the pipeline's policies, on or off), and `spill` (absent with spill
off): `rows`, `bytes`, `logical_bytes`, `format`, `decode_refused`, `ranges`, `frames` (row frames written), `opens` (spill file open attempts: every write, and a read only when its file's handle was not open; a resume's certificate validation not counted), `peak_bytes`, `refused`,
`max_bytes`, `reserve_bytes`, `peak_readers` (spill read descriptors open at once), `read_handles` (the spill read
handles: `hits`, `misses`, `raced`, `evictions` by reason, `capacity`, `retired`, `truncated`, `low_fd`, `closed`, and
`peak_open`), `frame_bytes` and `frame_rows` (row frames written by size: `p50`, `p90` and `p99`, each the bound of
the power-of-two bucket holding it, and `max`), `dir`, `fs_type`, and with durable spill
([internals/ordering.md](../internals/ordering.md#durable-spill)) `durable`: `slots` (the certificate cap),
`history` and `history_cap` (ledger records against H), `captured_ranges`, `capture_at` (by where the capture
began), `capture_pending_dropped`, `no_slot`, `history_exhausted`, `abandoned` (by reason), `sealed_files`,
`sealed_durable`, `flush_ns`, `flush_cycles`, `ledger_failed`, `retained_bytes`, `peak_retained_bytes`, `retired`
(by reason), `retire_deferred`, and on a resume `resume`: `ledger_dropped_bytes`, `ledger_cut`, `accepted`,
`accepted_bytes`, `rejected` (by reason), `orphans`, `replayed_rows`, `replay_skipped_rows`, `gaps`,
`requests_saved_est`. `durable_off` says why durable spill is off when it could have run. `drain` (sorted Parquet; absent otherwise) is the
drain: its tail once listing is done, and the drainer's times over the whole run
([internals/ordering.md](../internals/ordering.md), every field in
[internals/counters.md](../internals/counters.md)): `tail_ns`,
`worker_idle_after_listing_done_ns`, the drainer's `replay_read_ns`, `replay_decode_ns`, `ahead_wait_ns`,
`order_check_ns` and `sink_write_ns`; `read_ahead` (`frames`, `rows`, `drainer_frames`, `unplanned_frames`,
`drainer_waits`, `capped_waits`, `read_ns`, `decode_ns`, `max_reads`); `segments`, the parallel tail (`reason`,
`segments`, `workers`, `rows`, `max_rows`, `seam_checks`, `time_ns`, `max_time_ns`, `plan_ns`, `read_ns`,
`decode_ns`, `write_ns`, `max_workers`, `budget_waits`, `budget_wait_ns`, `max_held_bytes`, `workers_by`,
`per_worker_bytes`, `free_bytes`, `tree_bytes`, `durable_files_read`, `durable_boundaries`, `deferred_rows`); and `encoders_at_start`, `encoders_after_listing`,
`encoder_admission`. In `timings`, `listing_done_at` is when the provider finished (every range expanded).

**Source** (`source`: `name`, `stats`). The provider's own account: its counters under `counters` (one
namespace, [internals/counters.md](../internals/counters.md)).

**History** (`history`). The run's progress over time, at most 300 samples (about 30 KB):
the first at the start of listing, then one per `interval_ms` (1 s until the history fills; then every other
sample is dropped and the interval doubles), and the last at the end. Each of `samples`: `at`, `rows_fetched`,
`rows_emitted`, `rows_durable` (in journaled or committed parts, retained ones included), `http_attempts`,
`inflight`, `limit` (the limiter's, 0 without one), `spill_bytes` (live), `free_disk_bytes` (absent without an
output) and `rss_bytes` (the resident set now; Linux only, absent elsewhere). Absent for a run that stopped
before listing.

**Profiles** (`profiles`, development only). Present when `SWATH_CPUPROFILE` or `SWATH_MEMPROFILE` is set
([diagnostics.md](../internals/diagnostics.md#phase-profiles)): `phase_boundary_at`, the logical boundary between
the phases (the event `timings.listing_done_at` records, microseconds later; absent when listing never finished),
`cpu_cut_delay_ns` (from the boundary to the listing phase's CPU profile's stop: the listing file holds that much
of the tail), `cpu_gap_ns` (from that stop to the tail's CPU profile's start, which neither covers),
`mem_profile_rate` (bytes allocated per memory profile sample, 0: none), and `files`, each with `kind`
(`cpu_listing`, `cpu_tail`, `mem_listing`, `mem_exit`), `path`, `status` (`planned`: written later in the run;
`written`; `failed`, with `error`). `run.json`, written at the commit, has the tail's and the exit's files
`planned`; `--report`, written as the process ends, has every file's final status.
<!-- report:end -->
