# Run report and progress file

swath accounts for every run in one JSON document, the run report: `DIR/run.json` beside a committed dataset,
the file named by `--report FILE` (written when the attempt ends, on failure too), or stdout with
`--report -` where stdout carries no rows. While a dataset run goes, `DIR/.swath/progress.json` holds its live
counts.

```bash
swath list s3://bucket/logs/ -o logs-inventory --report attempt-1.json
jq '.outcome.class, .output.rows_committed, .requests.http_attempts' attempt-1.json
```

The fields most runs need:

| Field | What it says |
|---|---|
| `outcome.class`, `outcome.exit_code`, `outcome.cause` | how the attempt ended ([Exit codes](exit-codes.md)) |
| `outcome.complete` | nothing is missing from the output: the listing reached the end of its scope and read every entry (`outcome.unreadable` 0). A filtered run is complete for its filters (the dataset's `coverage.filters`: [whole-source check](../results.md#coverage-and-unreadable-entries)) |
| `attempt.id`, `run.id` | this attempt, and the run it belongs to (the same for a run and its resumes); `error.json`, `progress.json` and the log events carry the same |
| `output.rows_committed`, `output.committed_object_bytes` | the committed dataset's rows and the sizes of its objects, after a resume too |
| `output.object_bytes` | the bytes of the objects this attempt listed, each name once (a hard-linked file once per link) |
| `output.totals` | `swath du`'s totals (the rows themselves are only on stdout) |
| `requests.http_attempts`, `cost.cost_usd` | requests made, and their cost at the provider's reference list rate |
| `process.peak_rss_bytes` | the process's peak memory |
| `timings.wall_ms` | the run's duration |

`--report FILE` may not name a directory, a file in a directory that does not exist, the text output file,
one of the dataset directory's own entries (`manifest.json`, `data/`, `run.json`, `error.json`, `.swath/`)
or a path inside another swath dataset: those are refused before listing, with exit 2. A name of its own in
the output directory, such as `DIR/attempt-1.json`, is accepted. A file is replaced whole (a temporary file
renamed over it); a device, a descriptor or a named pipe (`/dev/stderr`, `/dev/fd/3`, a FIFO) is written as it
is, so `--report /dev/stderr` streams the report where stdout carries rows. A report that cannot be written at the end
fails the command (exit 1). `--report -` where stdout carries rows is refused (exit 2) and prints nothing on
stdout.

Every time in these files, and in the manifest, the journal and the log events, is RFC 3339 in UTC with exactly
six fraction digits (`2026-11-15T12:00:00.137198Z`): one width, so the strings sort as the times do.

## Progress file

While a dataset run holds its output, `DIR/.swath/progress.json` is rewritten every 2 seconds by an atomic
rename; the progress line on the terminal samples the same snapshot. When a run that started listing ends, while
it still holds the output, the file is written once more with its final counts and `state` `done` or `failed` (an
attempt refused before it listed, such as a resume of an unresumable run, leaves the file as it was); a file whose `state`
is neither, read after its run ended, is the last snapshot of a process that was killed. A supervisor watches that
file; `DIR/run.json` follows the commit. Its counts carry the run report's names.

| Field | Meaning |
|---|---|
| `progress_version` | 1 |
| `attempt_id`, `run_id` | the report's `attempt.id` and `run.id` (`run_id` once the run has opened its output) |
| `state` | `listing` (a resume's verification of its retained parts included), `draining` (the provider finished; the order stage hands on the rows it holds), `publishing` (the last part is written; the manifest is being committed), `done` (the dataset committed), `failed` |
| `rows_fetched` | rows listed so far (a local tree's entries, directories included; with `--all-versions`, versions and delete markers) |
| `rows_emitted`, `rows_encoded` | rows handed to the encoders, rows written into parts |
| `parts_written` | parts published |
| `verified_bytes` | a resume's bytes of retained parts checked so far |
| `scanned_bytes` | a resume's bytes of reused certificates its segments have read before their cut ([ordering.md](../internals/ordering.md#segments-parts-written-in-parallel-after-listing)) |
| `advance` | the sum of the above; rises in every phase |
| `answered` | requests the service answered usably (a sign the service is up, not progress) |
| `frontier` | the listing's pages: those that held an entry or ended a chain, and empty continuing pages within the first 1,024 of their run (the watchdog's liveness with `advance`) |
| `empty_chain_pages` | empty continuing pages past the first 1,024 of their run: answered, followed, not progress |
| `throttles`, `timeouts` | throttle failures; timeout failures, including credential waits before HTTP accounting starts |
| `part_age_ms` | time since the last part was published (or since the start): publication age, not the age of the rows a resume would list again |
| `mutex_wait_ns`, `max_mutex_hold_ns` | the range engine's decision mutex: summed wait, longest hold |
| `rows_retained` | rows a resume kept from the interrupted run (not in `rows_fetched`) |
| `http_attempts`, `retries` | HTTP attempts; retry-loop repetitions, including credential-wait failures before HTTP accounting starts ([accounting](../internals/clients.md#retry-policy-and-accounting)) |
| `inflight`, `limit` | attempts in flight now; the adaptive limiter's current limit (0 without one) |
| `cost_usd`, `rate_per_1k_usd` | `http_attempts` at the service's reference list rate, and that rate (absent without a known tariff, including Azure Blob and custom endpoints) |
| `order.nodes`, `order.markers`, `order.spill_readers`, `order.spill_bytes`, `order.ahead_rows` | the order stage's live resources, each with a `peak_` twin; `ahead_rows` is rows listed and not yet handed on (in memory or spilled) |
| `order.publish_waiters`, `order.admission_waiters`, `order.idle_workers` | the order stage's waiters now, each with a `peak_` twin: emitters waiting in publish on a full budget (a worker still listing its range), workers waiting to claim a queued range (budget and spill used), workers with nothing queued to list |
| `order.spill_pressure_since` | when listing began to wait on a full spill cap: spill refused while workers wait on it, in publish or at admission (present only while they do, and not after listing is done; a wait that comes back within 30 s keeps its start); at 2 minutes of it the `spill_pressure` event says so ([log events](log-events.md)) |
| `order.spill_max_bytes`, `order.spill_reserve_bytes` | the spill cap in effect (the default cap rises as it follows a disk grown under the run), and the free space spill leaves on its filesystem (0: none) |
| `started_at`, `as_of`, `duration_ms` | timing |

## Run report

<!-- report:begin -->
One schema (`report_version` 1) for `run.json` beside a committed dataset, `--report FILE`,
and what `--report -` prints. It is operational: a dataset is read by its manifest, never by
this report. It accounts for this invocation (rows it fetched
and emitted, requests it made) separately from the dataset (rows retained from an interrupted run, durable,
committed). Durations are integers with the unit in the name; a measurement that was not taken is absent,
never zero. Its members are its stable core, which changes only with a new `report_version`, but for
[`diagnostics`](#diagnostics): how the pipeline ran, under names that follow the algorithm and may change in any
release. A name says what kind of extreme it holds: `peak_` is a high-water mark over time (the most held at
once); `max_` on a configured value is a limit (`max_prefixes`, `spill.max_bytes`); and `max_` on a column or
an item is its largest value over the rows or items, as SQL's `max()` (`totals.max_last_modified`, the longest
segment's `max_time_ns`).

| Field | Meaning |
|---|---|
| `report_version` | 1 |
| `build` (`tool`, `version`, `revision`) | the binary: `swath`, the version (the release's `X.Y.Z` that `build.sh` stamps from `$SWATH_VERSION`, `devel` for a binary built from a source checkout), the VCS revision (`-dirty` if modified) |
| `attempt` (`id`, `started_at`) | this invocation: 128 random bits in hex, and when the command began; `error.json` carries the same |
| `run` (`id`) | the run this attempt belongs to: the journal's run id, the same for the run and every resume of it (for a dataset in a bucket, the `RUN` of its `data/RUN/ATTEMPT/` parts); a run without a journal is its one attempt, and has the attempt's id. Absent for an attempt that stopped before its output was opened; `error.json` carries the same |
| `invocation` | what was asked: `argv` (the command in its canonical spelling, each flag `--name=value`; a secret flag's value `[redacted]`; a URL given to `-o` or `--endpoint-url`, and SOURCE, with its user information, query or fragment `[redacted]`, in every spelling of the flag; every other value, a filter's included, as given, so a resume's filters equal the run's), `source` (the root, narrowed by the includes' literal prefix on S3 and GCS), `narrowing` (with includes: `narrowed`, or why not: [filters.md](../filters.md#what-narrows-the-listing)), `out`, `resume`, `schema_id`, `metadata` (the metadata tier, `--metadata`), `workers`, `encoders`, `sort_buffer_bytes` (`--sort-buffer`, the bytes buffered for sorted output; how it was chosen is `diagnostics.sort_buffer`), `part_size_bytes`, `prior` (`--seed-from`: `path`, `source` `dataset` or `cuts`, `cuts` offered, `counted`; for a dataset, `cuts_file`, how its cut file was taken: `read` (its cuts are the prior), `absent`, or `refused` with `cuts_refused` saying why ([dataset.md](../dataset.md#the-cut-file)); without it `cuts` is 0 for a dataset whose page index a sorted run reads as it goes) |
| `outcome` | `status` (`committed`, `written`, `listed`, `stopped`, `failed`), `error`, `complete` (nothing is missing from the output: the listing reached the end of its scope and `unreadable` is 0; false for a run that wrote no output; the manifest's `coverage.complete`), `stop_reason` (how the listing ended: `end_of_scope` at the end of its scope, `max_keys`, `downstream_closed`; absent for a run that wrote no output, whose `class` says why), `stop_source`, `unreadable` (the count of entries that could not be read: the rows and totals leave them out; the manifest's `coverage.unreadable`; absent before the source reported), `class` (`ok` when the command did what it was asked, `| head` included: [exit-codes.md](exit-codes.md)), `exit_code`, `resumable`, `cause`, `unrecorded` |
| `args_hash` | SHA-256 over the run's frozen identity (tool, format, schema, key cap, writer, part size, row bound, provider, the source's identity fields such as bucket, prefix, route and region, and the filters): the same for a run and its resumes, never a credential |

**Timings** (`timings`). `started_at` is the run's start (after the command line was read), `finished_at` its
end, `wall_ms` between them; `session_ms` is the whole invocation, from `attempt.started_at` (credential lookup,
a resume's verification and publication included). The phases follow one another: `listing_ms` from
`started_at` to `listing_finished_at`, the end of the order stage (every row listed and handed on in order; it
includes the setup and the order stage's drain after the last request), then `drain_ms` until the encoders have
written and closed the last part, then `publication_ms` until the manifest is installed (`drain_ms` and
`publication_ms` absent without a committed dataset). `provider_finished_at` is when the provider finished, every
range listed and none in flight (absent if it never did): the drain's tail lies between it and
`listing_finished_at`. `user_cpu_ms` and `sys_cpu_ms` are the process's CPU times (absent where the platform does
not measure them).

**Process** (`process`). `peak_rss_bytes` is the process's peak resident set (Linux VmHWM, macOS `getrusage`,
Windows the peak working set); absent, with the CPU times, where the platform does not measure them
(`diagnostics.resources.rusage_available` false).

**Rows and bytes** (`output`). `rows_fetched` is what this invocation listed from the source, `rows_filtered`
what the filters dropped of it, `rows_emitted` what was handed on in order (swath du, swath diag count: counted). On a fresh run
that succeeded, `rows_fetched` = `rows_filtered` + `rows_emitted`. A resume also hands on the rows it replays
from certified spill files, which it did not list: on a resume that succeeded, `rows_fetched` − `rows_filtered`
+ `diagnostics.resources.spill.durable.resume.replayed_rows` = `rows_emitted`. A run that stopped has also fetched rows
ahead of the frontier that it never emitted. `rows_retained` is kept from an interrupted run, `rows_durable` in parts
that stay in the output (the committed dataset's, or after a failure the journaled parts'), `rows_committed` in
the committed dataset (`rows_retained` + `rows_emitted`; a resume that finds the dataset already committed, by
an earlier attempt, takes it from the manifest and retains and emits nothing itself). `parts_written` and
`parts_retained` count parts. `replaced` is true when the committed dataset replaced a previous one in the
output (`--overwrite`), which stayed readable until this commit installed the new one in its place
([Overwrite and refresh](../running.md#overwrite-and-refresh)); absent otherwise.
`object_bytes` sums the sizes of the objects this invocation listed and the filters kept (the size column; absent for a schema without one; a local directory's own size is no object's, so a local tree's is the apparent bytes of the files kept), each name once: a file with several hard links counts once per link, where `swath du`'s `totals.bytes` counts it once. On a fresh committed run they are exactly those of `rows_emitted`; a resume does not count the rows it replays from spill, and a run that stopped also counts rows it listed and never handed on;
`output_bytes` is the dataset's part bytes. `committed_object_bytes` is the committed dataset's object bytes,
the same sum over every row of it, as the manifest's `extensions.swath.committed_object_bytes` states it: retained parts
and rows replayed from spill included, each row once whatever retries listed it. It is absent unless the dataset
was committed, and absent when unknown: a schema without a size column, or a resumed run whose retained parts
were recorded by a build that did not count them. 0 is a real total (no rows, or only empty objects). `text` is a text run's: `format`, `destination` (the file,
or `-` for stdout), `compression`, `rows`, `bytes` (delivered, compressed). `remote` is a
dataset written to a bucket's (`-o gs://...`, `-o s3://...` or `-o az://...`, absent otherwise; [dataset.md](../dataset.md#a-dataset-in-a-bucket)):
`destination` (the root's URI, and ` at ` its endpoint when one is set), `work_dir`, `commit` (`created`;
`confirmed`: the creation's answer was lost or refused and the manifest read back is the run's candidate;
`resumed`: a resume found the commit made and finished its aftermath; `lost_race`: another run's manifest is
there; `unresolved`: neither could be established, and everything is kept; empty when the run did not reach its
commit), `manifest`, `manifest_identity` (its GCS generation, S3 version id or ETag, or Azure ETag) and `manifest_sha256` once committed,
`uploads` (completed), `uploads_aborted`, `aborts_unconfirmed` (aborted uploads whose abort the store did not
confirm: an S3 multipart upload whose abort failed or that stayed listed, its parts kept until the bucket's
lifecycle rule removes them; absent when none, and always for GCS and Azure, whose aborts send nothing to
confirm), `upload_bytes` (handed to uploads), `upload_wait_ns` (encoder time in
upload writes and completions: a write waits for the chunk, part or block before it), `retries` and `retry_classes` (attempts
the SDK retried, by `http_STATUS`, `timeout`, `connection`, `other`), `objects_verified` (retained parts a resume
read whole and checked), and the orphans (objects this run created, by the generation its upload recorded, that no committed
manifest names): `orphans_deleted`, `orphans_gone` (absent, or replaced since: not this run's),
`orphans_left` (a delete failed) and `orphans_unproven` (names allocated with no recorded creation, never
touched). `totals` is a
`swath du` run's (absent otherwise), under the names of its rows ([du rows](../output.md#du-rows)): `depth`,
`prefixes` (the rows printed, the total included), `max_prefixes` (the cap), `primary` (the figure `--sort size`
orders by: `size`, or `allocated_bytes` with `--allocated`), the total's `objects` (directories excluded) and
`dirs`, its `size` and `allocated_bytes` (null when unknown: no object had one, or a source without allocated
bytes) with `size_missing` and `allocated_bytes_missing` (the objects counted without each), its
`max_last_modified` (null when no object had a time), `not_objects` (rows counted as no object: directories,
delete markers), `links_once` (a local tree's hard-linked files counted once: without `--count-links`, on rows
that identify their links, which `--metadata names` and Windows do not read), `linked_rows` (rows with more
than one hard link), `inodes` (the distinct inodes among them, each counted once) and `max_inodes` (their
cap). The byte totals are JSON numbers that can exceed 2^53: read them as 64-bit integers.

For versions schemas only, `versions`, `distinct_keys` and `live_objects` describe this attempt's
rows after filtering at the order stage's handoff (swath du, swath diag count: validated, counted rows).
`versions` excludes S3 delete markers; S3 also reports `delete_markers`, with
`rows_emitted` = `versions` + `delete_markers`. GCS reports `noncurrent` among its
`versions`, with `versions` = `noncurrent` + `live_objects`. A live S3 object has
`is_latest` true and `is_delete_marker` false; a live GCS generation has `is_latest`
true and null `deleted_at`. Zero counts are present. Retained resume rows, held tails, relisting,
and raw overfetch are excluded. `distinct_keys` excludes pending batches that never reached the sink, including on a failed run.

Object-store failure diagnostics retain `auth` (unusable credentials), `access_denied` (a listing
permission refusal), `not_found` (the source's bucket or Azure container does not exist: S3 `NoSuchBucket`, GCS's 404
`notFound` to a listing, Azure `ContainerNotFound`; never a missing object or prefix, which lists empty,
nor a missing output bucket),
`region_redirect` (S3 answered that the bucket is in another region and named it; the message says which),
`throttle`, `timeout`, `parse`, `unsupported` and `unreachable` (nothing in the run answered and every
connection was refused, its host not found or unroutable for 30 s; the message names the endpoint, and the
outcome is `stuck`, exit 75, cause `unreachable`). These labels describe the failure;
retry and engine requeue use its stored transient signal. Other service errors retain their HTTP status
and provider code or reason in the message without a separate report label. Invalid pages remain protocol
violations; caller cancellation keeps its stop disposition.

**Requests** (`requests`, `cost`; absent for a source that makes no requests). `http_attempts`
is the one request count: every counted HTTP attempt (pages, probes, client retries, engine requeues).
`retries` counts retry-loop repetitions, `throttles` counts throttle failures and `timeouts` counts
timeout failures. Credential waits can fail before HTTP accounting starts, so `retries` and `timeouts`
can increase with zero `http_attempts`; they are not subsets of the HTTP count. `failure_warnings` counts the persistent-failure warnings printed on stderr ([swath(1)](../commands/swath.md#stdout-and-stderr)), `log_dropped` the
stderr lines dropped because stderr did not drain. See
[retry accounting](../internals/clients.md#retry-policy-and-accounting). `cost_usd` is `http_attempts` at `basis` `rate_per_1k_usd` (0.005, `basis` `source`
`aws-list-reference-rate` for AWS S3, `gcs-class-a-reference-rate` for GCS's own endpoint, however
`--endpoint-url` spells it); both `null` on a
custom endpoint, whose tariff is unknown, and for every Azure Blob run (no tariff estimate). Latency
percentiles and in-flight time are the provider's counters (`diagnostics.source.stats.counters.values`
`inflight.p50_ns`, `inflight.p90_ns`, `inflight.p99_ns`, `inflight.busy_ns`).

### Diagnostics

`diagnostics` is how the pipeline ran: what bounded it, where it waited, what the provider's algorithm did. Its
names follow the algorithm ([internals/counters.md](../internals/counters.md)) and may change in any release
without a new `report_version`; a program that reads them is pinned to a build.

`sort_buffer` is how `invocation.sort_buffer_bytes` was chosen: `reason` `explicit`, `capacity_fraction` (1/16 of
memory), `cap` (1 GiB), `minimum` or `unknown_fallback` (128 MiB, no memory limit found); `memory_bytes` and
`bound_by` (`physical`, `cgroup` or `go_memory_limit`), the least of the limits found: `physical_bytes`,
`cgroup_bytes`, `go_memory_limit_bytes`. `seed_ms` overlaps `timings.listing_ms` at its start: the range engine's
seed, from its first delimiter request to its tiles (absent where none ran). `text` is a text run's sink:
`sink_wait_ns`, `output_wait_ns`, `formatters`, and `compressor` (`bytes_in`, `bytes_out`, `jobs`, `jobs_reason`:
`cores`, `encoder_allowance`, `encoders`, `gzip`). `efficiency`: `keys_per_sec` is `output.rows_emitted` over
`timings.listing_ms`; `http_attempts_per_1k_rows` is `requests.http_attempts` per 1,000 `output.rows_fetched`
(`null` without a row).

`totals_view` is `swath du`'s [view](../commands/swath-du.md#the-view), when it was drawn: `runs` (the ranges'
stretches of rows it read), `completed` (prefixes and objects complete inside one run, or local directories
whose listing ended), split into `remembered` (at 1 % of the bytes seen, or holding a hard link) and `dropped`,
`pruned` (remembered, dropped later for 100 larger ones of their depth), `partial` (cut by a run's edge, summed
across runs), and `depth_kept` with `lowered` (-1 and false, or the deepest level kept once more than
`output.totals.max_prefixes` were held).

**Resources** (`resources`). `rusage_available` (whether this platform measured the CPU times and
`process.peak_rss_bytes`), `peak_heap_live_bytes` (sampled every
100 ms), `peak_buffered_bytes` (peak reorder storage, against `invocation.sort_buffer_bytes`), `free_disk_min_bytes`
(the least free space seen, every second, on the filesystem the run writes to: the spill directory's, else the
output's; absent for swath du and swath diag count), `peak_ahead_rows`, `max_frontier_stuck_ns`, `frontier_wait_ns`,
`budget_wait_ns` and `encoder_wait_ns` (summed over goroutines: not elapsed time), the order stage's waits by site, each
summed over goroutines with the most goroutines waiting there at once: `publish_wait_ns` and `peak_publish_waiters`
(emitters waiting in publish on a full budget), `admission_wait_ns` and `peak_admission_waiters` (workers waiting to
claim a queued range; with `publish_wait_ns`, `budget_wait_ns`), `idle_wait_ns` and `peak_idle_workers` (workers with
nothing queued to list, read-ahead waits included, over the whole run), `peak_nodes`, `peak_markers`,
`part_time_cuts`, `part_row_cuts`, `features` (the pipeline's policies, on or off), and `spill` (absent with spill
off): `rows`, `bytes`, `logical_bytes`, `format`, `decode_refused`, `ranges`, `frames` (row frames written), `opens` (spill file open attempts: every write, and a read only when its file's handle was not open; a resume's certificate validation not counted), `peak_bytes`, `refused`,
`refused_ns` (the time spill stood refused: from a refused frame until a spill file's credit next returned, an interval
open at the end included), `refused_spells` (those intervals that ended), `max_by` (what set the cap: `spill_budget`, fixed; by default `disk`, the spill filesystem's free space after the reserve, for a dataset in a bucket, or `disk_half`, half of it otherwise (the filesystem shared with the output, or for a stream with other programs' temporary files), both following the disk; `none`), `start_max_bytes` (the cap the run started with), `max_bytes` (the cap in effect at the end), `cap_raises` (the times the cap rose: a disk grown under the run), `cap_rechecks` and `cap_recheck_raises` (rereads of the free space every 2 s while such a cap held the run back, and those that raised it), `reserve_bytes`, `peak_readers` (spill read descriptors open at once), `read_handles` (the spill read
handles: `hits`, `misses`, `raced`, `evictions` by reason, `capacity`, `retired`, `truncated`, `low_fd`, `closed`, and
`peak_open`), `frame_bytes` and `frame_rows` (row frames written by size: `p50`, `p90` and `p99`, each the bound of
the power-of-two bucket holding it, and `max`, exact), `dir`, `fs_type`, and with durable spill
([internals/ordering.md](../internals/ordering.md#durable-spill)) `durable`: `slots` (the certificate cap),
`history` and `history_cap` (ledger records against H), `limit_bytes` (the spill both derive from, one slot per
64 KiB) and `limited_by` (what bound the slots: `spill_cap`, `free_space` without a cap, `metadata` at the
262,144-certificate ceiling, from 16 GiB, or `fixed` when neither is known), `captured_ranges`, `capture_at` (by where the capture
began), `capture_pending_dropped`, `no_slot`, `history_exhausted`, `metadata_exhausted` (seals not certified at the
metadata bound), `abandoned` (by reason), `sealed_files`,
`sealed_durable`, `flush_ns`, `flush_cycles`, `ledger_failed`, `retained_bytes`, `peak_retained_bytes`, `retired`
(by reason), `retire_deferred`, and on a resume `resume`: `ledger_dropped_bytes`, `ledger_cut`, `accepted`,
`accepted_bytes`, `rejected` (by reason), `orphans`, `replayed_rows`, `replay_skipped_rows`,
`replayed_certificates` (by the drainer or the segments), `gaps`, `requests_saved_est`. `durable_off` says why durable spill is off when it could have run. `drain` (sorted Parquet; absent otherwise) is the
drain: its tail once listing is done, and the drainer's times over the whole run
([internals/ordering.md](../internals/ordering.md), every field in
[internals/counters.md](../internals/counters.md)): `tail_ns`,
`worker_idle_after_provider_finished_ns`, the drainer's `replay_read_ns`, `replay_decode_ns`, `ahead_wait_ns`,
`order_check_ns` and `sink_write_ns`; `writer_before_provider_finished` (the Parquet writer up to `timings.provider_finished_at`; absent
when listing never finished): `parts` and `rows` published, and the encoders' time on the batches written, `encode_ns`
encoding and `output_ns` in the parts' files or uploads; `read_ahead` (`frames`, `rows`, `drainer_frames`, `unplanned_frames`,
`drainer_waits`, `capped_waits`, `read_ns`, `decode_ns`, `max_reads`); `segments`, the parallel tail (`reason`,
`segments`, `workers`, `rows`, `max_rows` (the largest segment's), `seam_checks`, `time_ns`, `max_time_ns` (the longest), `plan_ns`, `read_ns`,
`decode_ns`, `write_ns`, `max_workers` (the cap), `budget_waits`, `budget_wait_ns`, `peak_held_bytes`, `workers_by`,
`per_worker_bytes`, `free_bytes`, `tree_bytes`, `durable_files_read`, `durable_boundaries`, `deferred_rows`, and on a
resume the reused certificates the segments replayed: `certificates_read`, `certified_rows`, `certificate_scan_ns`); and `encoders_at_start`, `encoders_after_listing`,
`encoder_admission`, with what they were admitted against: `encoder_allowance_bytes` (1/16 of memory, whatever
`--sort-buffer` says), `encoder_writer_bytes` and `encoder_upload_bytes` (one encoder's prices), and
`encoders_at_start_bound_by` and `encoders_after_listing_bound_by` (`memory`, `cpu`, `descriptors`, `minimum`,
`explicit`; after listing, the start's when the count was not raised). `window` (a run planned from a counted
`--seed-from`; absent otherwise) is the frontier window
([internals/ordering.md](../internals/ordering.md#the-frontier-window)): `tiles` admitted, `forced` of them by the
drainer past a refusal, `grown` (tiles whose rows reached 4 times the prior's), `rows` (the window's rows when last
evaluated) and `bound_by` (`rows`, the 8M target; `set`, `--seed-from-window-rows`; `spill_cap`; `budget`), `waits` (workers' waits at the window by the
refusal that held them: `rows`, `spill`, `growth`), `wait_ns` and `peak_waiters`, `plan_wait_ns` (the drainer
waiting for the prior to be read), `peak_tiles`, `peak_credit_rows` (the rows charged to the tiles held) and
`peak_retained_bytes` (the bytes the order stage held, in memory and live spill, at an admission), `spill_rows`
(the mean, over the admission probes the spill gate refused, forced admissions' included, of the rows charged to
the tiles held),
`peak_behind_bytes` (the most live spill behind the output: drained durable files not yet unlinked), `evicted`
(drained certificates the window evicted while live spill was past half the cap) and `wakes` (wakes of a held
worker on the paths that engage only with durable spill: `spill`, `read_ahead`, `read_ahead_share`).

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
the phases (the event `timings.provider_finished_at` records, microseconds later; absent when listing never finished),
`cpu_cut_delay_ns` (from the boundary to the listing phase's CPU profile's stop: the listing file holds that much
of the tail), `cpu_gap_ns` (from that stop to the tail's CPU profile's start, which neither covers),
`mem_profile_rate` (bytes allocated per memory profile sample, 0: none), and `files`, each with `kind`
(`cpu_listing`, `cpu_tail`, `mem_listing`, `mem_exit`), `path`, `status` (`planned`: written later in the run;
`written`; `failed`, with `error`). `run.json`, written at the commit, has the tail's and the exit's files
`planned`; `--report`, written as the process ends, has every file's final status.
<!-- report:end -->
