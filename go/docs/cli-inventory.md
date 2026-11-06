# Java CLI inventory

Every option, output mode, exit code, report field class and documented guarantee of the Java `swath` CLI
(`swath-cli`, [docs/cli.md](../../docs/cli.md), [docs/usage.md](../../docs/usage.md),
[docs/metrics-and-observability.md](../../docs/metrics-and-observability.md)) against this tool
([cli.md](cli.md), [upgrade.md](upgrade.md)). Dates are owner decisions.

Legend: **kept** (same name and meaning, differences noted), **renamed** (to the Go name given),
**removed** (with the recorded decision and its date, or "no decision recorded"), **deferred** (a recorded
decision to add it later).

## Connection and credentials

| Java | Go | Status | Decision |
|---|---|---|---|
| `--region REGION` | `--region` (default chain ends at `us-east-1`) | kept | – |
| `--profile NAME` | `--profile` | kept | – |
| `--endpoint-url URL` | `--endpoint URL` | renamed | – |
| `--force-path-style` / `--no-force-path-style` | `--path-style` (boolean) | renamed | – |
| `--no-sign-request` | `--anonymous` | renamed | – |
| `--requester-pays requester` | `--request-payer` (boolean) | renamed | – |
| `--fetch-owner` | `--fetch-owner` | kept | – |
| `--bearer-token-command CMD` | none; GCS has its own provider (`gs://`, `--gcs-credentials`) | removed | no decision recorded |
| `--bearer-token-refresh-interval` (45 m) | none | removed | no decision recorded |
| Credential-chain failure fails the run | a configured source that fails stops the run with cause `auth`; requests go unsigned only for an empty chain or `--anonymous` | kept | – |
| Source `s3://bucket[/prefix]`, path taken literally | the same; also a local directory and `gs://bucket[/prefix]` | kept | – |
| S3 directory buckets refused before the first request | not supported; no up-front refusal | removed | no decision recorded |

## Listing scope and filters

| Java | Go | Status | Decision |
|---|---|---|---|
| `--include REGEX` | none; `coverage.filters` is always `[]` | removed | no decision recorded |
| `--exclude REGEX` | none | removed | no decision recorded |
| `--min-size SIZE` | none | removed | no decision recorded |
| `--max-size SIZE` | none | removed | no decision recorded |
| `--modified-since DATE` | none | removed | no decision recorded |
| `--modified-until DATE` | none | removed | no decision recorded |
| `--storage-class CLASS[,CLASS]` | none | removed | no decision recorded |
| Current objects only; versions not listed | current objects only | kept | versions mode parked, 2026-11-05 |

## Output formats and datasets

| Java | Go | Status | Decision |
|---|---|---|---|
| `--format auto\|table\|tsv\|jsonl` | none; Parquet dataset only | deferred (TSV, JSONL) | TSV and JSONL after the port, streaming without resume allowed, 2026-11-06; `table` and `auto`: no decision recorded |
| `--format parquet` | the only format; no flag | kept | – |
| `--format discard` | `--count-only` | renamed | – |
| `-o`, `--output PATH` (file, directory or `-` for stdout) | `-o`, `--output DIR` (a dataset directory only) | kept | stdout and files deferred with TSV/JSONL, 2026-11-06 |
| `--output-type file\|dir` | none | removed | no decision recorded |
| `-o NAME.parquet` one-writer directory layout | none | removed | no decision recorded |
| `--compression none\|gzip\|zstd` (text only) | none | removed | no decision recorded |
| `--text-part-size` (256 MB) | none | removed | partitioned text datasets not ported, 2026-11-05 |
| `--text-writers N` (3) | none | removed | text writer lanes not ported, 2026-11-05 |
| `--writeback-size SIZE` (off) | none | removed | no decision recorded |
| stdout: at most once; a closed reader exits 0 | no stdout output | deferred | TSV and JSONL after the port, 2026-11-06 |
| Text file published atomically | none | deferred | TSV and JSONL after the port, 2026-11-06 |
| Text directory dataset, `_SUCCESS` last | none | removed | partitioned text datasets not ported, 2026-11-05 |
| `_SUCCESS` written last is the commit | `manifest.json` installed last (atomic rename, directory sync) is the commit | renamed | dataset format 2, 2026-11-05 |
| `symlink.txt` part list | none; the manifest lists the parts | removed | dataset format 2, 2026-11-05 |
| `manifest.json` (format 1, camelCase) | `manifest.json` format 2 with `coverage` | renamed | dataset format 2, 2026-11-05 |
| `.swath-state.json` | `.swath/` workspace (`lock`, `journal`, `parts/`, `spill/`) | renamed | – |
| Schema `swath/canonical`, 13 columns | `swath/s3-objects/v1`, 9 columns (`version_id`, `is_latest`, `is_delete_marker`, `row_type` dropped) | renamed | dataset format 2 and versions mode parked, 2026-11-05 |
| Footer stamps `swath.sort.*` | standard `sorting_columns`, `swath.profile*`, `swath.index_bound_bytes`, `swath.schema_id` | renamed | dataset format 2, 2026-11-05 |
| Non-empty output without ownership evidence refused; symlinked paths refused | non-empty `--output` refused unless `--overwrite`, which replaces only this tool's previous dataset; every write by pathname checks the output's identity | kept | – |
| One run per output at a time | output lock; journaled output refused on Windows | kept | – |

## Sorting and parts

| Java | Go | Status | Decision |
|---|---|---|---|
| `--sort` (globally key-sorted Parquet) | always sorted; no flag | kept | – |
| `--no-sort` (unsorted Parquet, direct dataset) | none | removed | no decision recorded |
| Sorted parts key-disjoint in unsigned byte order, named in key order | the same, every run | kept | – |
| Staging under `_staging/`, k-way merge, finalization | ordered emission with disposable spill under `.swath/spill/` | removed | staging and external sort not ported, 2026-11-05 |
| Free-space checks before and during sorted listing; `--tune sort.ignore-disk-check` | `--spill-max`, `--spill-reserve`; a full disk stops the run with cause `no_space` | renamed | sort settings are code constants, 2026-11-05 |
| `--tune sort.*` (sixteen settings) | none | removed | sort settings are code constants, 2026-11-05 |
| `--parquet-part-size SIZE` (256 MB) | `--part-size` (64 MiB) | renamed | – |
| `--part-rotation-interval DURATION` (30 s; 0 off) | `--part-interval` (60 s; 0 off) | renamed | – |
| `--part-rotation-max-rows N` (2,000,000) | none | removed | no decision recorded |
| Same input and configuration give the same parts at any encoder count | the same; with `--part-interval` cuts (`resources.part_time_cuts` > 0) boundaries depend on timing | kept | – |

## Resume and durability

| Java | Go | Status | Decision |
|---|---|---|---|
| `swath resume <dir>` | `swath resume DIR` | kept | – |
| `--checkpoint auto` (`.swath/checkpoint.sqlite`) | journal at `DIR/.swath/journal`, always on for S3 and GCS; local runs keep none | renamed | record journal replaces the SQLite checkpoint, 2026-11-05 |
| `--checkpoint PATH` | none | removed | record journal replaces the SQLite checkpoint, 2026-11-05 |
| `--checkpoint none` | `swath diag list --no-journal` | renamed | – |
| `--restart` | none; `--overwrite` starts over an interrupted run | renamed | – |
| `--overwrite`, `--force` | `--overwrite` | kept (alias `--force` removed) | no decision recorded (alias) |
| Finalized parts kept exactly once; the unfinished tail of each range re-listed from its durable cursor | committed parts kept exactly once; everything after the last committed part's key re-listed | kept (coarser) | part-granular journal, re-list after the last published key, 2026-11-05 |
| Resume refuses a changed identity (target, filters, output, run shape) | refuses a changed route or writer recipe (exit 1); a flag outside the runtime and operational lists is exit 2 | kept | – |
| Resume accepts every free operational option | accepts the listed runtime and operational flags only | kept (narrower) | – |
| Resume of a completed dataset exits 0 | exit 1, class `fatal`, "already complete" | removed | no decision recorded |
| Java checkpoint files | not read | removed | no migration of Java checkpoint files, 2026-11-05 |
| `dump-run` (hidden staging inspector) | `swath show [--journal] DIR` (manifest summary or journal dump) | renamed | staging not ported, 2026-11-05 |

## Limits and deadlines

| Java | Go | Status | Decision |
|---|---|---|---|
| `--max-duration DURATION` (exit 124, resumable) | `--max-duration` (0: none; exit 124, resumable) | kept | – |
| `--idle-timeout` (120 s, no activity) | `--stall-timeout` (10 min, no progress and no answered request) | renamed | one watchdog owns liveness, 2026-11-05 |
| `--no-progress-timeout` (10 min, no committed progress) | `--stall-timeout` | renamed | one watchdog owns liveness, 2026-11-05 |
| `--object-listing-queue-size N` (50,000 entries) | `--memory` (bytes; half bounds rows in flight) | renamed | one byte budget instead of the weighted channel, 2026-11-05 |

## Concurrency and throttling

| Java | Go | Status | Decision |
|---|---|---|---|
| `--concurrency N` (adaptive ceiling, default 64) | `--workers` (listing goroutines and the limiter's start, default `min(64, max(4, 3*GOMAXPROCS))`) | renamed | default: no decision recorded |
| `--request-rate N` (aggregate cap, 0: none) | `--request-rate` (every HTTP attempt counted, 0: none) | kept | – |
| Adaptive limit: ×0.7 per throttle answer, +1/s after a 10 s clean window | `--limiter` (default on), the same rule; `--limiter=false` fixes `--workers` in flight | kept | – |
| Slow start from 4, doubling per second | none; the limit starts at `--workers` | removed | no decision recorded |
| Thief splits (`steals`) | owner-only splits | removed | owner-only splits, 2026-11-05 |

## Observability and reports

| Java | Go | Status | Decision |
|---|---|---|---|
| `--report PATH` | `--report FILE` (written when the attempt ends, on failure too) | kept | – |
| `_swath_summary.json`, updated during the run | `run.json`, written after the commit | renamed | own report shape, Java fields not kept, 2026-11-05 and 2026-11-06 |
| `--stats` / `--no-stats` (stderr summary) | one stderr line always; `--report-format json` prints the report | removed | no decision recorded |
| `--progress` / `--no-progress` (stderr) | `DIR/.swath/progress.json` every 2 s | removed | no decision recorded |
| `--progress-interval DURATION` | none (fixed 2 s) | removed | no decision recorded |
| `list_run_summary` log line (`-v`) | the one-line text report | renamed | – |
| `--metrics-endpoint URL`, `SWATH_OTLP_ENDPOINT` | none | removed | no OTLP export, 2026-11-05 |
| `--no-metrics` | none | removed | no OTLP export, 2026-11-05 |
| Report: `schema_version` | `report_version` | renamed | – |
| Report: `run_id` | `attempt.id` | renamed | – |
| Report: `argv`, `config` | `invocation` | renamed | – |
| Report: `args_hash` | none (identity is in the journal) | removed | own report shape, 2026-11-06 |
| Report: `started_at`, `duration_ms`, `listing_duration_ms` | `timings.started_at`, `wall_ms`, `listing_ms` | renamed | – |
| Report: `session_duration_ms`, `keys_per_sec` | none | removed | own report shape, 2026-11-06 |
| Report: `objects`, `recovered_objects`, `bytes` | `output.rows_listed`, `rows_retained`, `bytes` | renamed | – |
| Report: `complete`, `exit_code`, `stop_reason` | `outcome.complete`, `exit_code`, `stop_reason` | renamed | – |
| Report: `error_class` | `outcome.cause` | renamed | – |
| Report: `stop_source` | none (`outcome.class`) | removed | own report shape, 2026-11-06 |
| Report: `engine_flags` | `resources.features` | renamed | – |
| Report: `recovered_errors`, `engine`, `seed`, `meters` | `source.stats.counters` (Go names; mapping in [upgrade.md](upgrade.md)) | renamed | no Java names, 2026-11-05 |
| Report: `cost`, `efficiency` | none | removed | own report shape, Java fields not kept, 2026-11-06 |
| Report: `sort` | none (spill: `resources.spill`) | removed | staging not ported, 2026-11-05 |
| Report: `trajectory`, `slow_ranges`, `probe_latency`, `client_cost`, `dataset_writer`, `demand_gate`, `shape` | none | removed | own report shape, Java fields not kept, 2026-11-06 |

## Exit codes

| Java | Go | Status | Decision |
|---|---|---|---|
| 0: success, empty result, already-complete resume, closed stdout | 0 `completed`; an already-complete resume is 1 | kept (narrower) | – |
| 1: unexpected failure; resumable sorted-disk guard (`sort_disk_exhausted`) | 1 `fatal` (resumable only with cause `no_space`) or `incomplete` (partial coverage) | kept | – |
| 2: bad argument, URI, config, changed resume identity, guarded output refusal | 2 `usage`: malformed command line only; changed identity and output refusals are 1 | kept (narrower) | – |
| 74: output filesystem full | 1, cause `no_space`, resumable | removed | no decision recorded |
| 75: stuck, resumable; `error_class` `stuck_api_timeouts`, `stuck_throttle`, `stuck_unknown` | 75 `stuck`, resumable; `cause` `stuck_timeouts`, `stuck_throttle`, `stuck_no_rows` | kept (causes renamed) | – |
| 124: `--max-duration` elapsed | 124 `deadline` | kept | – |
| 130: SIGINT | 130 `interrupted` | kept | – |
| 143: SIGTERM | 143 `terminated` | kept | – |
| Protocol violation outranks a resumable code and marks the run unresumable | exit 1, `resumable: false`; nothing recorded in the journal | kept (not journaled) | no decision recorded |

## Diagnostics

| Java | Go | Status | Decision |
|---|---|---|---|
| `--help`, `help` | `--help`, `help`, `swath list -h` | kept | – |
| `--version` (five lines) | `--version` (tool, version, revision) | kept | – |
| `-v`, `-vv`, `-vvv` | none | removed | no decision recorded |
| `-q`, `--quiet` | none | removed | no decision recorded |
| `--color auto\|always\|never` | none | removed | no decision recorded |
| `--engine-toggle NAME=VALUE` (hidden) | `swath diag list` with `--seed-descent`, `--probe`, `--floors`, `--structure`, `--sequential`, `--max-keys`, `--no-spill` | renamed | policies behind switches, 2026-11-05 |
| `--tune KEY=VALUE` (non-sort keys) | none | removed | no decision recorded |
| `--trace PATH` (JSONL run trace) | none; `SWATH_SPLIT_TRACE` traces S3 splits | removed | no decision recorded |
| Bare `swath s3://…` is an error that suggests `swath list` | usage error, exit 2, prints the command list | kept | – |
| Stuck: a snapshot of thread stacks and in-flight counts to the log before interrupting | goroutine stacks and progress counts to `DIR/.swath/stacks-stuck.txt` or `stacks-forced-exit.txt`, else stderr | kept | – |
