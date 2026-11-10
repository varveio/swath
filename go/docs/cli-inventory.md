# Java CLI inventory

Every option, output mode, exit code, report field class and documented guarantee of the Java `swath` CLI
(`swath-cli`, [docs/cli.md](../../docs/cli.md), [docs/usage.md](../../docs/usage.md),
[docs/metrics-and-observability.md](../../docs/metrics-and-observability.md)) against this tool
([cli.md](cli.md), [upgrade.md](upgrade.md)). Dates are owner decisions.

Legend (the Decision column records implementation dates and reasons for omitting Java features): **kept** (same name and meaning, differences noted), **renamed** (to the Go name given),
**removed** (with the recorded decision and its date, or "no decision recorded"), **deferred** (a recorded
decision to add it later).

## Connection and credentials

| Java | Go | Status | Decision |
|---|---|---|---|
| `--region REGION` | `--region` (default chain ends at `us-east-1`) | kept | – |
| `--profile NAME` | `--profile` | kept | – |
| `--endpoint-url URL` | `--endpoint URL` (one flag for S3, GCS and Azure Blob; `--gcs-endpoint` and `--azblob-endpoint` removed; also `--request-timeout`, default 30 s, for all three, `--gcs-request-timeout` and `--azblob-request-timeout` removed) | renamed | ported 2026-11-06 (owner: restore the cheap Java CLI features); shared by every object store 2026-11-09 (issue 338) |
| `--force-path-style` / `--no-force-path-style` | `--path-style` (boolean; default on with a custom endpoint, as Java's) | renamed | ported 2026-11-06 (owner: restore the cheap Java CLI features) (default) |
| `--no-sign-request` | `--anonymous` (one flag for S3, GCS and Azure Blob; `--gcs-anonymous` and `--azblob-anonymous` removed) | renamed | shared 2026-11-09 (owner: anonymity is explicit) |
| `--requester-pays requester` | `--request-payer` (boolean) | renamed | – |
| `--fetch-owner` | `--fetch-owner` | kept | – |
| `--bearer-token-command CMD` | `--bearer-token-command` (S3 client: `Authorization: Bearer`, fail closed, never recorded; a resume without it is refused with a message) | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `--bearer-token-refresh-interval` (45 m) | `--bearer-token-refresh-interval` (45 m; one refresh in flight) | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| Credential-chain failure fails the run | missing or unusable credentials stop listing; requests go unsigned only with `--anonymous`. See [credentials](operating.md#credentials) for the Azure admission exception to `auth` | kept | 2026-11-09: the Go port's automatic anonymity for an empty chain removed, as Java |
| Source `s3://bucket[/prefix]`, path taken literally | the same; also local/mounted directories, native `gs://bucket[/prefix]` and Azure flat namespaces at `az://account/container[/prefix]` | kept | – |
| S3 directory buckets refused before the first request | refused before any request (a name ending in `--x-s3`), exit 2, class `refused` | kept | restored 2026-11-06 (owner principle: keep what Java had for a production reason, `3ccb1a8`) |

## Listing scope and filters

| Java | Go | Status | Decision |
|---|---|---|---|
| `--include REGEX` | `--include` (Go RE2, found anywhere; `^literal` also narrows the listed prefix on S3 and GCS; Azure filters rows without prefix narrowing); `coverage.filters` records each filter | kept (RE2, not Java regex) | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `--exclude REGEX` | `--exclude` (Go RE2) | kept (RE2, not Java regex) | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `--min-size SIZE` | `--min-size` (Java's units) | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `--max-size SIZE` | `--max-size` | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `--modified-since DATE` | `--modified-since` | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `--modified-until DATE` | `--modified-until` | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `--storage-class CLASS[,CLASS]` | `--storage-class` | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| Current objects only; versions not listed | current objects by default; `--all-versions` adds S3 versions/delete markers or GCS generations | extended | implemented in Go; [current CLI](cli.md) and [schema registry](dataset.md#schema-registry) |

## Output formats and datasets

| Java | Go | Status | Decision |
|---|---|---|---|
| `--format auto\|table\|tsv\|jsonl` | `--format auto\|table\|tsv\|jsonl`, stdout only (auto: the table on a terminal, TSV otherwise); with `-o` refused, the name decides | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features); stdout only 2026-11-06 (owner: text output in the shapes people already know): a file's name already says its format |
| `--format parquet` | none: any `-o` without a text extension is the dataset | removed | not ported, 2026-11-06 (owner: text output in the shapes people already know): no value (the `-o` name already decides) |
| Text row fields `key size last_modified etag storage_class row_type` (+ JSONL `owner_id owner_display_name checksum_algorithm checksum_type`) | the source schema's columns: `key`, then every column in the dataset's order under its name, values as stored | renamed | 2026-11-06 the S3 API's object; 2026-11-10 (owner: text output works for every source through one interface, the schema) |
| `row_type` (always `OBJECT`) | none | removed | not ported, 2026-11-06 (owner: text output in the shapes people already know): no value (every row is an object) |
| `last_modified` `...Z` with 0, 3 or 6 fraction digits | every time column UTC with 6 fraction digits, `...Z` | renamed | 2026-11-10 (owner: text output works for every source through one interface, the schema) |
| TSV/table escaping: a control byte as `\xNN`, backslash unescaped | TSV and the table: `\\ \t \n \r`, other control bytes, DEL and bytes that are not UTF-8 as `\xHH`, `\N` a null; JSONL: JSON's escapes, `NAME_base64` for bytes that are not UTF-8 | renamed | 2026-11-10 (owner: text output works for every source through one interface, the schema): reversible for any bytes |
| Table: size in 14, time in 24, key | a header; the local time to the second, the size in binary units, `kind` and the version columns where the schema has them, the key | renamed | 2026-11-10 (owner: one rule for terminal tables) |
| `--format discard` | `swath diag count SOURCE` (rows counted as they arrive, never ordered, buffered or spilled; the Go port's `swath list --count-only` until 0.4, `swath count` 2026-11-07 to 2026-11-09) | renamed | command 2026-11-07 (owner: the cleanest CLI surface); `swath diag count` 2026-11-09 (owner: `swath du` totals a source per prefix with readable output, the measurement's one line moves to `diag`) |
| `-o`, `--output PATH` (file, directory or `-` for stdout) | `-o` only: a dataset directory, a `.tsv`/`.jsonl` file (optionally `.gz`/`.zst`), or `-`/none for stdout | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `--output-type file\|dir` | none: the `-o` name decides | removed | not ported, 2026-11-06 (owner: text output in the shapes people already know): no value (an extensionless text file names no format) |
| `-o NAME.parquet` one-writer directory layout | none (`-o NAME.parquet` is a dataset directory) | removed | not ported, 2026-11-06: no value now (Java's own FAQ calls it a trap) |
| `--compression none\|gzip\|zstd` (text only) | the `-o` name's `.gz`/`.zst`; stdout through a pipe (`\| zstd`) | removed | not ported, 2026-11-06 (owner: text output in the shapes people already know): no value (the name says it for a file; a pipe does it for stdout) |
| `--text-part-size` (256 MB) | none | removed | not ported, 2026-11-06: no value now (partitioned text datasets) |
| `--text-writers N` (3) | none | removed | not ported, 2026-11-06: no value now (partitioned text datasets) |
| `--writeback-size SIZE` (off) | none | removed | not ported, 2026-11-06: no value now |
| stdout: at most once; a closed reader exits 0 | stdout (no `-o`, or `-o -`); a closed reader exits 0, outcome `stopped`, `stop_reason` `downstream_closed`; not resumable | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| Text file published atomically | written beside the destination and installed when complete (synced, renamed, directory synced) | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| Text directory dataset, `_SUCCESS` last | none (`-o DIR --format tsv` is refused) | removed | not ported, 2026-11-06: no value now (partitioned text datasets) |
| `_SUCCESS` written last is the commit | `manifest.json` installed last (atomic rename, directory sync) is the commit | renamed | dataset format 2, 2026-11-05 |
| `symlink.txt` part list | none; the manifest lists the parts | removed | not ported, 2026-11-06: no value now |
| `manifest.json` (format 1, camelCase) | `manifest.json` format 2 with `coverage`; SeekParquet with `extensions.swath.coverage` | renamed | dataset format 2, 2026-11-05; SeekParquet, 2026-11-08 |
| `.swath-state.json` | `.swath/` workspace (`lock`, `journal`, `parts/`, `spill/`) | renamed | – |
| Schema `swath/canonical`, 13 columns | `swath/s3-objects/v1` for current S3 objects; source- and mode-specific schemas include S3 versions and GCS generations | renamed | [current schema registry](dataset.md#schema-registry); no reserved version columns in current-object schemas |
| Footer stamps `swath.sort.*` | standard `sorting_columns`, `swath.profile*`, `swath.index_bound_bytes`, `swath.schema_id`; then `seekparquet.*` | renamed | dataset format 2, 2026-11-05; SeekParquet, 2026-11-08 |
| Non-empty output without ownership evidence refused; symlinked paths refused | non-empty `-o` directory refused unless `--overwrite`, which replaces only this tool's previous dataset; every write by pathname checks the output's identity | kept | – |
| One run per output at a time | output lock; journaled output refused on Windows | kept | – |

## Sorting and parts

| Java | Go | Status | Decision |
|---|---|---|---|
| `--sort` (globally key-sorted Parquet) | always sorted; no flag | kept | – |
| `--no-sort` (unsorted Parquet, direct dataset) | none | removed | not ported, 2026-11-06: architecture, measured first |
| Sorted parts key-disjoint in unsigned byte order, named in key order | the same, every run | kept | – |
| Staging under `_staging/`, k-way merge, finalization | ordered emission with disposable spill under `.swath/spill/` | removed | staging and external sort not ported, 2026-11-05; not ported, 2026-11-06: no value now (staging and merge controls) |
| Free-space checks before and during sorted listing; `--tune sort.ignore-disk-check` | `--spill-budget` (one disk budget, `--spill-max` until 0.4; the reserve is derived, `swath diag list --spill-reserve` overrides it); a full disk stops the run with exit 74, cause `no_space` | renamed | sort settings are code constants, 2026-11-05 |
| `--tune sort.*` (three keys of Java's seven-key `--tune` registry; the other "sort" settings were JVM properties) | none | removed | sort settings are code constants, 2026-11-05 |
| `--parquet-part-size SIZE` (256 MB) | `--part-size` (64 MiB) | renamed | – |
| `--part-rotation-interval DURATION` (30 s; 0 off) | `--part-interval` (30 s; 0 off; in the Performance help group: the resume granularity) | renamed | default restored to Java's 30 s 2026-11-06 (owner principle, `7d6d746`); kept on `swath list` 2026-11-07 (owner: an operator's resume-granularity knob) |
| `--part-rotation-max-rows N` (2,000,000) | `--part-rotation-max-rows N` (default 0: off; part of the resume identity; `resources.part_row_cuts`) | kept (default off) | ported 2026-11-06 (owner: restore the cheap Java CLI features); default off: Go's 64 MiB parts already bound a part, and a default would move every run's boundaries |
| Same input and configuration give the same parts at any encoder count | the same; with `--part-interval` cuts (`resources.part_time_cuts` > 0) boundaries depend on timing | kept | – |

## Resume and durability

| Java | Go | Status | Decision |
|---|---|---|---|
| `swath resume <dir>` | `swath resume DIR` | kept | – |
| `--checkpoint auto` (`.swath/checkpoint.sqlite`) | journal at `DIR/.swath/journal`, on for S3, GCS and Azure Blob datasets where supported; local runs keep none | renamed | record journal replaces the SQLite checkpoint, 2026-11-05 |
| `--checkpoint PATH` | none | removed | record journal replaces the SQLite checkpoint, 2026-11-05; not ported, 2026-11-06: no value now (the SQLite checkpoint surface) |
| `--checkpoint none` | `swath diag list --no-journal` | renamed | – |
| `--restart` | none; `--overwrite` starts over an interrupted run | renamed | – |
| `--overwrite`, `--force` | `--overwrite` | renamed | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| Finalized parts kept exactly once; the unfinished tail of each range re-listed from its durable cursor | published parts retained; validated durable spill reused where supported, remaining gaps re-listed | changed | [current recovery contract](operating.md#stopping-and-resuming) replaces the earlier prefix-only description |
| Resume refuses a changed identity (target, filters, output, run shape) | refuses a changed route or writer recipe (exit 2, `refused`); a flag outside the runtime and operational lists is exit 2 | kept | – |
| Resume accepts every free operational option | accepts the listed runtime and operational flags only | kept (narrower) | – |
| Resume of a completed dataset exits 0 | exit 0, class `completed`, report `complete` true (partial coverage: 1, `incomplete`) | kept | restored 2026-11-06 (owner principle: keep what Java had for a production reason, `b62ddbb`) |
| Java checkpoint files | not read | removed | no migration of Java checkpoint files, 2026-11-05 |
| `dump-run` (hidden staging inspector) | `swath show [--journal] DIR` (manifest summary or journal dump) | renamed | staging not ported, 2026-11-05 |

## Limits and deadlines

| Java | Go | Status | Decision |
|---|---|---|---|
| `--max-duration DURATION` (exit 124, resumable) | `--max-duration` (0: none; exit 124, resumable) | kept | – |
| `--idle-timeout` (120 s, no activity) | `--stall-timeout` (10 min, no progress in key space; an empty continuing page counts within the first 1,024 of its run) | renamed | one watchdog owns liveness, 2026-11-05 |
| `--no-progress-timeout` (10 min, no committed progress) | `--stall-timeout` | renamed | one watchdog owns liveness, 2026-11-05 |
| `--object-listing-queue-size N` (50,000 entries) | `--row-budget` (bytes of rows in flight, default 128 MiB) | renamed | one byte budget instead of the weighted channel, 2026-11-05; `--memory` (twice the row budget) renamed 2026-11-06 to name what it bounds |

## Concurrency and throttling

| Java | Go | Status | Decision |
|---|---|---|---|
| `--concurrency N` (adaptive ceiling, default 64) | `--workers` (listing goroutines and the limiter's start, default 64, as Java's, whatever the CPU count; a local path, which Java did not list, `min(64, 4 × GOMAXPROCS)`, #388); the report names `invocation.workers` | renamed | default 64: owner decision 2026-11-06 |
| `--request-rate N` (aggregate cap, 0: none) | `--request-rate` (every HTTP attempt counted, 0: none) | kept | – |
| Adaptive limit: ×0.7 per throttle answer, +1/s after a 10 s clean window | `--limiter` (default on) adapts concurrency to sustained throttling and recovery; `--limiter=false` removes the limit. [Request admission](internals/clients.md#request-admission) owns the controller rules and counters. | changed | [Dated tuning evidence](measurements.md#red-team-fixes-2026-11-07) |
| Slow start from 4, doubling per second | `swath diag list --limiter-slow-start` enables an experimental ramp, off by default; [request admission](internals/clients.md#request-admission) owns its initial limit and growth rules. | kept | – |
| Thief splits (`steals`) | owner-only splits | removed | owner-only splits, 2026-11-05; not ported, 2026-11-06: no value now |

## Observability and reports

| Java | Go | Status | Decision |
|---|---|---|---|
| `--report PATH` | `--report FILE` (written when the attempt ends, on failure too) | kept | – |
| `_swath_summary.json`, updated during the run | `run.json`, written after the commit | renamed | own report shape, Java fields not kept, 2026-11-05 and 2026-11-06 |
| `--stats` / `--no-stats` (stderr summary) | `--stats` / `--no-stats`: Java's block on stderr with Java's gating; the one-line summary stays on stdout | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `--progress` / `--no-progress` (stderr) | `--progress` / `--no-progress`: one record redrawn on a terminal, appended elsewhere, from the snapshot `progress.json` is written from | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `--progress-interval DURATION` | `--progress-interval` (1 s redrawing, 30 s appended; floor 1 s) | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `list_run_summary` log line (`-v`) | the one-line text report | renamed | – |
| `--metrics-endpoint URL`, `SWATH_OTLP_ENDPOINT` | none | removed | no OTLP export, 2026-11-05; not ported, 2026-11-06: no value now |
| `--no-metrics` | none | removed | no OTLP export, 2026-11-05 |
| Report: `schema_version` | `report_version` | renamed | – |
| Report: `run_id` | `attempt.id` | renamed | – |
| Report: `argv`, `config` | `invocation` | renamed | – |
| Report: `args_hash` | `args_hash`: SHA-256 of the Go run's frozen identity (filters included; not Java's value) | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| Report: `started_at`, `duration_ms`, `listing_duration_ms` | `timings.started_at`, `wall_ms`, `listing_ms` | renamed | – |
| Report: `session_duration_ms`, `keys_per_sec` | `timings.session_ms` (from `attempt.started`), `efficiency.keys_per_sec` (`output.rows_emitted` over `listing_ms`) | renamed | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| Report: `objects`, `recovered_objects`, `bytes` | this attempt: `output.rows_fetched` (from the source), `rows_emitted` (after the filters), `object_bytes`; the dataset: `rows_retained`, `rows_durable`, `rows_committed`, `output_bytes` (Java's `objects` is the dataset's rows: `rows_committed`) | renamed | report version 2, 2026-11-07 (owner: the cleanest CLI surface) |
| Report: `complete`, `exit_code`, `stop_reason` | `outcome.complete`, `exit_code`, `stop_reason` (a run that did not commit: `complete` false, its class as `stop_reason`, `83668d0`) | renamed | – |
| Report: `error_class` | `outcome.cause` | renamed | – |
| Report: `stop_source` | `outcome.stop_source` (`sigint`, `sigterm`, `liveness_watchdog`, `timebox`, `+forced_exit`) | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| Report: `engine_flags` | `resources.features` | renamed | – |
| Report: `recovered_errors`, `engine`, `seed`, `meters` | `source.stats.counters` (Go names; mapping in [upgrade.md](upgrade.md)) | renamed | no Java names, 2026-11-05 |
| Report: `cost`, `efficiency` | `requests` (`http_attempts`: Java's `cost.api_calls`; `retries`, `throttles`, `timeouts`), `cost` (`cost_usd`, `basis`; null on a custom endpoint and for Azure Blob), `efficiency` (`keys_per_sec`, `http_attempts_per_1k_rows`) | renamed | ported 2026-11-06 (owner: restore the cheap Java CLI features); one request count, report version 2, 2026-11-07 (owner: the cleanest CLI surface) |
| Report: `sort` | none (spill: `resources.spill`) | removed | staging not ported, 2026-11-05 |
| Report: `trajectory`, `slow_ranges`, `probe_latency`, `client_cost`, `dataset_writer`, `demand_gate`, `shape` | none | removed | own report shape, 2026-11-06; not ported, 2026-11-06: no value now (the diagnostic evidence block) |

## Exit codes

| Java | Go | Status | Decision |
|---|---|---|---|
| 0: success, empty result, already-complete resume, closed stdout | 0 `completed`, an already-complete resume included | kept | – |
| 1: unexpected failure; resumable sorted-disk guard (`sort_disk_exhausted`) | 1 `fatal` (not resumable) or `incomplete` (partial coverage); a full disk is 74 | kept | – |
| 2: bad argument, URI, config, changed resume identity, guarded output refusal | 2 `usage` (malformed command line, unknown scheme) or `refused` (guarded output refusal, changed resume identity, nothing to resume) | kept | restored 2026-11-06 (owner principle: keep what Java had for a production reason, `309e562`) |
| 74: output filesystem full | 74 `disk_full`, cause `no_space`, resumable (a part, the journal or the manifest) | kept | restored 2026-11-06 (owner principle: keep what Java had for a production reason, `0f5fb48`) |
| 75: stuck, resumable; `error_class` `stuck_api_timeouts`, `stuck_throttle`, `stuck_unknown` | 75 `stuck`, resumable; `cause` `stuck_timeouts`, `stuck_throttle`, `stuck_no_rows`, `stuck_empty_chain` | kept (causes renamed) | `stuck_empty_chain` added 2026-11-06 (review R3): Java rejects a continuing empty page at once; Go follows them, bounded |
| 124: `--max-duration` elapsed | 124 `deadline` | kept | – |
| 130: SIGINT | 130 `interrupted` | kept | – |
| 143: SIGTERM | 143 `terminated` | kept | – |
| Protocol violation outranks a resumable code and marks the run unresumable | exit 1, `cause` `protocol`, `resumable: false`; the journal's terminal `unresumable` frame; `swath resume` refuses | kept | restored 2026-11-06 (owner principle: keep what Java had for a production reason, `f29d047`) |

## Diagnostics

| Java | Go | Status | Decision |
|---|---|---|---|
| `--help`, `help` | `--help`, `help`, and `--help` on every command (shared groups first, then provider hints by scheme within each section; limits and performance flags say when to change them) | kept | – |
| `--version` (five lines) | `--version` (tool, version, revision) | kept | – |
| `-v`, `-vv`, `-vvv` | `-v` (`-vv`, `-vvv` the same): a start line, progress as appended records | kept (one level) | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `-q`, `--quiet` | `-q` (no warnings, no automatic progress or statistics); `-qq` also no error line | renamed | ported 2026-11-06 (owner: restore the cheap Java CLI features); one name per flag 2026-11-07 (owner: no aliases) |
| `--color auto\|always\|never` | `--color auto\|always\|never` (`NO_COLOR`, `TERM=dumb`, `CLICOLOR_FORCE`) | kept | ported 2026-11-06 (owner: restore the cheap Java CLI features) |
| `--engine-toggle NAME=VALUE` (hidden) | `swath diag list` with `--floors`, `--structure=auto\|on\|off`, `--sequential`, `--max-keys`, `--no-spill` | renamed | policies behind switches, 2026-11-05 |
| `--tune seed.mode=none\|shallow` | `swath diag list --seed=false` (the seed is on by default) | renamed | moved to `swath diag` 2026-11-07 (owner: the cleanest CLI surface) |
| none (proposed as `--seed hints`, never built) | `--seed-from PATH`: start from a previous dataset's (or a cut file's) key distribution instead of the seed | added | 2026-11-07 (warm start; [internals/range-engine.md](internals/range-engine.md)) |
| `--tune KEY=VALUE` (other non-sort keys) | none | removed | not ported, 2026-11-06: no value now (`summary.interval`; `parquet.writers` belongs to the unsorted dataset; `engine.readahead`) |
| `--trace PATH` (JSONL run trace) | none; `SWATH_SPLIT_TRACE` traces S3 splits | removed | not ported, 2026-11-06: no value now (the diagnostic evidence block) |
| Bare `swath s3://…` is an error that suggests `swath list` | usage error, exit 2, prints the command list | kept | – |
| Stuck: a snapshot of thread stacks and in-flight counts to the log before interrupting | goroutine stacks and progress counts to `DIR/.swath/stacks-stuck.txt` or `stacks-forced-exit.txt`, else stderr | kept | – |
