# Migrating from 0.3

The latest published release, [v0.3.2](https://github.com/varveio/swath/releases/tag/v0.3.2), is an S3-only
command line with its own dataset format (format 1). The next release replaces it: a different command line,
more sources, and datasets in the [SeekParquet](https://github.com/varveio/seekparquet) format (`format_version`
0, `draft_revision` 1) defined by the swath dataset profile in [dataset.md](dataset.md). This page is the
migration, for two audiences:

- **Someone who ran 0.3**: what stays the same and what changed on the
  [command line](#the-command-line-what-stays-the-same), in the [run report](#run-report-and-counters), and
  what a format 1 dataset becomes ([Files](#files) through [Versions](#versions)); [Changes from 0.3 to
  0.4](#changes-from-03-to-04) sums them up, and [Unreleased builds of `main`](#unreleased-builds-of-main) lists
  what changed for anyone who ran a pre-release build.
- **A reader of the 0.3 layout** (the downstream reader, `bench/diffds.py`, `bench/summaries.py`): what it must
  change, in [What a reader of the Java layout changes](#what-a-reader-of-the-java-layout-changes).

Every 0.3 option, output mode, exit code and report field, with its counterpart and the decision behind it, is
in [internals/cli-inventory.md](internals/cli-inventory.md).

Neither format is a superset of the other: there are no aliases, so a reader of one refuses the other, and a
0.3 reader cannot open a SeekParquet dataset by design. DuckDB, the SeekParquet conformance suite and the
downstream reader are the acceptance gates ([Gates](#gates)).

## The command line: what stays the same

- **Sources.** `s3://bucket[/prefix]` on general-purpose buckets, with the same credential chain, profiles
  (`--s3-profile`), regions (`--s3-region`), S3-compatible endpoints (`--endpoint-url`) and bearer tokens for
  GCS's XML API (`--s3-bearer-token-command`). swath now adds native `gs://`
  through the GCS JSON API, `az://account/container[/prefix]` for Azure Blob flat namespaces,
  and local or mounted directory trees.
- **Output.** Object-store text rows as a table, TSV or JSONL, on stdout or in a `.tsv` or `.jsonl` file
  (optionally `.gz` or `.zst`), and a sorted Parquet dataset in a directory. Every source, a local
  tree included, lists as text rows or into a dataset; `swath du` prints totals on stdout. 0.3's filters are kept: its regular expressions are
  `--include-regex` and `--exclude-regex` (RE2 syntax, not Java's), `--include` and `--exclude` take globs, include
  and exclude patterns and storage classes accumulate when repeated, and for each size or time bound the last value
  wins. A size's `MB` is decimal (`M` and `MiB` binary).
- **Resume.** `swath resume DIR` continues an eligible journaled S3, GCS or Azure Blob dataset run.
  Local scans and text output cannot resume. Windows currently refuses journaled object-store runs
  and resume ([platform limits](running.md#known-limits)). The exit codes keep their meaning:
  0, 1, 2, 74, 75, 124, 130 and 143; 3 is new: the run finished, but entries could not be read or `--max-keys`
  stopped the listing, which 0.3 exited 1 for ([exit codes](reference/exit-codes.md)).
- **Controls.** `--max-duration`, `--request-rate`, `--report`, `--progress`, `--stats`, `-v` and `-q`.

## The command line: what behaves differently

- **Always sorted, while listing.** 0.3 writes unsorted parts unless asked for `--sort`, then sorts staged
  runs with a merge at the end. Here every dataset is globally key-sorted, and the order comes from the listing
  itself: rows wait in a memory budget (`--sort-buffer`: 1/16 of memory, at most 1 GiB, as 0.3 sized its sort
  segments from the heap), and rows listed ahead of the output go to
  disposable spill files: a dataset's in its workspace, a text file's beside it, a stdout stream's under `TMPDIR`
  (`--spill-budget`, by default half the spill filesystem's free space after a reserve; at the cap, listing waits). There is
  no merge pass at the end. A large sorted run spills almost every row once, so plan disk for it
  ([production.md](production.md#disk-sizing)).
- **The durable prefix.** A resume keeps published parts and continues after the last published key.
  Validated durable spill can preserve additional work where supported; gaps are listed again.
  This differs from 0.3's per-range checkpoints. It does not promise to retain every row already
  fetched ([recovery details](running.md#stopping-and-resuming)).
- **The dataset.** `manifest.json` installed last is the commit (not `_SUCCESS`). Schemas are source-
  and mode-specific: current objects, local entries, S3 versions and GCS generations have different
  fields ([dataset.md](dataset.md)). A 0.3 reader does not read this format ([Dispatch](#dispatch)).
- **The request limiter.** 0.3 cuts concurrency on every throttle; swath now uses a recent-answer window and
  at most one cut per round trip. S3, GCS and Azure Blob share the controller, with provider-specific throttle
  signals. See [request controls and tuning](running.md#throttling).
- **The seed.** Before ranges list, delimiter requests tile the bucket at its folders, in parallel waves, and
  the first wave also fetches the first data page, so a one-page bucket lists in one round trip. The seed is on
  by default. `--seed-from PATH` starts from a previous dataset instead.
- **Splits.** A busy range is split by its owner when workers are idle; there is no thief.
- **Counting is a command.** 0.3's `--format discard` is `swath diag count SOURCE`; `swath du SOURCE [-d N]`
  totals a source per prefix, as `du`.
- **Text rows** are the dataset's columns under its names (`key`, `size`, `last_modified`, ...), for every
  source, a local tree's included.

## Renamed flags

The flags kept, renamed and removed are in [internals/cli-inventory.md](internals/cli-inventory.md). The most common ones:
`--concurrency` is `--workers`, `--no-sign-request` is `--anonymous`, `--endpoint-url` keeps its
name, `--profile` and `--region` are `--s3-profile` and `--s3-region`, `--parquet-part-size` is `--part-size` (64 MiB by default, 0.3's was 256 MB), and
`--format parquet --sort -o DIR` is simply `-o DIR`. Object stores share `--anonymous`,
`--endpoint-url` and `--request-timeout`; provider-specific credentials and limits are described in
[sources.md](sources/README.md).

## The run report at a glance

The report is `run.json` beside a committed dataset and the `--report FILE`; 0.3's was
`_swath_summary.json`. The fields readers most often used were renamed:

| 0.3 | Now |
|---|---|
| `schema_version` | `report_version` (7) |
| `run_id` | `attempt.id` |
| `objects` (the dataset's rows) | `output.rows_committed`; this attempt's rows are `output.rows_emitted` |
| `bytes` | `output.committed_object_bytes` (the committed dataset's object sizes); this attempt's are `output.object_bytes`, the dataset's own size `output.output_bytes` |
| `cost.api_calls` | `requests.http_attempts` |
| `duration_ms`, `listing_duration_ms` | `timings.wall_ms`, `timings.listing_ms` |
| `error_class` | `outcome.cause` |
| `engine_flags` | `resources.features` |

0.3's engine, seed and meter blocks have no counterpart. The engine's counters are under
`source.stats.counters` with their own names ([internals/counters.md](internals/counters.md)). The full schema
is [reference/run-report.md](reference/run-report.md).

## What 0.3 had that is not here

- **Azure hierarchical namespaces.** swath lists flat namespaces only; with `--all-versions` it also lists
  blob versions, snapshots and soft-deleted blobs.
- **S3 directory buckets and GCS soft-deleted objects.** These are outside the supported listing scope.
  swath does support `--all-versions` for S3 versions and delete markers, GCS live and noncurrent
  generations and Azure Blob versions, snapshots and soft-deleted blobs, under their own schemas. 0.3 does not implement these modes.
- **0.3's thief splits, unsorted Parquet (`--no-sort`), the SQLite checkpoint, OTLP metrics, partitioned
  text datasets and `--tune`.** Each decision and its reason is in [internals/cli-inventory.md](internals/cli-inventory.md).

## Measured against 0.3

On 251 production key lists served through a fake at 50 ms per page, at 64 workers each, swath was
slower than the 0.3 command line on none and took a median 0.26x its wall time (`head1`, 2026-11-07). On the four
replayed recordings it was 3.5x to 13.5x faster (`recordings1`, 2026-11-07). On one core, sorted Parquet of
130M to 145M keys took 246 to 328 s against 0.3's 362 to 515 s (`delta1b`, 2026-11-07). On a live bucket of
1.09 billion objects at 1,024 workers, 0.3's sorted run was faster: 654 s against 846 s, with a 16 GB
heap against 2.2 GB RSS (`cogslive2`, 2026-11-07). That run was bound by the single Parquet part being written
after listing ended. The caveats: the fake and replay runs measure the engine's coordination, not S3; most
arms ran once; and the live run was cross-cloud, from GCP us-east1 to a bucket in AWS us-west-2. The tables
are in [internals/measurements.md](internals/measurements.md).

## Endpoint validation

S3 and GCS endpoints with a query or fragment are now refused; Azure Blob already refused
nonempty queries and fragments. All three also refuse empty `?` and `#` delimiters and user
information, and a path whose escapes decode to a query, a fragment, a percent sign or a control character
(`%3F`, `%23`, `%25`, `%0A`). A root with user information, `?` or `#` in its authority is refused.
Supply credentials through the provider's credential settings instead of `--endpoint-url`. A refused
endpoint is a usage error (exit 2). Refused endpoint messages use fixed rule text; recorded URL
arguments with credentials become `[redacted]`. Clean roots and endpoint values remain visible in diagnostics,
including resume mismatches. `swath show` and `swath show --journal` redact credentials
recorded by older builds without rewriting the journal or changing identity comparisons.
See [endpoint validation](internals/clients.md#endpoint-validation).

## SeekParquet

SeekParquet is a sorted-key format for Apache Parquet: ordinary Parquet parts, sorted by one byte-string key
and split into key-disjoint parts, and a manifest that lists them in key order and commits them. Any Parquet
reader reads the parts as a table; a reader that knows the format (the Go module
`github.com/varveio/seekparquet/go`, which `swath-replay` serves datasets through) seeks a key, reads key
ranges and lists a prefix with delimiter rollups from the manifest, the footers, the key column's page index
and a few pages. What swath adds on top (schema ids, `extensions.swath`) is in [dataset.md](dataset.md).

## Dispatch

A reader dispatches on `manifest.json`, before looking for anything else:

| `manifest.json` has | Format | A dataset when |
|---|---|---|
| `"format": "seekparquet", "format_version": 0, "draft_revision": 1` | SeekParquet | the manifest is present; its coverage (`extensions.swath.coverage`) records how much the listing reached ([Coverage](#manifest)) |
| `fileFormat`, `files`, `schemaId` (camelCase) | 1 (Java) | `_SUCCESS` exists, and the run summary covers the target (the legacy rule) |
| anything else | – | never: not a dataset of either tool |

A SeekParquet reader refuses every other version pair (`unsupported_version`). Builds of this port from `main`
between 2026-11-05 and 2026-11-08 wrote `"format": "swath-dataset"` with `format_version` 2 (format 2); no
reader reads it, and this tool does not take its directory for its own output (`--overwrite` refuses it), so
re-list it into a new directory.

## Files

| Format 1 | SeekParquet |
|---|---|
| `_SUCCESS` (written last; the commit) | removed: `manifest.json` is the commit, written after every part: in a local directory by an atomic rename and a directory sync, in a bucket by a create that succeeds only if no manifest is there |
| `symlink.txt` (the part list) | removed: the manifest lists the parts |
| `manifest.json` | `manifest.json`, new shape ([Manifest](#manifest)) |
| `_swath_summary.json` (S3), `_swath_local_summary.json` (local) | one `run.json` for every provider (`report_version` 7: [Run report and counters](#run-report-and-counters)); operational only, written after the commit, and never part of acceptance |
| `data/part-NNNNN.parquet` | unchanged |
| `.swath/journal`, `.swath.lock`, `data/*.tmp` | `.swath/` is the private workspace: `lock`, `journal`, `parts/` (unpublished parts), `spill/`. Readers ignore it |

## Manifest

The manifest's members and `extensions.swath` are defined in [dataset.md](dataset.md#the-swath-extension). Field
by field, from format 1:

| Format 1 field | SeekParquet field |
|---|---|
| `version` ("1") | `format`, `format_version`, `draft_revision` |
| `schemaId` | `schema_id` |
| – (the key column is `key`) | `key`: `{"column": "key", "unique": true}`; `unique` is false for the versions schemas (`--all-versions`), whose keys repeat on consecutive rows and never across parts |
| – | `extensions.swath.version`: `1`, the version of the extension's own contract; a reader refuses any other value, or none ("unsupported swath metadata version") |
| `creationTimestamp` (epoch ms) | `extensions.swath.published_at` (RFC 3339, UTC) |
| `sourceKind`, `sourceRoot`, `sourceBucket` | `extensions.swath.source.kind`, `.uri`, `.container` |
| – (inferred from the run summary's `config.target`) | `extensions.swath.scope.kind` (`whole` or `prefix`), `.prefix` |
| – (run summary `completed`, `config.filters`) | `extensions.swath.coverage` |
| `files[].key` | `parts[].path` (relative to the dataset root) |
| `files[].size`, `rowCount`, `MD5checksum`, `minKey`, `maxKey` | `parts[].bytes`, `rows`, `sha256`, `min_key_base64`, `max_key_base64` (exact, never truncated; canonical standard base64 of the key bytes) |
| – | `extensions.swath.committed_object_bytes` (the dataset's object sizes summed; absent when unknown) |
| `fileFormat`, `sorted`, `sortKey`, `fileSchema`, `totalRows` | removed (the format implies the first four; sum `rows` for the total) |

`parts[].sha256` is the part's SHA-256 in lowercase hexadecimal, SeekParquet's own part member (PART-5), which
the format's validators check at the digest level. MD5 is not written anywhere. Development builds before
`extensions.swath.version` existed wrote manifests without it (some with the parts' MD5s under
`extensions.swath.part_md5`). swath's metadata readers (`reader`, `swath show`, `--seed-from`) refuse them. A
generic SeekParquet reader still opens them, because it ignores the extension. Re-list them into a new directory.

**Keys are bytes.** Part bounds are the key bytes in standard base64, so a key that is not UTF-8 survives
JSON; readers decode them and compare bytes ([dataset.md](dataset.md#keys-and-order)).

**Coverage** moves from `_SUCCESS` and the run summary to the manifest's coverage, described in
[dataset.md](dataset.md#the-swath-extension); what a partial or filtered listing means for it is the application's
decision. A dataset of another producer may be SeekParquet without a swath extension.

## S3 schema

`swath/canonical` becomes `swath/s3-objects/v1`, the current-object schema: every row is a current object.

| Column | Format 1 | SeekParquet |
|---|---|---|
| `key`, `size`, `last_modified`, `etag`, `storage_class` | present | unchanged |
| `size` | INT64, unannotated | INT64 with the standard `INT(64,true)` annotation (physical type unchanged) |
| `owner_id`, `owner_display_name` | present | unchanged; null unless the run used `--s3-fetch-owner` |
| `checksum_algorithm`, `checksum_type` | present | unchanged; null when S3 omits them |
| `version_id` | always null for current objects | removed |
| `is_latest` | always null | removed |
| `is_delete_marker` | always false | removed |
| `row_type` | always `OBJECT` | removed |

The local schema becomes `swath/local-entries/v1`: `key` and `link_target` are unannotated BYTE_ARRAY (names are
bytes; a name that is not UTF-8 is listed exactly, where format 1 skipped it), `blocks` is renamed `allocated_bytes`, `ctime` is renamed
`metadata_changed_at`, and `mode`, `uid`, `gid`, `nlink` (`INT(32,false)`), `inode` and `dev`
(`INT(64,false)`) are unsigned.

A reader that needs the format-1 columns restores them as constants: `NULL`, `NULL`, `false`, `'OBJECT'`.
`bench/diffds.py` does exactly that, so a Go dataset compares with a Java dataset value for value.

## Footer

Every part's key/value metadata repeats the manifest's version and key declaration:

| Format 1 stamp | SeekParquet |
|---|---|
| `swath.sort.order` | removed; within-file order is the standard `sorting_columns` (key, ascending) on every row group |
| `swath.sort.mode` (`objects`) | removed |
| `swath.sort.format_version` | `seekparquet.format_version` = `0`, `seekparquet.draft_revision` = `1` |
| `swath.sort.file_index` | removed: the manifest orders the parts |
| `swath.sort.file_final` | removed: completeness is the manifest's |
| – | `seekparquet.schema_id` |
| – | `seekparquet.key_column` = `key`, `seekparquet.key_unique` = `true` or `false` (as the manifest's `key`) |

**Bounds.** Every column's ColumnIndex bounds are exact: each page's minimum and maximum (the key's: its first
and last key), as Parquet and SeekParquet (RG-5) require. The Java writer truncated key bounds and recorded
neither length it used: 1,024 bytes in served fixtures, the library default of 64 in older ones, cut on a code
point boundary, so a truncated bound can be as short as 61 bytes. Its readers therefore treat page bounds as
conservative; `swath-replay` treats every Java-era key bound of 61 bytes or more as possibly truncated
(`javaera.BoundBytes`). A SeekParquet reader need not. Statistics in the column chunk metadata are exact, as
they were.

**Replay fixtures.** `swath-replay` serves a SeekParquet dataset through the seekparquet module and a Java-era
fixture (format 1 sorted parts) through its own legacy reader (`internal/replay/legacy`); the startup line's
kind is `seekparquet` or `java-era`. The legacy reader is transitional: it goes once the Go replay has
replaced the Java replay (#352) and the fixtures are re-listed as SeekParquet datasets (#353); re-list a
Java-era fixture you still need before then. It refuses a dataset whose keys repeat (`key.unique` false, an
`--all-versions` listing) until the versions routes exist (#279): its routes continue after a key, never inside
a key's group.

## Versions

Format 1 had one version string. Five identifiers now move independently:
- the SeekParquet format and draft;
- the schema id, which versions the row contract (columns, key, a row's meaning);
- `extensions.swath.version`, which versions the extension's own contract (its members and their meaning), not
  the rows;
- the writer recipe;
- the private journal version.

The policy for each is in [dataset.md](dataset.md#evolution). This build writes journal
version 10 and reads versions 9 and 10 (9 records each part's SHA-256 where versions 7 and 8 recorded its MD5;
10 adds a bucket dataset's object names). Versions below 9 or above 10 are refused before recovery touches
anything, and earlier builds refuse version 10. Finish an interrupted run of an older build with
that build, or start it over with `--overwrite`. The writer recipe (`parquetout.Recipe`, `swath/parquet-r4`
since Parquet pages are compressed at zstd `SpeedFastest`) and the dataset format are in the resume identity: a build of another recipe or format
refuses the journal. `created_by` names the real build, so parts are byte-identical within a build only. A part
claimed but not recorded when a run stopped is reproduced on resume with its own footer's columns and
`created_by`, so that one part does not depend on the build that resumes.

## Run report and counters

`run.json` replaces Java's run summary (its schema: [reference/run-report.md](reference/run-report.md)). It keeps
these Java fields with their names: `cost.cost_usd` and `cost.basis` (`null` on a custom endpoint),
`efficiency.keys_per_sec`, `args_hash`
(the cloud importer's configuration digest; SHA-256 of the Go run's frozen identity, filters included, so its
value differs from Java's listing-scope hash), and `stop_source` (under `outcome`); Java's
`session_duration_ms` is `timings.session_ms`. Its other Java-only fields (`steals`, `sort.arm`,
`shape.fingerprint`, `trajectory`, `slow_ranges`, `probe_latency`, `demand_gate`, `shape`) are not kept
([internals/cli-inventory.md](internals/cli-inventory.md)). The downstream reader does not read it. A reader that tabulates runs
(`bench/summaries.py`) maps Java's top-level fields as: `completed` → `outcome.complete`, `objects` →
`output.rows_committed` (the dataset) or `output.rows_emitted` (this attempt), `cost.api_calls` →
`requests.http_attempts`, `duration_ms`/`listing_duration_ms` → `timings.wall_ms`/`listing_ms`, `config.target` →
`invocation.source`, `config.max_parallel_listings` → `invocation.workers`, `output.files` →
`output.parts_written` + `parts_retained`, `output.compressed_size_bytes` → `output.output_bytes`,
`shape.fingerprint.git_sha` → `build.revision`. Java meters map to the provider's counters
(`source.stats.counters.values`, here for S3) as follows:

| Java meter or field | Go (`source.stats.counters.values` unless noted) |
|---|---|
| `cost.api_calls`, `swath.api.calls` | `attempts` (the report's `requests.http_attempts`): every HTTP request sent, client retries and engine requeues included (Java counted fetch attempts with the SDK's retries off); `requests` counts logical calls |
| `swath.retries`, `recovered_errors.retries` | `retries` |
| `swath.throttle.events` | `throttles` (every throttled attempt, the last of an exhausted call included) |
| `engine.pages`, `swath.page.raw_count` | `pages` |
| `swath.entries.emitted`, `objects` | `objects` |
| `engine.splits` | `split.accepted_bounded` + `split.accepted_frontier` |
| `engine.steals`, `swath.steals` | – (splits are owner-side: always 0) |
| `swath.in_flight.avg`, `engine.avg_in_flight` | `inflight.busy_ns` / (`timings.listing_ms` × 10⁶): attempts in flight, send to body read; `mean_active` is ranges in `Expand`, which includes backoffs and admission waits |
| `engine.peak_in_flight` | `inflight.max_active` |
| `swath.workers.active` | `max_active` (ranges in `Expand` at once) |
| `swath.probe.fetches`, `swath.probe.empty_upper_bisections` | – (no 1-key probe; `child_empty.upper` counts the empty upper children it would have caught) |
| `swath.steal_reason{PIVOT,midpoint}` | `split.pivot.midpoint`, `split.pivot.reflect` (the anchor reflection, frontier and bounded); a sliver pivot is never taken: `split.sliver_refused` counts the refusals |
| `swath.steal_reason{PIVOT,extrapolate}` | `split.pivot.density.bounded` + `split.pivot.density.frontier` (the galloped density step) |
| `swath.steal_reason{STRUCTURE,*}` | `structure.*`; a split on a folder boundary is `split.pivot.structure` |
| `swath.steal_reason{OWNER_SPLIT,remaining_est_floor}` | no Go counterpart (the floors were removed); the admission gates are `split.skipped.remainder`, `split.skipped.lower_tiny` and `split.skipped.upper_tiny` |
| `swath.steal_reason{CHILD_MASS,empty}` | `child_empty.lower` + `child_empty.upper` |
| `swath.steal_reason{RETRY,cursor_passed_pivot}`, `swath.split.guard_aborts` | `split.rejected.cursor`, `split.rejected.*` |
| `swath.steal_reason{OWNER_SPLIT,demand_gated}` | `split.skipped.no_demand` |
| `swath.steal_reason{SEED,*}` | `seed.*` |
| `swath.sort.backpressure.wait` | `sink_wait_ns`; `resources`: `budget_wait_ns`, `frontier_wait_ns`, `encoder_wait_ns` |
| `swath.sort.segment.bytes` (staging) | `resources.spill.bytes` (spill replaces staging) |

A policy that was off reads `false` under `features` and its counters are absent; a present zero means the
policy was on and never fired ([internals/counters.md](internals/counters.md)).

The report's changes from version 1 to version 2 are in [Unreleased builds of `main`](#unreleased-builds-of-main).

## Command line

Launch scripts change as follows: `swath list SOURCE -out DIR` → `swath list SOURCE -o DIR`;
rerunning with `-resume` → `swath resume DIR`; `swath inspect DIR` → `swath show --journal DIR`; `-json` →
`--report -`; `-fields` → `--metadata`; `-sequential`, `-max-keys`, `-no-journal`,
`-no-spill` → `swath diag list …`; `swath list --count-only` → `swath diag count` (`swath count` from 2026-11-07 to 2026-11-09; its per-prefix totals are `swath du`); `SWATH_LOCAL_CPUPROFILE`/`MEMPROFILE`/`ENGINE`/`SPLIT_TRACE` →
`SWATH_CPUPROFILE`, `SWATH_MEMPROFILE`, `SWATH_LOCALFS_ENGINE`, `SWATH_SPLIT_TRACE`. Java's `--concurrency`
is `--workers`, default 64 as Java's, whatever the CPU count;
`--encoders` stays CPU-derived. Java's `--tune seed.mode=none` is `swath diag list --no-seed` (the seed is on
by default, as Java's `shallow` is), and `--format discard` is `swath diag count`, which counts without ordering or
spilling.
Text output keeps stdout (no `-o`, or `-o -`; a closed reader exits 0), `-o FILE.tsv|.jsonl[.gz|.zst]` and
`--format auto|table|tsv|jsonl` (stdout only; with `-o` the name decides). Its rows are the source schema's
columns under the dataset's names ([output.md](output.md#text-rows)): Java's `key`, `size`, `last_modified`,
`etag`, `storage_class`, `owner_id`, `owner_display_name`, `checksum_algorithm`, `checksum_type` keep their names,
in the dataset's order, with every other column of the schema (GCS's `generation`, a local tree's `kind`);
times have six fraction digits; a null is `\N` in TSV and `null` in JSONL; `row_type` is gone; the table has a
header, local time and binary units. Not
ported: `--output-type` and `--format parquet` (the `-o` name decides), `--compression` (the name compresses a
file; pipe stdout through `zstd`), a directory of text parts, `--text-part-size`, `--text-writers`, the
`.parquet` one-writer file. Text runs are not resumable: they keep no journal. Java's `--bearer-token-command` and `--bearer-token-refresh-interval` are `--s3-bearer-token-command` and `--s3-bearer-token-refresh-interval`; the command is
never recorded, so a resume must be given it again (Java's rule too) and is now refused with a message when it
is not, instead of failing on its first request. Java's `--endpoint-url` keeps its name, its `--force` is `--overwrite`, and its `--output` is
`-o, --output`; a custom endpoint is path-style by default, as Java's. The seven filters are
Java's flags with Java's semantics, except that regular expressions are Go's RE2 ([filters.md](filters.md#regular-expressions)).
The Java CLI's options, exit codes and report fields against this tool: [internals/cli-inventory.md](internals/cli-inventory.md).

**Changes between Go builds.** Before the first release a build makes no compatibility promise to an
earlier one: `swath resume` replays the recorded command with this build's flags, and a flag this build no
longer has is refused (exit 2, naming the flag); finish such a run with the build that
started it, or start over. The last renames before 0.4 are in [Unreleased builds of `main`](#unreleased-builds-of-main);
the list below is the earlier history of the unreleased builds.
- Anonymity is explicit: requests go unauthenticated only with `--anonymous`, one flag for S3, GCS and Azure
  Blob (`--gcs-anonymous` and `--azblob-anonymous` are gone; a resume that recorded either is refused as an
  unknown flag). A source with no usable credentials, an empty AWS chain, no Application Default Credentials,
  or an Entra ID chain that finds none, stops the run before listing. Credential failures have
  cause `auth`, during Azure namespace admission too (a SAS the service refuses there: `auth` or
  `access_denied`) ([credentials](sources/README.md#credentials-on-every-object-store)). Give `--anonymous` for a public bucket or container
  (it may be given to `swath resume`). An AWS or GCS credential lookup that fails transiently at start is now
  `auth` too, rather than a note and retried requests; later refreshes are still retried. The error is one
  line: the credential source, the cause's first line and, when swath was searching for credentials on its
  own, `--anonymous`; `-v` shows the whole cause.
- An endpoint URL with user information (`http://user:password@host/`, through `--endpoint-url`, then
  also `--gcs-endpoint` or `--azblob-endpoint`) is refused before anything is recorded, where it was accepted and
  its password recorded in the journal identity; give credentials through the provider's own configuration.
- S3 journals written by an earlier build do not resume. The S3 resume identity now records the listing's inputs
  as given (the endpoint, empty for AWS's own, the region, path-style, FIPS and dual-stack) in place of the
  endpoint URL the earlier client resolved (`route`), so `swath resume` of such a run is refused before any part
  is touched (`cannot resume: endpoint was (not recorded)`), whatever the region or endpoint; the interrupted
  dataset is left as it was. Finish the run with the build that started it, or start it over with `--overwrite`.
- GCS listings use Google's generated JSON API client only, and a GCS journal records the endpoint
  (`--endpoint-url`, then `--gcs-endpoint`, as the client sends to it) where it recorded the resolved route (`ENDPOINT/b/BUCKET/o`).
  A GCS run interrupted under an earlier build does not resume, even with unchanged flags: `swath resume`
  refuses it with `cannot resume: endpoint was (not recorded), now …`, and the run report's `args_hash` of a
  GCS run differs from an earlier build's. Finish such a run with the build that started it, or start it over
  with `--overwrite`. A program that lists through its own `gcs.Options.Client` cannot resume at all (before,
  one that named its route could).
- `--workers` defaults to 64 whatever the CPU count for an object store (it was `min(64, max(4, 3*GOMAXPROCS))`);
  a local path defaults to `min(64, 4 × GOMAXPROCS)`.
- A custom `--endpoint-url` is path-style by default: an interrupted run started with `--endpoint-url` on a host name
  and no `--s3-path-style` resolves the path-style route on resume, so the identity check refuses it; start it
  over with `--overwrite`.
- All size flags use one syntax: `KB`, `MB`, `GB` and `TB` are decimal; `K`, `M`, `G`, `T` and `KiB`, `MiB`, `GiB`,
  `TiB` are binary. Earlier unreleased builds used different resource-size rules; preserve the intended byte count
  when migrating their commands.
- Split counters: `split.pivot_extrapolated` is `split.pivot.density.bounded` + `split.pivot.density.frontier`,
  and `split.pivot_structure` is `split.pivot.structure`. Every split path now counts its engagement: the pivot
  sources (`split.pivot.*`), empty and small children by source (`child_empty.*`, `child_small.*`), the density
  step's refusals, the gallop and the reflection (`split.gallop.*`, `split.frontier.anchored`,
  `split.reflect.ceiling`, `split.density.shrink_prevented`, `split.sliver_refused`), the admission gates
  (`split.skipped.remainder`, `.lower_tiny`, `.upper_tiny`, `split.estimate_none`), depth (`split.max_depth`,
  `split.leaves`, `split.leaf_depth_sum`) and the probes' reasons (`structure.sent.*`, `structure.boundaries`,
  `structure.frontier_pivots`, `structure.suppressed_inherited`); the client adds `inflight.p50_ns`, `p90_ns`,
  `p99_ns` (9223372036854775807 when open-ended, past 2^40 ns), `inflight.latency_overflow` and, for GCS, `transport.pool_size` ([internals/counters.md](internals/counters.md)).
- Structure probes are on by default on their evidence (`auto`: the open frontier, and bounded ranges whose
  chain stays inside one folder); `swath diag list --structure` takes `auto`, `on` or `off` after `=`, and the
  bare flag is `on`.
- `--count-only` (now `swath diag count`) no longer runs the order stage: the report's `features.order` is false, `spill` is absent
  and `max_buffered_bytes` is 0.
- `swath diag list --seed-descent` was removed with the seed descent. The `seed.probes`, `seed.levels.*` and
  other counters of that descent are gone.
- `--seed` is on by default (`swath diag list --no-seed` turns it off) and no longer one delimiter page: it paginates the top
  level up to 4 pages and descends levels narrower than `--workers` up to 4 deep, in parallel waves (at most 64
  requests in 6 round trips, the empty pages it follows included), and it also tiles a resumed scope. Its new
  counters are `seed.pages`, `seed.requests`, `seed.round_trips`, `seed.levels`, `seed.level.<n>.cuts`,
  `seed.stop.<reason>` (`failed` when its listing fails or the run is canceled: the counters are still published)
  and `seed.resumed`; `seed.prefixes` counts the top level's folders over all its pages, and on a resume the
  `seed` feature is on and `resumed_ranges` counts the seed's tiles.
- `swath diag list --probe` was removed with the 1-key probe; the `probe.*` counters and
  `split.rejected.probe_empty` are gone.
- `--memory N` is `--row-budget N/2`: the flag names the bytes of rows in flight it bounds, which were half of
  `--memory`; the default was 128 MiB on every host (from 256 MiB of `--memory`) and is now 1/16 of memory, at most
  1 GiB (run report version 5).
  The run report's `invocation.memory_bytes` is `invocation.row_budget_bytes`. A journal that recorded `--memory` is refused on resume.
- `--row-budget` is `--sort-buffer` in 0.4, the same bytes under a name that says what it buffers (run report
  version 5: `invocation.sort_buffer_bytes`). A journal that recorded `--row-budget` resumes with its value as
  `--sort-buffer`; a `--sort-buffer` given to `swath resume` wins.
- `--spill-max` (now `--spill-budget`) is the one disk budget on `swath list`; `--spill-reserve` (now derived by default: one part per
  encoder at full size, at least 1 GiB: 1 GiB at the old 128 MiB row budget, more where a larger default
  admits more encoders) and `--spill-compress` moved to
  `swath diag list`. `swath resume` still accepts both. The report's `resources.spill` adds `reserve_bytes`.
- Spill frames are delta-coded and zstd-compressed (level 1) by default; `--no-spill-compress` (on `swath diag list` and `swath resume`)
  writes them uncompressed and `--spill-plain` without delta coding. The report's `resources.spill` adds
  `logical_bytes`, `format` and `decode_refused`.

## Changes from 0.3 to 0.4

The 0.3 releases (the repository's `v0.3.x` tags) are the Java tool; 0.4 is the first release of this Go tool,
and the Java tool is deprecated with it. Moving from Java 0.3 to Go 0.4 changes the launcher, the flags and the
dataset format: the flag-by-flag mapping is [above](#renamed-flags) and in
[internals/cli-inventory.md](internals/cli-inventory.md), the dataset changes are [Files](#files) through [Versions](#versions). A run interrupted under Java
is finished with Java or started over; `swath resume` reads only this tool's journal. The current flags are
the generated options of [commands/swath-list.md](commands/swath-list.md#options).

The binary reports its version (`swath --version`, the report's `build.version`): a release binary or image
reads the release's `X.Y.Z`, a binary built from a source checkout reads `devel`, and the revision it was built
from is `build.revision`.

Builds of `main` before 0.4 also renamed flags, report fields and formats between themselves: see
[Unreleased builds of `main`](#unreleased-builds-of-main) at the end of this page.

## What a reader of the Java layout changes

A reader of format 1 (the downstream reader is the reference) changes as follows. The contract it moves to is
[dataset.md](dataset.md).

- **Open.** Read `manifest.json` first ([Dispatch](#dispatch)). A SeekParquet manifest (`format` `seekparquet`,
  `format_version` 0, `draft_revision` 1) is the swath dataset path: require `extensions.swath.version` 1 and take
  swath's fields from `extensions.swath`. Never read `_SUCCESS` or a run summary. A format 1 manifest takes the
  legacy path; any other manifest is refused.
- **Coverage and source** come from `extensions.swath`, not from the run summary: the members are in
  [dataset.md](dataset.md#the-swath-extension). The application decides which listings it takes, and checks
  `source.kind` and the container against what it was asked to open.
- **Dispatch on the schema id.** Use the exact registered id (`swath/s3-objects/v1`, `swath/gcs-objects/v1`,
  `swath/azblob-objects/v1`, `swath/local-entries/v1` and the versions ids), with the columns
  [dataset.md](dataset.md#schema-registry) gives it. Its column set is not closed: ignore columns the id does not
  define, and read an optional column that a part lacks as null (`time_created` on GCS, the later Azure Blob properties). A run resumed by a build
  that writes a newly added optional column mixes parts with and without it under one id.
- **Columns Java wrote and the current-object schemas do not.** `version_id`, `is_latest`, `is_delete_marker` and
  `row_type` are gone from `swath/s3-objects/v1` ([S3 schema](#s3-schema)). Every version and delete marker is in
  the versions schemas (`--all-versions`) instead, one row per version, consecutive per key. A reader that assumes
  one row per key refuses those ids. A reader that wants current objects from them filters as
  [dataset.md](dataset.md#keys-and-order) says.
- **Keys** are bytes in unsigned order, and the manifest's part bounds are base64 ([dataset.md](dataset.md#keys-and-order)).
  Local keys may be non-UTF-8.
- **Part footers** carry the `seekparquet.*` stamps equal to the manifest's, and `key` ascending as the first
  sorting column ([Footer](#footer)).
- **Rows.** The downstream reader reads `key`, `size` and `last_modified`. It rejects a null or negative `size`
  (so a local dataset listed with names only is not readable by it) and a `last_modified` outside the years 1
  to 9999. A local dataset's directory rows are entries whose keys end in `/`.
- **In Go.** The reader module (`reader`, [its README](../reader/README.md)) implements this contract:
  `Open` (manifest, schema id and metadata), `CheckSchema` (key and columns), and concrete
  typed row decoders. Use `Dataset.SeekParquet()` for scans, listings, aggregation and pagination.
  Coverage/source admission and the downstream reader's size/time rules remain application policy.
- **Checking.** `python3 verify.py DIR [REFERENCE_DIR]` validates S3, GCS, Azure Blob or local datasets,
  and can compare the complete rows against an independent dataset. `python3 bench/diffds.py A B` compares whole-row multisets and ordered row
  sequences for both versions schemas, preserving nulls and within-key order; it refuses mismatched or unknown
  schema ids. `--ts-ms` is refused for versions, so timestamps stay exact. Reader fixtures: `python3 -m unittest
  test_dataset_readers` from the repository root; the SQL fixture tests need DuckDB. One dataset per schema written by this tool
  serves as the reader's fixture; the commands that produce them are in [internals/testing.md](internals/testing.md).

## Gates

- **DuckDB:** `bench/diffds.py` (row sets both ways with the projection above, strict ascending order within
  and across parts, min/max keys, counts); `verify.py` (the manifest, the stamps, the bounds, each part's SHA-256 against `parts[].sha256`).
- **SeekParquet conformance:** the suite's reader and validator targets against the seekparquet module this
  tool pins, and this tool's datasets validated by the suite's reference adapter and the module's validator
  (full and digest levels, every part declaring `sha256`) and read alike by both
  ([internals/testing.md](internals/testing.md#seekparquet-conformance)).
- **Downstream reader:** opens and streams this tool's datasets of every schema (section above); the recorded
  probe ([internals/measurements.md](internals/measurements.md#downstream-reader-compatibility)) predates SeekParquet and is
  historical.
- **Dropped:** "the Java reader opens Go output". Java stays the producer oracle (replay differential), not
  a reader.

## Unreleased builds of `main`

For anyone who ran a build of `main` before 0.4: what changed between those builds and 0.4.

**If you built from `main` before 0.4.** The Go port was on `main` (from `a2efe16e`) before it was released,
without a version and without a compatibility promise. Against those builds, 0.4 renames or moves these flags
(a removed name is an unknown flag, exit 2; there are no aliases, and a run interrupted under such a build is
finished with it or started over): `--force` is `--overwrite`; `--spill-max` is `--spill-budget`; `swath list --count-only` is the command
`swath diag count` (it refuses the output flags with exit 2; the builds of `main` from 2026-11-07 to 2026-11-09
called it `swath count`, and `swath count` is now an unknown command: `swath du` totals a source, `swath diag
count` measures its listing); `--no-seed` and `--adaptive-limit-slow-start` moved to `swath diag list`, and `swath
list` and `swath du` refuse every diagnostic flag with a message naming `swath diag`; the GCS and Azure copies
of the flags every object store has are gone, one shared flag serving S3, GCS and Azure Blob alike:
`--gcs-endpoint` and `--azblob-endpoint` are `--endpoint-url`, `--gcs-request-timeout` and
`--azblob-request-timeout` are `--request-timeout` (default 30s, as before); a resume of a run that recorded one
is refused naming it. The parser is GNU-style (`-d1`, `-vq`; a long name takes two dashes) and the provider flags carry their provider's prefix: `--endpoint` is `--endpoint-url`; `--region`, `--profile`, `--request-payer` are `--s3-region`, `--s3-profile`, `--s3-requester-pays`; `--path-style` is `--s3-path-style` or `--no-s3-path-style`; `--fips`, `--dualstack`, `--fetch-owner`, `--bearer-token-command`, `--bearer-token-refresh-interval` take the `--s3-` prefix; `--gcs-credentials` is `--gcs-credentials-file`, `--gcs-user-project` is `--gcs-billing-project`; `--azblob-sas` is `--azure-sas-token`, `--azblob-flat` is `--azure-assume-flat`; `--one-filesystem` is `--one-file-system`; `swath du`'s `--count-links-once` is the default on a local tree (a hard-linked file counts once, as `du`), and `-l`, `--count-links` counts every name; `--limiter=false` is `--no-adaptive-limit`, `--limiter-slow-start` is `--adaptive-limit-slow-start`, `--seed=false` is `--no-seed`, `--spill-compress=false` is `--no-spill-compress`; `--report-format json` is `--report -` and `--report-format` is gone; `swath list -o` prints no summary line on stdout; `--include` and `--exclude` take globs, the regular expressions they took are `--include-regex` and `--exclude-regex`, include and exclude patterns and storage classes accumulate when repeated, and for each size or time bound the last value wins (`coverage.filters` spells a regex `include_regex=…`); a size's `KB`, `MB`, `GB` and `TB` are decimal (`K`, `M`, `KiB`, `MiB`, ... binary). New in 0.4: `--seed-from PATH`, `swath diag list --spill-plain` and `--slow-split`; spill frames
are delta coded and zstd compressed by default. `SWATH_CPUPROFILE=FILE` profiles the listing and the drain's
tail apart: FILE (and FILE.listing) is the listing phase, no longer the whole run, and FILE.tail the tail; `go
tool pprof FILE.listing FILE.tail` merges them ([internals/diagnostics.md](internals/diagnostics.md#phase-profiles)).

**Local recursive exclusions.** A local or mounted tree no longer walks below directories covered by
exclude globs ending in a recursive `/**` segment. The retained rows have the same filter semantics;
the directory's own row is still eligible. The local report adds `source.stats.pruning` (configured-rule
classification) and `source.stats.pruned_dirs` (child traversals prevented). Unvisited descendants are
not counted in fetched or filtered rows. Errors below pruned directories are no longer observed, so
`outcome.unreadable` and `coverage.unreadable` can decrease for the same command. Original rules
stay in `coverage.filters`; pruning does not make an entry unreadable. Object-store exclusions and arbitrary
regex exclusions keep their existing behavior. See [filters](filters.md#globs).

**One name for unreadable entries: run report version 7.** The entries a listing could not read are
`unreadable` everywhere: the manifest's `coverage.skipped_entries` is `coverage.unreadable` (`reader`:
`Coverage.Unreadable`), `swath show` prints `unreadable=` where it printed `skipped=`, and the statistics line
`unreadable N` where it printed `errors N`. In the run report (version 7) `outcome.unreadable` is that count, where
it was a flag, and `outcome.entry_errors` is gone (`source.stats` keeps a local listing's error counters). A
version 6 reader changes the type it reads `outcome.unreadable` as.

**Run report: version 6.** Version 6 adds `output.committed_object_bytes`, the committed dataset's object
bytes: every row's size summed as the writers wrote them, retained parts and rows replayed from spill included,
so a resumed run reports the dataset's total where `output.object_bytes` holds only what that attempt listed. It is
absent when unknown (a schema without a size column, or retained parts recorded by an earlier build), never 0
for unknown; the manifest states the same as `extensions.swath.committed_object_bytes` ([dataset.md](dataset.md#the-swath-extension)).
A resume that finds the dataset committed by an earlier attempt (local, or a bucket's whose manifest's creation
was answered too late) now reports the committed dataset from its manifest: `rows_committed`, `rows_durable`,
`output_bytes` and `committed_object_bytes`, where it reported 0, in `--report` and in the `run.json` it writes
beside the manifest, whose `outcome.complete` and `stop_reason` are the manifest's coverage. `outcome.cause`
gains `not_found`: the bucket (S3, GCS) or Azure container does not exist, and `region_redirect`: S3 named the
bucket's region in a redirect (the message says which), where the report had no cause. A
version 5 reader reads version 6 by ignoring the new member.

**Run report: version 5.** Version 5 renames `invocation.row_budget_bytes` to `invocation.sort_buffer_bytes`
(`--sort-buffer`) and adds `invocation.sort_buffer` (how it was chosen: by default 1/16 of memory, at most
1 GiB, where it was 128 MiB on every host), `resources.spill.frames` and `opens`, and `profiles` (the development
profiles' files and phase boundary, present only with `SWATH_CPUPROFILE` or `SWATH_MEMPROFILE`), and
`resources.spill.read_handles` (the spill read handles' `hits`, `misses`, `raced`, `evictions` by reason and
`peak_open`) and `frame_bytes` and `frame_rows` (the row frames written by size: `p50`, `p90`, `p99` as power-of-two
bucket bounds, and the exact `max`); `opens` counts one open per read handle miss, not one per frame read; and
`output.remote`, present only for a dataset written to a bucket (`-o gs://...`, `-o s3://...` or `-o az://...`): `destination`, `work_dir`,
`commit`, `manifest`, `manifest_identity`, `manifest_sha256`, `uploads`, `uploads_aborted`,
`aborts_unconfirmed`, `upload_bytes`,
`upload_wait_ns`, `retries`, `retry_classes`, `objects_verified`, `orphans_deleted`, `orphans_gone`,
`orphans_left` and `orphans_unproven`. `outcome.class` gains `unresolved` (exit 75, `cause`
`commit_unresolved`), and `outcome.cause` `lost_race`, `remote_altered` and `candidate_mismatch`, all for a
dataset in a bucket. The journal's version is 10 (it names a bucket dataset's part objects); this build reads
the version 9 journals of earlier builds, which an earlier build cannot do the other way round. Version 5 also reports what the Parquet encoders were admitted against, beside their counts in
`resources.drain`: `encoder_allowance_bytes`, `encoder_writer_bytes`, `encoder_upload_bytes`,
`encoders_at_start_bound_by` and `encoders_after_listing_bound_by`; encoders are now admitted against an
allowance of their own, 1/16 of memory (at most 1 GiB) whatever `--sort-buffer` says, where earlier builds
divided the sort buffer, and a text output's `compressor.jobs_reason` `row_budget` is `encoder_allowance`.
Parquet pages are compressed at zstd `SpeedFastest` where earlier builds used `SpeedDefault`: in the
[2026-11-11 measurements](internals/measurements.md#parquet-pages-at-zstd-speedfastest-2026-11-11), the tested
listings' parts were about 0.3 to 1.1 % larger for less CPU a page, and read the same. The writer recipe is therefore `swath/parquet-r4`, and
an interrupted run of an earlier build (`swath/parquet-r3`) cannot be resumed by this build: finish it with that
build, or start it over with `--overwrite`.
A version 4 reader ignores
the additions but must change one lookup: the buffer is `sort_buffer_bytes`, no longer `row_budget_bytes`; a reader
of historical reports accepts both.

**Run report: version 4.** Version 4 adds `invocation.narrowing` (whether the includes narrowed the listed
prefix, or why not; [filters.md](filters.md#what-narrows-the-listing)), `outcome.unreadable` (a local listing skipped
entries it could not read) and `requests.failure_warnings` (the persistent-failure warnings printed); a
version 3 reader reads them by ignoring them.

**Run report: version 3.** Version 3 adds fields, and a version 2 reader reads version 3 by ignoring them; one
meaning is narrowed, a local tree's `output.object_bytes` (the last item):

- **Durable spill:** `resources.spill.durable` (the durable spill's account, and on a resume what it accepted,
  rejected and replayed), `resources.spill.durable_off`, and the counters `reuse.ranges`, `reuse.gaps` and
  `reuse.boundary_overfetch_keys`.
- **Drain tail:** `timings.listing_done_at` (the provider finished) and `resources.drain` (sorted Parquet: the
  tail's times, the read-ahead's frames and waits, the parallel segments' plan and holdings, and the encoders
  admitted at start and after listing; [reference/run-report.md](reference/run-report.md)).
- **Versions schemas** (`--all-versions`): attempt-scoped `output.versions`, `output.distinct_keys`,
  `output.live_objects` and S3 `output.delete_markers` or GCS `output.noncurrent`, present only for versions
  schemas (zero counts included). They count filtered rows handed on by order, or counted, and exclude
  retained resume rows.
- **Per-prefix totals** (`swath du`): `output.totals` (`depth`, `prefixes`, `max_prefixes`, `bytes_column`,
  `objects`, `bytes`, `dirs`, `not_objects`, `unsized`, `links_once`, `linked_rows`, `inodes`, `max_inodes`),
  present only on a `swath du` run.
- **`output.object_bytes` of a local tree** is the apparent bytes of the files the run accepted (after the
  filters): a directory's own size is no longer summed (it is no object's). On an unfiltered complete run it equals
  the local counters' `bytes`, and `swath du`'s total of the same run with `--count-links` and without
  `--allocated`. Object stores are unchanged. The local provider's counters add `other_device_dirs`
  (`--one-file-system`).

Version 2 introduced the names below; builds before 0.4 wrote version 1. A reader
dispatches on `report_version`; `internal/listing/testdata/report-schema.golden`
is the full schema and [reference/run-report.md](reference/run-report.md) its description.

| Version 1 | Version 2 |
|---|---|
| `report_version` 1 | `report_version` 2 |
| `output.rows_listed` (after the filters) | `output.rows_emitted`; new beside it `output.rows_fetched` (from the source, before the filters: `rows_filtered` + `rows_emitted` on a run that succeeded; a stopped run also fetched rows it never emitted) |
| `output.bytes` (part bytes) | `output.output_bytes`; new beside it `output.object_bytes` (the sizes of the objects the order stage accepted) |
| `cost.api_calls` | `requests.http_attempts`, with `requests.retries`, `throttles`, `timeouts` beside it; `cost` keeps `cost_usd` and `basis` |
| `efficiency.api_calls_per_1k_objects` (per row listed after the filters) | `efficiency.http_attempts_per_1k_rows` (per row fetched) |
| `resources.row_budget_bytes` | removed: `invocation.row_budget_bytes` holds the same value (`invocation.sort_buffer_bytes` from version 5) |
| – | `timings.seed_ms`, `drain_ms`, `publication_ms` (phase times; `listing_ms` unchanged) |
| – | `resources.free_disk_min_bytes` |
| – | `history` (`interval_ms`, `samples`: at most 300) |
| – | `invocation.prior` (the `--seed-from` prior) |
| – | `output.text.formatters`, `output.text.output_wait_ns`, `output.text.compressor` (`bytes_in`, `bytes_out`, `jobs`, `jobs_reason`) |
| `resources.spill` | adds `logical_bytes`, `format`, `decode_refused` |

**Counters** (`source.stats.counters`, [internals/counters.md](internals/counters.md)), against those builds. New: `seed.from.*`
(the warm start), `seed.first_page_keys`, `seed.flat_chains`, `seed.stop.flat`, `seed.stop.one_page`,
`split.reason.slow_range`, `split.slow.max_range_latency_ns`, `split.slow.run_latency_ns`,
`limiter.cuts_suppressed.round_trip`, `limiter.cuts_suppressed.tolerated`, `limiter.limit`,
`limiter.throttle_permille`, `spill.logical_bytes`, `spill.decode_refused`. Removed: `split.rejected.done`,
`split.skipped.not_continuing`.

**The dataset** changed against those builds: it is SeekParquet ([SeekParquet](#seekparquet)). Builds before
2026-11-05 wrote format 1, as the Java tool does; those of 2026-11-05 to 2026-11-08 wrote format 2, whose Parquet
schemas and data pages SeekParquet keeps but whose datasets no reader reads ([Dispatch](#dispatch)). **Unchanged since those builds:** the outcome classes, `progress.json` and `error.json`; the exit codes but one: `incomplete` (a key limit, which exited 1, and unreadable entries, which exited 0) exits 3. Spill files changed format (delta coding,
zstd), but they are private to one run and never read across builds.
