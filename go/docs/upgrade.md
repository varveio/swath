# Upgrading to the Go tool's datasets

The Java tool writes swath's format 1; this tool writes a swath dataset, a
[SeekParquet](https://github.com/varveio/seekparquet) dataset (`format_version` 0, `draft_revision` 1) defined by the swath dataset profile in [dataset.md](dataset.md). This guide is the
migration from one to the other, for two audiences:

- **A user of the Java tool** moving to this one: what a format 1 dataset becomes ([Files](#files) through
  [Versions](#versions)), what the run report and the command line become, and what changed from 0.3 to 0.4.
- **A reader of the Java layout** (the downstream reader, `bench/diffds.py`, `bench/summaries.py`): what it must
  change, in [What a reader of the Java layout changes](#what-a-reader-of-the-java-layout-changes).

Neither format is a superset of the other: there are no aliases, so a reader of one refuses the other, and a
Java reader cannot open a SeekParquet dataset by design. DuckDB, the SeekParquet conformance suite and the
downstream reader are the acceptance gates ([Gates](#gates)).

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
| `_SUCCESS` (written last; the commit) | removed: `manifest.json` is installed last, by an atomic rename and a directory sync, and is the commit |
| `symlink.txt` (the part list) | removed: the manifest lists the parts |
| `manifest.json` | `manifest.json`, new shape ([Manifest](#manifest)) |
| `_swath_summary.json` (S3), `_swath_local_summary.json` (local) | one `run.json` for every provider (`report_version` 4: [Run report and counters](#run-report-and-counters)); operational only, written after the commit, and never part of acceptance |
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
| `fileFormat`, `sorted`, `sortKey`, `fileSchema`, `totalRows` | removed (the format implies the first four; sum `rows` for the total) |

`parts[].sha256` is the part's SHA-256 in lowercase hexadecimal, SeekParquet's own part member (PART-5), which
the format's validators check at the digest level. MD5 is not written anywhere. Development builds before
`extensions.swath.version` existed wrote manifests without it (some with the parts' MD5s under
`extensions.swath.part_md5`). swath's metadata readers (`go/reader`, `swath show`, `--seed-from`) refuse them. A
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

The policy for each is in [dataset.md](dataset.md#evolution). This build reads journal
version 9, which records each part's SHA-256 where versions 7 and 8 recorded its MD5, so it refuses those journals
before recovery touches anything, and they refuse version 9. Finish an interrupted run of an older build with
that build, or start it over with `--overwrite`. The writer recipe (`parquetout.Recipe`, `swath/parquet-r3`
since SeekParquet) and the dataset format are in the resume identity: a build of another recipe or format
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

The report's changes from version 1 to version 2 are in [Changes from 0.3 to 0.4](#changes-from-03-to-04).

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
started it, or start over. The last renames before 0.4 are in [Changes from 0.3 to 0.4](#changes-from-03-to-04);
the list below is the earlier history of the unreleased builds.
- Anonymity is explicit: requests go unauthenticated only with `--anonymous`, one flag for S3, GCS and Azure
  Blob (`--gcs-anonymous` and `--azblob-anonymous` are gone; a resume that recorded either is refused as an
  unknown flag). A source with no usable credentials, an empty AWS chain, no Application Default Credentials,
  or an Entra ID chain that finds none, stops the run before listing. Credential failures have
  cause `auth`, during Azure namespace admission too (a SAS the service refuses there: `auth` or
  `access_denied`) ([credentials](sources.md#credentials-on-every-object-store)). Give `--anonymous` for a public bucket or container
  (it may be given to `swath resume`). An AWS or GCS credential lookup that fails transiently at start is now
  `auth` too, rather than a note and retried requests; later refreshes are still retried. The error ends
  with "(a public bucket needs --anonymous)".
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
- Sizes are binary in every unit (`64MB` is 64 MiB); `KB`, `MB` and `GB` were decimal for the resource sizes,
  so a run started that way sees a different part size on resume and is refused.
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
  `--memory`; the default (128 MiB, from 256 MiB of `--memory`) and the encoders it admits are unchanged.
  The run report's `invocation.memory_bytes` is `invocation.row_budget_bytes`. A journal that recorded `--memory` is refused on resume.
- `--spill-max` (now `--spill-budget`) is the one disk budget on `swath list`; `--spill-reserve` (now derived by default: one part per
  encoder at full size, at least 1 GiB, which is 1 GiB at the defaults) and `--spill-compress` moved to
  `swath diag list`. `swath resume` still accepts both. The report's `resources.spill` adds `reserve_bytes`.
- Spill frames are delta-coded and zstd-compressed (level 1) by default; `--no-spill-compress` (on `swath diag list` and `swath resume`)
  writes them uncompressed and `--spill-plain` without delta coding. The report's `resources.spill` adds
  `logical_bytes`, `format` and `decode_refused`.

## Changes from 0.3 to 0.4

The 0.3 releases (the repository's `v0.3.x` tags) are the Java tool; 0.4 is the first release of this Go tool,
and the Java tool is deprecated with it. Moving from Java 0.3 to Go 0.4 changes the launcher, the flags and the
dataset format: the flag-by-flag mapping is [from-java.md](from-java.md) and
[internals/cli-inventory.md](internals/cli-inventory.md), the dataset changes are [Files](#files) through [Versions](#versions). A run interrupted under Java
is finished with Java or started over; `swath resume` reads only this tool's journal. The current flags are
the generated options of [commands/swath-list.md](commands/swath-list.md#options).

The binary reports its version (`swath --version`, the report's `build.version`): a release binary or image
reads the release's `X.Y.Z`, a binary built from a source checkout reads `devel`, and the revision it was built
from is `build.revision`.

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
is refused naming it. The parser is GNU-style (`-d1`, `-vq`; a long name takes two dashes) and the provider flags carry their provider's prefix: `--endpoint` is `--endpoint-url`; `--region`, `--profile`, `--request-payer` are `--s3-region`, `--s3-profile`, `--s3-requester-pays`; `--path-style` is `--s3-path-style` or `--no-s3-path-style`; `--fips`, `--dualstack`, `--fetch-owner`, `--bearer-token-command`, `--bearer-token-refresh-interval` take the `--s3-` prefix; `--gcs-credentials` is `--gcs-credentials-file`, `--gcs-user-project` is `--gcs-billing-project`; `--azblob-sas` is `--azure-sas-token`, `--azblob-flat` is `--azure-assume-flat`; `--one-filesystem` is `--one-file-system`; `--limiter=false` is `--no-adaptive-limit`, `--limiter-slow-start` is `--adaptive-limit-slow-start`, `--seed=false` is `--no-seed`, `--spill-compress=false` is `--no-spill-compress`; `--report-format json` is `--report -` and `--report-format` is gone; `swath list -o` prints no summary line on stdout; `--include` and `--exclude` take globs, the regular expressions they took are `--include-regex` and `--exclude-regex`, and every filter flag is repeatable (the rules add up; `coverage.filters` spells a regex `include_regex=…`); a size's `KB`, `MB`, `GB` and `TB` are decimal (`K`, `M`, `KiB`, `MiB`, ... binary). New in 0.4: `--seed-from PATH`, `swath diag list --spill-plain` and `--slow-split`; spill frames
are delta coded and zstd compressed by default.

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
  the local counters' `bytes`, and `swath du`'s total of the same run without `--allocated` or
  `--count-links-once`. Object stores are unchanged. The local provider's counters add `other_device_dirs`
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
| `resources.row_budget_bytes` | removed: `invocation.row_budget_bytes` holds the same value |
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
schemas and data pages SeekParquet keeps but whose datasets no reader reads ([Dispatch](#dispatch)). **Unchanged since those builds:** the journal's format,
the exit codes and outcome classes, `progress.json` and `error.json`. Spill files changed format (delta coding,
zstd), but they are private to one run and never read across builds.

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
  define, and read an optional column that a part lacks as null (`time_created` on GCS). A run resumed by a build
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
- **In Go.** The reader module (`go/reader`, [its README](../reader/README.md)) implements this contract:
  `Open` (the manifest, the schema id and the metadata, with the coverage and the source), `CheckSchema` (key and
  columns), and `Stream` and `List` (current objects, with the row checks above). Judging the coverage and the
  source is the caller's.
- **Checking.** `python3 verify.py DIR [REFERENCE_DIR]` validates S3, GCS, Azure Blob or local datasets,
  and can compare the complete rows against an independent dataset. `python3 bench/diffds.py A B` compares whole-row multisets and ordered row
  sequences for both versions schemas, preserving nulls and within-key order; it refuses mismatched or unknown
  schema ids. `--ts-ms` is refused for versions, so timestamps stay exact. Reader fixtures: `python3 -m unittest
  test_dataset_readers` from `go/`; the SQL fixture tests need DuckDB. One dataset per schema written by this tool
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
