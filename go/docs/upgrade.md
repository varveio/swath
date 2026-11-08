# Upgrading dataset readers

This guide covers what a reader of swath datasets (the downstream reader, `bench/diffds.py`, `bench/summaries.py`) changes to
read what this tool writes: a [SeekParquet](https://github.com/varveio/seekparquet) dataset, format 1 draft
revision 1 (stamped `format_version` 0, `draft_revision` 1), from 0.4. Builds of this port from `main` before
0.4 wrote swath-dataset format 2; the Java tool writes format 1. [SeekParquet](#seekparquet) says what a
format 2 reader changes. The sections after it were written for format 2 and say what it changed from Java's
format 1; its columns, schema ids, part files and coverage are SeekParquet's too, in the places that section
gives. No format is a superset of another: there are no aliases, so a reader of one refuses the others, and a
Java reader cannot open either by design. DuckDB, the SeekParquet conformance suite and the downstream reader
are the acceptance gates (see "Gates").

## SeekParquet

SeekParquet is a sorted-key profile for Apache Parquet: ordinary Parquet parts, sorted by one byte-string key
and split into key-disjoint parts, and a manifest that lists them in key order and commits them. Any Parquet
reader reads the parts as a table, as before; a reader that knows the profile (the Go module
`github.com/varveio/seekparquet/go`, which `swath-replay` serves datasets through) seeks a key, reads key
ranges and lists a prefix with delimiter rollups from the manifest, the footers, the key column's page index
and a few pages. The specification is the SeekParquet repository's `spec/format-1.md`; what follows is what
changed against format 2. The data pages did not change with the format: for the rows `TestFooterOnlyChange`
writes (short unique keys, repeated keys, keys over 1,024 bytes), a format 2 build and this one write the same
pages, page indexes and footers, apart from the stamps and the bounds of values over 1,024 bytes. That is shown for
those inputs only: the same switch raised klauspost/compress from v1.19.1 to v1.20.3, whose zstd encoder may
choose its entropy tables differently on other data of more than one block, so a part of a build before and
after need not be byte-identical in general (the writer recipe, `swath-go/parquet-r3`, already keeps their
resumes apart).

**Dispatch.** `"format": "seekparquet"` with `format_version` 0 and `draft_revision` 1 is SeekParquet;
`"format": "swath-dataset"` with `format_version` 2 is format 2 ([Dispatch](#dispatch)). A SeekParquet reader
refuses every other version pair (`unsupported_version`).

**Manifest.** SeekParquet allows only its own members, at the top level, in `key` and in a part; a producer's
own metadata goes under `extensions`, by namespace. swath's is `extensions.swath`:

```json
{
  "format": "seekparquet",
  "format_version": 0,
  "draft_revision": 1,
  "schema_id": "swath/s3-objects/v1",
  "key": {"column": "key", "unique": true},
  "extensions": {
    "swath": {
      "published_at": "2026-11-08T18:00:00.123456Z",
      "source": {"kind": "s3", "uri": "s3://bucket/logs/", "container": "bucket"},
      "scope": {"kind": "prefix", "prefix": "logs/"},
      "coverage": {"complete": true, "stop_reason": "completed", "skipped_entries": 0,
                   "exclusions": [], "filters": [], "snapshot": false,
                   "listing_started_at": "…", "listing_finished_at": "…"},
      "part_md5": ["…"]
    }
  },
  "parts": [
    {"path": "data/part-00000.parquet", "rows": 1000, "bytes": 81234,
     "min_key_base64": "bG9ncy9h", "max_key_base64": "bG9ncy9r"}
  ]
}
```

| Format 2 | SeekParquet |
|---|---|
| `format` `swath-dataset`, `format_version` 2 | `format` `seekparquet`, `format_version` 0, `draft_revision` 1 |
| `schema_id` | `schema_id` (unchanged) |
| – (the key column was always `key`, keys always unique) | `key`: `{"column": "key", "unique": true}`; `unique` is false for the versions schemas (`--all-versions`), whose keys repeat on consecutive rows and never across parts |
| `published_at`, `source`, `scope`, `coverage` | `extensions.swath.published_at`, `.source`, `.scope`, `.coverage` (same members and meaning) |
| `parts[].md5` | `extensions.swath.part_md5[i]`, in manifest order (SeekParquet's part member is a `sha256`, which swath does not write) |
| `parts[].path`, `rows`, `bytes`, `min_key_base64`, `max_key_base64` | unchanged (exact bounds, canonical standard base64) |

Whole-source acceptance reads the same coverage fields from `extensions.swath` (below). A dataset of another
producer may be SeekParquet without a swath extension; a reader that needs swath's coverage refuses it.

**Footer.** Every part's key/value metadata repeats the manifest's version and key declaration:

| Format 2 stamp | SeekParquet |
|---|---|
| `swath.profile` = `swath-dataset`, `swath.profile_version` = `2` | `seekparquet.format_version` = `0`, `seekparquet.draft_revision` = `1` |
| `swath.schema_id` | `seekparquet.schema_id` |
| – | `seekparquet.key_column` = `key`, `seekparquet.key_unique` = `true` or `false` (as the manifest's `key`) |
| `swath.index_bound_bytes` = `1024` | removed: no column index bound is shortened |

**Bounds.** Every column's ColumnIndex bounds are exact: each page's minimum and maximum (the key's: its first
and last key). Format 2 shortened values longer than 1,024 bytes, and parquet-go shortens a maximum by
incrementing its last kept byte, so a value whose first 1,024 bytes are all `0xFF` got a maximum below it: no
upper bound at all, which Parquet and SeekParquet (RG-5) forbid. S3 and GCS keys and values are at most 1,024
bytes, so only filesystem keys and link targets change. Statistics in the column chunk metadata were exact
already.

**Resume.** An interrupted run of a format 2 build is finished with that build or started over with
`--overwrite`: the resume identity's dataset format and writer recipe changed, so this build refuses its journal.
`--seed-from`, `swath show` and the output checks still read a committed format 2 dataset.

**Replay fixtures.** `swath-replay` serves a SeekParquet dataset through the seekparquet module, a format 2
dataset and a Java-era fixture through its own readers (`internal/replay/legacy`); the startup line's kind is
`seekparquet`, `format-2` or `java-era`. It refuses a dataset whose keys repeat (`key.unique` false, an
`--all-versions` listing) until the versions routes exist (#279): its routes continue after a key, never inside
a key's group.

## Dispatch

A reader dispatches on `manifest.json`, before looking for anything else:

| `manifest.json` has | Format | Accepted when |
|---|---|---|
| `"format": "seekparquet", "format_version": 0, "draft_revision": 1` | SeekParquet | the manifest is present and its coverage (`extensions.swath.coverage`) satisfies the reader (below) |
| `"format": "swath-dataset", "format_version": 2` | 2 | the manifest is present and its coverage satisfies the reader (below) |
| `fileFormat`, `files`, `schemaId` (camelCase) | 1 | `_SUCCESS` exists, and the run summary covers the target (the legacy rule) |
| neither | – | never: not a dataset |

A format-2 manifest is never read by the format-1 rule.

## Files

| Format 1 | Format 2 |
|---|---|
| `_SUCCESS` (written last; the commit) | removed: `manifest.json` is installed last, by an atomic rename and a directory sync, and is the commit |
| `symlink.txt` (the part list) | removed: the manifest lists the parts |
| `manifest.json` | `manifest.json`, new shape (below) |
| `_swath_summary.json` (S3), `_swath_local_summary.json` (local) | one `run.json` for every provider (`report_version` 3: [Run report and counters](#run-report-and-counters)); operational only, written after the commit, and never part of acceptance |
| `data/part-NNNNN.parquet` | unchanged |
| `.swath/journal`, `.swath.lock`, `data/*.tmp` | `.swath/` is the private workspace: `lock`, `journal`, `parts/` (unpublished parts), `spill/`. Readers ignore it |

## Manifest

```json
{
  "format": "swath-dataset",
  "format_version": 2,
  "schema_id": "swath/s3-objects/v1",
  "published_at": "2026-11-05T18:00:00.123456Z",
  "source": {"kind": "s3", "uri": "s3://bucket/logs/", "container": "bucket"},
  "scope": {"kind": "prefix", "prefix": "logs/"},
  "coverage": {
    "complete": true, "stop_reason": "completed", "skipped_entries": 0,
    "exclusions": [], "filters": [], "snapshot": false,
    "listing_started_at": "…", "listing_finished_at": "…"
  },
  "parts": [
    {"path": "data/part-00000.parquet", "rows": 1000, "bytes": 81234,
     "min_key_base64": "bG9ncy9h", "max_key_base64": "bG9ncy9r", "md5": "…"}
  ]
}
```

| Format 1 field | Format 2 field |
|---|---|
| `version` ("1") | `format`, `format_version` |
| `schemaId` | `schema_id` |
| `creationTimestamp` (epoch ms) | `published_at` (RFC 3339, UTC) |
| `sourceKind`, `sourceRoot`, `sourceBucket` | `source.kind`, `source.uri`, `source.container` |
| – (inferred from the run summary's `config.target`) | `scope.kind` (`whole` or `prefix`), `scope.prefix` |
| – (run summary `completed`, `config.filters`) | `coverage` |
| `files[].key` | `parts[].path` (relative to the dataset root) |
| `files[].size`, `rowCount`, `MD5checksum`, `minKey`, `maxKey` | `parts[].bytes`, `rows`, `md5`, `min_key_base64`, `max_key_base64` (exact, never truncated; standard base64 of the key bytes) |
| `fileFormat`, `sorted`, `sortKey`, `fileSchema`, `totalRows` | removed (the format implies the first four; sum `rows` for the total) |

**Keys are bytes.** Part bounds are the key bytes in standard base64, so a key that is not UTF-8 survives
JSON. A reader decodes them and compares bytes (unsigned), never the base64 text. For the S3 schema the
decoded bounds are UTF-8, as the keys are.

**Acceptance.** A reader that needs the whole source accepts a dataset only if `coverage.complete` is true,
`coverage.skipped_entries` is 0, `coverage.filters` is empty and `scope.kind` is `whole`. A missing or
malformed `coverage` fails closed. `exclusions` lists what the listing left out by design (an output
directory inside a listed tree); `snapshot` is always false: entries that changed while the listing ran may
or may not be in it.

## S3 schema

`swath/canonical` becomes `swath/s3-objects/v1`, the current-object schema: every row is a current object.

| Column | Format 1 | Format 2 |
|---|---|---|
| `key`, `size`, `last_modified`, `etag`, `storage_class` | present | unchanged |
| `size` | INT64, unannotated | INT64 with the standard `INT(64,true)` annotation (physical type unchanged) |
| `owner_id`, `owner_display_name` | present | unchanged; null unless the run used `-fetch-owner` |
| `checksum_algorithm`, `checksum_type` | present | unchanged; null when S3 omits them |
| `version_id` | always null for current objects | removed |
| `is_latest` | always null | removed |
| `is_delete_marker` | always false | removed |
| `row_type` | always `OBJECT` | removed |

The local schema becomes `swath-local/v1`: `key` and `link_target` are unannotated BYTE_ARRAY (names are
bytes; a name that is not UTF-8 is listed exactly, where format 1 skipped it), `blocks` is renamed `allocated_bytes`, `ctime` is renamed
`metadata_changed_at`, and `mode`, `uid`, `gid`, `nlink` (`INT(32,false)`), `inode` and `dev`
(`INT(64,false)`) are unsigned.

A reader that needs the format-1 columns restores them as constants: `NULL`, `NULL`, `false`, `'OBJECT'`.
`bench/diffds.py` does exactly that, so a Go dataset compares with a Java dataset value for value.

## Footer

| Format 1 stamp | Format 2 |
|---|---|
| `swath.sort.order` | removed; within-file order is the standard `sorting_columns` (key, ascending) on every row group |
| `swath.sort.mode` (`objects`) | removed |
| `swath.sort.format_version` | `swath.profile` = `swath-dataset`, `swath.profile_version` = `2` |
| `swath.sort.file_index` | removed: the manifest orders the parts |
| `swath.sort.file_final` | removed: completeness is the manifest's |
| `swath.sort.index_bound_bytes` | `swath.index_bound_bytes` |
| – | `swath.schema_id` |

**Truncation.** ColumnIndex min/max values longer than `swath.index_bound_bytes` (1,024) are truncated, so
readers treat page bounds as conservative. Manifest bounds are exact.

## Versions

Four versions move independently: the dataset format (SeekParquet's `format_version` 0 and `draft_revision` 1,
in the manifest and the `seekparquet.*` stamps; format 2 builds wrote 2), the
logical schema (`schema_id`, kept when a column is added as optional; see "The downstream reader: dispatch on
schema id" below), the private journal (version 8, never read by readers: version 8 adds the run UUID that durable spill files
are bound to; a version 7 journal is read, with durable spill off, but every version 7 build wrote format 2, whose
runs this build refuses to resume by their dataset format; a version 7 build refuses a version 8 journal; an
interrupted run of an older build is finished with that build or started over with `--overwrite`) and the writer recipe
(`parquetout.Recipe`, `swath-go/parquet-r3` since SeekParquet, in the resume identity, with the dataset format: a
build of another recipe or format refuses the journal). `created_by` names the real build, so
parts are byte-identical within a build only; a part claimed but not recorded when a run stopped is reproduced
on resume with its own footer's columns and `created_by`, so that one part does not depend on the build that resumes.

## Run report and counters

`run.json` replaces Java's run summary (its schema: "Run report" in [cli.md](cli.md#run-report)). It keeps
these Java fields with their names: `cost.cost_usd` and `cost.basis` (`null` on a custom endpoint),
`efficiency.keys_per_sec`, `args_hash`
(the cloud importer's configuration digest; SHA-256 of the Go run's frozen identity, filters included, so its
value differs from Java's listing-scope hash), and `stop_source` (under `outcome`); Java's
`session_duration_ms` is `timings.session_ms`. Its other Java-only fields (`steals`, `sort.arm`,
`shape.fingerprint`, `trajectory`, `slow_ranges`, `probe_latency`, `demand_gate`, `shape`) are not kept
([cli-inventory.md](cli-inventory.md)). The downstream reader does not read it. A reader that tabulates runs
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
| `swath.steal_reason{OWNER_SPLIT,remaining_est_floor}` | `split.skipped.remaining_floor` (only with `swath diag list --floors`); the default gates are `split.skipped.remainder`, `split.skipped.lower_tiny` and `split.skipped.upper_tiny` |
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
`--report-format json`; `-fields` → `--metadata`; `-sequential`, `-max-keys`, `-no-journal`,
`-no-spill` → `swath diag list …`; `swath list --count-only` → `swath count` (2026-11-07); `SWATH_LOCAL_CPUPROFILE`/`MEMPROFILE`/`ENGINE`/`SPLIT_TRACE` →
`SWATH_CPUPROFILE`, `SWATH_MEMPROFILE`, `SWATH_LOCALFS_ENGINE`, `SWATH_SPLIT_TRACE`. Java's `--concurrency`
is `--workers`, default 64 as Java's, whatever the CPU count;
`--encoders` stays CPU-derived. Java's `--tune seed.mode=none` is `swath diag list --seed=false` (the seed is on
by default, as Java's `shallow` is), and `--format discard` is `swath count`, which counts without ordering or
spilling.
Text output keeps stdout (no `-o`, or `-o -`; a closed reader exits 0), `-o FILE.tsv|.jsonl[.gz|.zst]` and
`--format auto|table|tsv|jsonl` (stdout only; with `-o` the name decides). Its rows now use the S3 API's object
shape, so they are read with what already reads S3 listings ([cli.md](cli.md), "Text output"): Java's `key`,
`size`, `last_modified`, `etag`, `storage_class`, `owner_id`, `owner_display_name`, `checksum_algorithm`,
`checksum_type` are `Key`, `Size`, `LastModified` (`+00:00`), `ETag` (with its quotes), `StorageClass`,
`Owner.ID`, `Owner.DisplayName`, `ChecksumAlgorithm` (a list in JSONL), `ChecksumType`, in the API's order;
`row_type` is gone; control bytes use JSON's escapes, not `\xNN`; the table is the `aws s3 ls` line. Not
ported: `--output-type` and `--format parquet` (the `-o` name decides), `--compression` (the name compresses a
file; pipe stdout through `zstd`), a directory of text parts, `--text-part-size`, `--text-writers`, the
`.parquet` one-writer file. Text runs are not resumable: they keep no journal. `--bearer-token-command` and `--bearer-token-refresh-interval` are Java's; the command is
never recorded, so a resume must be given it again (Java's rule too) and is now refused with a message when it
is not, instead of failing on its first request. Java's `--endpoint-url` and `--force` are `--endpoint` and
`--overwrite`, and Java's `--output` is `-o`; a custom endpoint is path-style by default, as Java's. The seven filters are
Java's flags with Java's semantics, except that regular expressions are Go's RE2 ([cli.md](cli.md), "Filters").
The Java CLI's options, exit codes and report fields against this tool: [cli-inventory.md](cli-inventory.md).

**Changes between Go builds.** Before the first release a build makes no compatibility promise to an
earlier one: `swath resume` replays the recorded command with this build's flags, and a flag this build no
longer has is refused (exit 2, the flag parser's unknown-flag error); finish such a run with the build that
started it, or start over. The last renames before 0.4 are in [Changes from 0.3 to 0.4](#changes-from-03-to-04);
the list below is the earlier history of the unreleased builds.
- `--workers` defaults to 64 whatever the CPU count (it was `min(64, max(4, 3*GOMAXPROCS))`).
- A custom `--endpoint` is path-style by default: an interrupted run started with `--endpoint` on a host name
  and no `--path-style` resolves the path-style route on resume, so the identity check refuses it; start it
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
- `--count-only` (now `swath count`) no longer runs the order stage: the report's `features.order` is false, `spill` is absent
  and `max_buffered_bytes` is 0.
- `swath diag list --seed-descent` was removed with the seed descent. The `seed.probes`, `seed.levels.*` and
  other counters of that descent are gone.
- `--seed` is on by default (`swath diag list --seed=false` turns it off) and no longer one delimiter page: it paginates the top
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
- Spill frames are delta-coded and zstd-compressed (level 1) by default; `--spill-compress=false` (on `swath diag list` and `swath resume`)
  writes them uncompressed and `--spill-plain` without delta coding. The report's `resources.spill` adds
  `logical_bytes`, `format` and `decode_refused`.

## Changes from 0.3 to 0.4

The 0.3 releases (the repository's `v0.3.x` tags) are the Java tool; 0.4 is the first release of this Go tool,
and the Java tool is deprecated with it. Moving from Java 0.3 to Go 0.4 changes the launcher, the flags and the
dataset format: the flag-by-flag mapping is [from-java.md](from-java.md) and
[cli-inventory.md](cli-inventory.md), the dataset changes are the sections above. A run interrupted under Java
is finished with Java or started over; `swath resume` reads only this tool's journal. The current flags are
the generated help in [cli.md](cli.md#flags).

The binary reports its version (`swath --version`, the report's `build.version`) from the Go module build
information: a binary built from a source checkout reads `devel`, and the revision it was built from is
`build.revision`.

**If you built from `main` before 0.4.** The Go port was on `main` (from `a2efe16e`) before it was released,
without a version and without a compatibility promise. Against those builds, 0.4 renames or moves these flags
(a removed name is an unknown flag, exit 2; there are no aliases, and a run interrupted under such a build is
finished with it or started over): `--output` is `-o`; `--force` is `--overwrite`; `--endpoint-url` is
`--endpoint`; `--quiet` is `-q`; `--spill-max` is `--spill-budget`; `swath list --count-only` is the command
`swath count` (it refuses the output flags with exit 2); `--seed=false` and `--limiter-slow-start` moved to
`swath diag list`, and `swath list` and `swath count` refuse every diagnostic flag with a message naming
`swath diag`. New in 0.4: `--seed-from PATH`, `swath diag list --spill-plain` and `--slow-split`; spill frames
are delta coded and zstd compressed by default.

**Run report: version 3.** Version 3 only adds fields; nothing of version 2 changes, and a version 2 reader
reads version 3 by ignoring them:

- **Durable spill:** `resources.spill.durable` (the durable spill's account, and on a resume what it accepted,
  rejected and replayed), `resources.spill.durable_off`, and the counters `reuse.ranges`, `reuse.gaps` and
  `reuse.boundary_overfetch_keys`.
- **Drain tail:** `timings.listing_done_at` (the provider finished) and `resources.drain` (sorted Parquet: the
  tail's times, the read-ahead's frames and waits, the parallel segments' plan and holdings, and the encoders
  admitted at start and after listing; [cli.md](cli.md#run-report)).
- **Versions schemas** (`--all-versions`): attempt-scoped `output.versions`, `output.distinct_keys`,
  `output.live_objects` and S3 `output.delete_markers` or GCS `output.noncurrent`, present only for versions
  schemas (zero counts included). They count filtered rows handed on by order, or counted, and exclude
  retained resume rows.

Version 2 introduced the names below; builds before 0.4 wrote version 1. A reader
dispatches on `report_version`; `internal/listing/testdata/report-schema.golden`
is the full schema and "Run report" in [cli.md](cli.md#run-report) its description.

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

**The dataset** changed against those builds: it is SeekParquet ([SeekParquet](#seekparquet)); the Parquet
schemas and data pages are unchanged. **Unchanged since those builds:** the journal's format, the
exit codes and outcome classes, `progress.json` and `error.json`. Spill files changed format (delta coding,
zstd), but they are private to one run and never read across builds.

## The downstream reader: dispatch on schema id

The downstream reader reads format 2 for every provider; for 0.4 it adds SeekParquet ([SeekParquet](#seekparquet)).
What such a reader does, stated as the contract rather than as its code:

- **Open.** Read `manifest.json` first. `format` `seekparquet` (with `format_version` 0 and `draft_revision`
  1) is SeekParquet: take swath's fields from `extensions.swath`. `format` `swath-dataset` is format 2: require
  version 2. Neither reads `_SUCCESS` or a run summary. Any other manifest is format 1 and takes the legacy path.
- **Whole-source acceptance.** From the coverage (`extensions.swath.coverage`; format 2's top-level
  `coverage`): `complete` true, `stop_reason` `completed`, `skipped_entries` 0, `filters` present and empty,
  `scope.kind` `whole`; a missing field refuses.
- **Schema id.** Each schema id defines the columns a reader may rely on, by name and type; the reader dispatches
  on it (`swath/s3-objects/v1`, `swath/gcs-objects/v1`, `swath/azblob-objects/v1`, the local schema) and checks `source.kind` and the
  container against what it was asked to open. A schema id is not a closed column set: a part may hold columns its
  id does not define, which the reader ignores, and an optional column of the id may be absent from some parts or
  all of them, which the reader reads as null. So a column added as optional keeps the id, and a run resumed by a
  build that writes the column mixes parts with and without it under one id (the resume identity freezes the
  schema id, not the column set). `swath/gcs-objects/v1` has one optional column, `time_created`; every other
  column of every schema id is present in every part. `swath/azblob-objects/v1` is `key`, `size`,
  `last_modified` (`Last-Modified`, 1 s resolution), `etag`, `content_md5` (base64, null when absent),
  `content_type`, `blob_type`, `access_tier` (null when absent) and `creation_time`; its keys reach 3,072 bytes.
  As on GCS, `last_modified` also moves when only a blob's metadata changes and `etag` is opaque, not a content
  hash: compare content with `content_md5`, never ETags across stores.
- **Versions schemas** (`swath list --all-versions`). `swath/s3-versions/v1` is the `swath/s3-objects/v1`
  columns in the same order, `size` nullable (null on delete markers, as are `etag`, `storage_class` and the
  checksums), then `version_id` (verbatim; `"null"` is a real ID), `is_latest` and `is_delete_marker`
  (required). `swath/gcs-generations/v1` is the `swath/gcs-objects/v1` columns, then `time_deleted` (null for
  the live generation; there is no `is_latest`). In both, a key repeats once per version, on consecutive rows,
  in the service's order within the key (S3 newest first, versions and delete markers interleaved; GCS by
  generation, ascending), which the file order carries and no column rebuilds. On S3, the first row of a
  key's group has `is_latest`: it is the latest entry, and a delete marker means the object is deleted.
  On GCS, the last row of a name's group is the newest generation; it is live only if `time_deleted` is
  null, and a name may have no live generation. Parts stay key-disjoint (a key's
  rows are never split across parts), and `sorting_columns` is `key` ascending. A reader that assumes one row
  per key refuses these ids; a reader that wants the current objects filters `is_latest AND NOT
  is_delete_marker` (S3) or `time_deleted IS NULL` (GCS). The reserved Java-era version columns are absent from the current-object S3 schema;
  `row_type` is absent from both Go S3 schemas. Readers dispatch on the schema id before reading them.
  `python3 verify.py DIR [REFERENCE_DIR]` validates either mode and can compare the complete rows
  against an independent dataset. `python3 bench/diffds.py A B` compares whole-row multisets and
  ordered row sequences for both versions schemas, preserving nulls and within-key order; it refuses
  mismatched or unknown schema ids. `--ts-ms` is refused for versions so timestamps remain exact.
  Reader fixtures: `python3 -m unittest test_dataset_readers` from `go/`; the SQL fixture tests need DuckDB.

- **Keys.** Part bounds are base64 in the manifest and compare as bytes; local keys may be non-UTF-8.
- **Part footers.** Every part carries the `seekparquet.*` stamps equal to the manifest's (format 2:
  `swath.profile`, `swath.profile_version` 2 and `swath.schema_id`), and `key` ascending as the first sorting
  column.
- **Rows.** `key`, `size` and `last_modified` are read; a null `size` is rejected, so a local dataset listed
  with names only is not readable by such a reader; directory rows of a local dataset are entries whose
  keys end in `/`.
- **Fixtures.** One dataset per schema written by this tool serves as the reader's fixture; the commands
  that produce them are the ones in docs/testing.md.

## Gates

- **DuckDB:** `bench/diffds.py` (row sets both ways with the projection above, strict ascending order within
  and across parts, min/max keys, counts); `verify.py` (the manifest, the stamps, the bounds, the MD5s).
- **SeekParquet conformance:** the suite's reader target against the seekparquet module this tool pins, and
  this tool's datasets validated by the suite's reference adapter and read alike by both
  ([testing.md](testing.md#seekparquet-conformance)).
- **Downstream reader:** opens and streams format-2 datasets of every schema (section above);
  the probe is in [measurements.md](measurements.md#downstream-reader-compatibility).
- **Dropped:** "the Java reader opens Go output". Java stays the producer oracle (replay differential), not
  a reader.
