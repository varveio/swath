# Upgrading readers to dataset format 2

This guide covers what a reader of swath datasets (the downstream reader, `bench/diffds.py`, `bench/summaries.py`) changes to
read format 2, which this tool writes. Format 1 is the Java-compatible layout. Format 2 is not a superset of
it: there are no aliases, so a format-1 reader rejects a format-2 dataset, and a Java reader cannot open one by
design. DuckDB and the downstream reader are the acceptance gates (see "Gates").

## Dispatch

A reader dispatches on `manifest.json`, before looking for anything else:

| `manifest.json` has | Format | Accepted when |
|---|---|---|
| `"format": "swath-dataset", "format_version": 2` | 2 | the manifest is present and its coverage satisfies the reader (below) |
| `fileFormat`, `files`, `schemaId` (camelCase) | 1 | `_SUCCESS` exists, and the run summary covers the target (the legacy rule) |
| neither | – | never: not a dataset |

A format-2 manifest never falls into a reader's thin-manifest (legacy) fallback.

## Files

| Format 1 | Format 2 |
|---|---|
| `_SUCCESS` (written last; the commit) | removed: `manifest.json` is installed last, by an atomic rename and a directory sync, and is the commit |
| `symlink.txt` (the part list) | removed: the manifest lists the parts |
| `manifest.json` | `manifest.json`, new shape (below) |
| `_swath_summary.json` (S3), `_swath_local_summary.json` (local) | one `run.json` for every provider (`report_version` 1); operational only, written after the commit, and never part of acceptance |
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

Four versions move independently: the dataset format (`format_version` and `swath.profile_version`, 2), the
logical schema (`schema_id`), the private journal (version 7, never read by readers; an interrupted run of an earlier build is finished
with that build or started over with `--overwrite`) and the writer recipe
(`parquetout.Recipe`, `swath-go/parquet-r2`, in the resume identity). `created_by` names the real build, so
parts are byte-identical within a build only.

## Run report and counters

`run.json` replaces Java's run summary. It keeps the Java fields that have consumers, with their names:
`cost.api_calls` (every HTTP attempt; the cloud importer's request count), `cost.cost_usd` and `cost.basis`
(`null` on a custom endpoint), `efficiency.keys_per_sec` and `efficiency.api_calls_per_1k_objects`, `args_hash`
(the cloud importer's configuration digest; SHA-256 of the Go run's frozen identity, filters included, so its
value differs from Java's listing-scope hash), and `stop_source` (under `outcome`); Java's
`session_duration_ms` is `timings.session_ms`. Its other Java-only fields (`steals`, `sort.arm`,
`shape.fingerprint`, `trajectory`, `slow_ranges`, `probe_latency`, `demand_gate`, `shape`) are not kept
([cli-inventory.md](cli-inventory.md)). The downstream reader does not read it. A reader that tabulates runs
(`bench/summaries.py`) maps Java's top-level fields as: `completed` → `outcome.complete`, `objects` →
`output.rows_listed`, `duration_ms`/`listing_duration_ms` → `timings.wall_ms`/`listing_ms`, `config.target` →
`invocation.source`, `config.max_parallel_listings` → `invocation.workers`, `output.files` →
`output.parts_written` + `parts_retained`, `output.compressed_size_bytes` → `output.bytes`,
`shape.fingerprint.git_sha` → `build.revision`. Java meters map to the provider's counters
(`source.stats.counters.values`, here for S3) as follows:

| Java meter or field | Go (`source.stats.counters.values` unless noted) |
|---|---|
| `cost.api_calls`, `swath.api.calls` | `attempts`: every HTTP request sent, client retries and engine requeues included (Java counted fetch attempts with the SDK's retries off); `requests` counts logical calls |
| `swath.retries`, `recovered_errors.retries` | `retries` |
| `swath.throttle.events` | `throttles` (every throttled attempt, the last of an exhausted call included) |
| `engine.pages`, `swath.page.raw_count` | `pages` |
| `swath.entries.emitted`, `objects` | `objects` |
| `engine.splits` | `split.accepted_bounded` + `split.accepted_frontier` |
| `engine.steals`, `swath.steals` | – (splits are owner-side: always 0) |
| `swath.in_flight.avg`, `engine.avg_in_flight` | `inflight.busy_ns` / (`timings.listing_ms` × 10⁶): attempts in flight, send to body read; `mean_active` is ranges in `Expand`, which includes backoffs and admission waits |
| `engine.peak_in_flight` | `inflight.max_active` |
| `swath.workers.active` | `max_active` (ranges in `Expand` at once) |
| `swath.probe.fetches` | `probe.requests` |
| `swath.probe.empty_upper_bisections` | `probe.moved` (an empty upper child found, the pivot moved toward the cursor) |
| `swath.steal_reason{PIVOT,midpoint}` | splits minus `split.pivot_extrapolated` |
| `swath.steal_reason{PIVOT,extrapolate}` | `split.pivot_extrapolated` |
| `swath.steal_reason{CHILD_MASS,empty}` | `child_empty.lower` + `child_empty.upper` |
| `swath.steal_reason{RETRY,cursor_passed_pivot}`, `swath.split.guard_aborts` | `split.rejected.cursor`, `split.rejected.*` |
| `swath.steal_reason{OWNER_SPLIT,demand_gated}` | `split.skipped.no_demand` |
| `swath.steal_reason{SEED,*}` | `seed.*` |
| `swath.sort.backpressure.wait` | `sink_wait_ns`; `resources`: `budget_wait_ns`, `frontier_wait_ns`, `encoder_wait_ns` |
| `swath.sort.segment.bytes` (staging) | `resources.spill.bytes` (spill replaces staging) |

A policy that was off reads `false` under `features` and its counters are absent; a present zero means the
policy was on and never fired ([internals/counters.md](internals/counters.md)).

## Command line

Launch scripts change as follows: `swath list SOURCE -out DIR` → `swath list SOURCE --output DIR` (`-o`);
rerunning with `-resume` → `swath resume DIR`; `swath inspect DIR` → `swath show --journal DIR`; `-json` →
`--report-format json`; `-fields` → `--metadata`; `-sequential`, `-probe`, `-max-keys`, `-no-journal`,
`-no-spill` → `swath diag list …`; `SWATH_LOCAL_CPUPROFILE`/`MEMPROFILE`/`ENGINE`/`SPLIT_TRACE` →
`SWATH_CPUPROFILE`, `SWATH_MEMPROFILE`, `SWATH_LOCALFS_ENGINE`, `SWATH_SPLIT_TRACE`. Java's `--concurrency`
is `--workers`, default 64 as Java's, whatever the CPU count (an earlier Go build derived it from the CPU
count, `min(64, max(4, 3*GOMAXPROCS))`); `--encoders` stays CPU-derived.
Java's text workflows keep their flags: `--format auto|table|tsv|jsonl`, stdout (no `-o`, or `-o -`; a
closed reader exits 0), `-o FILE.tsv|.jsonl[.gz|.zst]`, `--output-type file`, `--compression` and Java's TSV
and JSONL row contract ([cli.md](cli.md), "Text output"); a script that reads Java's TSV header and columns
reads Go's unchanged. Not ported: a directory of text parts (`-o DIR --format tsv` is refused; name one file),
`--text-part-size`, `--text-writers`, the `.parquet` one-writer file. Text runs are not resumable, as Java's
streams were not. `--bearer-token-command` and `--bearer-token-refresh-interval` are Java's; the command is
never recorded, so a resume must be given it again (Java's rule too) and is now refused with a message when it
is not, instead of failing on its first request. `--endpoint-url` and `--force` are accepted as Java's names
of `--endpoint` and `--overwrite`; a custom endpoint is path-style by default, as Java's (an interrupted Go run
started by an earlier build with `--endpoint` on a host name and no `--path-style` now resolves the path-style
route, so its resume is refused by the identity check; start it over with `--overwrite`). The seven filters are
Java's flags with Java's semantics, except that regular expressions are Go's RE2 ([cli.md](cli.md), "Filters").
The Java CLI's options, exit codes and report fields against this tool: [cli-inventory.md](cli-inventory.md).

## The downstream reader: dispatch on schema id

The downstream reader reads format 2 for every provider; this tool is unchanged for it. What such a reader
does, stated as the contract rather than as its code:

- **Open.** Read `manifest.json` first. A manifest with a `format` field is format 2: require version 2 and
  read no `_SUCCESS` and no run summary. Any other manifest is format 1 and takes the legacy path.
- **Whole-source acceptance.** From the manifest's coverage: `complete` true, `stop_reason` `completed`,
  `skipped_entries` 0, `filters` present and empty, `scope.kind` `whole`; a missing field refuses.
- **Schema id.** Each schema id defines the exact columns by name and type; the reader dispatches on it
  (`swath/s3-objects/v1`, `swath/gcs-objects/v1`, the local schema) and checks `source.kind` and the
  container against what it was asked to open. The Java-era `version_id`, `is_latest`, `is_delete_marker`
  and `row_type` columns are absent in format 2, not read as null, so the current-object row check and the
  `row_type` statistics rule apply to format 1 only.
- **Keys.** Part bounds are base64 in the manifest and compare as bytes; local keys may be non-UTF-8.
- **Part footers.** Every part carries `swath.profile`, `swath.profile_version` 2, a `swath.schema_id` equal
  to the manifest's, and `key` ascending as the first sorting column.
- **Rows.** `key`, `size` and `last_modified` are read; a null `size` is rejected, so a local dataset listed
  with names only is not readable by such a reader; directory rows of a local dataset are entries whose
  keys end in `/`.
- **Fixtures.** One dataset per schema written by this tool serves as the reader's fixture; the commands
  that produce them are the ones in docs/testing.md.

## Gates

- **DuckDB:** `bench/diffds.py` (row sets both ways with the projection above, strict ascending order within
  and across parts, min/max keys, counts).
- **Downstream reader:** opens, streams and browses format-2 datasets of every schema (section above);
  the earlier probe is in REPORT.md, "Downstream reader compatibility".
- **Dropped:** "the Java reader opens Go output". Java stays the producer oracle (replay differential), not
  a reader.
