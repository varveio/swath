# Upgrading readers to dataset format 2

This guide covers what a reader of swath datasets (Varve, `bench/diffds.py`, `bench/summaries.py`) changes to
read format 2, which this tool writes. Format 1 is the Java-compatible layout. Format 2 is not a superset of
it: there are no aliases, so a format-1 reader rejects a format-2 dataset, and a Java reader cannot open one by
design. DuckDB and the Varve probe are the acceptance gates (see "Gates").

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
| `_swath_summary.json` (S3), `_swath_local_summary.json` (local) | unchanged names; operational only, written after the commit, and never part of acceptance |
| `data/part-NNNNN.parquet` | unchanged |
| `.swath/journal`, `-out/.swath.lock`, `data/*.tmp` | `.swath/` is the private workspace: `lock`, `journal`, `parts/` (unpublished parts), `spill/`. Readers ignore it |

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
     "min_key": "logs/a", "max_key": "logs/k", "md5": "…"}
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
| `files[].size`, `rowCount`, `MD5checksum`, `minKey`, `maxKey` | `parts[].bytes`, `rows`, `md5`, `min_key`, `max_key` (exact, never truncated) |
| `fileFormat`, `sorted`, `sortKey`, `fileSchema`, `totalRows` | removed (the format implies the first four; sum `rows` for the total) |

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
| `owner_id`, `owner_display_name` | present | unchanged; null unless the run used `-fetch-owner` |
| `checksum_algorithm`, `checksum_type` | present | unchanged; null when S3 omits them |
| `version_id` | always null for current objects | removed |
| `is_latest` | always null | removed |
| `is_delete_marker` | always false | removed |
| `row_type` | always `OBJECT` | removed |

A reader that needs the format-1 columns restores them as constants: `NULL`, `NULL`, `false`, `'OBJECT'`.
`bench/diffds.py` does exactly that, so a Go dataset compares with a Java dataset value for value. The local
schema (`swath-local/v0`) is unchanged.

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

## Run summary and counters

The run summary keeps the Varve coverage fields of the Java schema (`schema_version`, `completed`,
`config.target`, `config.filters`), but format 2 no longer needs them for acceptance. This tool's own report
sits under `go`, with Go names. Java meters map to them as follows (for `bench/summaries.py`):

| Java meter or field | Go (`go.s3` unless noted) |
|---|---|
| `cost.api_calls`, `swath.api.calls` | `requests` (logical calls); `attempts` counts HTTP requests with retries |
| `swath.retries`, `recovered_errors.retries` | `retries` |
| `swath.throttle.events` | `throttles` |
| `engine.pages`, `swath.page.raw_count` | `pages` |
| `swath.entries.emitted`, `objects` | `objects` |
| `engine.splits` | `split.accepted_bounded` + `split.accepted_frontier` |
| `engine.steals`, `swath.steals` | – (splits are owner-side: always 0) |
| `swath.in_flight.avg`, `engine.avg_in_flight` | `mean_active` |
| `engine.peak_in_flight`, `swath.workers.active` | `max_active` |
| `swath.probe.fetches` | `probe.requests` |
| `swath.probe.empty_upper_bisections` | `probe.moved` (an empty upper child found, the pivot moved toward the cursor) |
| `swath.steal_reason{PIVOT,midpoint}` | splits minus `split.pivot_extrapolated` |
| `swath.steal_reason{PIVOT,extrapolate}` | `split.pivot_extrapolated` |
| `swath.steal_reason{CHILD_MASS,empty}` | `child_empty.lower` + `child_empty.upper` |
| `swath.steal_reason{RETRY,cursor_passed_pivot}`, `swath.split.guard_aborts` | `split.rejected.cursor`, `split.rejected.*` |
| `swath.steal_reason{OWNER_SPLIT,demand_gated}` | `split.skipped.no_demand` |
| `swath.steal_reason{SEED,*}` | `seed.*` |
| `swath.sort.backpressure.wait` | `sink_wait_ns`; top level: `budget_wait_ns`, `frontier_wait_ns`, `encoder_wait_ns` |
| `swath.sort.segment.bytes` (staging) | top level `spill.bytes` (spill replaces staging) |

A policy that was off is listed under `disabled` and its counters are absent; a present zero means the
policy was on and never fired ([internals/counters.md](internals/counters.md)).

## Gates

- **DuckDB:** `bench/diffds.py` (row sets both ways with the projection above, strict ascending order within
  and across parts, min/max keys, counts).
- **Varve:** the Varve probe reads a format-2 dataset through the reader with this guide's changes applied
  (REPORT.md, "Varve reader compatibility").
- **Dropped:** "the Java reader opens Go output". Java stays the producer oracle (replay differential), not
  a reader.
