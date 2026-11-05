# Parquet output

What `internal/parquet` and `internal/dataset` write, and why. Each choice lists what depends on it. The dataset
contract is swath's ([contracts §4 and §4.1](../../../docs/internals/contracts.md), `docs/usage.md` "Directory
dataset layout"); this page says how the Go writer meets it.

## Writer choices

| Choice | Value | Why / who depends on it |
|---|---|---|
| Row path | parquet-go `GenericWriter[any]` fed column values (`[]parquet.Value` per column, one page at a time) | No per-row reflection: a schema mismatch with a struct row silently selects parquet-go's reflective conversion. The column path keeps parquet-go's 64-row write quantum, so pages are identical to its row path. |
| `size` and other integers | bare INT64 / INT32, no INT annotation (`bareInt` adapter) | swath's canonical schema declares `size` as unannotated INT64; parquet-go would add `INT(64,true)`. Java's schema check and Varve's reader compare the physical schema. |
| Timestamps | INT64 `TIMESTAMP(MICROS, UTC)` | canonical `last_modified`. |
| Key encoding | `DELTA_BYTE_ARRAY` | sorted keys share long prefixes. It is also what makes the column-index adapter possible: with dictionary keys the type is library-private. |
| Key column index | one-page indexes stamped `BoundaryOrder = ASCENDING` (`ascendingKey` adapter); multi-page indexes untouched | older Java readers reject a one-page index with `UNORDERED`; Varve and current Java accept both. Built through parquet-go's public indexer factory, not by rewriting footers. |
| `etag` | `DELTA_BYTE_ARRAY` | ETags are unique hex: a dictionary never pays. parquet-mr (Java swath) tries a dictionary and falls back to `DELTA_BYTE_ARRAY`; writing delta directly matches its result and is 2–3 % smaller per dataset than dictionary-then-PLAIN. |
| Other byte arrays | `RLE_DICTIONARY`, 8 KiB dictionary limit per chunk | storage class, owner and checksum columns repeat. |
| Other integers | `DELTA_BINARY_PACKED` | parquet-mr writes a dictionary and switches to delta mid-chunk when it overflows; parquet-go can only fall back to PLAIN. On buckets with few distinct timestamps (noaa-normals-pds) that leaves Go's `last_modified` about 40 % larger than Java's, the main part of a 3–7 % dataset size gap (REPORT.md "Step 4a"). |
| Booleans | `PLAIN` | |
| Pages | v2, 1024 rows, page-header statistics off | served geometry: point lookups decode one 1024-row page. The column index carries page bounds (truncated to 1024 bytes); header statistics would duplicate them. |
| Row groups | byte target 8 MiB on the writer's `Size()` estimate, checked at page boundaries; ceiling 1,000,000 rows | served geometry; the estimate is the encoded payload, not a heap bound. |
| Codec | zstd, klauspost `SpeedDefault` with entropy coding of all-literal blocks enabled | pure Go (`CGO_ENABLED=0`). The default wrapper skips entropy coding on all-literal blocks, which loses most of the gain on hex ETags. |
| Column parallelism | a page's columns may be encoded on several goroutines | used when fewer parts than encoders are open, so a one-part run uses every core. Output is byte-identical to serial encoding (tested). |
| Key length | per schema (`schema.MaxKeyBytes`): 1,024 bytes for S3 (contracts §6; a longer key is the typed `schema.ErrKeyTooLong`), no cap for filesystem paths | filesystem keys longer than the 1,024-byte bound truncation get truncated, conservative column-index bounds (min truncated, max rounded up); `TestKeyLimits` covers both. |
| `sorting_columns` | every row group records `key` ascending (standard Parquet metadata) | generic readers learn the order without the swath stamps; metadata only, rows already arrive sorted. |
| `created_by` | `swath-local 0.0.0-spike` | changes at the first release; byte-identity checks pin it. |

## Run reports

S3 datasets carry swath's run summary as `_swath_summary.json` (`listing.RunSummary`): Java swath's
schema_version 2 field set where this tool has a counterpart (target, completion, objects, durations, cost,
efficiency, engine, seed, sort), plus this tool's own report under `go`. Varve reads `schema_version`,
`completed`, `config.target` and `config.filters` to accept a dataset as whole-bucket coverage. Filesystem
datasets carry this tool's report as `_swath_local_summary.json`; Java has no filesystem mode to mirror.

## Footer stamps

Every part carries swath's `SortedParquetWriter` stamps: `swath.sort.order`
(`key_bytes_unsigned,version_id_null_first,row_type_rank`), `swath.sort.mode=objects`,
`swath.sort.format_version=1`, `swath.sort.file_index` (1-based), `swath.sort.file_final=true` on the last
part only, and `swath.sort.index_bound_bytes=1024`: the column-index truncation, so readers know bounds of
longer values are conservative. `sorting_columns` repeats the key order in standard Parquet metadata. Readers use them to trust the global order without re-sorting; `file_final` lets a reader tell a
complete dataset from a truncated copy.

## Part router and deterministic boundaries

The router is the order stage's sink. It owns part boundaries, so they never depend on encoder timing:

- A row's logical size is its uncompressed field bytes.
- Part 0 is cut at `PartSize / 0.1` logical bytes (0.1 is an assumed compression ratio).
- When part 0 closes, its measured ratio is frozen; every later part is cut at `PartSize / ratio`.
- Ordinals are dense and zero-based; the router knows the last part and sets `file_final`.

The same rows therefore produce the same parts, byte for byte, for any `-workers` and `-encoders`. Each part is
written by its own goroutine and zstd encoder from a bounded channel; at most `Encoders` parts are open.

## Publication gate and order

Before anything is renamed, `dataset` checks dense ordinals, strictly ascending and disjoint part key ranges,
and rows written equal to rows routed. Then: parts (written as `.tmp`, fsynced on close) are renamed in
ordinal order, the `data/` directory is fsynced, each JSON file is written `.tmp`, fsynced and renamed, and
`_SUCCESS` is written last. A failure or cancellation removes the `.tmp` parts and writes no `_SUCCESS`.

```text
out/
  data/part-00000.parquet ...   dense, zero-based %05d; name order == key order; parts strictly disjoint
  manifest.json                 swath's consumer manifest fields + schemaId, sourceKind, sourceRoot|sourceBucket, totalRows
  symlink.txt                   data/<part> paths
  _swath_summary.json           S3: swath's run summary (schema_version 2) with this tool's report under "go"
  _swath_local_summary.json     filesystem: this tool's run report
  _SUCCESS                      empty, written last
```

`-out` safety: the source root is validated first; `-out` may not be the root or an ancestor of it (by path
and by dev/inode); a non-empty `-out` is refused unless `-overwrite`, which replaces only a previous dataset of
this tool, outside the root, removing only its own entries. An `-out` inside the root is skipped by the walk.

## What readers rely on

- **Varve's Go reader** opens S3 datasets when: `manifest.json` `sourceBucket` names the bucket;
  `_swath_summary.json`, if present, covers the whole bucket (a prefix dataset is refused); the required
  columns `is_delete_marker` and `row_type` are present, with `version_id` and `is_latest` as optional
  columns; statistics show pure-object rows.
- **swath's Java reader** needs the canonical physical schema (bare INT64 `size`), the sort stamps, and a key
  column index it accepts (ascending one-page indexes for older releases).
- **DuckDB** (`verify.py`) reads any part; nothing depends on it beyond standard Parquet.

## Schemas

Schemas are data (`schema.Schema`: name, id, sort order, columns with type, required flag and an accessor on
`Row`). `Message()` renders the parquet-mr message string stored in the manifest; the Parquet node is built
from the same list.

**`schema.S3`** (`swath/canonical`) is swath's canonical 13 columns: `key`, `size`, `last_modified`, `etag`,
`storage_class`, `version_id` (null), `is_latest` (null), `is_delete_marker` (required, false), `owner_id`,
`owner_display_name` (only with `-fetch-owner`), `checksum_algorithm` (the first listed), `checksum_type`,
`row_type` (required, `OBJECT`). ETags lose one pair of surrounding quotes, as swath does.

**`schema.Local`** (`swath-local/v0`) has one row per entry below the root. `key`, `size` and
`last_modified` match the canonical schema; the rest replace the S3 columns:

| Column | Parquet physical / logical | Null? | Source |
|---|---|---|---|
| `key` | BINARY (STRING) | required | path relative to the root, `/`-separated, no leading `/`; directories end in `/` |
| `size` | INT64, unannotated | optional | `st_size` (apparent bytes) |
| `last_modified` | INT64 TIMESTAMP(MICROS, UTC) | optional | `st_mtime` |
| `kind` | BINARY (STRING) | required | `FILE`, `DIR`, `SYMLINK`, `FIFO`, `SOCKET`, `CHAR_DEVICE`, `BLOCK_DEVICE`, `UNKNOWN` |
| `mode` | INT32, unannotated | optional | `st_mode & 0o7777` (permission + setuid/setgid/sticky) |
| `uid`, `gid` | INT32, unannotated | optional | `st_uid`, `st_gid` (bit pattern: ids >= 2^31 read back negative) |
| `nlink` | INT32, unannotated | optional | `st_nlink` |
| `inode` | INT64, unannotated | optional | `st_ino` (bit pattern) |
| `dev` | INT64, unannotated | optional | `st_dev` (statx: `makedev(major, minor)`) |
| `blocks` | INT64, unannotated | optional | `st_blocks * 512` = allocated bytes (sparse files show `blocks < size`) |
| `ctime`, `atime` | INT64 TIMESTAMP(MICROS, UTC) | optional | `st_ctime`, `st_atime` |
| `birth_time` | INT64 TIMESTAMP(MICROS, UTC) | optional | statx `stx_btime` (only `--fields statx`, mask `all`, when the filesystem reports it); Windows `CreationTime` |
| `link_target` | BINARY (STRING) | optional | `readlink` result, symlinks only (stat/statx tiers unless `--no-link-target`) |

Directories produce `kind=DIR` rows keyed `a/b/` (`-no-dirs` omits them). The Parquet message name is
`swath_listing` for both schemas.
