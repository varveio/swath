# Parquet output

What `internal/parquet` and `internal/dataset` write, and why. Each choice lists what depends on it. The dataset
contract is swath's ([contracts §4 and §4.1](../../../docs/internals/contracts.md), `docs/usage.md` "Directory
dataset layout"); this page says how the Go writer meets it.

## Writer choices

| Choice | Value | Why / who depends on it |
|---|---|---|
| Row path | parquet-go `GenericWriter[any]` fed column values (`[]parquet.Value` per column, one page at a time) | No per-row reflection: a schema mismatch with a struct row silently selects parquet-go's reflective conversion. The column path keeps parquet-go's 64-row write quantum, so pages are identical to its row path. |
| `size` and other integers | INT64 / INT32 with the standard `INT(64,true)` / `INT(32,true)` annotation | Format 2 drops the Java adapter that stripped it; Varve's reader checks the physical type only. |
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
| `created_by` | `swath-go version <module version or devel> (build <revision>[-dirty])`, from the build itself | bytes are identical within a build; across builds only the recipe (`parquet.Recipe`, `swath-go/parquet-r2`) is promised. |

## Run reports

S3 datasets carry swath's run summary as `_swath_summary.json` (`listing.RunSummary`): Java swath's
schema_version 2 field set where this tool has a counterpart (target, completion, objects, durations, cost,
efficiency, engine, seed, sort), plus this tool's own report under `go`. Filesystem datasets carry this
tool's report as `_swath_local_summary.json`. Both are operational: written after the commit, and not part
of acceptance, which reads the manifest's coverage ([../upgrade.md](../upgrade.md)).

## Footer stamps

Every part carries four stamps: `swath.profile=swath-dataset`, `swath.profile_version=2`, `swath.schema_id`
and `swath.index_bound_bytes=1024` (the column-index truncation, so readers know bounds of longer values are
conservative). Within-file order is the standard `sorting_columns` (key, ascending) on every row group. Order
across parts, exact bounds and completeness are the manifest's; nothing in a footer says where a part sits
in the dataset.

## Part router and deterministic boundaries

The router is the order stage's sink. It owns part boundaries, so they never depend on encoder timing:

- A row's logical size is its uncompressed field bytes.
- Part 0 is cut at `PartSize / 0.1` logical bytes (0.1 is an assumed compression ratio).
- When part 0 closes, its measured ratio is frozen; every later part is cut at `PartSize / ratio`.
- Ordinals are dense and zero-based; the router knows the last part (the journal records it, so a resume
  after the final part only publishes).

The same rows therefore produce the same parts, byte for byte, for any `-workers` and `-encoders`. Each part is
written by its own goroutine and zstd encoder from a bounded channel; at most `Encoders` parts are open.

## Publication gate and order

Before anything is renamed, `dataset` checks dense ordinals, strictly ascending and disjoint part key ranges,
and rows written equal to rows routed. Then: parts (written in `.swath/parts/`, fsynced on close) are renamed
into `data/` in ordinal order, `data/` is fsynced, and `manifest.json` is written to a temporary name, fsynced,
renamed and its directory fsynced. That rename is the commit: without a manifest there is no dataset. The
run reports follow; a report that cannot be written is a warning, and the dataset stands. A failure or
cancellation removes the unpublished parts and writes no manifest.

```text
out/
  data/part-00000.parquet ...   dense, zero-based %05d; name order == key order; parts strictly disjoint
  manifest.json                 format 2: source, scope, coverage, ordered parts with exact bounds (the commit)
  _swath_summary.json           S3: swath's run summary (schema_version 2) with this tool's report under "go"
  _swath_local_summary.json     filesystem: this tool's run report
  .swath/                       private workspace: lock, journal, parts/ (unpublished), spill/
```

The manifest's fields and the reader's acceptance rule are in [../upgrade.md](../upgrade.md).

`-out` safety: the source root is validated first; `-out` may not be the root or an ancestor of it (by path
and by dev/inode); a non-empty `-out` is refused unless `-overwrite`, which replaces only a previous dataset of
this tool, outside the root, removing only its own entries. An `-out` inside the root is skipped by the walk.

## What readers rely on

- **Varve's Go reader** opens format-2 S3 datasets with the changes in [../upgrade.md](../upgrade.md): it
  dispatches on the manifest's format, accepts whole-bucket coverage from the manifest, reads the
  current-object schema and the `swath.profile` stamp (REPORT.md, "Varve reader compatibility").
- **swath's Java reader** does not open format 2, by design (no sort stamps, no reserved columns).
- **DuckDB** (`verify.py`) reads any part; nothing depends on it beyond standard Parquet.

## Schemas

Schemas are data (`schema.Schema`: name, id, columns with type, required flag and an accessor on `Row`); the
Parquet node is built from that list, and `TestFooterMatchesSchema` checks the footer against it. There is no
message string any more.

**`schema.S3`** (`swath/s3-objects/v1`) is the current-object schema, nine columns: `key`, `size`,
`last_modified`, `etag`, `storage_class`, `owner_id`, `owner_display_name` (null unless `-fetch-owner`),
`checksum_algorithm` (the first listed), `checksum_type` (null when S3 omits them). Every row is a current
object, so Java's `version_id`, `is_latest`, `is_delete_marker` and `row_type` are not stored; `bench/diffds.py`
restores their constant values for comparison. ETags lose one pair of surrounding quotes, as swath does.

**`schema.Local`** (`swath-local/v1`) has one row per entry below the root, directories included, each with a
named `kind`. `key`, `size` and `last_modified` match the S3 schema. Identifiers are unsigned. v1 renamed
`blocks` to `allocated_bytes` and `ctime` to `metadata_changed_at`, and made the identifiers unsigned:

| Column | Parquet physical / logical | Null? | Source |
|---|---|---|---|
| `key` | BINARY (STRING) | required | path relative to the root, `/`-separated, no leading `/`; directories end in `/` |
| `size` | INT64 `INT(64,true)` | optional | `st_size` (apparent bytes) |
| `last_modified` | INT64 TIMESTAMP(MICROS, UTC) | optional | `st_mtime` |
| `kind` | BINARY (STRING) | required | `FILE`, `DIR`, `SYMLINK`, `FIFO`, `SOCKET`, `CHAR_DEVICE`, `BLOCK_DEVICE`, `UNKNOWN` |
| `mode` | INT32 `INT(32,false)` | optional | `st_mode & 0o7777` (permission + setuid/setgid/sticky) |
| `uid`, `gid` | INT32 `INT(32,false)` | optional | `st_uid`, `st_gid` |
| `nlink` | INT32 `INT(32,false)` | optional | `st_nlink` |
| `inode` | INT64 `INT(64,false)` | optional | `st_ino` |
| `dev` | INT64 `INT(64,false)` | optional | `st_dev` (statx: `makedev(major, minor)`) |
| `allocated_bytes` | INT64 `INT(64,true)` | optional | `st_blocks * 512` (sparse files show `allocated_bytes < size`) |
| `metadata_changed_at`, `atime` | INT64 TIMESTAMP(MICROS, UTC) | optional | `st_ctime`, `st_atime` |
| `birth_time` | INT64 TIMESTAMP(MICROS, UTC) | optional | statx `stx_btime` (only `--fields statx`, mask `all`, when the filesystem reports it); Windows `CreationTime` |
| `link_target` | BINARY (STRING) | optional | `readlink` result, symlinks only (stat/statx tiers unless `--no-link-target`) |

Directories produce `kind=DIR` rows keyed `a/b/` (`-no-dirs` omits them). In the S3 schema a key ending in
`/` is an object like any other: delimiter prefixes are derived by readers, never stored. The Parquet schema
name is `swath_listing` for both schemas.
