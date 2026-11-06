# Parquet output

What `internal/parquetout` and `internal/dataset` write, and why. Each choice lists what depends on it. The dataset
contract is swath's ([contracts §4 and §4.1](../../../docs/internals/contracts.md), `docs/usage.md` "Directory
dataset layout"); this page says how the Go writer meets it.

## Writer choices

| Choice | Value | Why / who depends on it |
|---|---|---|
| Row path | parquet-go `GenericWriter[any]` fed column values (`[]parquet.Value` per column, one page at a time) | No per-row reflection: a schema mismatch with a struct row silently selects parquet-go's reflective conversion. The column path keeps parquet-go's 64-row write quantum, so pages are identical to its row path. |
| `size` and other integers | INT64 / INT32 with the standard `INT(64,true)` / `INT(32,true)` annotation | Format 2 drops the Java adapter that stripped it; the downstream reader checks the physical type only. |
| Timestamps | INT64 `TIMESTAMP(MICROS, UTC)` | canonical `last_modified`. |
| Key encoding | `DELTA_BYTE_ARRAY` | sorted keys share long prefixes. It is also what makes the column-index adapter possible: with dictionary keys the type is library-private. |
| Key column index | one-page indexes stamped `BoundaryOrder = ASCENDING` (`ascendingKey` adapter); multi-page indexes untouched | older Java readers reject a one-page index with `UNORDERED`; the downstream reader and current Java accept both. Built through parquet-go's public indexer factory, not by rewriting footers. |
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
| `created_by` | `swath-go version <module version or devel> (build <revision>[-dirty])`, from the build itself | bytes are identical within a build; across builds only the recipe (`parquetout.Recipe`, `swath-go/parquet-r2`) is promised. |

## Run reports

Every dataset carries one run report, `run.json` (`listing.Report`, `report_version` 1); `--report-format json` prints the
same type, also for a dry run or a failure, and `--report FILE` writes it to FILE when the attempt ends (also for a
resume refused before it runs). Sections: `build` (tool, module version, VCS revision), `attempt` (`id`, random
per invocation, and `started`; `error.json` carries the same, so a reader holding the report can tell a stale
`error.json` from this attempt's),
`invocation` (argv, source, output, schema, workers, encoders, memory and part size), `outcome` (`committed`,
`listed` or `failed`, the error, completeness, stop reason, per-entry errors, and the command's `class`,
`exit_code`, `resumable` and `cause`, as in [../cli.md](../cli.md), and `unrecorded`: why `error.json` does not hold this outcome, when it does not), `timings` (timestamps and
integer `*_ms` durations, wall, listing, user and system CPU), `output` (rows this invocation listed, rows
retained from an interrupted run, rows committed; parts written and retained; bytes), `resources` (peak RSS,
row budget, peak buffered, waits in `*_ns`, spill, `features`) and `source` (the provider's statistics under
its name: `s3`, `local`; requests and attempts are this invocation's). It is operational: written after the
commit (a failure to write it is a warning, the dataset stands) and never part of acceptance, which reads the
manifest's coverage ([../upgrade.md](../upgrade.md)). There is no cost estimate and no Java-only field.

## Footer stamps

Every part carries four stamps: `swath.profile=swath-dataset`, `swath.profile_version=2`, `swath.schema_id`
and `swath.index_bound_bytes=1024` (the column-index truncation, so readers know bounds of longer values are
conservative). Within-file order is the standard `sorting_columns` (key, ascending) on every row group. Order
across parts, exact bounds and completeness are the manifest's; nothing in a footer says where a part sits
in the dataset.

## Part router and deterministic boundaries

The router is the order stage's sink. It owns part boundaries, so they never depend on encoder timing:

- A row's logical size is its key plus the payload's uncompressed column bytes (`Schema.Bytes`, defined by
  the producer: string lengths, 8 or 4 bytes per present integer).
- Parts are cut at `PartSize / 0.1` logical bytes (0.1 is an assumed compression ratio) until the
  calibrating part closes: the first part closed by size (never one the part interval closed; a run whose
  parts are all closed by time keeps the 0.1 estimate). Its measured ratio is frozen (the journal records it); every later part is cut at
  `PartSize / ratio`. Without the interval that is part 0. A part the interval closed small does not
  calibrate: its encoded size is more footer, and its ratio would freeze a limit far below the part size
  for the whole run and its resumes (`TestTimeCutPartDoesNotCalibrate`: 546 parts against a clean run's 12
  before; now at most the clean run's count plus the parts the interval closed, plus two).
- Ordinals are dense and zero-based; the router knows the last part (the journal records it, so a resume
  after the final part only publishes).

The same rows therefore produce the same parts, byte for byte, for any `-workers` and `-encoders`, unless the
part interval cuts (below).

**The part interval** (`listing.Limits.PartInterval`, default 30 s; `parquetout.Options.Interval`) bounds the
age of the recovery point on a slow frontier: a part open that long with rows is closed although not full,
and a journaled run publishes it, so a crash loses at most about that long of frontier progress instead of a
whole part's worth (the default is Java's rotation interval, 30 s; a cut part can be far below its target
size). The order stage hands on its batch before the drainer waits on the frontier, so the
open part holds every row the walk has reached. A cut by time makes that run's boundaries depend on timing:
the report counts such cuts (`resources.part_time_cuts`); with none, or with the interval off (a negative
value), boundaries depend only on the rows and the byte-identity guarantees above hold
(`TestIntervalClosesAnAgedPart`, `TestPartIntervalBoundsRecoveryPointAge`: a 2 s trickle keeps the recovery
point under 260 ms old at a 200 ms interval, against 1.96 s by size only). A resume that must reproduce a
claimed part ([durability.md](durability.md#the-journal)) closes that ordinal at the claimed part's row count
(`Options.Cut`) whatever the interval. The command sets it with
`--part-interval` ([../cli.md](../cli.md)). Each part is
written by its own goroutine and zstd encoder from a bounded channel; at most `Encoders` parts are open.

## Publication gate and order

Order is checked at three places, each before the step it protects:

- **Every row**, in the order stage (`order.Run`'s stream-wide guard): keys strictly ascending across the
  whole run, and after the last published key on a resume. No sink, Parquet or other, receives a descending
  or repeated key, even from a provider that breaks its ordering contract at a range boundary.
- **Every part, before it is journaled.** A journaled run publishes parts early, so the router checks each
  part against the part before it (the resumed seam included) before `Publish` claims, records or renames
  it: nonempty, and its first key after the previous part's last. The journal's `Append` checks the same
  (`journal.Follows`: the next ordinal, after no final record, strictly after the last record's keys, a row
  total that adds up) and refuses before writing, and a resume refuses a journal whose records do not chain
  (`TestRouterRefusesOverlapBeforePublish`, `TestAppendRefusesNonAdjacentRecord`,
  `TestBoundaryDefectNeverReachesTheJournal`). Each part's rows are checked by its writer.
- **The whole set, before the manifest.** `Router.Close` repeats dense ordinals, strictly ascending and
  disjoint part key ranges, and rows written equal to rows routed (`dataset.Check`), and the pipeline
  reconciles the row counts of the attempt hop by hop: rows the provider emitted, the order stage received
  and handed on, and the router routed (`*dataset.InvariantError` on any mismatch).

Then: parts (written in `.swath/parts/`, fsynced on close) are renamed
into `data/` in ordinal order, `data/` is fsynced, and `manifest.json` is written to a temporary name, fsynced,
renamed and its directory fsynced. That rename is the commit: without a manifest there is no dataset. The
run reports follow; a report that cannot be written is a warning, and the dataset stands. A failure or
cancellation removes the unpublished parts and writes no manifest.

```text
out/
  data/part-00000.parquet ...   dense, zero-based %05d; name order == key order; parts strictly disjoint
  manifest.json                 format 2: source, scope, coverage, ordered parts with exact bounds (the commit)
  run.json                      the run report (operational, written after the commit)
  error.json                    the outcome of a run that did not complete (removed when one completes)
  .swath/                       private workspace: lock, journal, parts/ (unpublished), spill/, progress.json
```

The manifest's fields and the reader's acceptance rule are in [../upgrade.md](../upgrade.md).

Output safety: the source root is validated first; the output may not be the root or an ancestor of it (by path
and by dev/inode); a non-empty output is refused unless `--overwrite`, which replaces only a previous dataset of
this tool, outside the root, removing only its own entries. An output inside the root is skipped by the walk.

**`--overwrite` is a destructive replacement, not an atomic one.** Under the output lock, before anything is
listed, the previous dataset's commit record (`manifest.json`, or an earlier layout's `_SUCCESS`) is removed
and the directory synced, then its other entries. From that moment the path holds no dataset: a run that fails
or is interrupted leaves none there (an interrupted journaled run can be resumed), and readers see no dataset
until the new manifest is installed. The commit goes first so that a crash part way through the removal never
leaves a manifest naming parts already gone (`TestOverwriteRemovesTheCommitFirst`). The removal cannot wait
for the new run's first part or its publication: parts are published early under the same names in `data/`,
which the old manifest would then name with different bytes. Where readers must always find a dataset (an
hourly refresh, say), write each refresh to a new directory, never overwritten, and switch readers to it
through a pointer outside the datasets (a generation file, a database row, a symlink swapped by rename) once
its `manifest.json` exists; delete a generation only when no reader uses it.

## What readers rely on

- **The downstream reader** opens format-2 S3 datasets with the changes in [../upgrade.md](../upgrade.md): it
  dispatches on the manifest's format, accepts whole-bucket coverage from the manifest, reads the
  current-object schema and the `swath.profile` stamp (REPORT.md, "Downstream reader compatibility").
- **swath's Java reader** does not open format 2, by design (no sort stamps, no reserved columns).
- **DuckDB** (`verify.py`) reads any part; nothing depends on it beyond standard Parquet.

## Schemas

Schemas are data (`schema.Schema[P]`: name, id, the key type, then columns with type, required flag, an
accessor of the payload `P` and an encoding preference: `Delta` writes DELTA_BYTE_ARRAY, used for `etag`). Each
producer defines its schema beside its code. The Parquet node is built from that list; `TestFooterMatchesSchema`
checks the footer against a test schema with one column of every type. There is no message string any more.

**`s3.Schema`** (`swath/s3-objects/v1`) is the current-object schema, nine columns: `key`, `size`,
`last_modified`, `etag`, `storage_class`, `owner_id`, `owner_display_name` (null unless `-fetch-owner`),
`checksum_algorithm` (the first listed), `checksum_type` (null when S3 omits them). Every row is a current
object, so Java's `version_id`, `is_latest`, `is_delete_marker` and `row_type` are not stored; `bench/diffds.py`
restores their constant values for comparison. ETags lose one pair of surrounding quotes, as swath does.

**Keys are bytes.** Every schema's `key` is a required BYTE_ARRAY ordered as unsigned bytes. It carries the
STRING annotation only where the producer guarantees UTF-8 (S3 keys are UTF-8); filesystem keys and link
targets are unannotated BYTE_ARRAY, written exactly as the kernel returned them, never skipped, replaced or
normalised. Manifest bounds are written in base64 (`min_key_base64`, `max_key_base64`) so every key survives
JSON; readers compare the decoded bytes (`dataset.Key`).

**`fs.Schema`** (`swath-local/v1`) has one row per entry below the root, directories included, each with a
named `kind`. `size` and `last_modified` match the S3 schema; `key` is binary. Identifiers are unsigned. v1 renamed
`blocks` to `allocated_bytes` and `ctime` to `metadata_changed_at`, and made the identifiers unsigned:

| Column | Parquet physical / logical | Null? | Source |
|---|---|---|---|
| `key` | BYTE_ARRAY (unannotated) | required | path relative to the root, `/`-separated, no leading `/`; directories end in `/`; the name's exact bytes |
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
| `birth_time` | INT64 TIMESTAMP(MICROS, UTC) | optional | statx `stx_btime` (only `--metadata statx`, mask `all`, when the filesystem reports it); Windows `CreationTime` |
| `link_target` | BYTE_ARRAY (unannotated) | optional | `readlink` result, symlinks only (stat/statx tiers unless `--no-link-target`) |

Directories produce `kind=DIR` rows keyed `a/b/` (`-no-dirs` omits them). In the S3 schema a key ending in
`/` is an object like any other: delimiter prefixes are derived by readers, never stored. The Parquet schema
name is `swath_listing` for both schemas.
