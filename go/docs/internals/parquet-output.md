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
| Other integers | `DELTA_BINARY_PACKED` | parquet-mr writes a dictionary and switches to delta mid-chunk when it overflows; parquet-go can only fall back to PLAIN. On buckets with few distinct timestamps (noaa-normals-pds) that leaves Go's `last_modified` about 40 % larger than Java's, the main part of a 3–7 % dataset size gap ([measurements](../measurements.md#output-identity-with-the-java-tool)). |
| Booleans | `PLAIN` | |
| Pages | v2, 1024 rows, page-header statistics off | served geometry: point lookups decode one 1024-row page. The column index carries page bounds (truncated to 1024 bytes); header statistics would duplicate them. |
| Row groups | byte target 8 MiB on the writer's `Size()` estimate, checked at page boundaries; ceiling 1,000,000 rows | served geometry; the estimate is the encoded payload, not a heap bound. |
| Codec | zstd, klauspost `SpeedDefault` with entropy coding of all-literal blocks enabled | pure Go (`CGO_ENABLED=0`). The default wrapper skips entropy coding on all-literal blocks, which loses most of the gain on hex ETags. |
| Codec states | one pool per router, shared by its writers: single-state zstd encoders made as concurrent page compressions need them, up to the encoders admitted (raised by `Router.Admit`), reused last-in first-out | a state keeps the history of the largest page it compressed (about 10.6 MiB once a page passes one 128 KiB zstd block, with zstd's lower encoder memory, which leaves the bytes unchanged: `TestLowerEncoderMemIsByteIdentical`), so a pool handed round in turn warms every state; output does not depend on the state (`EncodeAll` resets it). |
| Writer admission | `parquetout.EncoderBytes(schema)`: the 8 MiB row group, a 1 MiB page buffer per column, 2.5× the key at its cap and 4× each column with `schema.Spec.MaxBytes`, and one codec state (12 MiB once a page passes one zstd block, else 2 MiB): 31.5 MiB for S3, 32.5 for GCS, 61 for a local path | the 12 MiB state, the 1 MiB column buffer and the 2.5× and 4× factors are measurements against klauspost/compress v1.19.1 and parquet-go v0.32.0 (live heap after GC across 24 pages a writer, random values at the longest lengths: one S3 writer at 1,024-byte keys 24.5 MiB, four 25.6 each; the filesystem schema with 4,096-byte keys and link targets 48 each, all with zstd's lower encoder memory), not values derived from the libraries: remeasure on an upgrade of either, and keep `TestWriterAdmissionCoversHeap` passing. The price holds a margin over the measurements: four S3 writers 84 MiB of 126 priced (33 %); four local writers with binary 4,096-byte keys and link targets, 24 pages each, 170 of 244 in the gate and 216 to 218 with 800 MB of rows live beside them (12 %, the worst case measured). A value is held to its column's `MaxBytes` (`schema.ErrValueTooLong`), so the widths priced are the widths written. |
| Column parallelism | a page's columns may be encoded on several goroutines | used when fewer parts than encoders are open, so a one-part run uses every core. Output is byte-identical to serial encoding (tested). |
| Key length | per schema (`schema.MaxKeyBytes`): 1,024 bytes for S3 (contracts §6; a longer key is the typed `schema.ErrKeyTooLong`), no cap for filesystem paths | filesystem keys longer than the 1,024-byte bound truncation get truncated, conservative column-index bounds (min truncated, max rounded up); `TestKeyLimits` covers both. |
| `sorting_columns` | every row group records `key` ascending (standard Parquet metadata) | generic readers learn the order without the swath stamps; metadata only, rows already arrive sorted. |
| `created_by` | `swath-go version <module version or devel> (build <revision>[-dirty])`, from the build itself (`internal/buildinfo`: the revision `build.sh` links in, else the toolchain's VCS stamp; shortened to 12 hex digits) | bytes are identical within a build; across builds only the recipe (`parquetout.Recipe`, `swath-go/parquet-r2`) is promised. A part a resume reproduces (claimed, never recorded) keeps the claimed part's own `created_by` and columns. |

## Run reports

Every dataset carries one run report, `run.json` (`listing.Report`, `report_version` 3); `--report-format json`
prints the same type, also for `swath count` or a failure, and `--report FILE` writes it to FILE when the attempt
ends (also for a resume refused before it runs). Its schema has one owner, "Run report" in
[../cli.md](../cli.md#run-report) (pinned by `internal/listing/testdata/report-schema.golden`). It is
operational: written after the commit (a failure to write it is a warning, the dataset stands) and never part of
acceptance, which reads the manifest's coverage ([../upgrade.md](../upgrade.md)).

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
  for the whole run and its resumes (`TestTimeCutPartDoesNotCalibrate`: such a ratio cut 546 parts where a
  clean run cuts 12; the test allows at most the clean run's count plus the parts the interval closed, plus two).
  A resume that reproduces a claimed, unrecorded part closes it at its recorded row count; if that is the
  calibrating part it reaches the size target there, as the original did, and calibrates the same ratio
  (`TestPinnedCalibratingPartCalibrates`).
- Ordinals are dense and zero-based; the router knows the last part (the journal records it, so a resume
  after the final part only publishes).

The same rows therefore produce the same parts, byte for byte, for any `-workers` and `-encoders`, unless the
part interval cuts (below).

After listing, the order stage may write the rest as segments: consecutive parts opened in order and written
by several goroutines at once (`Router.Plan`, `OpenSegment`; [ordering](ordering.md#segments-parts-written-in-parallel-after-listing)).
It cuts them with the router's own rule once part 0 has frozen the ratio, so the parts are the same; the
interval closes no part from then on. Publication stays in ordinal order with the adjacency check, and the
abort closes every segment's queue unpublished.

**The part limit.** A dataset has at most 100,000 parts (`part-00000` to `part-99999`: ordinals have five digits, `dataset.MaxParts`); at the defaults (64 MiB parts, about 30 bytes per row) it takes about 200 billion objects or 35 days of uninterrupted 30-second time cuts; in practice a small `--part-size` or `--part-rotation-max-rows` reaches it. The router refuses to open part 100,000
(`dataset.ErrPartLimit`, checked when it would open the part) after the parts before it are published, so the
journal's last record is part 99,999; the command exits 1, `cause` `part_limit`, not resumable
(`TestRouterRefusesPartPastLimit`); a segment refused there stops waiting for that publication when the run is cancelled, and gives its encoder slot back (`TestSegmentPastLimitCancels`). `dataset.PartOrdinal` refuses a six-digit name.

**The part interval** (`listing.Limits.PartInterval`, default 30 s; `parquetout.Options.Interval`) is a
rotation target: a part open that long with rows is closed although not full (the router's ticker cuts only an
existing part whose boundary no resume fixed), and a journaled run publishes it in ordinal order behind the parts
before it. It bounds how long rows wait in an open part, not the age of the recovery point: the closed part
still encodes and syncs, and while no rows reach the output no part is open to close. Rows listed past the
frontier (buffered, spilled, or in parts not yet recorded) are relisted whatever the interval
([durability.md](durability.md)). The
default is Java's rotation interval, 30 s; a cut part can be far below its target size. The order stage hands on its batch before the drainer waits on the frontier, so the
open part holds every row the walk has reached. A cut by time makes that run's boundaries depend on timing:
the report counts such cuts (`resources.part_time_cuts`); with none, or with the interval off (zero;
`--part-interval` refuses a negative value), boundaries depend only on the rows and the byte-identity guarantees above hold
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
  data/part-00000.parquet ...   dense, zero-based, five digits (at most 100,000 parts); name order == ordinal
                                order == key order (the manifest's part list is the order); parts strictly disjoint
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
  current-object schema and the `swath.profile` stamp ([measurements](../measurements.md#downstream-reader-compatibility)).
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
