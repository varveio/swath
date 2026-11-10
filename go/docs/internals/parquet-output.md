# Parquet output

What `internal/parquetout` and `internal/dataset` write, and why. Each choice lists what depends on it. The dataset
is a [SeekParquet](https://github.com/varveio/seekparquet) dataset, format 1 draft revision 1 (the format's
`spec/format-1.md`; the swath dataset profile it carries is [../dataset.md](../dataset.md); what changed from the Java tool's format 1 is in [../upgrade.md](../upgrade.md#seekparquet)); swath's own
guarantees are in [contracts.md](contracts.md#dataset-guarantees-readers-rely-on-these). This page says how the
Go writer meets them.

The format's mechanisms are the [seekparquet Go module](https://github.com/varveio/seekparquet)'s, at the
commit `go/go.mod` pins (a pseudo-version of its `main` until it releases): the part writer (`write.NewPart`:
pages, row groups, page indexes with exact bounds, the ascending key index, `sorting_columns`, the stamps, the
key-order and profile checks, the SHA-256 computed as bytes are written), the manifest's JSON
(`seekparquet.MarshalManifest`, `ParseManifest`) and footer inspection (`InspectFooter`, `InspectKeyPages`).
swath supplies the schema binding (`nodeOf`: column types, annotations, delta columns), the rows (`fill`), the
codec pool, the column concurrency, the geometry and `created_by`, and keeps the router, the value caps
(checked before a row reaches the module), the journal and publication.

## Writer choices

| Choice | Value | Why / who depends on it |
|---|---|---|
| Row path | the module's page intake, `write.PartWriter.WritePage`: swath buffers a page of rows (1,024) and the module asks for each column's values (`fillKey`, `fill`, from the schema's accessors), borrowing them until it returns, and writes each column's page through parquet-go's column writers, 64 rows at a time | No per-row reflection and no copy: a schema mismatch with a struct row silently selects parquet-go's reflective conversion. The 64-row write quantum keeps pages identical to parquet-go's row path. `WritePage` checks the key order but not that a `STRING` key is valid UTF-8 (KEY-6): swath declares the key `STRING` only for providers whose keys are UTF-8 by their protocol (S3, GCS) and `BINARY` for filesystem paths, so it adds no check. |
| `size` and other integers | INT64 / INT32 with the standard `INT(64,true)` / `INT(32,true)` annotation | The Java tool's adapter stripped it; the downstream reader checks the physical type only. |
| Timestamps | INT64 `TIMESTAMP(MICROS, UTC)` | canonical `last_modified`. |
| Key encoding | `DELTA_BYTE_ARRAY` | sorted keys share long prefixes. It is also what makes the column-index adapter possible: with dictionary keys the type is library-private. |
| Key column index | one-page indexes stamped `BoundaryOrder = ASCENDING` (the module's `ascendingKey` adapter); multi-page indexes untouched | older Java readers reject a one-page index with `UNORDERED`; the downstream reader and current Java accept both. Built through parquet-go's public indexer factory, not by rewriting footers. |
| `etag` | `DELTA_BYTE_ARRAY` | ETags are unique hex: a dictionary never pays. parquet-mr (Java swath) tries a dictionary and falls back to `DELTA_BYTE_ARRAY`; writing delta directly matches its result and is 2–3 % smaller per dataset than dictionary-then-PLAIN. |
| Other byte arrays | `RLE_DICTIONARY`, 8 KiB dictionary limit per chunk | storage class, owner and checksum columns repeat. |
| Other integers | `DELTA_BINARY_PACKED` | parquet-mr writes a dictionary and switches to delta mid-chunk when it overflows; parquet-go can only fall back to PLAIN. On buckets with few distinct timestamps (noaa-normals-pds) that leaves Go's `last_modified` about 40 % larger than Java's, the main part of a 3–7 % dataset size gap ([measurements](../measurements.md#output-identity-with-the-java-tool)). |
| Booleans | `PLAIN` | |
| Pages | v2, 1024 rows, page-header statistics off | served geometry: point lookups decode one 1024-row page (SeekParquet GEO-1 recommends at most 1,024 key rows a page). The column index carries page bounds; header statistics would duplicate them. |
| Column index bounds | exact for every column: each page's minimum and maximum (the module's writer, no shortening) | Parquet's bounds, and SeekParquet RG-5, require a true upper bound. parquet-go shortens a long maximum by incrementing its last kept byte, which leaves one whose kept bytes are all `0xFF` below the value. Exact bounds therefore matter for long filesystem paths and link targets. `TestKeyLimits` (a 1,025-byte `0xFF` link target) and `TestFooterOnlyChange` check exact bounds. |
| Row groups | byte target 8 MiB on the writer's `Size()` estimate, checked at page boundaries; ceiling 1,000,000 rows | served geometry; the estimate is the encoded payload, not a heap bound. |
| Codec | zstd, klauspost `SpeedDefault` with entropy coding of all-literal blocks enabled | pure Go (`CGO_ENABLED=0`). The default wrapper skips entropy coding on all-literal blocks, which loses most of the gain on hex ETags. |
| Codec states | one pool per router, shared by its writers: single-state zstd encoders made as concurrent page compressions need them, up to the encoders admitted (raised by `Router.Admit`), reused last-in first-out | a state keeps the history of the largest page it compressed (about 10.6 MiB once a page passes one 128 KiB zstd block, with zstd's lower encoder memory, which changes how the encoder allocates its history and scratch, not its window or matching: `TestLowerEncoderMemIsByteIdentical` compares 33 raw inputs, three patterns at eleven sizes up to 9 MiB, with and without it; it is evidence, not proof for every frame or reuse history, and exercises no schema). The 10.6 MiB and the 12 MiB priced are measured residency and allowance, not a bound the library guarantees. The spill frames' zstd encoders are separate and set apart: `order.spillEncoderOptions`, a 1 MiB window with the same lower-memory mode, about 1.5 MiB each ([ordering](ordering.md)); the Parquet codec keeps the default 8 MiB window, which its bytes (`parquetout.Recipe`) depend on, so a pool handed round in turn warms every state; output does not depend on the state (`EncodeAll` resets it). |
| Writer admission | `parquetout.EncoderBytes(schema)`: the 8 MiB row group, a 1 MiB page buffer per column, 2.5× the key at its cap and 4× each column with `schema.Spec.MaxBytes`, and one codec state (12 MiB once a page passes one zstd block, else 2 MiB); current schema prices are below | the 12 MiB state, the 1 MiB column buffer and the 2.5× and 4× factors are measurements against klauspost/compress v1.19.1 (v1.20.3, which the seekparquet module requires, passes the heap gate and writes the same pages; not remeasured) and parquet-go v0.32.0 (live heap after GC across 24 pages a writer, random values at the longest lengths: one S3 writer at 1,024-byte keys 24.5 MiB, four 25.6 each; the filesystem schema with 4,096-byte keys and link targets 48 each, all with zstd's lower encoder memory), not values derived from the libraries: remeasure on an upgrade of either, and keep `TestWriterAdmissionCoversHeap` passing. The module's page intake (`WritePage`) borrows the values from the rows and copies nothing, so the price is the one swath's own writer had; remeasured on it (2026-11-08, `SWATH_HEAP_GATES=1`): one S3 writer 24.2 of 31.5 MiB, four 64.4 of 126, four local writers 137.4 of 244 with rows made a page at a time and 219.5 with all rows kept live (10 %). The price holds a margin over the measurements: four S3 writers 84 MiB of 126 priced (33 %); four local writers with binary 4,096-byte keys and link targets, 24 pages each, about 170 of 244 with rows made a page at a time and 216 with all 800 MB of rows kept live beside them, every admitted codec state warmed (12 %, the worst case measured). The margin is measured, not derived, and `TestWriterAdmissionCoversHeap` exercises it: both local arms run in the gate. A value is held to its column's `MaxBytes` (`schema.ErrValueTooLong`), so the widths priced are the widths written. |
| Column parallelism | a page's columns may be encoded on several goroutines | used when fewer parts than encoders are open, so a one-part run uses every core. Output is byte-identical to serial encoding (tested). |
| Key length | per schema (`schema.Info.MaxKeyBytes`): 1,024 bytes for S3 (contracts §6; a longer key is the typed `schema.ErrKeyTooLong`), no cap for filesystem paths | the key's column-index bounds are exact at any length (above); `TestKeyLimits` covers both. |
| `sorting_columns` | every row group records `key` ascending (standard Parquet metadata) | SeekParquet RG-1; generic readers learn the order without the stamps; metadata only, rows already arrive sorted. |
| `created_by` | `swath-go version <release version or devel> (build <revision>[-dirty])`, from the build itself (`internal/buildinfo`: the version and revision `build.sh` links in, else the toolchain's VCS stamp; shortened to 12 hex digits) | bytes are identical within a build; across builds only the recipe (`parquetout.Recipe`, `swath-go/parquet-r3`: r2 was format 2's stamps and shortened key bounds) is promised. A part a resume reproduces (claimed, never recorded) keeps the claimed part's own `created_by` and columns. |

## Writer buffers and admission

`parquetout.EncoderBytes` calculates the following allowances, including the key column. The page
buffer is 1 MiB per column; these are calculated admission prices, not process-memory bounds.

| Schema | Columns including key | Page buffers | Total per encoder |
|---|---:|---:|---:|
| S3 current objects | 9 | 9 MiB | 31.5 MiB |
| S3 versions | 12 | 12 MiB | 34.5 MiB |
| GCS current objects | 11 | 11 MiB | 33.5 MiB |
| GCS generations | 12 | 12 MiB | 34.5 MiB |
| Azure Blob current objects | 9 | 9 MiB | 36.5 MiB |
| Local entries | 15 | 15 MiB | 61 MiB |

Each total adds the 8 MiB row group, 2.5 times a 1,024-row key page at the schema's key cap,
4 times a 1,024-row page of any columns with `Spec.MaxBytes`, and one codec state. Local keys
have no schema cap and are priced at 4,096 bytes, with a 4,096-byte link target. Azure keys
are priced at 3,072 bytes; S3 and GCS at 1,024 bytes. All these schemas price a 12 MiB codec
state. The measurement dates and margins are in the writer-admission row above.

Resource admission limits the default encoder count; an explicit `--encoders N` bypasses its
memory and descriptor limits, with a warning on a shortfall. At the default 128 MiB allowance,
S3 current objects admit 4 encoders and GCS current objects 3, subject to cores and descriptors.
After listing, the allowance adds 1 MiB per worker: at 64 workers the calculated counts are
6 for S3 and 5 for GCS; at 256 workers, 12 and 11. Explicit counts are never raised. See
[encoder admission](ordering.md#the-budget) for the runtime transition and report fields.

## Writer cost

300,000 rows of the test schema, one part, best of four alternating runs, one shared Xeon Platinum 8581C host
(2026-11-08): through `WritePage` 1,235 thousand rows/s with one column goroutine against 1,303 for swath's own
writer (−5 %), 1,907 against 2,166 with four (−12 %); through the module's row intake (`WriteRows`, which
checks and copies every row) it was 803 and 914. What remains is the module's key check (KEY-3, each key
compared with the one before it, on the page's goroutine before the columns go parallel). The digest is not
the cost: SHA-256 hashes the encoded bytes, about 1 % of the profile. The shape suite's Parquet arms show no difference beyond runner noise ([measurements](../measurements.md#the-seekparquet-modules-part-writer-2026-11-08)).

## Byte identity

A build writes byte-identical parts for the same rows on machines that take the same encoder path (the
exception for parts closed by time is in [contracts.md](contracts.md#execution-and-recovery)). There are two
such paths, and the architecture alone does not decide which one a machine takes:

- **amd64 with AVX2**: parquet-go v0.32.0 encodes dictionary indexes (the RLE/bit-packed hybrid) with AVX2
  kernels. The one that decides where a bit-packed run ends (`encodeInt32IndexEqual8ContiguousAVX2`)
  broadcasts the first index of each 128-bit lane (`VPSHUFD` on a YMM register). So it also stops at a group
  of eight indexes whose halves are each uniform, such as four 3s followed by four 5s.
- **Every other platform** (arm64, amd64 without AVX2, a build with the `purego` tag) uses parquet-go's
  generic code, or on amd64 its SSE kernel. These stop only at a group of eight equal indexes.

Both encodings are valid and decode to the same values. Where a dictionary-encoded column meets such a group,
its pages, and so the chunk sizes, offsets and part digest, differ between the two paths. The rows do not. In the
golden scenarios neither do the part boundaries, row counts, statistics or the key column's index (the key is
`DELTA_BYTE_ARRAY`), but that is not promised: the router calibrates the limit of later parts from the first
part's encoded size (`router.go`), and a row group closes on parquet-go's encoded-size estimate (`writer.go`),
so a size difference near either threshold moves boundaries, and with them counts, statistics and the key index. zstd adds no difference: klauspost/compress's encoder has assembly
only for match lengths, which return the same values, and the `noasm` tag leaves the bytes unchanged.
`TestFooterOnlyChange` pins a golden for each path (#305). Pinning the generic path everywhere would take the
`purego` tag, which slows every parquet-go kernel, so swath does not do it.

What depends on it: a resume that must reproduce a claimed part byte for byte ([durability.md](durability.md))
refuses with `ErrAmbiguousTail` if it runs on the other path and the part's pages differ, even though the
source is unchanged. It fails loudly and never writes wrong data. Gates and baselines that compare digests compare
runs on one path.

## Run reports

Every dataset carries one run report, `run.json` (`listing.Report`, `report_version` 3); `--report-format json`
prints the same type, also for `swath du`, `swath diag count` or a failure, and `--report FILE` writes it to FILE when the attempt
ends (also for a resume refused before it runs). Its schema has one owner, "Run report" in
[../cli.md](../cli.md#run-report) (pinned by `internal/listing/testdata/report-schema.golden`). It is
operational: written after the commit (a failure to write it is a warning, the dataset stands) and never part of
acceptance, which reads the manifest's coverage ([../dataset.md](../dataset.md#the-swath-extension)).

## Footer stamps

Every part carries SeekParquet's five stamps (STAMP-1), equal to the manifest's: `seekparquet.format_version=0`,
`seekparquet.draft_revision=1`, `seekparquet.schema_id`, `seekparquet.key_column=key` and
`seekparquet.key_unique` (`true`, or `false` for a schema whose keys repeat, `schema.Info.Groups`). Within-file order is the standard `sorting_columns` (key, ascending) on every row group. Order
across parts, exact bounds and completeness are the manifest's; nothing in a footer says where a part sits
in the dataset. `TestFooterOnlyChange` pins the digests of the data pages, page indexes and footer of a frozen
golden (written by the 2026-11-05 layout) for its rows and requires them unchanged apart from the stamps and the
bounds of keys over 1,024 bytes. That is shown for those rows only: the switch to SeekParquet raised
klauspost/compress from v1.19.1 to v1.20.3, whose zstd encoder may choose its entropy tables differently on
other data of more than one block, so parts of builds before and after need not be byte-identical in general
(the writer recipe, `swath-go/parquet-r3`, keeps their resumes apart).

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
(`TestRouterRefusesPartPastLimit`); a segment refused there stops waiting for that publication when the run is canceled, and gives its encoder slot back (`TestSegmentPastLimitCancels`). `dataset.PartOrdinal` refuses a six-digit name.

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

**Group mode** (`schema.Info.Groups`: a versions listing). A part never ends inside a key's rows: the journal
records its last key as published, and a resume lists after it. A cut the size target, `MaxRows` or the interval
asks for is latched (`full`, with its reason) and taken before the first row of a greater key, or at `Close`;
rows that continue the part's last key join it (`Router.DeferredRows`), so a part can pass its targets by one
key's history. The cut counters count cuts taken, not asked for. The router keeps its own copy of the last
routed key (never a view of a page). A pinned part (a claimed part reproduced on resume) that reaches its
recorded row count while the next row repeats its last key fails with `ErrAmbiguousTail` before anything is
acknowledged: a correct cold run never cut there, so the listing is not the one the part was claimed from. The
writer accepts repeated keys and refuses a descending one; pages and row groups may split a key's rows
(`TestGroupedRowCutsWaitForTheGroup`, `TestGroupedIntervalWaitsForTheGroup`, `TestGroupedPinnedAmbiguousTail`).

## Publication gate and order

Order is checked at three places, each before the step it protects:

- **Every row**, in the order stage (`order.Run`'s stream-wide guard). Keys are strictly ascending across the
  whole run for a schema with unique keys, and nondecreasing for a versions schema, whose keys repeat on
  consecutive rows. On a resume, every key is strictly after the last published key (an exclusive boundary).
  No sink, Parquet or other, receives a descending key, or a repeated key under a unique schema, even from a
  provider that breaks its ordering contract at a range boundary.
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
  manifest.json                 SeekParquet: version, schema id, key, ordered parts with exact bounds; swath's
                                source, scope and coverage under extensions.swath; each part's sha256 (the commit)
  run.json                      the run report (operational, written after the commit)
  error.json                    the outcome of a run that did not complete (removed when one completes)
  .swath/                       private workspace: lock, journal, parts/ (unpublished), spill/, progress.json
```

The manifest's fields and the reader's acceptance rule are in [../dataset.md](../dataset.md#the-swath-extension).

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

- **SeekParquet readers** (the Go module `github.com/varveio/seekparquet/go`, which `swath-replay` uses) open the
  dataset by its manifest and stamps and seek through the key column's page index; the CI conformance step
  checks this writer's datasets against the format's reference validator ([../testing.md](../testing.md#seekparquet-conformance)).
- **The downstream reader** dispatches on the manifest's format, accepts whole-bucket coverage from the
  manifest's swath extension, and reads the SeekParquet stamps and each schema id's columns
  ([../upgrade.md](../upgrade.md#what-a-reader-of-the-java-layout-changes)); the recorded probe
  ([measurements](../measurements.md#downstream-reader-compatibility)) predates SeekParquet.
- **swath's Java reader** does not open these datasets, by design (no sort stamps, no reserved columns).
- **DuckDB** (`verify.py`) reads any part; nothing depends on it beyond standard Parquet.

## Schemas

Schemas are data (`schema.Schema[P]`: name, id, the key type, then columns with type, required flag, an
accessor of the payload `P` and an encoding preference: `Delta` writes DELTA_BYTE_ARRAY, used for `etag`). Each
producer defines its schema beside its code. All producers bind their accessors to the specification
`go/reader` registers for the schema id (`schema.Bind`, `reader.Schema`), which owns column order, names,
types, nullability and key settings. This includes Azure's `reader.AzblobObjectsV1` binding in
`internal/source/azblob/schema.go`; Azure keeps its accessors, `Delta` preferences, logical and heap accounting,
and codec beside the provider. `azblob` `TestSchemaBinding` checks its reader contract and field behavior.
`listing` `TestProducerSchemasGolden` pins the five S3, GCS and local schemas. The Parquet node is built from the bound
columns; `parquetout` `TestFooterMatchesSchema` checks the footer against a test schema with one column of
every type. `schema.NonEmpty` adapts a string accessor so the empty string is null; providers choose where
that interpretation applies.

### Version roles

`reader.SchemaSpec.Versions` (`reader.VersionSpec`) declares the version ID, exactly one of latest or
deleted-at, and an optional delete-marker column. `schema.Bind` copies the declaration into
`schema.Info.Versions` and checks it against the producer accessors when the package loads.
`schema.BindVersions` derives `ID`, `Latest` and optional `DeleteMarker` accessors: IDs are strings or signed
64-bit integers formatted in decimal; latest is a required boolean or the absence of a nullable deleted-at
timestamp. A present zero timestamp means deleted. Latest and delete-marker flags must be required booleans
with accessors; missing, repeated or mistyped role columns are refused. A schema with roles must group keys.
`Info.Groups` is the ordering property; grouped rows need not be versions.

`registry.Bind` resolves these accessors once when the provider opens, and panics on a malformed producer
declaration before the listing creates a journal, dataset or text file. Generic lister consumers—text
output, version counts and per-prefix totals—share this interpretation. They do not infer roles from names
such as `is_latest`, `generation` or `time_deleted`. The reader module's typed decoders still bind named
columns for their specific row contracts. `registry` `TestVersionRolesFollowDeclaration` and `listing`
`TestVersionReportFollowsDeclaration` place contradictory conventional columns beside declared roles;
`TestVersionRolesRefusedBeforeOutput` in `listing` checks refusal before output creation.

The Parquet schema name is `swath_listing` for every schema. The schema ids, their grammar
(`swath/<row-contract>/vN`) and their row contracts (columns, types, nullability, key encoding) are in
[../dataset.md](../dataset.md#schema-registry).

How the writer meets the key contract: `key` is a required BYTE_ARRAY with the STRING annotation only where the
producer guarantees UTF-8 (the object stores' keys). Filesystem keys and link targets are unannotated, written
exactly as the kernel returned them, never skipped, replaced or normalized. Manifest bounds are written in
base64 (`min_key_base64`, `max_key_base64`); readers compare the decoded bytes (`dataset.Key`). S3 ETags lose one
pair of surrounding quotes; Azure ETags are kept as listed. In the object-store schemas a key ending in `/` is an
object like any other: delimiter prefixes are derived by readers, never stored. In `swath/local-entries/v1` a
key ending in `/` is a directory.
