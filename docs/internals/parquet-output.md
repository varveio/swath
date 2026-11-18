# Parquet output

What `internal/parquetout` and `internal/dataset` write, and why. Each choice lists what depends on it. The dataset
is a [SeekParquet](https://github.com/varveio/seekparquet) dataset, format 1 draft revision 1 (the format's
`spec/format-1.md`; the swath dataset profile it carries is [../dataset.md](../dataset.md); what changed from the Java tool's format 1 is in [../upgrade.md](https://github.com/varveio/swath/blob/a13a5d21/docs/upgrade.md#seekparquet)); swath's own
guarantees are in [contracts.md](contracts.md#dataset-guarantees-readers-rely-on-these). This page says how the
Go writer meets them.

The format's mechanisms are the [seekparquet Go module](https://github.com/varveio/seekparquet)'s, at the
commit `go.mod` pins (a pseudo-version of its `main` until it releases): the part writer (`write.NewPart`:
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
| Other integers | `DELTA_BINARY_PACKED` | parquet-mr writes a dictionary and switches to delta mid-chunk when it overflows; parquet-go can only fall back to PLAIN. On buckets with few distinct timestamps (noaa-normals-pds) that leaves Go's `last_modified` about 40 % larger than Java's, the main part of a 3–7 % dataset size gap ([measurements](measurements.md#output-identity-with-the-java-tool)). |
| Booleans | `PLAIN` | |
| Pages | v2, 1024 rows or 4 MiB of keys, whichever comes first (only a page of keys averaging past the 4,096 bytes admission prices, in a local tree near or past PATH_MAX, reaches it), page-header statistics off | served geometry: point lookups decode one 1024-row page (SeekParquet GEO-1 recommends at most 1,024 key rows a page). The column index carries page bounds; header statistics would duplicate them. |
| Column index bounds | exact for every column: each page's minimum and maximum (the module's writer, no shortening) | Parquet's bounds, and SeekParquet RG-5, require a true upper bound. parquet-go shortens a long maximum by incrementing its last kept byte, which leaves one whose kept bytes are all `0xFF` below the value. Exact bounds therefore matter for long filesystem paths and link targets. `TestKeyLimits` (a 1,025-byte `0xFF` link target) and `TestFooterOnlyChange` check exact bounds. |
| Row groups | byte target 8 MiB on the writer's `Size()` estimate, checked at page boundaries; ceiling 1,000,000 rows | served geometry; the estimate is the encoded payload, not a heap bound. |
| Codec | zstd, klauspost `SpeedFastest` (`parquetout.pageLevel`) with entropy coding of all-literal blocks enabled | pure Go (`CGO_ENABLED=0`). The default wrapper skips entropy coding on all-literal blocks, which loses most of the gain on hex ETags. `SpeedFastest` since `swath/parquet-r4` (`SpeedDefault` before): page compression is a large share of a sorted run's tail CPU, and the fastest level spends 15 to 18 % less of it a page for 0.2 to 1.1 % more bytes, the difference in the key and `last_modified` columns (the ETags compress alike); on the whole aws-public-blockchain replay, 3.5 to 5 % less CPU an object and 6 to 8 % less tail for 0.3 % more bytes ([measurements](measurements.md#parquet-pages-at-zstd-speedfastest-2026-11-11)). |
| Codec states | one pool per router, shared by its writers: single-state zstd encoders made as concurrent page compressions need them, up to the encoders admitted (raised by `Router.Admit`), reused last-in first-out | a state keeps the history of the largest page it compressed (about 10.6 MiB once a page passed one 128 KiB zstd block, measured at `SpeedDefault`'s 8 MiB window; whose match tables are larger than `SpeedFastest`'s, with zstd's lower encoder memory, which changes how the encoder allocates its history and scratch, not its window or matching: `TestLowerEncoderMemIsByteIdentical` compares 33 raw inputs, three patterns at eleven sizes up to 9 MiB, with and without it; it is evidence, not proof for every frame or reuse history, and exercises no schema). The 10.6 MiB and the 12 MiB priced are measured residency and allowance, not a bound the library guarantees. The spill frames' zstd encoders are separate and set apart: `order.spillEncoderOptions`, a 1 MiB window with the same lower-memory mode, about 1.5 MiB each ([ordering](ordering.md)); the Parquet codec keeps `SpeedFastest`'s default 4 MiB window and 64 KiB blocks (the 8 MiB window and 128 KiB block above are `SpeedDefault`'s, measured under r3), which its bytes (`parquetout.Recipe`) depend on, so a pool handed round in turn warms every state; output does not depend on the state (`EncodeAll` resets it). |
| Writer admission | `parquetout.EncoderBytes(schema)`: the 8 MiB row group, a 1 MiB page buffer per column, 2.5× the key at its cap and 4× each column with `schema.Spec.MaxBytes`, and one codec state (12 MiB once a page passes one zstd block, else 2 MiB); current schema prices are below | the 12 MiB state, the 1 MiB column buffer and the 2.5× and 4× factors are measurements against klauspost/compress v1.19.1 (v1.20.3 passed the heap gate and wrote the same pages, not remeasured; the modules now select v1.20.4) and parquet-go v0.32.0 (live heap after GC across 24 pages a writer, random values at the longest lengths: one S3 writer at 1,024-byte keys 24.5 MiB, four 25.6 each; the filesystem schema with 4,096-byte keys and link targets 48 each, all with zstd's lower encoder memory), not values derived from the libraries: remeasure on an upgrade of either, and keep `TestWriterAdmissionCoversHeap` passing. The module's page intake (`WritePage`) borrows the values from the rows and copies nothing, so the price is the one swath's own writer had; remeasured on it (2026-11-08, `SWATH_HEAP_GATES=1`): one S3 writer 24.2 of 31.5 MiB, four 64.4 of 126, four local writers 137.4 of 244 with rows made a page at a time and 219.5 with all rows kept live (10 %). The price holds a margin over the measurements: four S3 writers 84 MiB of 126 priced (33 %); four local writers with binary 4,096-byte keys and link targets, 24 pages each, about 170 of 244 with rows made a page at a time and 216 with all 800 MB of rows kept live beside them, every admitted codec state warmed (12 %, the worst case measured). The margin is measured, not derived, and `TestWriterAdmissionCoversHeap` exercises it: both local arms run in the gate. A value is held to its column's `MaxBytes` (`schema.ErrValueTooLong`), so the widths priced are the widths written. |
| Column parallelism | a page's columns may be encoded on several goroutines | used when fewer parts than encoders are open, so a one-part run uses every core. Output is byte-identical to serial encoding (tested). |
| Key length | per schema (`schema.Info.MaxKeyBytes`): 1,024 bytes for S3 (contracts §6; a longer key is the typed `schema.ErrKeyTooLong`), no schema cap for filesystem paths (the local provider leaves out a key past 32 KiB) | the key's column-index bounds are exact at any length (above); `TestKeyLimits` covers both. |
| `sorting_columns` | every row group records `key` ascending (standard Parquet metadata) | SeekParquet RG-1; generic readers learn the order without the stamps; metadata only, rows already arrive sorted. |
| `created_by` | `swath version <release version or devel> (build <revision>[-dirty])`, from the build itself (`internal/buildinfo`: the version and revision `build.sh` links in, else the toolchain's VCS stamp; shortened to 12 hex digits) | bytes are identical within a build; across builds only the recipe (`parquetout.Recipe`, `swath/parquet-r5`: r4 cut a page at 1,024 rows only, not also at 4 MiB of keys; r3 compressed pages at zstd `SpeedDefault`; r2 was format 2's stamps and shortened key bounds) is promised. A part a resume reproduces (claimed, never recorded) keeps the claimed part's own `created_by` and columns. |

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
memory and descriptor limits, with a warning on a shortfall. The allowance is its own, 1/16 of the memory
found (at most 1 GiB), not `--sort-buffer`; a dataset written to a bucket adds its upload buffer (a GCS chunk, an S3 part or an Azure block) to each
encoder's price ([encoder admission](ordering.md#the-budget)). At a 128 MiB allowance, written
locally, S3 current objects (31.5 MiB a writer) admit 4 encoders and GCS current objects (33.5 MiB)
3, subject to cores and descriptors. After listing, the allowance adds 1 MiB per worker: at 64
workers the calculated counts are 6 for S3 and 5 for GCS; at 256 workers, 12 and 11. Written to a
bucket with an 8 MiB upload buffer (39.5 and 41.5 MiB an encoder), both admit 3 at start, 4 after
listing at 64 workers and 9 at 256. Explicit counts are never raised. See
[encoder admission](ordering.md#the-budget) for the runtime transition and report fields.

## Writer cost

300,000 rows of the test schema, one part, best of four alternating runs, one shared Xeon Platinum 8581C host
(2026-11-08): through `WritePage` 1,235 thousand rows/s with one column goroutine against 1,303 for swath's own
writer (−5 %), 1,907 against 2,166 with four (−12 %); through the module's row intake (`WriteRows`, which
checks and copies every row) it was 803 and 914. What remains is the module's key check (KEY-3, each key
compared with the one before it, on the page's goroutine before the columns go parallel). The digest is not
the cost: SHA-256 hashes the encoded bytes, about 1 % of the profile. The shape suite's Parquet arms show no difference beyond runner noise ([measurements](measurements.md#the-seekparquet-modules-part-writer-2026-11-08)).

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

Every dataset carries one run report, `run.json` (`inventory.Report`, `report_version` 1); `--report -`
prints the same type, also for `swath du`, `swath diag count` or a failure, and `--report FILE` writes it to FILE when the attempt
ends (also for a resume refused before it runs). Its schema has one owner, "Run report" in
[../reference/run-report.md](../reference/run-report.md) (pinned by `internal/inventory/testdata/report-schema.golden`). It is
operational: written after the commit (a failure to write it is a warning, the dataset stands) and never part of
the dataset, whose coverage the manifest records ([../dataset.md](../dataset.md#the-swath-extension)).

## Footer stamps

Every part carries SeekParquet's five stamps (STAMP-1), equal to the manifest's: `seekparquet.format_version=0`,
`seekparquet.draft_revision=1`, `seekparquet.schema_id`, `seekparquet.key_column=key` and
`seekparquet.key_unique` (`true`, or `false` for a schema whose keys repeat, `schema.Info.Groups`). Within-file order is the standard `sorting_columns` (key, ascending) on every row group. Order
across parts, exact bounds and completeness are the manifest's; nothing in a footer says where a part sits
in the dataset. `TestFooterOnlyChange` pins the digests of the data pages, page indexes and footer of a frozen
golden (written by the 2026-11-05 layout; since `swath/parquet-r4` its data pages, offset indexes and footer
digests are those of the same pages at zstd `SpeedFastest`, the value and key indexes unchanged) for its rows
and requires them unchanged apart from the stamps and the bounds of keys over 1,024 bytes. That is shown for those rows only: the switch to SeekParquet raised
klauspost/compress from v1.19.1 to v1.20.3, whose zstd encoder may choose its entropy tables differently on
other data of more than one block, so parts of builds before and after need not be byte-identical in general
(the writer recipe, now `swath/parquet-r5`, keeps their resumes apart).

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

The same rows therefore produce the same parts, byte for byte, for any `--workers` and `--encoders`, unless the
part interval cuts (below).

After listing, the order stage may write the rest as segments: consecutive parts opened in order and written
by several goroutines at once (`Router.Plan`, `OpenSegment`; [ordering](ordering.md#segments-parts-written-in-parallel-after-listing)).
It cuts them with the router's own rule once part 0 has frozen the ratio, so the parts are the same; the
interval closes no part from then on. Publication stays in ordinal order with the adjacency check, and the
abort closes every segment's queue unpublished.

**The part limit.** A dataset has at most 100,000 parts (`part-00000` to `part-99999`: ordinals have five digits, `dataset.MaxParts`); at the defaults (64 MiB parts, about 30 bytes per row) it takes about 200 billion objects or 35 days of uninterrupted 30-second time cuts; in practice a small `--part-size` or `--part-max-rows` reaches it. The router refuses to open part 100,000
(`dataset.ErrPartLimit`, checked when it would open the part) after the parts before it are published, so the
journal's last record is part 99,999; the command exits 1, `cause` `part_limit`, not resumable
(`TestRouterRefusesPartPastLimit`); a segment refused there stops waiting for that publication when the run is canceled, and gives its encoder slot back (`TestSegmentPastLimitCancels`). `dataset.PartOrdinal` refuses a six-digit name.

**The part interval** (`inventory.Limits.PartInterval`, default 30 s; `parquetout.Options.Interval`) is a
rotation target: a part open that long with rows is closed although not full (the router's ticker cuts only an
existing part whose boundary no resume fixed), and a journaled run publishes it in ordinal order behind the parts
before it. It bounds how long rows wait in an open part, not the age of the recovery point: the closed part
still encodes and syncs, and while no rows reach the output no part is open to close. Rows listed past the
frontier (buffered, spilled, or in parts not yet recorded) are relisted whatever the interval
([durability.md](durability.md)). The
default is Java's rotation interval, 30 s; a cut part can be far below its target size. The order stage hands on its batch before the drainer waits on the frontier, so the
open part holds every row the walk has reached. A cut by time makes that run's boundaries depend on timing:
the report counts such cuts (`diagnostics.resources.part_time_cuts`); with none, or with the interval off (zero;
`--part-interval` refuses a negative value), boundaries depend only on the rows and the byte-identity guarantees above hold
(`TestIntervalClosesAnAgedPart`, `TestPartIntervalBoundsRecoveryPointAge`: a 2 s trickle keeps the recovery
point under 260 ms old at a 200 ms interval, against 1.96 s by size only). A resume that must reproduce a
claimed part ([durability.md](durability.md#the-journal)) closes that ordinal at the claimed part's row count
(`Options.Cut`) whatever the interval. The command sets it with
`--part-interval` ([../commands/swath-list.md](../commands/swath-list.md)). Each part is
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
run reports follow, then the cut file; one that cannot be written is a warning, and the dataset stands. A failure or
cancellation removes the unpublished parts and writes no manifest.

**The cut file** ([dataset.md](../dataset.md#the-cut-file)). Each writer keeps its part's page ends
(`dataset.PageEnd`: the last key of every page it hands the module, owned, and the part's rows through it,
dropping one when the next page starts with its key), at most one key per 1,024 rows of the part being written
(or per 4 MiB of its keys, which only a local listing's reach, and a local listing is never resumed):
about 1/1,024 of its keys' bytes plus 24 bytes a page, a few hundred KiB for a 64 MiB part, held per open part.
As each part is published, in ordinal order (segments' included), the router adds them to the cut set
(`dataset.Cuts`; each part's encoder in turn, after the part before it; once the abort is seen, a part whose
predecessor released its turn publishes nothing and adds no cuts, while a publication already under way
finishes, `TestAbortKeepsThePublicationTurn`), which keeps the cuts at the current R and doubles R in place as soon as they exceed the file's
bounds (the cuts at 2R are those at R where the rows reach a multiple of 2R since the previous cut), so it holds
at most 262,144 cuts and 16 MiB of lines. After the commit and the run report the parts a resume retained are
read back from their key page index (`parquetout.PageEnds`: a page end at every multiple of 1,024 rows and the
part's last row, the module's shorter pages inside a writer's page skipped; a bucket dataset's parts through the
object version their records name), eight parts at a time and taken in order, at most eight parts' page ends
held (`retainedCuts`), and the run's cuts joined after theirs, so a resumed dataset's file is an uninterrupted
run's. The reads stop when the run is stopped; a bucket's also when their own bound runs out (the aftermath's
length, `FinalizeTimeout`, after the orphan cleanup and `run.json`), and the run commits without the file,
warning once. A local file is replaced durably beside the manifest; a bucket's is created only if absent. The
manifest and the parts do not depend on it. A source that takes no prior (not `source.Seeded`: a local tree) gets
no cut file (`writeCuts` returns at once); an `--overwrite` retires the replaced dataset's with its other entries.

What the file holds follows from that. A cut falls only where a 1,024-row key page the writer filled ends,
never where the next page continues a key's versions. With R rows a cut, it is the first such page end where the
dataset's rows reach each multiple of R; R is the least power of two (1: every page end) that keeps the file
within its bounds, 262,144 lines and 16 MiB. With keys of about 60 bytes that is every page end up to about 250
million rows, then one cut about every 8,192 rows at 2 billion and every 131,072 at 30 billion, about 16 MB at
most; longer keys double R sooner. A key holding a line feed cannot be a line: its rows count toward the next
line, and a dataset whose last key holds one has no cut file. When `--seed-from DIR` takes the file as its prior,
and when it reads the parts' key page index instead, is the range engine's warm start
([range-engine.md](range-engine.md), "Warm start").

```text
out/
  data/part-00000.parquet ...   dense, zero-based, five digits (at most 100,000 parts); name order == ordinal
                                order == key order (the manifest's part list is the order); parts strictly disjoint
  manifest.json                 SeekParquet: version, schema id, key, ordered parts with exact bounds; swath's
                                source, scope and coverage under extensions.swath; each part's sha256 (the commit)
  cuts.txt                      the cut file, for the next --seed-from (advisory, written last; not for a local tree)
  run.json                      the run report (operational, written after the commit)
  error.json                    the outcome of a run that did not complete (removed when one completes)
  .swath/                       private workspace: lock, journal, parts/ (unpublished), spill/, progress.json
```

The manifest's fields are in [../dataset.md](../dataset.md#the-swath-extension).

Output safety: the source root is validated first; the output may not be the root or an ancestor of it (by path
and by dev/inode); a non-empty output is refused unless `--overwrite`, which replaces only a previous dataset of
this tool, outside the root, removing only its own entries. An output inside the root is skipped by the walk
(`source.Excluder`), as a text file, its temporary and its scratch directory are.

**`--overwrite` of a committed dataset keeps it until the replacement commits.** Parts are published early
under the same names in `data/`, which the old manifest would then name with different bytes, so the
replacement does not list into the directory itself: under the output's lock, held for the whole run, it lists
into `.swath/next/`, a dataset directory of its own (`dataset.NextDir`, with its own lock, journal and parts),
and readers keep reading the previous dataset. Its commit installs it, in the same commit step
(`dataset.InstallNext`): every entry of the previous dataset moves into `.swath/next/retiring/`, the manifest
first, and both directories are synced before any other entry moves; `retiring/` is then renamed `retired/`;
the new `data/` moves into the output, synced, and its manifest last, which is the installation. Every step is
a synced rename, so a crash anywhere leaves a state the next `swath resume DIR` or `--overwrite` finishes
first (`inventory.InstallReplacement`, `TestInstallNextFinishesAPartialRetirement`), and the leftovers of an installed one
(a crash before their removal) are removed the same way. The path holds no committed dataset between the first
and the last of those renames. The part paths are reused, so a reader that read the old manifest before the
installation can find its parts missing or replaced by the new dataset's: nothing is promised to a reader
during it. The run report and cut file are written to the output after it; the retired dataset and the
replacement's workspace are removed last. A replacement
that fails or is interrupted before its commit leaves the previous dataset whole; one with a journal waits in `.swath/next/` for
`swath resume DIR` (`swath show DIR` names it; a new `--overwrite` starts it over), and one without is removed
(`TestReplaceKeepsPreviousUntilCommit`, `TestReplaceInstallationCrash`, `TestReplaceFailureLeavesPrevious`).
`DIR/error.json` holds a failed replacement's outcome beside the previous dataset. An installation refused
while the previous dataset's entries move aside (Windows: a reader holds a part) is undone: when every entry
comes back, the previous dataset is readable again and the replacement stays pending for `swath resume DIR`.
When an entry cannot come back, and when an installation fails after every entry is aside (which is not
undone), the path holds no dataset until `swath resume DIR` or `--overwrite` finishes the installation, and
the error says so (`TestReplaceRefusedInstallation`; the Windows item of [the journal](durability.md#the-journal)). An output that holds an
interrupted run instead (no manifest) is replaced as before: its entries are removed, the commit record first,
before anything is listed (`TestOverwriteRemovesTheCommitFirst`).

## What readers rely on

- **SeekParquet readers** (the Go module `github.com/varveio/seekparquet/go`, which `swath-replay` uses) open the
  dataset by its manifest and stamps and seek through the key column's page index; the CI conformance step
  checks this writer's datasets against the format's reference validator ([testing.md](testing.md#seekparquet-conformance)).
- **The downstream reader** dispatches on the manifest's format, accepts whole-bucket coverage from the
  manifest's swath extension, and reads the SeekParquet stamps and each schema id's columns
  ([../upgrade.md](https://github.com/varveio/swath/blob/a13a5d21/docs/upgrade.md#what-a-reader-of-the-java-layout-changes)); the recorded probe
  ([measurements](measurements.md#downstream-reader-compatibility)) predates SeekParquet.
- **swath's Java reader** does not open these datasets, by design (no sort stamps, no reserved columns).
- **DuckDB** (`verify.py`) reads any part; nothing depends on it beyond standard Parquet.

## Validating a dataset

The user pages check a copied dataset's parts against the manifest's SHA-256 digests and read its coverage
with `swath show` ([query.md](../query.md#verify-a-dataset)). The format, key order and pages take one of two
validators, run by hand on a local copy.

**SeekParquet's validator** ([the "Validating" section of its `go/README.md`](https://github.com/varveio/seekparquet)).
From the root of a SeekParquet checkout that includes `cmd/seekparquet` and supports this dataset's format
(format version 0, draft revision 1): swath pins `github.com/varveio/seekparquet/go v0.2.0` in `go.mod`, the
release that fixed draft revision 1; use that release (tag `go/v0.2.0`) or a later one with a matching format.

```bash
go -C go run ./cmd/seekparquet validate --level digest /path/to/dataset && \
go -C go run ./cmd/seekparquet validate --level full /path/to/dataset
```

`digest` streams each part with a declared SHA-256 and compares its bytes with the manifest's digest;
swath declares one for every part. `full` separately checks the format, key order and decoded pages
(including the structural and profile levels); it does not check digests or swath's extension and
schema semantics. Neither proves that the listing covers the source intended. The validator exits 0 when all
requested levels complete, 1 when any level fails, and 2 when validation is incomplete, cannot run, or the
arguments are invalid. With `go run`, a nonzero validator status is printed as `exit status N`; the `go`
command itself exits 1. CI runs the same validator levels ([testing.md](testing.md#seekparquet-conformance)).

**`verify.py`**, from the repository root with DuckDB installed: `python3 verify.py /path/to/dataset`. It checks
S3, GCS, Azure Blob and local datasets independently of the Go reader: swath metadata, schema-specific rules,
row counts, key order and the manifest's part bounds, footer stamps and part SHA-256 digests; for Azure also
the `azure` source kind and every part's column types and nullability, including UTC microsecond timestamps.
It expects the producer's flat `data/part-NNNNN.parquet` layout and `run.json`, so it is not a general
validator for a bucket dataset copied locally with its nested run/attempt part paths. A dataset with no parts
(an empty source) fails it: DuckDB finds no file to read in `data/`, and the script stops with that error,
exit 1. Its checks and optional reference comparison: [testing.md](testing.md#verifypy).

## Schemas

Schemas are data (`schema.Schema[P]`: name, id, the key type, then columns with type, required flag, an
accessor of the payload `P` and an encoding preference: `Delta` writes DELTA_BYTE_ARRAY, used for `etag`). Each
producer defines its schema beside its code. All producers bind their accessors to the specification
`reader` registers for the schema id (`schema.Bind`, `reader.Schema`), which owns column order, names,
types, nullability and key settings. This includes Azure's `reader.AzureObjectsV1` binding in
`internal/source/azblob/schema.go`; Azure keeps its accessors, `Delta` preferences, logical and heap accounting,
and codec beside the provider. `azblob` `TestSchemaBinding` checks its reader contract and field behavior.
`inventory` `TestProducerSchemasGolden` pins the five S3, GCS and local schemas. The Parquet node is built from the bound
columns; `parquetout` `TestFooterMatchesSchema` checks the footer against a test schema with one column of
every type. `schema.NonEmpty` adapts a string accessor so the empty string is null; providers choose where
that interpretation applies.

### Version roles

`reader.SchemaSpec.Versions` (`reader.VersionSpec`) declares the version ID, the latest column and an
optional delete-marker column. `schema.Bind` copies the declaration into
`schema.Info.Versions` and checks it against the producer accessors when the package loads.
`schema.BindVersions` derives `ID`, `Latest` and optional `DeleteMarker` accessors: IDs are strings or signed
64-bit integers formatted in decimal. Latest and delete-marker flags must be required booleans
with accessors; missing, repeated or mistyped role columns are refused. A schema with roles must group keys.

A producer also declares its storage-class column, `schema.Info.StorageClass` (`storage_class` for S3, GCS and Azure
Blob, none for a local tree): `--storage-class` filters on it, and `schema.Bind` refuses
a declaration that names no string column. A local tree declares its containers the same way,
`schema.Info.Containers` (the rows whose `kind` is `dir`): `schema.Bind` derives `Schema.Container` from it,
which `swath du` counts in `dirs` rather than as objects, and refuses a `Container` set directly;
`schema.Info.Links` (the rows whose `kind` is `symlink`) are the rows `swath du --from-dataset` refuses as SOURCE,
since a live listing follows a link to a directory. Its `schema.Info.NamesTier` (`names`) is the metadata tier, recorded in a manifest's `collected.metadata`, whose rows
carry no link identity: `swath du --from-dataset` counts such a dataset's hard-linked files under every name, as
the live run's `CheckTotals` does. Each provider
lists its bound schemas' `Info` in `registry.Factory.Schemas`, so `registry.SchemaInfo` finds a dataset's
declarations by its schema id: `swath du --from-dataset` binds the same totals and filters to a payload that reads
the dataset's columns (`registry.DatasetTotals`). It seeks to SOURCE's prefix and reads the key and only the
columns the totals and the filters need, in parallel scans, once every part's footer and columns are checked
against the dataset's schema (a dataset that breaks it is refused, exit 1).
`Info.Groups` is the ordering property; grouped rows need not be versions.

`registry.Bind` resolves these accessors once when the provider opens, and panics on a malformed producer
declaration before the listing creates a journal, dataset or text file. Generic lister consumers—text
output, version counts and per-prefix totals—share this interpretation. They do not infer roles from names
such as `is_latest`, `generation` or `deleted_at`. The reader module's typed decoders still bind named
columns for their specific row contracts. `registry` `TestVersionRolesFollowDeclaration` and `inventory`
`TestVersionReportFollowsDeclaration` place contradictory conventional columns beside declared roles;
`TestVersionRolesRefusedBeforeOutput` in `inventory` checks refusal before output creation.

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
