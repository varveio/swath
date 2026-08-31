# PR 6b container decision spike evidence

Date: 2026-08-31. Branch/base: `spike/pr6b-avro-container` at `c5560c9` before
this spike commit. This is measurement-only code in `swath-core`'s test source set; no
production class or production-scoped dependency is changed.

## Result

The Avro OCF candidate works as a sequential container, but **sync-marker-only integrity
is not acceptable**. The current `PageBlock` bytes have no checksum of their own. A
test flips one bit in an LZ4-packed payload: the custom PageRun frame rejects it at its
CRC32C check, while OCF accepts the record and the Avro reader emits changed row data
without an exception. OCF sync markers recover the next block's alignment, but they do
not authenticate the damaged block and production cannot skip that block without losing
rows.

Against the owner's condition—choose **(b) if it removes more custom format code and
improves debuggability**—the recommendation is **choose (a), the stripped custom frame,
on the present evidence**. Relative to the brief's approximately 200-line post-PR-6a
custom frame, the production-shaped portion of (b) is approximately 312 SLOC (about
+112, before adding the checksum that the integrity result requires), so it does not
remove more custom format code. Avro improves generic envelope inspection: official
`avro-tools` shows the schema, codec, metadata, and record count, and sync positions are
easy to enumerate. It does not interpret the opaque `PageBlock` payload or understand the
custom seal convention without swath-specific code, while this base already has
`dump-run`; therefore the observed debuggability gain is too narrow to satisfy the second
half of the condition.

## Implemented candidate

The test-only OCF writer and reader use Avro 1.11.5 from the version catalog:

- OCF codec is `null`; each `PageBlock` remains packed once with swath's LZ4 codec.
- Each `PAGE` record is forced into one OCF block with `sync()`.
- A projected routing record duplicates min key, max key, count, and raw payload length,
  so the benchmark can perform a header pass without materializing a `PageBlock` object.
- A final `SEAL` record carries exact page count, entry count, and last key. The reader
  requires it to be the last record and cross-checks it against every preceding page.
- The reader also cross-checks duplicated routing metadata against the serialized
  `PageBlock` header and enforces strictly disjoint, ascending OBJECTS page ranges.
- Successful close forces the file and parent directory, matching the custom writer's
  durability work.

The harness is runnable with:

```bash
./gradlew :swath-core:pr6bSpike \
  -PspikeArgs='measure build/pr6b-spike 1000000'
```

The focused correctness suite is:

```bash
./gradlew :swath-core:test \
  --tests 'io.varve.swath.sort.AvroPageRunContainerTest'
```

## Protocol and hardware

The large corpus has 1,000,000 sorted object rows split round-robin across K=8 sorted
segments, so the output merge genuinely interleaves all inputs. Pages contain 1,000 rows.
Keys are deterministic hierarchical paths averaging exactly 109 bytes; object sizes span
512 bytes through approximately 64 MiB; timestamps, 32-hex-character etags, storage
classes, owner IDs, and optional checksum fields are deterministic and realistic. Each
page is packed once with `PageCodec.LZ4`, then the exact same `PageBlock` objects are sent
to the existing listing `PageRunSegmentWriter.flush` and the OCF writer. Avro-level
compression is `null`.

The listing-cost case uses four prepacked 1,000-row pages per segment at 8 and 64
simultaneously active segments. Retained heap is `(used heap after GC with N open
writers - baseline after GC) / N`; it is a coarse HotSpot heap-delta measurement.
Per-flush CPU and allocated bytes are summed from `ThreadMXBean` counters on the N worker
threads and divided by N. Both full flush paths are warmed first. The payload column is
the shared serialized `PageBlock` allocation; the remaining allocation identifies the
container shape.

Merge samples run in fresh child JVMs with `-Xms128m -Xmx2g`. The timed region includes a
sequential projected-header pass over every segment, reopening the files, K=8 body decode,
and `StreamingMerger` consumption into a count and key digest. RSS is Linux `VmHWM` from
`/proc/self/status`. Samples are interleaved custom/Avro to reduce page-cache order bias.

Host: a KVM Google Cloud VM with 8 vCPUs (4 cores / 2 threads), Intel Xeon Platinum 8581C
at 2.30 GHz, 15 GiB RAM, no swap, and ext4 local storage. The build uses Eclipse Temurin
JDK 25.0.4+7-LTS. The host was not CPU-isolated, so these are comparative observations,
not release performance claims.

## 1. Writer cost during listing

Each flush writes 4,000 rows in four LZ4-packed pages. Heap figures below are bytes per
retained open low-level writer; CPU and allocation are per completed, durable flush.

| Arm | Open segments | Heap/open (bytes) | CPU/flush (ms) | Allocated/flush (bytes) | Shared PageBlock bytes | Container allocation (bytes) |
|---|---:|---:|---:|---:|---:|---:|
| custom | 8 | 1,364 | 1.041 | 188,475 | 109,459 | 79,016 |
| Avro OCF | 8 | 89,834 | 1.092 | 220,883 | 109,459 | 111,424 |
| custom | 64 | 522 | 0.710 | 188,371 | 109,459 | 78,912 |
| Avro OCF | 64 | 89,333 | 0.722 | 220,887 | 109,459 | 111,428 |

The stable allocation shape is one common serialized `byte[]` per page in both arms.
Avro adds about 32.5 KiB per flush through `GenericRecord`, `ByteBuffer`, datum-encoder,
and OCF block-buffer objects. Its open `DataFileWriter` retains about 87.5 KiB per segment
versus the custom encoder's sub-2-KiB coarse heap delta; at 64 opens that is approximately
5.45 MiB for Avro. CPU was close after warmup (Avro +4.9% at 8 and +1.7% at 64).

## 2. Torn-file and corruption behavior

| Damage | Custom PageRun | Avro OCF + required seal |
|---|---|---|
| Truncate in header | Rejected on open | Rejected on open |
| Truncate in first page block | Rejected because the trailer is absent/torn | OCF decode/header scan fails |
| Truncate exactly after a complete page block | Rejected because the trailer is absent | OCF framing reaches clean EOF, then swath reader rejects `missing final SEAL record` |
| Damage first block, ask to resync from inside it | No resync convention; CRC rejects the page | `DataFileReader.sync` lands on the following `PAGE` block and exposes its expected min key |
| One-bit mutation in LZ4 PageBlock payload | Per-record CRC32C mismatch before PageBlock handoff | OCF framing and seal remain valid; reader emits row data different from the original |

Resync is useful for diagnosis and alignment discovery only. It cannot make a production
listing complete after a damaged page, because continuing at the next marker creates an
undetectable logical gap unless the run is abandoned and rebuilt.

## 3. Sequential header pass + K=8 decode/merge

Every sample produced exactly 1,000,000 rows and digest
`-5792676755096495795` in both arms.

| Sample | Arm | Wall (ms) | Peak RSS (KiB) |
|---:|---|---:|---:|
| 1 | custom | 647.405 | 263,404 |
| 1 | Avro OCF | 935.880 | 274,516 |
| 2 | custom | 695.367 | 265,788 |
| 2 | Avro OCF | 1,043.019 | 245,960 |
| 3 | custom | 663.416 | 263,968 |
| 3 | Avro OCF | 1,084.080 | 243,784 |
| median | custom | 663.416 | 263,968 |
| median | Avro OCF | 1,043.019 | 245,960 |

Avro's median wall time is 57.2% higher. `VmHWM` varied enough across fresh JVMs that it
does not support an RSS penalty claim: Avro was higher in sample 1 and lower in samples 2
and 3. The extra OCF reader/schema work is visible in wall time; output identity is pinned
by count and digest.

## 4. Bytes on disk

| Arm | Bytes across 8 segments | Difference vs as-is custom |
|---|---:|---:|
| custom on this base | 25,730,651 | — |
| Avro OCF | 25,724,987 | -5,664 (-0.022%) |

The as-is custom files include 255,080 bytes of the listing trailer extension across the
eight segments. Subtracting only that extension gives 25,475,571 bytes for the sequential
custom frame; against that relevant post-PR-6a shape, Avro costs 249,416 extra bytes
(+0.979%). This spike deliberately did not reimplement a stripped custom writer.

## 5. LOC and debuggability

SLOC below means nonblank, noncomment Java source lines. The production projection removes
the spike-only resync/inspector/layout-return bookkeeping but keeps schema, writer,
projected header pass, reader, seal validation, metadata validation, and durability.

| Item | SLOC | Treatment |
|---|---:|---|
| Post-PR-6a stripped custom frame | ~200 | Deletion opportunity stated in the spike brief; PR 6a is not in this worktree |
| OCF candidate container, actual spike file | 365 | Writer, reader, seal, resync, inspector, helpers |
| OCF production-shaped projection | ~312 | Added production custom integration code |
| Net production format code vs owner's condition | **about +112** | 312 added minus about 200 deleted |
| OCF focused tests | 230 | Added test code |
| Measurement harness | 501 | Spike-only; not production candidate |

For context only, the current base's four format-owning files total 844 SLOC, but that is
not a valid deletion claim: much of `PageRunSegmentIo` is logical routing/ordering
validation that an Avro reader still needs, and PR 6a is expected to remove the seek/index
surface independently.

Official tool transcript (Avro tools is isolated from the benchmark classpath):

```text
$ ./gradlew :swath-core:pr6bAvroTools
avro.schema    {"type":"record","name":"PageRunFrame",...}
avro.codec     null
swath.page-codec       lz4
swath.format   swath-pageseg-avro-spike-v1

$ ./gradlew :swath-core:pr6bAvroTools \
    -PavroToolArgs='count build/pr6b-spike/avro-00.avro'
126
```

That count is 125 one-record `PAGE` blocks plus one `SEAL` block. The spike inspector uses
the Avro library's sync/block APIs to make the block convention visible:

```text
$ ./gradlew :swath-core:pr6bSpike \
    -PspikeArgs='inspect build/pr6b-spike/avro-00.avro'
metadata avro.codec=null
metadata swath.format=swath-pageseg-avro-spike-v1
metadata swath.page-codec=lz4
block 1 sync=26067 records=1 kind=PAGE entries=1000 pageBytes=25252
...
block 125 sync=3193782 records=1 kind=PAGE entries=1000 pageBytes=25254
block 126 sync=3193923 records=1 kind=SEAL totalRecords=125 totalEntries=125000
blocks=126 records=126
```

The official tool clearly improves generic metadata and record-count inspection. Two
limits matter: the packed page remains opaque bytes, and `SEAL` is a swath convention,
not an OCF guarantee. In addition, Avro tools 1.11.5's Hadoop path adapter calls a JDK API
removed in JDK 25, so the documented Gradle task runs the official tool on the installed
JDK 21; the OCF writer/reader and all swath tests still compile and run on JDK 25.

## Validation

- `./gradlew spotlessApply`
- `./gradlew :swath-core:test --tests 'io.varve.swath.sort.AvroPageRunContainerTest'`
- `./gradlew :swath-core:pr6bSpike -PspikeArgs='measure build/pr6b-spike 1000000'`
- `./gradlew :swath-core:pr6bAvroTools` and the `count` invocation shown above
- `./gradlew build -PnoIntegration`

All completed successfully on the host described above.

## Integrity decision for (b)

**Reject (b) as specified with sync-marker-only integrity.** If Avro is reconsidered, add
an explicit CRC32C field over the exact serialized `PageBlock` bytes (or put an equivalent
checksum inside `PageBlock`) and verify it before parsing or handing off the block. Keep
the final seal for truncation/completeness; checksum and seal solve different failures.
Do not treat sync resync as permission to continue a production merge after corruption.

## What to remeasure after PR 6a lands

The (a) arm here is the base branch's existing writer and still writes the trailer
extension. After PR 6a lands, rerun the same harness against its final sequential writer
and reader, specifically: bytes on disk without the 255,080-byte extension observed here;
8/64-open retained heap; flush CPU/allocation; three header-pass-plus-merge wall/RSS
samples; and the exact production SLOC deleted by choosing Avro. Replace the brief's
approximately 200-line estimate with a mechanical diff against PR 6a before making the
owner-condition decision. The torn-file tests should also be rerun, although removing an
unused extension should not change the custom frame's header, record CRC, or sealed-tail
behavior.
