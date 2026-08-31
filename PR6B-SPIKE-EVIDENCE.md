# PR 6b container decision spike evidence

Date: 2026-08-31. Branch/base: `spike/pr6b-avro-container` at `c5560c9` before
this spike commit. This is measurement-only code in `swath-core`'s test source set; no
production class or production-scoped dependency is changed.

This corrected record incorporates the independent read-only gate at
`../swath-notes/notes/reviews/2026-08-31-pr6b-spike-opus.md` and the hands-on
re-measurement at
`../swath-notes/notes/reviews/2026-08-31-pr6b-deepdive-opus.md`. The corrected
measurements were made on `spike/pr6b-avro-deepdive`; this branch's spike code remains
unchanged.

## Result

The Avro OCF candidate works as a sequential container, but **sync-marker-only integrity
is not acceptable**. The current `PageBlock` bytes have no checksum of their own. A
test flips one bit in an LZ4-packed payload: the custom PageRun frame rejects it at its
CRC32C check, while OCF accepts the record and the Avro reader emits changed row data
without an exception. OCF sync markers recover the next block's alignment, but they do
not authenticate the damaged block and production cannot skip that block without losing
rows. The result was independently verified on four Avro read shapes. Adding the needed
CRC32C costs about 0.8 ms over a 25.5 MB merge, or about 0.3%, but requires more custom
format code.

Against the owner's condition—choose **(b) if it removes more custom format code and
improves debuggability**—the recommendation remains **choose (a), the stripped custom
frame**. The decisive axes are integrity, LOC, and debuggability. Performance is
explicitly not an argument for (a): warm Avro merge overhead is at most 5.8% for the
spike-shaped reader and 1.6% for the production-shaped routing-first raw-block variant.

The brief's approximately 200-SLOC post-PR-6a custom frame is an estimate, not a
measurement; the mechanical count must wait for PR 6a. The Avro candidate is about 312
production-shaped SLOC as spiked. Variants that reduce warm merge overhead to 1.6% are
about 360–400 SLOC because their hand-written record decoder and OCF block walk are
themselves custom format code, before the required CRC32C is added. On the available
evidence, (b) does not remove more custom format code, and it fails that condition harder
as it approaches performance parity. The post-PR-6a mechanical diff remains necessary to
replace the estimated custom magnitude with a measured one.

Avro improves generic envelope inspection: official `avro-tools` shows the schema,
codec, metadata, and record count, and sync positions are easy to enumerate. It does not
interpret the opaque `PageBlock` payload or understand the custom seal and checksum
conventions without swath-specific code, while this base already has `dump-run`.
Moreover, `avro-tools` 1.11.5 cannot run on JDK 25 because its Hadoop path adapter calls
a removed JDK API; the official tool needs a different JDK than swath builds and ships
on. The debuggability gain is therefore real but too narrow to satisfy the condition.

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

The original harness is runnable with:

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
Corrected retention measurements cover both just-opened writers and writers after one
real page. Per-flush CPU and allocated bytes are summed from `ThreadMXBean` counters on
the N worker threads and divided by N. Both full flush paths are warmed first. The
payload column is the shared serialized `PageBlock` allocation; the remaining allocation
identifies the container shape.

The original merge samples each ran once in a fresh child JVM with `-Xms128m -Xmx2g`.
That protocol put cold Avro schema-parser and Jackson class initialization inside the
timed region, so it measures one-shot child-JVM startup, not steady-state merge cost. The
corrected merge protocol repeats the complete header-pass-plus-K=8-decode-and-merge path
inside the same JVM and reports the third-run steady-state medians. Arm order was reversed
in one round to check page-cache and ordering bias.

Host: a KVM Google Cloud VM with 8 vCPUs (4 cores / 2 threads), Intel Xeon Platinum 8581C
at 2.30 GHz, 15 GiB RAM, no swap, and ext4 local storage. The build uses Eclipse Temurin
JDK 25.0.4+7-LTS. The host was not CPU-isolated, so these are comparative observations,
not release performance claims.

## 1. Writer cost during listing

Each flush writes 4,000 rows in four LZ4-packed pages. The original warmed flush
measurements were:

| Arm | Open segments | CPU/flush (ms) | Allocated/flush (bytes) | Shared PageBlock bytes | Container allocation (bytes) |
|---|---:|---:|---:|---:|---:|
| custom | 8 | 1.041 | 188,475 | 109,459 | 79,016 |
| Avro OCF | 8 | 1.092 | 220,883 | 109,459 | 111,424 |
| custom | 64 | 0.710 | 188,371 | 109,459 | 78,912 |
| Avro OCF | 64 | 0.722 | 220,887 | 109,459 | 111,428 |

Avro adds about 32.5 KiB per flush through `GenericRecord`, `ByteBuffer`, datum-encoder,
and OCF block-buffer objects. CPU was close after warmup (Avro +4.9% at 8 and +1.7% at
64). The custom flush also builds the extension PR 6a is expected to remove and performs
page repack checks, so this comparison is conservative toward Avro.

The original heap figures measured untuned, idle Avro writers and therefore did not
describe the structural steady-state floor. The corrected retained-heap measurements are:

| Shape | 8 open, just opened | 8 open, after 1 page | 64 open, just opened | 64 open, after 1 page |
|---|---:|---:|---:|---:|
| custom | 1,561 B | 570 B | 538 B | 539 B |
| Avro, `syncInterval=64` | 10,055 B | 64,858 B | 9,998 B | 64,239 B |
| Avro, `syncInterval=32768` | 50,902 B | 51,443 B | 50,879 B | 51,520 B |

Tuning drops the idle Avro floor to about 10 KiB, but after one real page the best
observed structural floor is about 51 KiB per open writer versus about 0.54 KiB custom:
roughly 95×, or about 3.2 MiB at 64 opens. OCF must buffer a complete block before it can
write the block-length prefix, so this floor is structural rather than tunable. The
custom heap arm models a retained low-level encoder even though current production opens
and closes that encoder within each flush; it is an Avro-shaped comparison, not a claim
about current listing retention.

## 2. Torn-file and corruption behavior

| Damage | Custom PageRun | Avro OCF + required seal |
|---|---|---|
| Truncate in header | Rejected on open | Rejected on open |
| Truncate in first page block | Rejected on open because the trailer is absent/torn | OCF decode/header scan fails |
| Truncate exactly after a complete page block | Rejected at open because the trailer is absent, before any row is handed off | OCF reaches clean EOF and rejects only after reading every preceding record because the final `SEAL` is missing |
| Damage first block, ask to resync from inside it | No resync convention; CRC rejects the page | `DataFileReader.sync` lands on the following `PAGE` block and exposes its expected min key |
| One-bit mutation in LZ4 PageBlock payload | Per-record CRC32C mismatch before PageBlock handoff | OCF framing and seal remain valid; reader emits row data different from the original |

Both integrity findings were independently verified across four Avro read shapes: the
spike's `GenericRecord` reader, raw-block iteration, routing-first raw-block iteration,
and the seeking header pass. The seeking header pass is silent by construction because
it skips the payload. Resync is useful for diagnosis and alignment discovery only. It
cannot make a production listing complete after a damaged page, because continuing at
the next marker creates an undetectable logical gap unless the run is abandoned and
rebuilt.

An explicit CRC32C over the exact serialized `PageBlock` bytes fixes payload integrity.
Its measured cost was 0.809 ms over 1,000 page bodies totaling 25,465,371 bytes, about
0.3% of a warm merge. It is cheap in CPU but adds a custom field, writer logic, reader
verification, and corruption reporting. The custom body arm already performs this CRC
verification for every record while no Avro arm verifies payload integrity, and the
custom arm still wins the warm comparison; performance is conservative toward Avro in
that respect.

## 3. Sequential header pass + K=8 decode/merge

Every sample produced exactly 1,000,000 rows and digest
`-5792676755096495795` in all arms.

The corrected steady-state medians are the third complete run in the same JVM:

| Arm | Wall (ms) | Header (ms) | Body + merge (ms) | Peak RSS (KiB) | Difference vs custom |
|---|---:|---:|---:|---:|---:|
| custom | **273.8** | 5.39 | 267.9 | 258,088 | — |
| Avro, spike shape | 289.7 | 10.68 | 277.3 | 275,244 | **+5.8%** |
| Avro, raw-block reader | 282.4 | 8.74 | 271.1 | 254,940 | **+3.1%** |
| Avro, routing-first raw-block reader (`avro-raw2`) | 278.2 | **2.89** | 273.4 | 265,636 | **+1.6%** |

The routing-first schema places routing fields before the opaque page and uses the OCF
block length to seek over the payload. Its 2.89 ms header pass is 1.9× faster than the
custom frame's 5.39 ms pass, but it requires a hand-written OCF block walk. The spike
shape instead streams all 25 MB because its payload precedes routing metadata; projection
avoids decoding that payload but cannot avoid reading it.

Performance is not a reason to reject Avro: warm overhead is 1.6–5.8%, and the custom arm
performs the CRC32C work the Avro arms omit. Warm peak RSS does not support a general Avro
penalty claim because the raw-block arm is slightly below custom while the other Avro
shapes are above it.

### Historical aside: what a one-shot child JVM measures

The original fresh-JVM table is retained only to document the measurement artifact:

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

The apparent +57.2% is not a merge penalty. The first Avro schema parse in a fresh JVM
takes 291–312 ms versus 0.27–0.39 ms warm; total fixed Avro bootstrap is about 342 ms
versus about 6 ms custom. That approximately 336 ms difference explains essentially the
entire cold gap. Nothing in production loads Avro today, so adoption would add roughly
300 ms of startup latency once per process. Finalization occurs in the long-lived listing
process after segment writes would already have loaded Avro, so that startup cost does
not belong in merge throughput.

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

SLOC means nonblank, noncomment Java source lines. The custom figure below is the spike
brief's estimate; it has not been mechanically measured because PR 6a is not in this
worktree. The Avro counts are measured or projected from working spike variants.

| Item | SLOC | Treatment |
|---|---:|---|
| Post-PR-6a stripped custom frame | ~200 | Brief estimate; mechanical diff waits for PR 6a |
| OCF candidate container, actual spike file | 365 | Writer, reader, seal, resync, inspector, helpers |
| OCF production-shaped projection, spike reader | ~312 | Keeps schema, writer, projected pass, reader, seal, validation, and durability |
| OCF production-shaped fast variants | ~360–400 | Adds hand-written record decode and OCF block walk; about 399 SLOC in the deep-dive subset |
| OCF focused tests | 230 | Added test code |
| Measurement harness | 501 | Spike-only; not production candidate |

The earlier "+112 net SLOC" was not a valid measured comparison and is withdrawn. The
approximately 312-SLOC spike projection already exceeds the estimated approximately
200-SLOC custom target, and the variants that reach +1.6% warm overhead cost more because
their decoder and block walk are custom format code. Production integration concerns and
the required CRC32C add further Avro-side code. Thus the condition "removes more custom
format code" still fails on the available evidence, and fails harder the faster Avro
gets. The exact magnitude remains the one unmeasured part of the decision and must come
from a mechanical diff against what PR 6a actually leaves.

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

The official tool improves generic metadata and record-count inspection, but the packed
page remains opaque bytes and `SEAL` and CRC32C are swath conventions rather than OCF
guarantees. `avro-tools` 1.11.5 cannot run on the project's JDK 25 because its Hadoop path
adapter calls a removed JDK API; the documented Gradle task requires the separately
installed JDK 21. The OCF writer/reader and swath tests themselves still compile and run
on JDK 25.

## Validation provenance

The original spike completed:

- `./gradlew spotlessApply`
- `./gradlew :swath-core:test --tests 'io.varve.swath.sort.AvroPageRunContainerTest'`
- `./gradlew :swath-core:pr6bSpike -PspikeArgs='measure build/pr6b-spike 1000000'`
- `./gradlew :swath-core:pr6bAvroTools` and the `count` invocation shown above
- `./gradlew build -PnoIntegration`

The deep-dive correction completed in-JVM phase-split measurements across the original
and production-shaped readers, writer-heap measurements before and after appending a
page, a CRC32C cost probe, cold-bootstrap attribution, and integrity tests on all four
Avro read shapes. Both reviews independently reproduced the disk bytes, row count,
digest, integrity result, and the cold measurement shape.

## Integrity decision for (b)

**Reject (b) as specified with sync-marker-only integrity.** If Avro is reconsidered, add
an explicit CRC32C field over the exact serialized `PageBlock` bytes (or put an equivalent
checksum inside `PageBlock`) and verify it before parsing or handing off the block. Keep
the final seal for truncation/completeness; checksum and seal solve different failures.
Do not treat sync resync as permission to continue a production merge after corruption.

## What to remeasure after PR 6a lands

The (a) arm here is the base branch's existing writer and still writes the trailer
extension. After PR 6a lands, rerun the harness against its final sequential writer and
reader: bytes on disk without the 255,080-byte extension; 8/64-open retained heap before
and after a real page; flush CPU/allocation; header-pass-plus-merge wall/RSS; and the
exact production SLOC deleted by choosing Avro. Rerun the torn-file tests as well,
although removing an unused extension should not change the custom frame's header,
record CRC, or sealed-tail behavior.

Merge measurements must use repeated runs inside one JVM, or an explicit warmup before
the timed sample—never a one-shot child JVM. The mechanical LOC diff against what PR 6a
actually leaves is the one unmeasured piece of the decision; replace the brief's
approximately 200-line estimate with that result.
