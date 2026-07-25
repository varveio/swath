/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

/**
 * PROP guard for the parallel-range merge ({@code swath.sort.merge-parallelism > 1},
 * {@link ParallelRangeMerge} / {@link RangeFilteredStream}), guarding the CONTRACT it must uphold:
 *
 * <p><b>The core property (byte-exact equivalence to serial).</b> For the SAME input segments,
 * running the merge with {@code merge-parallelism=1} (the untouched serial path) and with
 * {@code merge-parallelism=R} (R &gt; 1) must produce output whose <em>content</em> is byte-exact:
 * the concatenation of the range parts, in filename (== key == range) order, is the identical
 * multiset of rows in the identical global order — same rows, globally ascending, no dropped row (no
 * gap), no duplicated row (no overlap), position-for-position equal to the serial output.
 *
 * <p>Independence from the serial path (non-tautology): the multiset is also checked against the
 * ORIGINAL input (independent ground truth, not the serial output), so "two identical bugs that both
 * drop the same row" cannot pass — a dropped/duplicated row fails the input multiset check regardless
 * of what serial did. The generators are constrained so every produced {@link ListEntry} round-trips
 * through the Parquet codec as an identity (see {@link #entry}), making that input-multiset equality
 * exact rather than up-to-canonicalization.
 *
 * <p><b>Byte-identity does NOT extend to distinct-payload ties.</b> The strict byte-exact
 * property below constrains the generator so any two <em>equal-comparing</em> rows (same
 * {@code key}+{@code version_id}+{@code row_type}, differing only in payload like {@code size}) are
 * BYTE-IDENTICAL. That is deliberate: when equal-comparing rows with
 * <em>distinct</em> payload are spread across different segments, the parallel path does NOT reproduce
 * the serial path's relative order of them (e.g. two {@code key=k001, version=null} objects of size 0
 * and 1 emerge 0-then-1 serially but 1-then-0 in parallel). Both are still valid global sorts (same
 * multiset, globally ascending) — the divergence is only in the order of rows the comparator calls
 * equal, because the k-way merge heap ({@link StreamingMerger}) is NOT stable across streams and the
 * per-range merge starts from different heap/fast-path state than the whole-keyspace serial merge.
 * This case only arises from duplicate/malformed data; live S3 listings have unique
 * {@code (key, version_id)} so no two rows ever compare equal. {@link
 * #parallelIsAValidCompleteSortWithDistinctPayloadTies} pins the achievable guarantee for that case
 * (valid, complete, ascending — but NOT byte-identical to serial), documenting current behavior so a
 * regression is caught; whether the concurrent merge must be made stable-by-stream is an owner call.
 *
 * <p>In-package so package-private seams ({@link SortTransform}, {@link SortConfig},
 * {@link SortedFileWriterFactory}, {@link SortStamp}) are reachable. jqwik has no {@code @TempDir}, so
 * each run creates and recursively deletes its own temp root; a deterministic {@code seed} makes every
 * failing case reproducible.
 */
class SortTransformParallelMergePropTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    /** Adversarial key shapes: dense adjacency, ties, dense clusters, and binary/prefix keys. */
    enum KeyStyle {
        /** {@code k000}..{@code k063}: dense, adjacent, heavy duplicates — boundaries land on real keys. */
        DENSE_SEQUENTIAL,
        /** Single letter a..e: far fewer distinct keys than R ⇒ few ranges, heavy equal-key runs. */
        SMALL_ALPHABET,
        /** {@code c<cluster>-<nnn>}: dense runs separated by cluster gaps — boundary-adjacent keys. */
        CLUSTERED,
        /** {@code 0x00}/{@code 0xFF} bytes and prefix relationships (empty key, prefixes) — S3 byte order. */
        BINARY_ADVERSARIAL
    }

    private static final byte[][] BIN_POOL = {
            new byte[]{},
            new byte[]{0},
            new byte[]{0, 0},
            new byte[]{0, (byte) 0xFF},
            new byte[]{(byte) 0xFF},
            new byte[]{(byte) 0xFF, 0},
            new byte[]{(byte) 0xFF, (byte) 0xFF},
    };

    // ---------------------------------------------------------------------
    // Core property: parallel concatenation is byte-exact to serial, for many
    // randomized (distribution × row-count × segment-count × R × seed) inputs.
    // ---------------------------------------------------------------------

    @Property(tries = 120)
    void parallelConcatenationIsByteExactToSerial(
            @ForAll @IntRange(min = 1, max = 6) int segmentCount,
            @ForAll @IntRange(min = 1, max = 320) int entryCount,
            @ForAll KeyStyle style,
            @ForAll @IntRange(min = 2, max = 6) int ranges,
            @ForAll long seed) throws IOException {
        assertParallelMatchesSerial(buildScenario(segmentCount, entryCount, style, seed, false),
                ranges, Long.MAX_VALUE, SortedFileWriterFactory.DEFAULT, true);
    }

    /**
     * Same core property, but each range ALSO rolls into multiple parts (a decorator forces a roll
     * every ~5 rows). Stresses intra-range file boundaries stacked on top of the inter-range
     * boundaries: filename order across the whole {@code part-00001..part-N} sequence must still be the
     * exact global sort, byte-exact to a serial run rolled the same way.
     */
    @Property(tries = 30)
    void rolledParallelConcatenationIsByteExactToSerial(
            @ForAll @IntRange(min = 1, max = 5) int segmentCount,
            @ForAll @IntRange(min = 1, max = 120) int entryCount,
            @ForAll KeyStyle style,
            @ForAll @IntRange(min = 2, max = 5) int ranges,
            @ForAll long seed) throws IOException {
        // dataSize() is decorated to 1000 bytes/row; a 5000-byte roll threshold ⇒ ~5 rows per file.
        assertParallelMatchesSerial(buildScenario(segmentCount, entryCount, style, seed, false),
                ranges, 5000L, new RollEveryNRows(SortedFileWriterFactory.DEFAULT), true);
    }

    /**
     * The achievable guarantee for the malformed/duplicate-data case the strict property excludes:
     * with DISTINCT-payload equal-comparing rows present (same key+version+row_type,
     * different {@code size}), the parallel concatenation is still a VALID, COMPLETE global sort —
     * exact input multiset (no dropped/duplicated row), globally ascending, total rows conserved — for
     * every R. It is NOT asserted byte-identical to serial, because the k-way merge is not stable
     * across streams (see the class doc's note on distinct-payload ties); this pins current behavior
     * so a regression that drops/duplicates a row, or breaks global order, is still caught even for
     * tie-heavy inputs.
     */
    @Property(tries = 60)
    void parallelIsAValidCompleteSortWithDistinctPayloadTies(
            @ForAll @IntRange(min = 1, max = 6) int segmentCount,
            @ForAll @IntRange(min = 1, max = 320) int entryCount,
            @ForAll KeyStyle style,
            @ForAll @IntRange(min = 2, max = 6) int ranges,
            @ForAll long seed) throws IOException {
        assertParallelMatchesSerial(buildScenario(segmentCount, entryCount, style, seed, true),
                ranges, Long.MAX_VALUE, SortedFileWriterFactory.DEFAULT, false);
    }

    // ---------------------------------------------------------------------
    // Adversarial boundary / degenerate examples.
    // ---------------------------------------------------------------------

    @Example
    void emptyInputPublishesOneEmptyFileForEveryR() throws IOException {
        for (int r : new int[]{2, 3, 5}) {
            assertParallelMatchesSerial(scenario(List.of(List.of())), r,
                    Long.MAX_VALUE, SortedFileWriterFactory.DEFAULT, true);
        }
    }

    @Example
    void singleRowInputIsByteExactForEveryR() throws IOException {
        for (int r : new int[]{2, 3, 5}) {
            assertParallelMatchesSerial(scenario(List.of(List.of(obj("only", (Integer) null)))), r,
                    Long.MAX_VALUE, SortedFileWriterFactory.DEFAULT, true);
        }
    }

    @Example
    void singleSegmentIsUnsplittableAndFallsBackToSerial() throws IOException {
        // One segment ⇒ one distinct sample first-key ⇒ boundaries() returns null ⇒ serial path.
        SortTransformResult parallel = assertParallelMatchesSerial(
                scenario(List.of(objs("a", "b", "c", "d"))), 4,
                Long.MAX_VALUE, SortedFileWriterFactory.DEFAULT, true);
        assertThat(parallel.finalFiles()).as("unsplittable ⇒ single serial file").hasSize(1);
    }

    @Example
    void allIdenticalKeysAreUnsplittableAndStillByteExact() throws IOException {
        // Every row shares one key across many segments: no split point exists ⇒ serial FALLBACK (the
        // parallel path runs the exact serial code), so it is byte-identical even with distinct-payload
        // ties — no cross-stream reordering happens when there is no split.
        List<List<ListEntry>> segs = List.of(
                List.of(obj("dup", 0), obj("dup", 1)),
                List.of(obj("dup", 2), dm("dup", "v1")),
                List.of(obj("dup", (Integer) null), obj("dup", 5)));
        SortTransformResult parallel = assertParallelMatchesSerial(scenario(segs), 5,
                Long.MAX_VALUE, SortedFileWriterFactory.DEFAULT, true);
        assertThat(parallel.finalFiles()).as("unsplittable ⇒ single serial file").hasSize(1);
    }

    @Example
    void fewerDistinctKeysThanRangesStillByteExact() throws IOException {
        // Two distinct keys, R=5 requested ⇒ at most 2 ranges; still a total, gap-free partition.
        assertParallelMatchesSerial(scenario(List.of(objs("a", "b"), objs("a", "b"), objs("a", "b"))),
                5, Long.MAX_VALUE, SortedFileWriterFactory.DEFAULT, true);
    }

    @Example
    void boundaryKeysAreOwnedByExactlyOneRange() throws IOException {
        // Distinct keys a..h spread one-per-segment so EACH is a sample first-key ⇒ with R=8 the
        // boundaries land exactly on real keys. A boundary key must appear exactly once in the output
        // (inclusive-lo / exclusive-hi): the input-multiset check would fail if one were dropped by
        // both sides of a boundary or duplicated into both ranges. All rows for one key live in ONE
        // segment (and so ONE range), so even the distinct-payload tie order is preserved (a single
        // stream keeps its own order) — assert full byte-exactness.
        List<List<ListEntry>> segs = new ArrayList<>();
        for (String k : List.of("a", "b", "c", "d", "e", "f", "g", "h")) {
            segs.add(List.of(obj(k, (Integer) null), obj(k, 0), dm(k, "v1")));
        }
        assertParallelMatchesSerial(scenario(segs), 8, Long.MAX_VALUE, SortedFileWriterFactory.DEFAULT, true);
    }

    // ---------------------------------------------------------------------
    // Non-tautology self-check: the comparison helpers actually REJECT a
    // dropped or duplicated boundary row (so a passing property is meaningful).
    // ---------------------------------------------------------------------

    @Example
    void theGuardRejectsADroppedRow() {
        List<ListEntry> serial = List.of(obj("a", 0), obj("b", 0), obj("c", 0));
        List<ListEntry> droppedBoundary = List.of(obj("a", 0), obj("c", 0));   // lost "b"
        assertThatThrownBy(() -> assertByteExactToSerial(serial, droppedBoundary))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> assertThat(droppedBoundary).containsExactlyInAnyOrderElementsOf(serial))
                .isInstanceOf(AssertionError.class);
    }

    @Example
    void theGuardRejectsADuplicatedBoundaryRow() {
        List<ListEntry> serial = List.of(obj("a", 0), obj("b", 0), obj("c", 0));
        // "b" leaked into both adjacent ranges (an inclusive-hi bug) ⇒ duplicated at the boundary.
        List<ListEntry> duplicatedBoundary = List.of(obj("a", 0), obj("b", 0), obj("b", 0), obj("c", 0));
        assertThatThrownBy(() -> assertByteExactToSerial(serial, duplicatedBoundary))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> assertThat(duplicatedBoundary).containsExactlyInAnyOrderElementsOf(serial))
                .isInstanceOf(AssertionError.class);
    }

    @Example
    void theGuardRejectsAnOutOfOrderConcatenation() {
        List<ListEntry> serial = List.of(obj("a", 0), obj("b", 0), obj("c", 0));
        List<ListEntry> swapped = List.of(obj("a", 0), obj("c", 0), obj("b", 0));   // ranges concatenated wrong
        assertThatThrownBy(() -> assertByteExactToSerial(serial, swapped))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> assertThat(swapped).isSortedAccordingTo(cmp))
                .isInstanceOf(AssertionError.class);
    }

    // ---------------------------------------------------------------------
    // Pins the CURRENT completeness-stamp behavior on the parallel path (range-local file_index, no
    // file marked final) so a regression is caught. This test documents the current limitation; it
    // does not establish the missing global completeness proof.
    // ---------------------------------------------------------------------

    @Example
    void parallelPartsCarryRangeLocalFileIndexAndNoFinalStamp_serialStampsProperly() throws IOException {
        List<List<ListEntry>> segs = List.of(objs("a", "d", "g"), objs("b", "e", "h"), objs("c", "f", "i"));
        SortedFileWriterFactory stamped =
                new SortedParquetWriterFactory(config(1, Long.MAX_VALUE), SortMode.OBJECTS);
        Path root = Files.createTempDirectory("prangeprop-stamp-");
        try {
            Scenario s = scenario(segs);
            // Serial baseline: the single file carries a PROPER stamp — file_index 1, file_final true.
            SortTransformResult serial = run(s, 1, Long.MAX_VALUE, root, "serial", stamped);
            assertThat(serial.finalFiles()).hasSize(1);
            SortStamp serialStamp = readStamp(serial.finalFiles().get(0));
            assertThat(serialStamp.fileIndex()).isEqualTo(1);
            assertThat(serialStamp.fileFinal()).as("serial single file is stamped final").isTrue();

            // Parallel path: 3 ranges ⇒ 3 parts. Current (documented-gap) behavior — each part is
            // range-LOCAL file_index 1 (NOT the global 1..N), and NO part is stamped final.
            SortTransformResult parallel = run(s, 3, Long.MAX_VALUE, root, "parallel", stamped);
            assertThat(parallel.finalFiles()).hasSize(3);
            for (Path part : parallel.finalFiles()) {
                SortStamp stamp = readStamp(part);
                assertThat(stamp.fileIndex())
                        .as("range-local file_index, not global 1..N")
                        .isEqualTo(1);
                assertThat(stamp.fileFinal())
                        .as("no parallel part is stamped final")
                        .isFalse();
            }
            // Content is still a correct global sort despite the stamp gap.
            assertThat(readKeys(parallel.finalFiles())).containsExactly("a", "b", "c", "d", "e", "f", "g", "h", "i");
        } finally {
            deleteRecursively(root);
        }
    }

    // =====================================================================
    // Harness
    // =====================================================================

    private record Scenario(List<List<ListEntry>> segments, List<ListEntry> allEntries) {
    }

    /**
     * Build a scenario from primitives + a deterministic seed (reproducible on any failure).
     * {@code distinctTies=false} makes any two equal-comparing rows BYTE-IDENTICAL (payload is a pure
     * function of the comparator identity) — the strict-byte-exact regime. {@code distinctTies=true}
     * gives equal-comparing rows DISTINCT payloads — the malformed-data regime, valid+complete but not
     * byte-identical to serial.
     */
    private Scenario buildScenario(int segmentCount, int entryCount, KeyStyle style, long seed,
                                   boolean distinctTies) {
        Random r = new Random(seed);
        List<List<ListEntry>> segs = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            segs.add(new ArrayList<>());
        }
        List<ListEntry> all = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            ListEntry e = distinctTies ? entryWithDistinctPayload(key(style, r), r) : entry(key(style, r), r);
            all.add(e);
            segs.get(r.nextInt(segmentCount)).add(e);
        }
        for (List<ListEntry> seg : segs) {
            seg.sort(cmp);   // each staging segment must be individually sorted
        }
        return new Scenario(segs, all);
    }

    /** A scenario from explicit segments (each is sorted defensively; flat multiset is the ground truth). */
    private Scenario scenario(List<List<ListEntry>> segments) {
        List<List<ListEntry>> sorted = new ArrayList<>();
        List<ListEntry> all = new ArrayList<>();
        for (List<ListEntry> seg : segments) {
            List<ListEntry> copy = new ArrayList<>(seg);
            copy.sort(cmp);
            sorted.add(copy);
            all.addAll(seg);
        }
        return new Scenario(sorted, all);
    }

    private byte[] key(KeyStyle style, Random r) {
        return switch (style) {
            case DENSE_SEQUENTIAL -> String.format("k%03d", r.nextInt(64)).getBytes(StandardCharsets.UTF_8);
            case SMALL_ALPHABET -> new byte[]{(byte) ('a' + r.nextInt(5))};
            case CLUSTERED -> String.format("c%d-%03d", r.nextInt(3), r.nextInt(8))
                    .getBytes(StandardCharsets.UTF_8);
            case BINARY_ADVERSARIAL -> BIN_POOL[r.nextInt(BIN_POOL.length)].clone();
        };
    }

    /**
     * A random {@link ListEntry} whose payload is a DETERMINISTIC function of its comparator identity
     * ({@code key}, {@code version}, {@code kind}), so any two equal-comparing rows are byte-identical
     * (true duplicates) — the strict-byte-exact regime. Round-trips through the codec as an identity
     * (so the input-multiset check is exact): an {@code ObjectEntry}'s {@code is_latest} is only built
     * true when it has a {@code versionId} (the one codec canonicalization), and {@code
     * last_modified=0} is the codec's absent-sentinel that decodes to 0.
     */
    private ListEntry entry(byte[] key, Random r) {
        int kind = r.nextInt(5);
        String version = r.nextInt(4) == 0 ? null : "v" + r.nextInt(3);
        int identity = tieIdentity(key, version);
        if (kind <= 2) {
            long size = Math.floorMod(identity, 3);
            boolean latest = version != null && (identity % 2 == 0);
            return new ObjectEntry(KeyBytes.of(key), size, 0L, null, null, version, latest,
                    null, null, null, null);
        }
        if (kind == 3) {
            return new DeleteMarkerEntry(KeyBytes.of(key), version, identity % 2 == 0, 0L, null);
        }
        return new CommonPrefixEntry(KeyBytes.of(key));
    }

    /**
     * As {@link #entry}, but the payload varies FREELY of the comparator identity — so two rows sharing
     * (key, version, row_type) can differ in {@code size}/{@code isLatest} (a genuine distinct-payload
     * tie, only reachable via malformed/duplicate data, §0.5). Still a codec identity round-trip.
     */
    private ListEntry entryWithDistinctPayload(byte[] key, Random r) {
        int kind = r.nextInt(5);
        String version = r.nextInt(4) == 0 ? null : "v" + r.nextInt(3);
        if (kind <= 2) {
            boolean latest = version != null && r.nextBoolean();
            long size = r.nextInt(3);   // free payload variance ⇒ distinct-payload equal-comparing ties
            return new ObjectEntry(KeyBytes.of(key), size, 0L, null, null, version, latest,
                    null, null, null, null);
        }
        if (kind == 3) {
            return new DeleteMarkerEntry(KeyBytes.of(key), version, r.nextBoolean(), 0L, null);
        }
        return new CommonPrefixEntry(KeyBytes.of(key));
    }

    /** A stable int derived from the comparator-relevant identity (key bytes + version). */
    private static int tieIdentity(byte[] key, String version) {
        return Arrays.hashCode(key) * 31 + (version == null ? 0 : version.hashCode());
    }

    /**
     * Run the SAME scenario serially (R=1) and in parallel (R), then assert the parallel concatenation
     * is byte-exact to serial in content and order, complete versus the ORIGINAL input, and named as an
     * ascending {@code part-0000N} sequence. Returns the parallel result for extra assertions.
     */
    private SortTransformResult assertParallelMatchesSerial(Scenario s, int ranges, long finalFileBytes,
                                                            SortedFileWriterFactory factory,
                                                            boolean requireByteExactToSerial)
            throws IOException {
        Path root = Files.createTempDirectory("prangeprop-");
        try {
            SortTransformResult serial = run(s, 1, finalFileBytes, root, "serial", factory);
            SortTransformResult parallel = run(s, ranges, finalFileBytes, root, "parallel", factory);

            List<ListEntry> input = s.allEntries();
            List<ListEntry> serialRows = readAll(serial.finalFiles());
            List<ListEntry> parallelRows = readAll(parallel.finalFiles());

            // Row-count conservation: total out == total in, both paths.
            assertThat(serial.totalRows()).as("serial rows out == rows in").isEqualTo(input.size());
            assertThat(parallel.totalRows()).as("parallel rows out == rows in").isEqualTo(input.size());
            assertThat(serialRows).hasSameSizeAs(input);
            assertThat(parallelRows).hasSameSizeAs(input);

            // Serial is a correct, complete sort of the input (independent ground truth).
            assertThat(serialRows).as("serial is globally ascending").isSortedAccordingTo(cmp);
            assertThat(serialRows).as("serial is the exact input multiset (no gap/no overlap)")
                    .containsExactlyInAnyOrderElementsOf(input);

            // Parallel is ALWAYS a valid, complete global sort: globally ascending + exact input
            // multiset (no dropped/duplicated row — the no-gap/no-overlap boundary guarantee).
            assertThat(parallelRows).as("parallel is globally ascending").isSortedAccordingTo(cmp);
            assertThat(parallelRows).as("parallel is the exact input multiset (no dropped/duplicated row)")
                    .containsExactlyInAnyOrderElementsOf(input);

            // Byte-exact to serial position-for-position — only where equal-comparing rows are
            // byte-identical (the strict regime); NOT asserted for distinct-payload ties.
            if (requireByteExactToSerial) {
                assertByteExactToSerial(serialRows, parallelRows);
            }

            assertSequentialPartNames(parallel.finalFiles());
            return parallel;
        } finally {
            deleteRecursively(root);
        }
    }

    /** The byte-exactness assertion: same size, globally ascending, and position-for-position equal. */
    private void assertByteExactToSerial(List<ListEntry> serial, List<ListEntry> parallel) {
        assertThat(parallel).as("parallel concat has the same row count as serial").hasSameSizeAs(serial);
        assertThat(parallel).as("parallel concat is globally ascending").isSortedAccordingTo(cmp);
        assertThat(parallel).as("parallel concat equals serial position-for-position (byte-exact content)")
                .containsExactlyElementsOf(serial);
    }

    private void assertSequentialPartNames(List<Path> files) {
        List<String> names = files.stream().map(p -> p.getFileName().toString()).toList();
        assertThat(names).as("final files enumerated in ascending name order").isSorted();
        for (int i = 0; i < names.size(); i++) {
            assertThat(names.get(i)).isEqualTo(String.format("part-%05d.parquet", i + 1));
        }
    }

    private SortTransformResult run(Scenario s, int parallelism, long finalFileBytes, Path root,
                                    String name, SortedFileWriterFactory factory) throws IOException {
        Path output = Files.createDirectories(root.resolve(name));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        List<Path> segs = new ArrayList<>();
        for (int i = 0; i < s.segments().size(); i++) {
            segs.add(writeSegment(staging, "seg-" + i + ".parquet", s.segments().get(i)));
        }
        SortTransform transform = new SortTransform(new SortRun(config(parallelism, finalFileBytes), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, factory));
        return transform.transform(segs, output, staging, PublishListener.NO_OP);
    }

    /** Large budgets/fan-in so neither path cascades — a single pass makes tie ordering deterministic. */
    private SortConfig config(int mergeParallelism, long finalFileBytes) {
        return SortConfigs.base().withFinalFileBytes(finalFileBytes).withMergeParallelism(mergeParallelism);
    }

    private Path writeSegment(Path dir, String name, List<ListEntry> sorted) throws IOException {
        Path path = dir.resolve(name);
        SegmentWriter writer = new SegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, 1L << 20);
        try (SortedCursor cursor = new InMemoryCursor(sorted, cmp, DuplicateHook.NO_OP)) {
            writer.writeIntermediate(cursor, path);
        }
        return path;
    }

    private List<ListEntry> readAll(List<Path> files) throws IOException {
        List<ListEntry> out = new ArrayList<>();
        for (Path f : files) {
            try (SegmentReader r = new SegmentReader(f)) {
                while (r.hasNext()) {
                    out.add(r.next());
                }
            }
        }
        return out;
    }

    private List<String> readKeys(List<Path> files) throws IOException {
        List<String> out = new ArrayList<>();
        for (ListEntry e : readAll(files)) {
            out.add(e.key().asString());
        }
        return out;
    }

    private static SortStamp readStamp(Path file) throws IOException {
        Optional<SortStamp> stamp = SortStamp.read(file);
        assertThat(stamp).as("stamped writer wrote a readable sort stamp for %s", file).isPresent();
        return stamp.get();
    }

    // --- entry builders ---

    private static ObjectEntry obj(String key, Integer size) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), size == null ? 1L : size, 0L, null, null,
                null, false, null, null, null, null);
    }

    private static ObjectEntry obj(String key, String versionId) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, versionId, versionId != null,
                null, null, null, null);
    }

    private static DeleteMarkerEntry dm(String key, String versionId) {
        return new DeleteMarkerEntry(KeyBytes.ofUtf8(key), versionId, true, 0L, null);
    }

    private List<ListEntry> objs(String... keys) {
        List<ListEntry> out = new ArrayList<>();
        for (String k : keys) {
            out.add(obj(k, (Integer) null));
        }
        return out;
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup of a test temp tree
                }
            });
        }
    }

    /**
     * Decorates a {@link SortedFileWriterFactory} so {@link SortedFileWriter#dataSize()} reports a
     * fixed 1000 bytes per row — deterministic, codec-independent rolling for the rolled property (a
     * {@code final-file-bytes=5000} threshold ⇒ a roll every ~5 rows), so the parallel merge's
     * intra-range multi-part path is exercised without depending on real Parquet buffer accounting.
     */
    private record RollEveryNRows(SortedFileWriterFactory delegate) implements SortedFileWriterFactory {
        @Override
        public SortedFileWriter create(Path path, int fileIndex) throws IOException {
            SortedFileWriter inner = delegate.create(path, fileIndex);
            return new SortedFileWriter() {
                @Override
                public void write(ListEntry e) throws IOException {
                    inner.write(e);
                }

                @Override
                public long rows() {
                    return inner.rows();
                }

                @Override
                public long dataSize() {
                    return inner.rows() * 1000L;
                }

                @Override
                public void markFinal() {
                    inner.markFinal();
                }

                @Override
                public void close() throws IOException {
                    inner.close();
                }
            };
        }
    }
}
