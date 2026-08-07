/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

/**
 * PROP guard for the parallel range merge over <b>page-run</b> staging — the live listing lane's
 * format, and therefore the only one that decides whether {@code swath.sort.merge-parallelism}
 * does anything on a real run.
 *
 * <p>{@link SortTransformParallelMergePropTest} is the sibling guard over columnar Parquet staging.
 * The two paths differ in every mechanism that could lose a row — a page skip
 * ({@link RangeScopedPageFrontier}) instead of a row-group skip, {@link PageAwareMerger}'s
 * decode-free page-whole fast path instead of the entry-typed {@link StreamingMerger}, the
 * {@code [lo, hi)} trim applied ABOVE the merge ({@link RangeFilteredCursor}) instead of around each
 * input, and page-run cascade intermediates instead of Parquet ones — so the Parquet guard says
 * nothing about this one.
 *
 * <p>The core property is the same and is checked against TWO independent oracles: byte-exact
 * equivalence to the serial merge of the same segments, and the exact input multiset (so two
 * symmetric bugs that both drop the same row cannot pass).
 */
class SortTransformPageRunParallelMergePropTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    /** Adversarial key shapes: heavy duplicate runs, tiny alphabets, and S3 byte-order extremes. */
    enum KeyStyle {
        DENSE_SEQUENTIAL,
        SMALL_ALPHABET,
        CLUSTERED,
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
    // Core property
    // ---------------------------------------------------------------------

    @Property(tries = 120)
    void pageRunParallelConcatenationIsByteExactToSerial(
            @ForAll @IntRange(min = 1, max = 6) int segmentCount,
            @ForAll @IntRange(min = 1, max = 320) int entryCount,
            @ForAll KeyStyle style,
            @ForAll @IntRange(min = 2, max = 6) int ranges,
            @ForAll long seed) throws IOException {
        assertParallelMatchesSerial(build(segmentCount, entryCount, style, seed), ranges,
                Long.MAX_VALUE, Long.MAX_VALUE);
    }

    /**
     * The same property with each range forced to CASCADE (a 1-byte merge budget pins every range at
     * the {@code max(2, …)} fan-in floor). This is the arm that exercises
     * {@code writeIntermediate}'s page-run cascade output and the re-scoping of an intermediate that
     * is already range-filtered — neither of which the single-pass property reaches.
     */
    @Property(tries = 60)
    void cascadingPageRunRangesAreStillByteExactToSerial(
            @ForAll @IntRange(min = 3, max = 6) int segmentCount,
            @ForAll @IntRange(min = 1, max = 320) int entryCount,
            @ForAll KeyStyle style,
            @ForAll @IntRange(min = 2, max = 6) int ranges,
            @ForAll long seed) throws IOException {
        assertParallelMatchesSerial(build(segmentCount, entryCount, style, seed), ranges,
                Long.MAX_VALUE, 1L);
    }

    /**
     * Rolled output: each range also splits into several parts. Stacks intra-range file boundaries on
     * top of the inter-range ones — the whole {@code part-00001..N} sequence must still be the exact
     * global sort.
     */
    @Property(tries = 30)
    void rolledPageRunParallelOutputIsByteExactToSerial(
            @ForAll @IntRange(min = 1, max = 5) int segmentCount,
            @ForAll @IntRange(min = 1, max = 120) int entryCount,
            @ForAll KeyStyle style,
            @ForAll @IntRange(min = 2, max = 5) int ranges,
            @ForAll long seed) throws IOException {
        assertParallelMatchesSerial(build(segmentCount, entryCount, style, seed), ranges, 4096L,
                Long.MAX_VALUE);
    }

    // ---------------------------------------------------------------------
    // Duplicate-hook equivalence: the page skip hands a range whole straddling
    // pages, so an unscoped hook would let two adjacent ranges report the same
    // out-of-range duplicate pair.
    // ---------------------------------------------------------------------

    @Property(tries = 40)
    void duplicateHookFiresExactlyAsOftenAsTheSerialMerge(
            @ForAll @IntRange(min = 2, max = 5) int segmentCount,
            @ForAll @IntRange(min = 1, max = 200) int entryCount,
            @ForAll @IntRange(min = 2, max = 6) int ranges,
            @ForAll long seed) throws IOException {
        // SMALL_ALPHABET maximises adjacent-equal pairs, which is what the hook reports.
        Scenario s = build(segmentCount, entryCount, KeyStyle.SMALL_ALPHABET, seed);
        Path root = Files.createTempDirectory("prange-pagerun-hook-");
        try {
            AtomicInteger serialHits = new AtomicInteger();
            AtomicInteger parallelHits = new AtomicInteger();
            run(s, 1, root, "serial", Long.MAX_VALUE, Long.MAX_VALUE,
                    (prev, dup) -> serialHits.incrementAndGet());
            run(s, ranges, root, "parallel", Long.MAX_VALUE, Long.MAX_VALUE,
                    (prev, dup) -> parallelHits.incrementAndGet());
            assertThat(parallelHits.get())
                    .as("parallel duplicate-hook count == serial (no double-count across boundaries)")
                    .isEqualTo(serialHits.get());
        } finally {
            deleteRecursively(root);
        }
    }

    // ---------------------------------------------------------------------
    // Adversarial: the completeness cross-check DEMOTES on this path (only the
    // last range walks a segment to EOF), so pin that it still fires.
    // ---------------------------------------------------------------------

    /**
     * A segment whose trailer under-declares {@code totalEntries} must still fail an R&gt;1 parallel
     * merge. This is the guard for the deliberate demotion documented on
     * {@link RangeScopedPageFrontier}: ranges that abandon their tail never reach
     * {@code checkComplete}, so the guarantee now rests structurally on the LAST range
     * ({@code hi == null}) walking every page of every segment. If that reasoning were wrong, a
     * corrupt segment would publish silently — the worst failure this path could have.
     */
    @Example
    void aCorruptTrailerStillFailsTheParallelMerge() throws IOException {
        Path root = Files.createTempDirectory("prange-pagerun-corrupt-");
        try {
            Scenario s = build(4, 200, KeyStyle.DENSE_SEQUENTIAL, 42L);
            Path staging = Files.createDirectories(root.resolve("parallel").resolve("_staging"));
            List<Path> segs = stage(staging, s.segments());
            corruptTotalEntries(segs.get(0));

            SortConfig config = SortConfigs.base().withMergeParallelism(4);
            SortTransform transform = new SortTransform(new SortRun(config, cmp, DuplicateHook.NO_OP,
                    SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT));
            assertThatThrownBy(() -> transform.transform(segs, Files.createDirectories(
                    root.resolve("parallel").resolve("data")), staging, PublishListener.NO_OP))
                    .isInstanceOf(IOException.class);
        } finally {
            deleteRecursively(root);
        }
    }

    /** Flip the trailer's {@code totalEntries} down by one — a segment that silently lost a row. */
    private static void corruptTotalEntries(Path segment) throws IOException {
        // Fixed trailer tail: trailerStart u64 + totalRecords u32 + totalEntries u64 + maxRecordLen u32
        // + magic u32 = 28 bytes from EOF; totalEntries starts 16 bytes before the end.
        try (FileChannel ch = FileChannel.open(segment, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long pos = ch.size() - 16;
            ByteBuffer buf = ByteBuffer.allocate(8);
            ch.read(buf, pos);
            buf.flip();
            long declared = buf.getLong();
            ByteBuffer out = ByteBuffer.allocate(8).putLong(declared - 1).flip();
            ch.write(out, pos);
        }
    }

    // =====================================================================
    // Harness
    // =====================================================================

    private record Scenario(List<List<ListEntry>> segments, List<ListEntry> allEntries) {
    }

    private Scenario build(int segmentCount, int entryCount, KeyStyle style, long seed) {
        Random r = new Random(seed);
        List<List<ListEntry>> segs = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            segs.add(new ArrayList<>());
        }
        List<ListEntry> all = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            ListEntry e = entry(key(style, r), r);
            all.add(e);
            segs.get(r.nextInt(segmentCount)).add(e);
        }
        for (List<ListEntry> seg : segs) {
            seg.sort(cmp);   // each staging segment is individually sorted
        }
        return new Scenario(segs, all);
    }

    private void assertParallelMatchesSerial(Scenario s, int ranges, long finalFileBytes,
                                             long mergeBudgetBytes) throws IOException {
        Path root = Files.createTempDirectory("prange-pagerun-");
        try {
            SortTransformResult serial =
                    run(s, 1, root, "serial", finalFileBytes, Long.MAX_VALUE, DuplicateHook.NO_OP);
            SortTransformResult parallel =
                    run(s, ranges, root, "parallel", finalFileBytes, mergeBudgetBytes, DuplicateHook.NO_OP);

            List<ListEntry> input = s.allEntries();
            List<ListEntry> serialRows = readAll(serial.finalFiles());
            List<ListEntry> parallelRows = readAll(parallel.finalFiles());

            assertThat(serial.totalRows()).as("serial rows out == rows in").isEqualTo(input.size());
            assertThat(parallel.totalRows()).as("parallel rows out == rows in").isEqualTo(input.size());

            assertThat(serialRows).as("serial is the exact input multiset")
                    .containsExactlyInAnyOrderElementsOf(input);
            assertThat(parallelRows).as("parallel is globally ascending").isSortedAccordingTo(cmp);
            assertThat(parallelRows).as("parallel is the exact input multiset (no dropped/duplicated row)")
                    .containsExactlyInAnyOrderElementsOf(input);
            assertThat(parallelRows).as("parallel equals serial position-for-position")
                    .containsExactlyElementsOf(serialRows);

            List<String> names = parallel.finalFiles().stream()
                    .map(p -> p.getFileName().toString()).toList();
            for (int i = 0; i < names.size(); i++) {
                assertThat(names.get(i)).isEqualTo(String.format("part-%05d.parquet", i + 1));
            }
        } finally {
            deleteRecursively(root);
        }
    }

    private SortTransformResult run(Scenario s, int parallelism, Path root, String name,
                                    long finalFileBytes, long mergeBudgetBytes, DuplicateHook hook)
            throws IOException {
        Path output = Files.createDirectories(root.resolve(name));
        Path staging = Files.createDirectories(output.resolve("_staging"));
        List<Path> segs = stage(staging, s.segments());
        SortConfig config = SortConfigs.base()
                .withFinalFileBytes(finalFileBytes)
                .withMergeBudgetBytes(mergeBudgetBytes)
                .withMergeParallelism(parallelism);
        SortTransform transform = new SortTransform(
                new SortRun(config, cmp, hook, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT));
        return transform.transform(segs, output, staging, PublishListener.NO_OP);
    }

    /** Stage each pre-sorted segment as a page-run {@code .pageseg} file — the LIVE listing format. */
    private List<Path> stage(Path dir, List<List<ListEntry>> segs) throws IOException {
        PageRunSegmentWriter writer =
                new PageRunSegmentWriter(cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE);
        List<Path> out = new ArrayList<>();
        for (int i = 0; i < segs.size(); i++) {
            Path path = dir.resolve("seg-" + i + ".pageseg");
            try (SortedCursor cursor = new InMemoryCursor(segs.get(i), cmp, DuplicateHook.NO_OP)) {
                writer.writeIntermediate(cursor, path);
            }
            out.add(path);
        }
        return out;
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
     * A {@link ListEntry} whose payload is a deterministic function of its comparator identity, so any
     * two equal-comparing rows are byte-identical — the regime in which serial and parallel output are
     * required to match position-for-position (see the Parquet sibling's class doc for why
     * distinct-payload ties are excluded).
     */
    private ListEntry entry(byte[] key, Random r) {
        int kind = r.nextInt(4);
        String version = r.nextInt(4) == 0 ? null : "v" + r.nextInt(3);
        int identity = java.util.Arrays.hashCode(key) * 31 + (version == null ? 0 : version.hashCode());
        if (kind <= 2) {
            return new ObjectEntry(KeyBytes.of(key), Math.floorMod(identity, 3), 0L, null, null,
                    version, version != null && identity % 2 == 0, null, null, null, null);
        }
        return new DeleteMarkerEntry(KeyBytes.of(key), version, identity % 2 == 0, 0L, null);
    }

    private List<ListEntry> readAll(List<Path> files) throws IOException {
        List<ListEntry> out = new ArrayList<>();
        for (Path f : files) {
            try (SegmentReader reader = new SegmentReader(f)) {
                while (reader.hasNext()) {
                    out.add(reader.next());
                }
            }
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
}
