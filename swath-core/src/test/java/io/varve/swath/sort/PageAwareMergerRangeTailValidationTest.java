/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Adversarial close/range-cutoff coverage for decoded page tails owned by {@link PageAwareMerger}. */
class PageAwareMergerRangeTailValidationTest {

    private static final ListEntryComparator COMPARATOR = new ListEntryComparator();

    @Test
    void coherentUnderstatedPageTailIsRejectedBySerialAndParallelRangeMerge(@TempDir Path root)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path segment = understatedSegment(staging.resolve("understated.pageseg"));
        PageRunCatalog catalog = indexedCatalog(segment);
        assertThat(catalog.descriptors().getFirst().extension().status())
                .isEqualTo(PageRunPageIndex.Status.EMBEDDED);
        assertThat(catalog.descriptors().getFirst().trailer().segMaxKey())
                .containsExactly(bytes("h"));

        assertBodyCorruption(catchThrowable(() -> serialEntries(segment)));
        assertBodyCorruption(catchThrowable(() -> rangeEntries(
                segment, null, bytes("m"), DuplicateHook.NO_OP, SortMetrics.NO_OP)));

        TrackingWriterFactory writers = new TrackingWriterFactory();
        ParallelRangeMerge merge = parallelMerge(writers);
        assertThatThrownBy(() -> merge.run(catalog, staging, List.of(bytes("m")), units -> { }))
                .isInstanceOf(SegmentCorruptionException.class)
                .extracting(error -> ((SegmentCorruptionException) error).errorClass())
                .isEqualTo(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION);

        assertThat(writers.openNow.get()).isZero();
        assertThat(writers.closed.get()).isEqualTo(writers.opened.get());
        assertNoLiveWorkers(merge.workerThreadPrefix());
        assertNoOwnedDebris(staging);
        assertThat(segment).exists();
    }

    @Test
    void validBoundaryRowsRemainOwnedByTheHigherRangeByteExactlyAndWithMultiplicity(
            @TempDir Path dir) throws IOException {
        List<ListEntry> rows = List.of(
                object("a"), object("n"), object("n"), object("y"), object("z"));
        Path segment = SortTestSupport.writeIndexedPages(
                dir.resolve("valid.pageseg"), List.of(rows));
        AtomicInteger serialDuplicates = new AtomicInteger();
        AtomicInteger lowDuplicates = new AtomicInteger();
        AtomicInteger highDuplicates = new AtomicInteger();
        SortTestSupport.CountingMetrics lowMetrics = new SortTestSupport.CountingMetrics();

        List<ListEntry> serial = serialEntries(segment,
                (previous, duplicate) -> serialDuplicates.incrementAndGet());
        List<ListEntry> low = rangeEntries(segment, null, bytes("m"),
                (previous, duplicate) -> lowDuplicates.incrementAndGet(), lowMetrics);
        List<ListEntry> high = rangeEntries(segment, bytes("m"), null,
                (previous, duplicate) -> highDuplicates.incrementAndGet(), SortMetrics.NO_OP);
        List<ListEntry> parallel = new ArrayList<>(low);
        parallel.addAll(high);

        assertThat(keys(low)).containsExactly(KeyBytes.ofUtf8("a"));
        assertThat(keys(high)).containsExactly(
                KeyBytes.ofUtf8("n"), KeyBytes.ofUtf8("n"),
                KeyBytes.ofUtf8("y"), KeyBytes.ofUtf8("z"));
        assertThat(parallel).containsExactlyElementsOf(serial).containsExactlyElementsOf(rows);
        assertThat(serialDuplicates).hasValue(1);
        assertThat(lowDuplicates).hasValue(0);
        assertThat(highDuplicates).hasValue(1);
        assertThat(lowMetrics.count("SORT.page_whole_emitted"))
                .as("close-time validation does not plan another page or duplicate engagement metrics")
                .isEqualTo(1);

        AtomicInteger parallelDuplicates = new AtomicInteger();
        TrackingWriterFactory writers = new TrackingWriterFactory();
        ParallelRangeMerge merge = parallelMerge(writers,
                (previous, duplicate) -> parallelDuplicates.incrementAndGet());
        List<ParallelRangeWorker.Result> results = merge.run(
                indexedCatalog(segment), dir, List.of(bytes("m")), units -> { });
        List<ListEntry> concurrent = writers.entriesInRangeOrder(results);
        for (ParallelRangeWorker.Result result : results) {
            for (SortedFileWriter writer : result.writers()) {
                writer.close();
            }
        }
        assertThat(concurrent).containsExactlyElementsOf(serial);
        assertThat(parallelDuplicates).hasValue(1);
        assertThat(writers.openNow).hasValue(0);
        assertThat(writers.closed).hasValue(writers.opened.get());
        assertNoLiveWorkers(merge.workerThreadPrefix());
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ActiveTail.class)
    void closeValidatesSelectedUnselectedAndEqualMinimumActivePages(
            ActiveTail tail, @TempDir Path dir) throws IOException {
        List<TrackingFrontier> frontiers = tail.frontiers(dir);
        PageAwareMerger merger = new PageAwareMerger(new ArrayList<>(frontiers), COMPARATOR,
                MergeScope.CROSS_SEGMENT, SortMetrics.NO_OP);

        assertBodyCorruption(catchThrowable(merger::close));
        assertThat(frontiers).allSatisfy(frontier -> assertThat(frontier.closes).isEqualTo(1));
    }

    @Test
    void closeDrainsTheDecodedWholePageButDoesNotDecodeAnUntouchedFrontierPage() {
        TrackingFrontier frontier = new TrackingFrontier(List.of(
                PageBlock.pack(List.of(object("a"), object("b"), object("c")), COMPARATOR),
                PageBlock.pack(List.of(object("x"), object("y")), COMPARATOR)));
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        PageAwareMerger merger = new PageAwareMerger(List.of(frontier), COMPARATOR,
                MergeScope.CROSS_SEGMENT, metrics);
        merger.close();

        assertThat(frontier.decodes).isEqualTo(1);
        assertThat(frontier.closes).isEqualTo(1);
        assertThat(metrics.count("SORT.page_whole_emitted")).isEqualTo(1);
    }

    @Test
    void constructorKeepsItsReadFailureAndSuppressesTailValidationAndCloseFailures(
            @TempDir Path dir) throws IOException {
        TrackingFrontier decoded = new TrackingFrontier(List.of(corruptBlock(
                dir.resolve("constructor-tail.pageseg"),
                List.of(object("a"), object("n"), object("y"), object("z")), "h")));
        TrackingFrontier failing = new TrackingFrontier(
                List.of(PageBlock.pack(List.of(object("c"), object("d")), COMPARATOR)),
                true, true);
        AtomicInteger classifications = new AtomicInteger();

        Throwable thrown = catchThrowable(() -> new PageAwareMerger(
                List.of(decoded, failing), COMPARATOR,
                MergeScope.CROSS_SEGMENT, SortMetrics.NO_OP,
                (copyable, interleaved) -> classifications.incrementAndGet()));

        assertThat(thrown).isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("page-aware merge read failed")
                .cause().hasMessageContaining("injected advance failure");
        assertThat(thrown.getSuppressed()).hasSize(1);
        Throwable validation = thrown.getSuppressed()[0];
        assertBodyCorruption(validation);
        assertThat(validation.getSuppressed()).singleElement()
                .satisfies(close -> assertThat(close).cause()
                        .hasMessageContaining("injected close failure"));
        assertThat(decoded.closes).isEqualTo(1);
        assertThat(failing.closes).isEqualTo(1);
        assertThat(classifications).hasValue(0);
    }

    @Test
    void tryWithResourcesKeepsConsumerFailureAndSuppressesCloseTimeValidation(
            @TempDir Path dir) throws IOException {
        TrackingFrontier frontier = new TrackingFrontier(List.of(corruptBlock(
                dir.resolve("consumer-tail.pageseg"),
                List.of(object("a"), object("n"), object("y"), object("z")), "h")));
        IllegalStateException consumerFailure = new IllegalStateException("injected consumer failure");

        Throwable thrown = catchThrowable(() -> {
            try (PageAwareMerger ignored = new PageAwareMerger(List.of(frontier), COMPARATOR,
                    MergeScope.CROSS_SEGMENT, SortMetrics.NO_OP)) {
                throw consumerFailure;
            }
        });

        assertThat(thrown).isSameAs(consumerFailure);
        assertThat(thrown.getSuppressed()).hasSize(1);
        assertBodyCorruption(thrown.getSuppressed()[0]);
        assertThat(frontier.closes).isEqualTo(1);
    }

    private static Path understatedSegment(Path path) throws IOException {
        SortTestSupport.writeIndexedPages(path, List.of(List.of(
                object("a"), object("n"), object("y"), object("z"))));
        PageRunRawFixtures.understatePageMaxAndRepairIndex(path, 0, bytes("h"));
        return path;
    }

    private static PageRunCatalog indexedCatalog(Path segment) throws IOException {
        return PageRunCatalog.preflight(List.of(segment),
                path -> PageRunSegmentIo.open(path, SortMetrics.NO_OP), Optional.of(key -> { }));
    }

    private static ParallelRangeMerge parallelMerge(SortedFileWriterFactory writers) {
        return parallelMerge(writers, DuplicateHook.NO_OP);
    }

    private static ParallelRangeMerge parallelMerge(SortedFileWriterFactory writers,
                                                     DuplicateHook hook) {
        SortConfig config = SortConfigs.base()
                .withMergeParallelism(2)
                .withMergeBudgetBytes(64L << 20);
        SortRun run = new SortRun(config, COMPARATOR, hook,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, writers,
                MergeInputProfile.STRUCTURED_RANGE_OWNED_PAGES, RangeMergeTimer.NO_OP,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);
        return new ParallelRangeMerge(run);
    }

    private static List<ListEntry> serialEntries(Path segment) throws IOException {
        return serialEntries(segment, DuplicateHook.NO_OP);
    }

    private static List<ListEntry> serialEntries(Path segment, DuplicateHook hook) throws IOException {
        try (SortedCursor cursor = new DuplicateReporting(new PageAwareMerger(
                List.of(new PageFrontierReader(segment, SortMetrics.NO_OP)), COMPARATOR,
                MergeScope.CROSS_SEGMENT, SortMetrics.NO_OP), COMPARATOR, hook)) {
            return drain(cursor);
        }
    }

    private static List<ListEntry> rangeEntries(Path segment, byte[] lo, byte[] hi,
            DuplicateHook hook, SortMetrics metrics) throws IOException {
        PageRunTrailer.Trailer trailer = PageRunTrailer.read(segment);
        RangeScopedPageFrontier frontier = new RangeScopedPageFrontier(
                new PageFrontierReader(segment, SortMetrics.NO_OP), lo, hi,
                trailer.totalRecords(), 0, metrics, null);
        try (SortedCursor cursor = new DuplicateReporting(new RangeFilteredCursor(
                new PageAwareMerger(List.of(frontier), COMPARATOR,
                        MergeScope.CROSS_SEGMENT, metrics), lo, hi), COMPARATOR, hook)) {
            return drain(cursor);
        }
    }

    private static List<ListEntry> drain(SortedCursor cursor) {
        List<ListEntry> entries = new ArrayList<>();
        while (cursor.hasNext()) {
            entries.add(cursor.next());
        }
        return entries;
    }

    private static List<KeyBytes> keys(List<ListEntry> entries) {
        return entries.stream().map(ListEntry::key).toList();
    }

    private static PageBlock corruptBlock(Path path, List<ListEntry> rows, String forgedMax)
            throws IOException {
        SortTestSupport.writeIndexedPages(path, List.of(rows));
        PageRunRawFixtures.understatePageMaxAndRepairIndex(path, 0, bytes(forgedMax));
        try (PageFrontierReader reader = new PageFrontierReader(path, SortMetrics.NO_OP)) {
            return reader.decodeCurrentPage();
        }
    }

    private static void assertBodyCorruption(Throwable failure) {
        assertThat(failure).isNotNull();
        Throwable current = failure;
        while (current != null && !(current instanceof SegmentCorruptionException)) {
            current = current.getCause();
        }
        assertThat(current).isInstanceOf(SegmentCorruptionException.class)
                .extracting(error -> ((SegmentCorruptionException) error).errorClass())
                .isEqualTo(SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION);
    }

    private static void assertNoLiveWorkers(String prefix) {
        assertThat(Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(name -> name.startsWith(prefix)))
                .isEmpty();
    }

    private static void assertNoOwnedDebris(Path staging) throws IOException {
        for (String glob : List.of(StagingNames.RANGE_TMP_GLOB,
                StagingNames.RANGE_CASCADE_PAGE_RUN_GLOB,
                StagingNames.RANGE_PROOF_TMP_GLOB)) {
            try (var files = Files.newDirectoryStream(staging, glob)) {
                assertThat(files.iterator().hasNext()).as("no debris matching %s", glob).isFalse();
            }
        }
    }

    private static ListEntry object(String key) {
        return SortTestSupport.object(key);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private enum ActiveTail {
        SELECTED {
            @Override
            List<TrackingFrontier> frontiers(Path dir) throws IOException {
                return List.of(
                        trackingCorrupt(dir.resolve("selected.pageseg"),
                                List.of(object("a"), object("n"), object("y"), object("z"))),
                        tracking(List.of(object("c"), object("d"))));
            }
        },
        UNSELECTED {
            @Override
            List<TrackingFrontier> frontiers(Path dir) throws IOException {
                return List.of(
                        tracking(List.of(object("a"), object("b"), object("h"))),
                        trackingCorrupt(dir.resolve("unselected.pageseg"),
                                List.of(object("c"), object("n"), object("y"), object("z"))));
            }
        },
        EQUAL_MINIMUM {
            @Override
            List<TrackingFrontier> frontiers(Path dir) throws IOException {
                return List.of(
                        tracking(List.of(object("a"), object("b"), object("h"))),
                        trackingCorrupt(dir.resolve("equal-min.pageseg"),
                                List.of(object("a"), object("n"), object("y"), object("z"))));
            }
        };

        abstract List<TrackingFrontier> frontiers(Path dir) throws IOException;

        static TrackingFrontier tracking(List<ListEntry> rows) {
            return new TrackingFrontier(List.of(PageBlock.pack(rows, COMPARATOR)));
        }

        static TrackingFrontier trackingCorrupt(Path path, List<ListEntry> rows) throws IOException {
            return new TrackingFrontier(List.of(corruptBlock(path, rows, "h")));
        }
    }

    private static final class TrackingFrontier implements PageFrontierStream {
        private final List<PageBlock> pages;
        private final boolean failAdvance;
        private final boolean failClose;
        private int index;
        private int decodes;
        private int closes;

        TrackingFrontier(List<PageBlock> pages) {
            this(pages, false, false);
        }

        TrackingFrontier(List<PageBlock> pages, boolean failAdvance, boolean failClose) {
            this.pages = pages;
            this.failAdvance = failAdvance;
            this.failClose = failClose;
        }

        @Override
        public boolean hasPage() {
            return index < pages.size();
        }

        @Override
        public byte[] minKey() {
            return pages.get(index).firstKeyUnsafe();
        }

        @Override
        public byte[] maxKey() {
            return pages.get(index).lastKeyUnsafe();
        }

        @Override
        public int count() {
            return pages.get(index).count();
        }

        @Override
        public PageBlock decodeCurrentPage() {
            decodes++;
            return pages.get(index);
        }

        @Override
        public void advance() throws IOException {
            if (failAdvance) {
                throw new IOException("injected advance failure");
            }
            index++;
        }

        @Override
        public void close() throws IOException {
            closes++;
            if (failClose) {
                throw new IOException("injected close failure");
            }
        }
    }

    private static final class TrackingWriterFactory implements SortedFileWriterFactory {
        private final AtomicInteger opened = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();
        private final AtomicInteger openNow = new AtomicInteger();
        private final ConcurrentHashMap<Path, List<ListEntry>> entries = new ConcurrentHashMap<>();

        @Override
        public SortedFileWriter create(Path path, int fileIndex) throws IOException {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            opened.incrementAndGet();
            openNow.incrementAndGet();
            List<ListEntry> written = new ArrayList<>();
            entries.put(path, written);
            return new SortedFileWriter() {
                private final AtomicBoolean isClosed = new AtomicBoolean();
                private long rows;

                @Override
                public void write(ListEntry entry) {
                    rows++;
                    written.add(entry);
                }

                @Override
                public long rows() {
                    return rows;
                }

                @Override
                public long dataSize() {
                    return rows;
                }

                @Override
                public void setFileIndex(int ignored) {
                }

                @Override
                public void close() throws IOException {
                    if (isClosed.compareAndSet(false, true)) {
                        try {
                            channel.close();
                        } finally {
                            closed.incrementAndGet();
                            openNow.decrementAndGet();
                        }
                    }
                }
            };
        }

        List<ListEntry> entriesInRangeOrder(List<ParallelRangeWorker.Result> results) {
            List<ListEntry> ordered = new ArrayList<>();
            for (ParallelRangeWorker.Result result : results) {
                for (Path path : result.tmpParts()) {
                    ordered.addAll(entries.get(path));
                }
            }
            return ordered;
        }
    }
}
