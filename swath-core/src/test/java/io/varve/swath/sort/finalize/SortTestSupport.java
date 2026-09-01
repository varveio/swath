/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.sorted.SortedParquetWriter;
import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.FinalPartMetadata;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortedEntryCursor;
import io.varve.swath.sort.SortedFileWriter;
import io.varve.swath.sort.spill.PageBlock;
import io.varve.swath.sort.spill.PageCompression;
import io.varve.swath.sort.spill.PageRunWriter;
import io.varve.swath.sort.spill.SpillTestFixtures;
import io.varve.swath.sort.stage.PageRunFixtures;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Shared fixtures for sort tests, with narrow public helpers used by {@code output.sorted} tests. */
public final class SortTestSupport {

    private static final int CASCADE_SOURCE_PAGE_ROWS = 600;
    /** Enough pages that the cascade drains more than one intermediate write batch before the last. */
    private static final int CASCADE_SOURCE_PAGES = 4;
    private static final int CASCADE_SOURCE_MULTI_PAGE_SEGMENT = 2;

    private SortTestSupport() {
    }

    public static ObjectEntry object(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null,
                false, null, null, null, null);
    }

    /** Write pre-sorted entries in the production page-run staging format. */
    public static Path writePageRun(Path path, List<ListEntry> sorted, Comparator<ListEntry> comparator)
            throws IOException {
        PageRunWriter writer =
                new PageRunWriter(comparator, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCompression.NONE);
        try (SortedEntryCursor cursor = new ListCursor(sorted)) {
            writer.writeIntermediate(cursor, path);
        }
        return path;
    }

    public static SortedEntryCursor cursor(List<ListEntry> entries) {
        return new ListCursor(entries);
    }

    /** Write one single-row listing page per ordinal, including the production type-2 index. */
    public static Path writePages(Path path, int pages, int keyOffset) throws IOException {
        List<List<ListEntry>> generated = new ArrayList<>(pages);
        for (int page = 0; page < pages; page++) {
            generated.add(List.of(object(String.format("k%05d", keyOffset + page))));
        }
        return writePages(path, generated);
    }

    /** Write caller-supplied sorted listing pages, including the production type-2 index. */
    public static Path writePages(Path path, List<? extends List<? extends ListEntry>> pages)
            throws IOException {
        return writePages(path, pages, SortMode.OBJECTS);
    }

    /** Write caller-supplied sorted listing pages with an explicit persisted ordering mode. */
    public static Path writePages(Path path, List<? extends List<? extends ListEntry>> pages,
            SortMode orderingMode)
            throws IOException {
        return writePages(path, pages, orderingMode, PageCompression.NONE);
    }

    /** Write caller-supplied indexed pages with an explicit payload codec. */
    public static Path writePages(Path path, List<? extends List<? extends ListEntry>> pages,
            SortMode orderingMode,
            PageCompression codec) throws IOException {
        return PageRunFixtures.writePages(path, pages, orderingMode, codec);
    }

    /**
     * Stage five sorted page-run sources named {@code seg-0}…{@code seg-4}, where {@code seg-2}
     * carries {@link #CASCADE_SOURCE_PAGES} pages. Under fan-in two the cascade reduces them in the
     * groups {@code (seg-0, seg-1)}, {@code (seg-2, seg-3)}, {@code (seg-4)}, so damaging a late page
     * of {@code seg-2} fails the second group only, and only after the first has committed and the
     * second has already written page bytes of its own.
     */
    public static List<Path> writeCascadeSources(Path staging) throws IOException {
        List<Path> sources = new ArrayList<>(5);
        String[] prefixes = {"a", "b", "c", "d", "e"};
        for (int segment = 0; segment < prefixes.length; segment++) {
            List<List<ListEntry>> pages = new ArrayList<>();
            int pageCount = segment == CASCADE_SOURCE_MULTI_PAGE_SEGMENT ? CASCADE_SOURCE_PAGES : 1;
            for (int page = 0; page < pageCount; page++) {
                List<ListEntry> rows = new ArrayList<>(CASCADE_SOURCE_PAGE_ROWS);
                for (int row = 0; row < CASCADE_SOURCE_PAGE_ROWS; row++) {
                    rows.add(object(String.format("%s%05d", prefixes[segment],
                            page * CASCADE_SOURCE_PAGE_ROWS + row)));
                }
                pages.add(rows);
            }
            sources.add(writePages(
                    staging.resolve("seg-" + segment + StagingNames.PAGE_RUN_SUFFIX), pages));
        }
        return List.copyOf(sources);
    }

    /** Total rows staged by {@link #writeCascadeSources}, for post-retry completeness assertions. */
    public static int cascadeSourceRows() {
        return (4 + CASCADE_SOURCE_PAGES) * CASCADE_SOURCE_PAGE_ROWS;
    }

    /**
     * Make the multi-page source's last page unreadable and return it, so the group that owns it
     * fails only after the cascade has committed the previous group and written pages of its own.
     */
    public static Path corruptLateCascadeSourcePage(List<Path> sources) throws IOException {
        Path damaged = sources.get(CASCADE_SOURCE_MULTI_PAGE_SEGMENT);
        corruptPageFrame(damaged, CASCADE_SOURCE_PAGES - 1);
        return damaged;
    }

    /** Flip one byte of the {@code frameIndex}-th persisted page so reading it fails mid-stream. */
    private static void corruptPageFrame(Path segment, int frameIndex) throws IOException {
        byte[] bytes = Files.readAllBytes(segment);
        int frame = SpillTestFixtures.pageRunHeaderBytes();
        for (int i = 0; i < frameIndex; i++) {
            frame += 8 + ByteBuffer.wrap(bytes, frame, 4).getInt();
        }
        bytes[frame + 8] ^= 0x01;
        Files.write(segment, bytes);
    }

    /** Write canonical Parquet input for tests of CaptureSorter/ParquetEntryReader, not internal staging. */
    public static Path writeCanonicalParquet(Path path, List<ListEntry> entries) throws IOException {
        try (SortedFileWriter writer =
                     new SortedParquetWriter(path, SortConfigs.base(), SortMode.VERSIONS, 1)) {
            for (ListEntry entry : entries) {
                writer.write(entry);
            }
        }
        return path;
    }

    /** Drain a cursor, closing it. */
    public static List<ListEntry> drain(SortedEntryCursor cursor) {
        List<ListEntry> out = new ArrayList<>();
        try (cursor) {
            while (cursor.hasNext()) {
                out.add(cursor.next());
            }
        }
        return out;
    }

    /** Write the complete buffer even when the channel performs a short write. */
    public static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /** Delegates every {@link SortedFileWriter} method unless a focused test overrides it. */
    public abstract static class DelegatingSortedFileWriter implements SortedFileWriter {
        private final SortedFileWriter delegate;

        public DelegatingSortedFileWriter(SortedFileWriter delegate) {
            this.delegate = delegate;
        }

        protected final SortedFileWriter delegate() {
            return delegate;
        }

        @Override
        public void write(ListEntry entry) throws IOException {
            delegate.write(entry);
        }

        @Override
        public long rows() {
            return delegate.rows();
        }

        @Override
        public long dataSize() {
            return delegate.dataSize();
        }

        @Override
        public void markFinal() {
            delegate.markFinal();
        }

        @Override
        public Optional<FinalPartMetadata> finalMetadata() {
            return delegate.finalMetadata();
        }

        @Override
        public void discard() throws IOException {
            delegate.discard();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /**
     * In-memory {@link CascadeReducer.SegmentIo} keyed by an int handle — lets the cascade be exercised
     * without writing Parquet. Intermediates are drained into fresh lists.
     */
    static final class InMemorySegments implements CascadeReducer.SegmentIo<Integer> {
        private final Map<Integer, List<ListEntry>> store = new HashMap<>();
        private final AtomicInteger seq = new AtomicInteger();

        Integer add(List<ListEntry> sorted) {
            int id = seq.incrementAndGet();
            store.put(id, new ArrayList<>(sorted));
            return id;
        }

        int liveCount() {
            return store.size();
        }

        boolean isLive(Integer segment) {
            return store.containsKey(segment);
        }

        @Override
        public CascadeReducer.PageStream openPages(Integer segment) {
            return new ListPageStream(store.get(segment), () -> { });
        }

        @Override
        public Integer writeIntermediate(SortedEntryCursor sorted) {
            List<ListEntry> buf = new ArrayList<>();
            while (sorted.hasNext()) {
                buf.add(sorted.next());
            }
            return add(buf);
        }

        @Override
        public void delete(Integer segment) {
            store.remove(segment);
        }
    }

    private static final class ListCursor implements SortedEntryCursor {
        private final java.util.Iterator<ListEntry> entries;

        ListCursor(List<ListEntry> entries) {
            this.entries = entries.iterator();
        }

        @Override
        public boolean hasNext() {
            return entries.hasNext();
        }

        @Override
        public ListEntry next() {
            return entries.next();
        }

        @Override
        public void close() {
        }
    }

    /**
     * Wraps an {@link InMemorySegments} store, tracking the high-water mark of concurrently-open
     * page streams across an entire {@link CascadeReducer#merge} call — the "SegmentIo open-count"
     * assertion point for the merge-memory-budget bound (I11): {@link CascadeReducer}
     * must never hold more streams open at once than the caller's {@code fanIn} constructor arg
     * allows.
     */
    static final class PeakTrackingSegments implements CascadeReducer.SegmentIo<Integer> {
        private final InMemorySegments delegate = new InMemorySegments();
        private int live;
        private int peak;

        Integer add(List<ListEntry> sorted) {
            return delegate.add(sorted);
        }

        /** The highest number of streams ever open at the same instant, across every pass. */
        int peakOpen() {
            return peak;
        }

        @Override
        public CascadeReducer.PageStream openPages(Integer segment) {
            live++;
            peak = Math.max(peak, live);
            return new ListPageStream(delegate.store.get(segment), () -> live--);
        }

        @Override
        public Integer writeIntermediate(SortedEntryCursor sorted) throws IOException {
            return delegate.writeIntermediate(sorted);
        }

        @Override
        public void delete(Integer segment) throws IOException {
            delegate.delete(segment);
        }
    }

    /** One in-memory page used to exercise the production page-only cascade in unit tests. */
    private static final class ListPageStream implements CascadeReducer.PageStream {
        private final PageBlock page;
        private final Runnable onClose;
        private boolean available = true;
        private boolean closed;

        ListPageStream(List<ListEntry> entries, Runnable onClose) {
            page = entries.isEmpty()
                    ? null
                    : PageBlock.pack(entries, new ListEntryComparator(), PageCompression.NONE);
            available = page != null;
            this.onClose = onClose;
        }

        @Override
        public boolean hasPage() {
            return available;
        }

        @Override
        public byte[] minKey() {
            return page.firstKey();
        }

        @Override
        public byte[] maxKey() {
            return page.lastKey();
        }

        @Override
        public PageBlock decodeCurrentPage() {
            return page;
        }

        @Override
        public void advance() {
            available = false;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                onClose.run();
            }
        }
    }

    /** Counts recordStealReason calls per {@code outcome.reason}. */
    public static final class CountingMetrics implements SortMetrics {
        final Map<String, Integer> counts = new ConcurrentHashMap<>();
        final LongAdder progress = new LongAdder();
        final LongAdder pipelinePagesForwarded = new LongAdder();
        final LongAdder pipelineClusterPages = new LongAdder();
        final AtomicLong pipelineDecodedPageBytesPeak = new AtomicLong();

        @Override
        public void recordStealReason(String outcome, String reason) {
            counts.merge(outcome + "." + reason, 1, Integer::sum);
        }

        @Override
        public void markProgress() {
            progress.increment();
        }

        @Override
        public void recordPipelinePagesForwarded(long pages) {
            pipelinePagesForwarded.add(pages);
        }

        @Override
        public void recordPipelineCluster(long pages, long rows) {
            pipelineClusterPages.add(pages);
        }

        @Override
        public void recordPipelineDecodedPagePeak(long bytes) {
            pipelineDecodedPageBytesPeak.getAndAccumulate(bytes, Math::max);
        }

        public int count(String key) {
            return counts.getOrDefault(key, 0);
        }
    }
}
