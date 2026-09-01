/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.sorted.SortedParquetWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

    private SortTestSupport() {
    }

    public static ObjectEntry object(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null,
                false, null, null, null, null);
    }

    /** Write pre-sorted entries in the production page-run staging format. */
    public static Path writePageRun(Path path, List<ListEntry> sorted, Comparator<ListEntry> comparator)
            throws IOException {
        PageRunSegmentWriter writer =
                new PageRunSegmentWriter(comparator, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE);
        try (SortedCursor cursor = new InMemoryCursor(sorted, comparator, DuplicateHook.NO_OP)) {
            writer.writeIntermediate(cursor, path);
        }
        return path;
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
    public static Path writePages(Path path, List<List<ListEntry>> pages) throws IOException {
        return writePages(path, pages, SortMode.OBJECTS);
    }

    /** Write caller-supplied sorted listing pages with an explicit persisted ordering mode. */
    static Path writePages(Path path, List<List<ListEntry>> pages, SortMode orderingMode)
            throws IOException {
        return writePages(path, pages, orderingMode, PageCodec.NONE);
    }

    /** Write caller-supplied indexed pages with an explicit payload codec. */
    static Path writePages(Path path, List<List<ListEntry>> pages, SortMode orderingMode,
            PageCodec codec) throws IOException {
        ListEntryComparator comparator = new ListEntryComparator();
        SortBuffer buffer = new SortBuffer(SortConfigs.base().withSegmentCodec(codec), comparator);
        for (int page = 0; page < pages.size(); page++) {
            buffer.admit(page, pages.get(page));
        }
        new PageRunSegmentWriter(comparator, DuplicateHook.NO_OP, SortMetrics.NO_OP, codec,
                orderingMode)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
        return path;
    }

    /** Write canonical Parquet input for tests of CaptureSorter/ParquetEntryReader, not internal staging. */
    static Path writeCanonicalParquet(Path path, List<ListEntry> entries) throws IOException {
        try (SortedFileWriter writer =
                     new SortedParquetWriter(path, SortConfigs.base(), SortMode.VERSIONS, 1)) {
            for (ListEntry entry : entries) {
                writer.write(entry);
            }
        }
        return path;
    }

    /** Drain a cursor, closing it. */
    static List<ListEntry> drain(SortedCursor cursor) {
        List<ListEntry> out = new ArrayList<>();
        try (cursor) {
            while (cursor.hasNext()) {
                out.add(cursor.next());
            }
        }
        return out;
    }

    /** Write the complete buffer even when the channel performs a short write. */
    static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /** Delegates every {@link SortedFileWriter} method unless a focused test overrides it. */
    abstract static class DelegatingSortedFileWriter implements SortedFileWriter {
        private final SortedFileWriter delegate;

        DelegatingSortedFileWriter(SortedFileWriter delegate) {
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
     * In-memory {@link KWayMerge.SegmentIo} keyed by an int handle — lets the cascade be exercised
     * without writing Parquet. Intermediates are drained into fresh lists.
     */
    static final class InMemorySegments implements KWayMerge.SegmentIo<Integer> {
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
        public KWayMerge.PageStream openPages(Integer segment) {
            return new ListPageStream(store.get(segment), () -> { });
        }

        @Override
        public Integer writeIntermediate(SortedCursor sorted) {
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

    /**
     * Wraps an {@link InMemorySegments} store, tracking the high-water mark of concurrently-open
     * page streams across an entire {@link KWayMerge#merge} call — the "SegmentIo open-count"
     * assertion point for the merge-memory-budget bound (I11): {@link KWayMerge}
     * must never hold more streams open at once than the caller's {@code fanIn} constructor arg
     * allows.
     */
    static final class PeakTrackingSegments implements KWayMerge.SegmentIo<Integer> {
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
        public KWayMerge.PageStream openPages(Integer segment) {
            live++;
            peak = Math.max(peak, live);
            return new ListPageStream(delegate.store.get(segment), () -> live--);
        }

        @Override
        public Integer writeIntermediate(SortedCursor sorted) throws IOException {
            return delegate.writeIntermediate(sorted);
        }

        @Override
        public void delete(Integer segment) throws IOException {
            delegate.delete(segment);
        }
    }

    /** One in-memory page used to exercise the production page-only cascade in unit tests. */
    private static final class ListPageStream implements KWayMerge.PageStream {
        private final PageBlock page;
        private final Runnable onClose;
        private boolean available = true;
        private boolean closed;

        ListPageStream(List<ListEntry> entries, Runnable onClose) {
            page = entries.isEmpty()
                    ? null
                    : PageBlock.pack(entries, new ListEntryComparator(), PageCodec.NONE);
            available = page != null;
            this.onClose = onClose;
        }

        @Override
        public boolean hasPage() {
            return available;
        }

        @Override
        public byte[] minKey() {
            return page.firstKeyUnsafe();
        }

        @Override
        public byte[] maxKey() {
            return page.lastKeyUnsafe();
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
