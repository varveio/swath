/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared fixtures for the {@code io.varve.swath.sort} unit tests (all in-package, so package-private types are reachable). */
final class SortTestSupport {

    private SortTestSupport() {
    }

    static ObjectEntry object(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null,
                false, null, null, null, null);
    }

    /** Write pre-sorted entries in the production page-run staging format. */
    static Path writePageRun(Path path, List<ListEntry> sorted, Comparator<ListEntry> comparator)
            throws IOException {
        PageRunSegmentWriter writer =
                new PageRunSegmentWriter(comparator, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE);
        try (SortedCursor cursor = new InMemoryCursor(sorted, comparator, DuplicateHook.NO_OP)) {
            writer.writeIntermediate(cursor, path);
        }
        return path;
    }

    /** Write canonical Parquet input for tests of CaptureSorter/SegmentReader, not internal staging. */
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

    /** An in-memory {@link EntryStream} over a pre-sorted list — for merger/KWayMerge tests without Parquet. */
    static final class ListEntryStream implements EntryStream {
        private final List<ListEntry> entries;
        private int i;

        ListEntryStream(List<ListEntry> entries) {
            this.entries = entries;
        }

        @Override
        public boolean hasNext() {
            return i < entries.size();
        }

        @Override
        public ListEntry peek() {
            return i < entries.size() ? entries.get(i) : null;
        }

        @Override
        public ListEntry next() {
            return entries.get(i++);
        }

        @Override
        public void close() {
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
        public EntryStream open(Integer segment) {
            return new ListEntryStream(store.get(segment));
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
     * {@link EntryStream}s across an entire {@link KWayMerge#merge} call — the "SegmentIo open-count"
     * assertion point for the merge-memory-budget bound (I11): {@link KWayMerge}
     * must never hold more streams open at once than the caller's {@code fanIn} constructor arg
     * allows, since each open stream stands in for a {@link SegmentReader} that preloads one full
     * segment row group.
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
        public EntryStream open(Integer segment) throws IOException {
            live++;
            peak = Math.max(peak, live);
            EntryStream inner = delegate.open(segment);
            return new EntryStream() {
                @Override
                public boolean hasNext() {
                    return inner.hasNext();
                }

                @Override
                public ListEntry peek() {
                    return inner.peek();
                }

                @Override
                public ListEntry next() throws IOException {
                    return inner.next();
                }

                @Override
                public void close() throws IOException {
                    inner.close();
                    live--;
                }
            };
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

    /** Counts recordStealReason calls per {@code outcome.reason}. */
    static final class CountingMetrics implements SortMetrics {
        final Map<String, Integer> counts = new HashMap<>();

        @Override
        public void recordStealReason(String outcome, String reason) {
            counts.merge(outcome + "." + reason, 1, Integer::sum);
        }

        int count(String key) {
            return counts.getOrDefault(key, 0);
        }
    }
}
