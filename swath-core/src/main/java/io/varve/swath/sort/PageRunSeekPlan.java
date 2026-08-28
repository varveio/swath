/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Pre-worker range positioning plan over untrusted type-2 page-index hints.
 *
 * <p>Each segment retains only {@code O(R)} primitive seams. Sample keys exist only while the
 * extension cursor is being scanned; neither descriptors nor the plan retain a sample list. A seam
 * is not proof that its ordinal/offset/accounting is true. The range reader verifies its next frame,
 * and the coordinator later chains every physical zone from the fixed header to the trailer before
 * releasing any writer.
 */
final class PageRunSeekPlan {

    static final long NO_INDEX_ENTRY = -1L;

    /** One chosen type-2 entry, represented entirely by primitive extension fields. */
    record SeekSeam(long entryPayloadOffset, int sampleIndex, long pageOrdinal,
                    long frameOffset, long cumulativeEntries, long cumulativeFramedBytes) {
        boolean indexed() {
            return entryPayloadOffset != NO_INDEX_ENTRY;
        }
    }

    /** One range's physical proof interval and the index entries it owns. */
    record Zone(SeekSeam start, SeekSeam end, long samplePayloadOffset, int sampleCount) {
        boolean empty() {
            return start.pageOrdinal() == end.pageOrdinal();
        }
    }

    /** Primitive seams for one segment, indexed by range. */
    static final class SegmentPlan {
        private final PageRunSegmentDescriptor descriptor;
        private final int segmentIndex;
        private final long[] payloadOffsets;
        private final int[] sampleIndexes;
        private final long[] ordinals;
        private final long[] frameOffsets;
        private final long[] cumulativeEntries;
        private final long[] cumulativeFramedBytes;

        private SegmentPlan(PageRunSegmentDescriptor descriptor, int segmentIndex, int ranges) {
            this.descriptor = descriptor;
            this.segmentIndex = segmentIndex;
            this.payloadOffsets = new long[ranges];
            this.sampleIndexes = new int[ranges];
            this.ordinals = new long[ranges];
            this.frameOffsets = new long[ranges];
            this.cumulativeEntries = new long[ranges];
            this.cumulativeFramedBytes = new long[ranges];
            java.util.Arrays.fill(payloadOffsets, NO_INDEX_ENTRY);
            java.util.Arrays.fill(frameOffsets, PageRunSegmentWriter.HEADER_BYTES);
        }

        Path path() {
            return descriptor.path();
        }

        PageRunSegmentDescriptor descriptor() {
            return descriptor;
        }

        int segmentIndex() {
            return segmentIndex;
        }

        int ranges() {
            return ordinals.length;
        }

        SeekSeam start(int range) {
            return new SeekSeam(payloadOffsets[range], sampleIndexes[range], ordinals[range],
                    frameOffsets[range], cumulativeEntries[range], cumulativeFramedBytes[range]);
        }

        Zone zone(int range) {
            SeekSeam start = start(range);
            SeekSeam end = range + 1 < ranges()
                    ? start(range + 1)
                    : new SeekSeam(NO_INDEX_ENTRY, descriptor.extension().entryCount(),
                            descriptor.trailer().totalRecords(), descriptor.trailerStart(),
                            descriptor.trailer().totalEntries(), descriptor.trailerStart()
                                    - PageRunSegmentWriter.HEADER_BYTES);
            int sampleCount = start.indexed()
                    ? Math.max(0, end.sampleIndex() - start.sampleIndex())
                    : 0;
            return new Zone(start, end, start.entryPayloadOffset(), sampleCount);
        }

        PageRunPageIndex.EntryRead readTarget(PageRunSegmentIo io, int range) throws IOException {
            SeekSeam seam = start(range);
            if (!seam.indexed()) {
                return null;
            }
            PageRunPageIndex.EntryRead read = PageRunPageIndex.readEntryAt(
                    io, descriptor.extension(), seam.entryPayloadOffset());
            PageRunPageIndex.LocatedEntry located = read.located();
            PageRunPageIndex.IndexEntry entry = located.entry();
            if (located.payloadOffset() != seam.entryPayloadOffset()
                    || entry.pageOrdinal() != seam.pageOrdinal()
                    || entry.fileOffset() != seam.frameOffset()
                    || entry.cumulativeEntries() != seam.cumulativeEntries()
                    || entry.cumulativeFramedBytes() != seam.cumulativeFramedBytes()) {
                throw io.indexMismatch("page-index seek entry changed after planning", null);
            }
            return read;
        }

        private void set(int range, int sampleIndex, PageRunPageIndex.LocatedEntry located) {
            PageRunPageIndex.IndexEntry entry = located.entry();
            payloadOffsets[range] = located.payloadOffset();
            sampleIndexes[range] = sampleIndex;
            ordinals[range] = entry.pageOrdinal();
            frameOffsets[range] = entry.fileOffset();
            cumulativeEntries[range] = entry.cumulativeEntries();
            cumulativeFramedBytes[range] = entry.cumulativeFramedBytes();
        }
    }

    private final List<SegmentPlan> segments;
    private final Map<Path, SegmentPlan> byPath;
    private final int ranges;

    private PageRunSeekPlan(List<SegmentPlan> segments, int ranges) {
        this.segments = List.copyOf(segments);
        this.byPath = this.segments.stream().collect(Collectors.toUnmodifiableMap(
                SegmentPlan::path, segment -> segment, (first, duplicate) -> first));
        this.ranges = ranges;
    }

    static PageRunSeekPlan plan(List<PageRunSegmentDescriptor> descriptors,
                                List<byte[]> boundaries, SortMetrics metrics) throws IOException {
        int ranges = boundaries.size() + 1;
        List<SegmentPlan> planned = new ArrayList<>(descriptors.size());
        for (int segmentIndex = 0; segmentIndex < descriptors.size(); segmentIndex++) {
            MergeCancellation.check();
            PageRunSegmentDescriptor descriptor = descriptors.get(segmentIndex);
            SegmentPlan segment = new SegmentPlan(descriptor, segmentIndex, ranges);
            if (descriptor.extension().valid() && descriptor.extension().entryCount() > 0) {
                scanType2(segment, boundaries, metrics);
            }
            planned.add(segment);
        }
        return new PageRunSeekPlan(planned, ranges);
    }

    private static void scanType2(SegmentPlan segment, List<byte[]> boundaries,
                                  SortMetrics metrics) throws IOException {
        PageRunSegmentDescriptor descriptor = segment.descriptor();
        try (PageRunSegmentIo io = PageRunSegmentIo.open(descriptor.path(), SortMetrics.NO_OP)) {
            if (io.fileSize != descriptor.fileSize() || io.trailerStart != descriptor.trailerStart()) {
                throw new SegmentCorruptionException(descriptor.path(),
                        SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH,
                        "segment changed after its page index was validated");
            }
            PageRunPageIndex.Cursor cursor = PageRunPageIndex.cursor(io, descriptor.extension());
            int sampleIndex = 0;
            while (cursor.hasNext()) {
                MergeCancellation.check();
                PageRunPageIndex.LocatedEntry located = cursor.next();
                if (sampleIndex == 0) {
                    for (int range = 0; range < segment.ranges(); range++) {
                        segment.set(range, sampleIndex, located);
                    }
                } else {
                    byte[] prefixMax = located.entry().prefixMax();
                    for (int range = 1; range < segment.ranges(); range++) {
                        if (KeyBytes.compareUnsigned(prefixMax, boundaries.get(range - 1)) < 0) {
                            segment.set(range, sampleIndex, located);
                        }
                    }
                }
                sampleIndex++;
                metrics.markProgress();
            }
            metrics.recordRangeIndexBytes(cursor.bytesRead());
        }
    }

    List<SegmentPlan> segments() {
        return segments;
    }

    SegmentPlan segment(Path path) {
        return byPath.get(path);
    }

    int ranges() {
        return ranges;
    }
}
