/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/** Header-to-trailer physical-zone proof for one parallel page-run merge. */
final class PageRunZoneVerifier {

    private PageRunZoneVerifier() {
    }

    /** One range's exactly-once summary, aligned to the seek plan's segment order. */
    record RangeSummary(int range, SegmentSummary[] segments) {
        RangeSummary {
            segments = segments.clone();
        }
    }

    /** Facts observed while this range read its owned physical zone of one segment. */
    record SegmentSummary(Path path, PageRunSeekPlan.Zone zone, long pages, long entries,
                          long framedBytes, long firstFrameOffset, long endFrameOffset,
                          byte[] firstMin, byte[] lastMin, byte[] zoneMax,
                          int verifiedSamples, String sampleMismatch,
                          byte[] firstSamplePrefixMax, byte[] firstSamplePageMax) {
    }

    /** Per-worker collector. Original segments must each register and close exactly once. */
    static final class RangeBuilder {
        private final int range;
        private final SegmentSummary[] summaries;
        private final boolean[] opened;

        RangeBuilder(int range, int segments) {
            this.range = range;
            this.summaries = new SegmentSummary[segments];
            this.opened = new boolean[segments];
        }

        Tracker open(PageRunSeekPlan.SegmentPlan plan, PageFrontierReader reader,
                     SortMetrics metrics) throws IOException {
            int index = plan.segmentIndex();
            if (opened[index]) {
                throw new IOException("page-run zone opened original segment more than once: "
                        + plan.path());
            }
            opened[index] = true;
            return new Tracker(plan, range, reader, metrics,
                    summary -> summaries[index] = summary);
        }

        RangeSummary finish() throws IOException {
            for (int i = 0; i < summaries.length; i++) {
                if (!opened[i] || summaries[i] == null) {
                    throw new IOException("page-run zone proof is missing original segment summary "
                            + i + " for range " + range);
                }
            }
            return new RangeSummary(range, summaries);
        }
    }

    @FunctionalInterface
    private interface SummarySink {
        void accept(SegmentSummary summary);
    }

    /** Streaming validator for the samples owned by one zone. */
    static final class Tracker {
        private final PageRunSeekPlan.SegmentPlan plan;
        private final PageRunSeekPlan.Zone zone;
        private final PageRunPageIndex.Cursor samples;
        private final int expectedSamples;
        private final SortMetrics metrics;
        private final SummarySink sink;
        private PageRunPageIndex.IndexEntry nextSample;
        private long pages;
        private long entries;
        private long framedBytes;
        private long firstFrameOffset = -1;
        private long endFrameOffset = -1;
        private byte[] firstMin;
        private byte[] lastMin;
        private byte[] zoneMax;
        private byte[] rollingSamplePrefix;
        private byte[] firstSamplePrefix;
        private byte[] firstSamplePageMax;
        private int verifiedSamples;
        private String sampleMismatch;
        private boolean finished;

        private Tracker(PageRunSeekPlan.SegmentPlan plan, int range, PageFrontierReader reader,
                        SortMetrics metrics, SummarySink sink) throws IOException {
            this.plan = plan;
            this.zone = plan.zone(range);
            this.expectedSamples = zone.sampleCount();
            this.metrics = metrics;
            this.sink = sink;
            this.samples = expectedSamples == 0 ? null : reader.indexCursor(
                    plan.descriptor().extension(), zone.samplePayloadOffset(), expectedSamples);
            loadNextSample();
        }

        void observe(PageRunSegmentIo.PagePosition position, byte[] minKey, byte[] maxKey,
                     int count) throws IOException {
            long ordinal = position.pageOrdinal();
            if (ordinal == zone.end().pageOrdinal() && endFrameOffset < 0) {
                endFrameOffset = position.frameOffset();
            }
            if (ordinal < zone.start().pageOrdinal()) {
                mismatch("zone observed page ordinal " + ordinal + " below planned start "
                        + zone.start().pageOrdinal());
                return;
            }
            if (ordinal >= zone.end().pageOrdinal()) {
                return;
            }

            long expectedOrdinal = zone.start().pageOrdinal() + pages;
            if (ordinal != expectedOrdinal) {
                mismatch("zone page ordinal jumped from " + expectedOrdinal + " to " + ordinal);
            }
            if (pages == 0) {
                firstFrameOffset = position.frameOffset();
                firstMin = minKey.clone();
            }
            lastMin = minKey.clone();
            if (zoneMax == null || Arrays.compareUnsigned(maxKey, zoneMax) > 0) {
                zoneMax = maxKey.clone();
            }

            if (rollingSamplePrefix != null
                    && Arrays.compareUnsigned(maxKey, rollingSamplePrefix) > 0) {
                rollingSamplePrefix = maxKey.clone();
            }
            if (nextSample != null && nextSample.pageOrdinal() == ordinal) {
                validateSample(position, minKey, maxKey);
                loadNextSample();
            } else if (nextSample != null && nextSample.pageOrdinal() < ordinal) {
                mismatch("zone passed indexed sample ordinal " + nextSample.pageOrdinal());
            }

            pages++;
            entries += count;
            framedBytes += position.framedBytes();
        }

        void exhausted(long physicalOffset) {
            if (zone.end().pageOrdinal() == plan.descriptor().trailer().totalRecords()
                    && endFrameOffset < 0) {
                endFrameOffset = physicalOffset;
            }
        }

        void finish() {
            if (finished) {
                return;
            }
            finished = true;
            if (nextSample != null || verifiedSamples != expectedSamples) {
                mismatch("zone verified " + verifiedSamples + " of " + expectedSamples
                        + " owned page-index samples");
            }
            sink.accept(new SegmentSummary(plan.path(), zone, pages, entries, framedBytes,
                    firstFrameOffset, endFrameOffset, firstMin, lastMin, zoneMax,
                    verifiedSamples, sampleMismatch, firstSamplePrefix, firstSamplePageMax));
        }

        private void validateSample(PageRunSegmentIo.PagePosition position, byte[] minKey,
                                    byte[] maxKey) {
            PageRunPageIndex.IndexEntry sample = nextSample;
            long localEntriesBefore = entries;
            if (sample.pageOrdinal() != position.pageOrdinal()
                    || sample.fileOffset() != position.frameOffset()
                    || sample.cumulativeEntries()
                            != zone.start().cumulativeEntries() + localEntriesBefore
                    || sample.cumulativeFramedBytes() != position.cumulativeFramedBytes()
                    || !Arrays.equals(sample.minKey(), minKey)) {
                mismatch("sample fields disagree at ordinal " + position.pageOrdinal());
            }
            if (verifiedSamples == 0) {
                firstSamplePrefix = sample.prefixMax().clone();
                firstSamplePageMax = maxKey.clone();
                if (Arrays.compareUnsigned(sample.prefixMax(), maxKey) < 0) {
                    mismatch("sample prefixMax is below its page max at ordinal "
                            + position.pageOrdinal());
                }
                rollingSamplePrefix = sample.prefixMax().clone();
            } else if (!Arrays.equals(sample.prefixMax(), rollingSamplePrefix)) {
                mismatch("sample prefixMax disagrees at ordinal " + position.pageOrdinal());
            }
            verifiedSamples++;
        }

        private void loadNextSample() throws IOException {
            if (samples == null || !samples.hasNext()) {
                nextSample = null;
                return;
            }
            try {
                nextSample = samples.next().entry();
            } catch (IOException e) {
                metrics.recordStealReason("SORT", "page_run_index_mismatch");
                throw new SegmentCorruptionException(plan.path(),
                        SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH,
                        "cannot read planned page-index proof entry", e);
            }
        }

        private void mismatch(String message) {
            if (sampleMismatch == null) {
                sampleMismatch = message;
            }
        }
    }

    static void verify(PageRunSeekPlan plan, List<RangeSummary> ranges,
                       SortMetrics metrics) throws IOException {
        MergeCancellation.check();
        int expectedRanges = plan.segments().isEmpty()
                ? ranges.size() : plan.segments().getFirst().ranges();
        if (ranges.size() != expectedRanges) {
            throw new IOException("parallel range merge returned " + ranges.size()
                    + " zone summaries for " + expectedRanges + " planned ranges");
        }
        for (int range = 0; range < expectedRanges; range++) {
            RangeSummary summary = ranges.get(range);
            if (summary == null || summary.range() != range
                    || summary.segments().length != plan.segments().size()) {
                throw new IOException("parallel range merge returned an invalid zone summary for range "
                        + range);
            }
        }
        for (PageRunSeekPlan.SegmentPlan segment : plan.segments()) {
            MergeCancellation.check();
            verifySegment(segment, ranges, metrics);
        }
        metrics.recordStealReason("SORT", "merge_zone_proof_complete");
    }

    private static void verifySegment(PageRunSeekPlan.SegmentPlan plan,
                                      List<RangeSummary> ranges,
                                      SortMetrics metrics) throws IOException {
        PageRunTrailer.Trailer trailer = plan.descriptor().trailer();
        long pages = 0;
        long entries = 0;
        long framedBytes = 0;
        long expectedOffset = PageRunSegmentWriter.HEADER_BYTES;
        int verifiedSamples = 0;
        byte[] firstMin = null;
        byte[] previousLastMin = null;
        byte[] globalMax = null;

        for (int range = 0; range < ranges.size(); range++) {
            MergeCancellation.check();
            SegmentSummary summary = ranges.get(range).segments()[plan.segmentIndex()];
            PageRunSeekPlan.Zone zone = plan.zone(range);
            requireSameZone(plan.path(), zone, summary.zone(), metrics);
            if (summary.sampleMismatch() != null
                    || summary.verifiedSamples() != zone.sampleCount()) {
                throw indexMismatch(plan.path(), "range " + range + ": "
                        + (summary.sampleMismatch() == null
                                ? "sample verification count mismatch"
                                : summary.sampleMismatch()), metrics);
            }
            long zonePages = zone.end().pageOrdinal() - zone.start().pageOrdinal();
            if (summary.pages() != zonePages
                    || summary.firstFrameOffset() != (zonePages == 0
                            ? -1 : zone.start().frameOffset())
                    || summary.endFrameOffset() != zone.end().frameOffset()
                    || zone.start().frameOffset() != expectedOffset
                    || zone.start().cumulativeEntries() != entries
                    || zone.start().cumulativeFramedBytes() != framedBytes
                    || summary.framedBytes()
                            != zone.end().frameOffset() - zone.start().frameOffset()) {
                throw indexMismatch(plan.path(), "range " + range
                        + " does not tile its planned physical zone", metrics);
            }

            if (zonePages > 0) {
                if (firstMin == null) {
                    firstMin = summary.firstMin();
                }
                if (previousLastMin != null
                        && Arrays.compareUnsigned(summary.firstMin(), previousLastMin) < 0) {
                    metrics.recordStealReason("SORT", "page_run_min_regression");
                    throw new SegmentCorruptionException(plan.path(),
                            SegmentCorruptionException.PAGE_RUN_MIN_REGRESSION,
                            "page minKey regressed across physical zone seam before range " + range);
                }
                if (zone.sampleCount() > 0) {
                    byte[] expectedPrefix = max(globalMax, summary.firstSamplePageMax());
                    if (!Arrays.equals(expectedPrefix, summary.firstSamplePrefixMax())) {
                        throw indexMismatch(plan.path(), "range " + range
                                + " first sample prefixMax disagrees with prior physical zones", metrics);
                    }
                }
                previousLastMin = summary.lastMin();
                globalMax = max(globalMax, summary.zoneMax());
            }
            pages += summary.pages();
            entries += summary.entries();
            framedBytes += summary.framedBytes();
            expectedOffset = zone.end().frameOffset();
            verifiedSamples += summary.verifiedSamples();
        }

        long expectedFramedBytes = plan.descriptor().trailerStart()
                - PageRunSegmentWriter.HEADER_BYTES;
        if (pages != trailer.totalRecords() || entries != trailer.totalEntries()
                || framedBytes != expectedFramedBytes
                || expectedOffset != plan.descriptor().trailerStart()
                || !Arrays.equals(emptyIfNull(firstMin), trailer.segMinKey())
                || !Arrays.equals(emptyIfNull(globalMax), trailer.segMaxKey())) {
            throw new SegmentCorruptionException(plan.path(),
                    SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                    "physical zone totals or bounds disagree with the page-run trailer");
        }
        if (plan.descriptor().extension().valid()
                && verifiedSamples != plan.descriptor().extension().entryCount()) {
            throw indexMismatch(plan.path(), "physical zones verified " + verifiedSamples
                    + " of " + plan.descriptor().extension().entryCount() + " index samples", metrics);
        }
    }

    private static void requireSameZone(Path path, PageRunSeekPlan.Zone expected,
                                        PageRunSeekPlan.Zone actual,
                                        SortMetrics metrics) throws IOException {
        if (!expected.equals(actual)) {
            throw indexMismatch(path, "worker returned a different physical zone than planned", metrics);
        }
    }

    private static byte[] max(byte[] first, byte[] second) {
        if (first == null) {
            return second == null ? null : second.clone();
        }
        if (second == null || Arrays.compareUnsigned(first, second) >= 0) {
            return first;
        }
        return second.clone();
    }

    private static byte[] emptyIfNull(byte[] key) {
        return key == null ? new byte[0] : key;
    }

    private static SegmentCorruptionException indexMismatch(Path path, String message,
                                                            SortMetrics metrics) {
        metrics.recordStealReason("SORT", "page_run_index_mismatch");
        return new SegmentCorruptionException(path,
                SegmentCorruptionException.PAGE_RUN_INDEX_MISMATCH, message);
    }
}
