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

    /** One range's exactly-once topology plus its closed variable-key spool. */
    record RangeSummary(int range, Path spool, int segments) {
    }

    /** Per-worker primitive collector with one exact-key cache backed by a temporary spool. */
    static final class RangeBuilder implements AutoCloseable {
        private final int range;
        private final int segments;
        private final Path spoolPath;
        private final PageRunProofSpool.Writer spool;
        private final boolean[] opened;
        private final boolean[] finished;
        private Tracker active;
        private byte[] activeZoneMax;
        private byte[] activeRollingPrefix;
        private boolean handedOff;

        RangeBuilder(PageRunProofSpool.Writer spool, Path spoolPath,
                     int range, int segments) {
            this.range = range;
            this.segments = segments;
            this.spoolPath = spoolPath;
            this.spool = spool;
            this.opened = new boolean[segments];
            this.finished = new boolean[segments];
        }

        Tracker open(PageRunSeekPlan.SegmentPlan plan, PageFrontierReader reader,
                     SortMetrics metrics) throws IOException {
            int index = plan.segmentIndex();
            if (opened[index]) {
                throw new IOException("page-run zone opened original segment more than once: "
                        + plan.path());
            }
            opened[index] = true;
            spool.markOpen(slot(index));
            return new Tracker(plan, range, reader, metrics, this);
        }

        RangeSummary finish() throws IOException {
            for (int i = 0; i < segments; i++) {
                if (!opened[i] || !finished[i]) {
                    throw new IOException("page-run zone proof is missing original segment summary "
                            + i + " for range " + range);
                }
            }
            flushActive();
            handedOff = true;
            return new RangeSummary(range, spoolPath, segments);
        }

        void writeKey(Tracker tracker, PageRunProofSpool.KeyField field, byte[] key)
                throws IOException {
            activate(tracker);
            spool.writeKey(tracker.spoolIndex(), field, key);
        }

        byte[] zoneMax(Tracker tracker) throws IOException {
            activate(tracker);
            return activeZoneMax;
        }

        void zoneMax(Tracker tracker, byte[] key) throws IOException {
            activate(tracker);
            activeZoneMax = key.clone();
        }

        byte[] rollingPrefix(Tracker tracker) throws IOException {
            activate(tracker);
            return activeRollingPrefix;
        }

        void rollingPrefix(Tracker tracker, byte[] key) throws IOException {
            activate(tracker);
            activeRollingPrefix = key.clone();
        }

        void complete(Tracker tracker, long pages, long entries, long framedBytes,
                      long firstFrameOffset, long endFrameOffset, int verifiedSamples,
                      boolean mismatch) throws IOException {
            activate(tracker);
            flushActive();
            spool.finish(tracker.spoolIndex(), pages, entries, framedBytes,
                    firstFrameOffset, endFrameOffset, verifiedSamples, mismatch);
            finished[tracker.segmentIndex()] = true;
            active = null;
            activeZoneMax = null;
            activeRollingPrefix = null;
        }

        private void activate(Tracker tracker) throws IOException {
            if (active == tracker) {
                return;
            }
            flushActive();
            active = tracker;
            activeZoneMax = tracker.zoneMaxStored
                    ? spool.readKey(tracker.spoolIndex(), PageRunProofSpool.KeyField.ZONE_MAX)
                    : null;
            activeRollingPrefix = tracker.rollingPrefixStored
                    ? spool.readKey(tracker.spoolIndex(),
                            PageRunProofSpool.KeyField.ROLLING_SAMPLE_PREFIX)
                    : null;
        }

        private void flushActive() throws IOException {
            if (active == null) {
                return;
            }
            if (activeZoneMax != null) {
                spool.writeKey(active.spoolIndex(), PageRunProofSpool.KeyField.ZONE_MAX,
                        activeZoneMax);
                active.zoneMaxStored = true;
            }
            if (activeRollingPrefix != null) {
                spool.writeKey(active.spoolIndex(),
                        PageRunProofSpool.KeyField.ROLLING_SAMPLE_PREFIX, activeRollingPrefix);
                active.rollingPrefixStored = true;
            }
        }

        @Override
        public void close() throws IOException {
            if (handedOff) {
                return;
            }
            flushActive();
        }

        private int slot(int segment) {
            return Math.addExact(Math.multiplyExact(range, segments), segment);
        }
    }

    /** Streaming sample validator; all retained fields are primitive. */
    static final class Tracker {
        private final PageRunSeekPlan.SegmentPlan plan;
        private final PageRunSeekPlan.Zone zone;
        private final PageFrontierReader reader;
        private final RangeBuilder builder;
        private final long sampleStride;
        private long nextSampleOffset;
        private int samplesRemaining;
        private long pages;
        private long entries;
        private long framedBytes;
        private long firstFrameOffset = -1;
        private long endFrameOffset = -1;
        private int verifiedSamples;
        private boolean sampleMismatch;
        private boolean zoneMaxStored;
        private boolean rollingPrefixStored;
        private boolean finished;

        private Tracker(PageRunSeekPlan.SegmentPlan plan, int range, PageFrontierReader reader,
                        SortMetrics metrics, RangeBuilder builder) {
            this.plan = plan;
            this.zone = plan.zone(range);
            this.reader = reader;
            this.builder = builder;
            this.sampleStride = PageRunBoundarySample.stride(
                    plan.descriptor().trailer().totalRecords());
            this.nextSampleOffset = zone.samplePayloadOffset();
            this.samplesRemaining = zone.sampleCount();
        }

        int segmentIndex() {
            return plan.segmentIndex();
        }

        int spoolIndex() {
            return builder.slot(segmentIndex());
        }

        void observe(long ordinal, long frameOffset, long cumulativeEntries,
                     long cumulativeFramedBytes, int pageFramedBytes,
                     byte[] minKey, byte[] maxKey, int count) throws IOException {
            if (ordinal == zone.end().pageOrdinal() && endFrameOffset < 0) {
                endFrameOffset = frameOffset;
            }
            if (ordinal < zone.start().pageOrdinal()) {
                sampleMismatch = true;
                return;
            }
            if (ordinal >= zone.end().pageOrdinal()) {
                return;
            }
            if (ordinal != zone.start().pageOrdinal() + pages) {
                sampleMismatch = true;
            }
            if (pages == 0) {
                firstFrameOffset = frameOffset;
                builder.writeKey(this, PageRunProofSpool.KeyField.FIRST_MIN, minKey);
            }
            builder.writeKey(this, PageRunProofSpool.KeyField.LAST_MIN, minKey);
            byte[] zoneMaximum = builder.zoneMax(this);
            if (zoneMaximum == null || Arrays.compareUnsigned(maxKey, zoneMaximum) > 0) {
                builder.zoneMax(this, maxKey);
            }
            byte[] rolling = builder.rollingPrefix(this);
            if (rolling != null && Arrays.compareUnsigned(maxKey, rolling) > 0) {
                builder.rollingPrefix(this, maxKey);
            }

            long expectedSampleOrdinal = samplesRemaining == 0 ? -1
                    : (long) (zone.start().sampleIndex() + verifiedSamples) * sampleStride;
            if (ordinal == expectedSampleOrdinal) {
                validateSample(ordinal, frameOffset, cumulativeEntries,
                        cumulativeFramedBytes, minKey, maxKey);
            } else if (expectedSampleOrdinal >= 0 && expectedSampleOrdinal < ordinal) {
                sampleMismatch = true;
            }
            pages++;
            entries += count;
            framedBytes += pageFramedBytes;
        }

        void exhausted(long physicalOffset) {
            if (zone.end().pageOrdinal() == plan.descriptor().trailer().totalRecords()
                    && endFrameOffset < 0) {
                endFrameOffset = physicalOffset;
            }
        }

        void finish() throws IOException {
            if (finished) {
                return;
            }
            finished = true;
            if (samplesRemaining != 0 || verifiedSamples != zone.sampleCount()) {
                sampleMismatch = true;
            }
            builder.complete(this, pages, entries, framedBytes, firstFrameOffset,
                    endFrameOffset, verifiedSamples, sampleMismatch);
        }

        private void validateSample(long ordinal, long frameOffset, long cumulativeEntries,
                                    long cumulativeFramedBytes, byte[] minKey, byte[] maxKey)
                throws IOException {
            PageRunPageIndex.EntryRead read = reader.readIndexEntry(
                    plan.descriptor().extension(), nextSampleOffset);
            nextSampleOffset += read.bytesRead();
            samplesRemaining--;
            PageRunPageIndex.IndexEntry sample = read.located().entry();
            if (sample.pageOrdinal() != ordinal || sample.fileOffset() != frameOffset
                    || sample.cumulativeEntries() != zone.start().cumulativeEntries() + entries
                    || sample.cumulativeFramedBytes() != cumulativeFramedBytes
                    || !Arrays.equals(sample.minKey(), minKey)) {
                sampleMismatch = true;
            }
            if (verifiedSamples == 0) {
                builder.writeKey(this, PageRunProofSpool.KeyField.FIRST_SAMPLE_PREFIX,
                        sample.prefixMax());
                builder.writeKey(this, PageRunProofSpool.KeyField.FIRST_SAMPLE_PAGE_MAX, maxKey);
                if (Arrays.compareUnsigned(sample.prefixMax(), maxKey) < 0) {
                    sampleMismatch = true;
                }
                builder.rollingPrefix(this, sample.prefixMax());
            } else if (!Arrays.equals(sample.prefixMax(), builder.rollingPrefix(this))) {
                sampleMismatch = true;
            }
            verifiedSamples++;
        }
    }

    static void verify(PageRunSeekPlan plan, List<RangeSummary> supplied,
                       SortMetrics metrics) throws IOException {
        MergeCancellation.check();
        PageRunProofSpool.Reader reader = null;
        Throwable primary = null;
        try {
            RangeSummary[] ranges = topology(plan, supplied);
            reader = new PageRunProofSpool.Reader(ranges[0].spool());
            for (PageRunSeekPlan.SegmentPlan segment : plan.segments()) {
                MergeCancellation.check();
                verifySegment(segment, plan.ranges(), plan.segments().size(), reader, metrics);
            }
            metrics.recordStealReason("SORT", "merge_zone_proof_complete");
        } catch (IOException | RuntimeException e) {
            primary = e;
            throw e;
        } finally {
            IOException failure = null;
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    failure = append(failure, e);
                }
            }
            for (RangeSummary summary : supplied) {
                if (summary != null && summary.spool() != null) {
                    try {
                        PageRunProofSpool.delete(summary.spool());
                    } catch (IOException e) {
                        failure = append(failure, e);
                    }
                }
            }
            if (failure != null && primary != null) {
                primary.addSuppressed(failure);
            } else if (failure != null) {
                throw failure;
            }
        }
    }

    private static RangeSummary[] topology(PageRunSeekPlan plan, List<RangeSummary> supplied)
            throws IOException {
        if (supplied.size() != plan.ranges()) {
            throw new IOException("parallel range merge returned " + supplied.size()
                    + " zone summaries for " + plan.ranges() + " planned ranges");
        }
        RangeSummary[] ordered = new RangeSummary[plan.ranges()];
        for (RangeSummary summary : supplied) {
            if (summary == null || summary.range() < 0 || summary.range() >= plan.ranges()
                    || summary.segments() != plan.segments().size()
                    || ordered[summary.range()] != null) {
                throw new IOException("parallel range merge returned missing, duplicate, or invalid "
                        + "zone range topology");
            }
            ordered[summary.range()] = summary;
        }
        for (RangeSummary summary : ordered) {
            if (summary == null) {
                throw new IOException("parallel range merge returned incomplete zone range topology");
            }
            if (!summary.spool().equals(ordered[0].spool())) {
                throw new IOException("parallel range merge returned multiple proof spool identities");
            }
        }
        return ordered;
    }

    private static void verifySegment(PageRunSeekPlan.SegmentPlan plan,
                                      int ranges, int segmentCount, PageRunProofSpool.Reader reader,
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
        for (int range = 0; range < ranges; range++) {
            MergeCancellation.check();
            PageRunSeekPlan.Zone zone = plan.zone(range);
            long zonePages = zone.end().pageOrdinal() - zone.start().pageOrdinal();
            int spoolIndex = Math.addExact(Math.multiplyExact(range, segmentCount),
                    plan.segmentIndex());
            PageRunProofSpool.Summary summary = reader.read(spoolIndex,
                    zonePages > 0, zone.sampleCount() > 0);
            if (summary.sampleMismatch() || summary.verifiedSamples() != zone.sampleCount()) {
                throw indexMismatch(plan.path(), "range " + range
                        + " did not verify every owned page-index sample", metrics);
            }
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
                if (zone.sampleCount() > 0
                        && !Arrays.equals(max(globalMax, summary.firstSamplePageMax()),
                                summary.firstSamplePrefix())) {
                    throw indexMismatch(plan.path(), "range " + range
                            + " first sample prefixMax disagrees with prior physical zones", metrics);
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

    private static IOException append(IOException first, IOException next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }
}
