/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ByteMidpoint;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Header-to-trailer physical-zone proof for one parallel page-run merge. */
final class PageRunZoneVerifier {

    @FunctionalInterface
    interface ProofReaderFactory {
        PageRunProofSpool.Reader open(Path path, int expectedSlots,
                                      PageRunProofSpool.Stats stats) throws IOException;
    }

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
        private final KeyCache activeLastMin = new KeyCache();
        private final KeyCache activeZoneMax = new KeyCache();
        private final KeyCache activeRollingPrefix = new KeyCache();
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

        int compareZoneMax(Tracker tracker, byte[] key) throws IOException {
            activate(tracker);
            return activeZoneMax.compareTo(key);
        }

        void zoneMax(Tracker tracker, byte[] key) throws IOException {
            activate(tracker);
            activeZoneMax.set(key);
        }

        boolean hasRollingPrefix(Tracker tracker) throws IOException {
            activate(tracker);
            return activeRollingPrefix.present();
        }

        boolean rollingPrefixEquals(Tracker tracker, byte[] key) throws IOException {
            activate(tracker);
            return activeRollingPrefix.equalsKey(key);
        }

        int compareRollingPrefix(Tracker tracker, byte[] key) throws IOException {
            activate(tracker);
            return activeRollingPrefix.compareTo(key);
        }

        void rollingPrefix(Tracker tracker, byte[] key) throws IOException {
            activate(tracker);
            activeRollingPrefix.set(key);
        }

        void lastMin(Tracker tracker, byte[] key) throws IOException {
            activate(tracker);
            activeLastMin.set(key);
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
            activeLastMin.clear();
            activeZoneMax.clear();
            activeRollingPrefix.clear();
        }

        private void activate(Tracker tracker) throws IOException {
            if (active == tracker) {
                return;
            }
            flushActive();
            active = tracker;
            load(activeLastMin, tracker.lastMinStored, tracker,
                    PageRunProofSpool.KeyField.LAST_MIN);
            load(activeZoneMax, tracker.zoneMaxStored, tracker,
                    PageRunProofSpool.KeyField.ZONE_MAX);
            load(activeRollingPrefix, tracker.rollingPrefixStored, tracker,
                    PageRunProofSpool.KeyField.ROLLING_SAMPLE_PREFIX);
        }

        private void flushActive() throws IOException {
            if (active == null) {
                return;
            }
            if (activeLastMin.present()) {
                spool.writeKey(active.spoolIndex(), PageRunProofSpool.KeyField.LAST_MIN,
                        activeLastMin.bytes(), activeLastMin.length());
                active.lastMinStored = true;
            }
            if (activeZoneMax.present()) {
                spool.writeKey(active.spoolIndex(), PageRunProofSpool.KeyField.ZONE_MAX,
                        activeZoneMax.bytes(), activeZoneMax.length());
                active.zoneMaxStored = true;
            }
            if (activeRollingPrefix.present()) {
                spool.writeKey(active.spoolIndex(),
                        PageRunProofSpool.KeyField.ROLLING_SAMPLE_PREFIX,
                        activeRollingPrefix.bytes(), activeRollingPrefix.length());
                active.rollingPrefixStored = true;
            }
        }

        private void load(KeyCache cache, boolean stored, Tracker tracker,
                          PageRunProofSpool.KeyField field) throws IOException {
            cache.clear();
            if (stored) {
                cache.setLength(spool.readKey(tracker.spoolIndex(), field, cache.bytes()));
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

        /** Reusable exact-key storage: three fixed buffers per active range, never per page. */
        private static final class KeyCache {
            private final byte[] bytes = new byte[ByteMidpoint.MAX_KEY_LEN];
            private int length = -1;

            byte[] bytes() {
                return bytes;
            }

            int length() {
                return length;
            }

            boolean present() {
                return length >= 0;
            }

            void clear() {
                length = -1;
            }

            void set(byte[] key) {
                System.arraycopy(key, 0, bytes, 0, key.length);
                length = key.length;
            }

            void setLength(int length) {
                this.length = length;
            }

            int compareTo(byte[] key) {
                return present()
                        ? Arrays.compareUnsigned(bytes, 0, length, key, 0, key.length)
                        : -1;
            }

            boolean equalsKey(byte[] key) {
                return present() && length == key.length
                        && Arrays.equals(bytes, 0, length, key, 0, key.length);
            }
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
        private boolean lastMinStored;
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
            builder.lastMin(this, minKey);
            if (builder.compareZoneMax(this, maxKey) < 0) {
                builder.zoneMax(this, maxKey);
            }
            if (builder.hasRollingPrefix(this)
                    && builder.compareRollingPrefix(this, maxKey) < 0) {
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
            } else if (!builder.rollingPrefixEquals(this, sample.prefixMax())) {
                sampleMismatch = true;
            }
            verifiedSamples++;
        }
    }

    static void verify(PageRunSeekPlan plan, List<RangeSummary> supplied,
                       SortMetrics metrics) throws IOException {
        PageRunProofSpool.Stats stats = new PageRunProofSpool.Stats(metrics);
        verify(plan, supplied, metrics, PageRunProofSpool.Reader::new, stats);
    }

    static void verify(PageRunSeekPlan plan, List<RangeSummary> supplied,
                       SortMetrics metrics, ProofReaderFactory readerFactory,
                       PageRunProofSpool.Stats stats) throws IOException {
        MergeCancellation.check();
        PageRunProofSpool.Reader reader = null;
        Throwable primary = null;
        boolean verified = false;
        try {
            RangeSummary[] ranges = topology(plan, supplied);
            int expectedSlots = Math.multiplyExact(plan.ranges(), plan.segments().size());
            reader = readerFactory.open(ranges[0].spool(), expectedSlots, stats);
            for (PageRunSeekPlan.SegmentPlan segment : plan.segments()) {
                MergeCancellation.check();
                verifySegment(segment, plan.ranges(), plan.segments().size(), reader, metrics);
            }
            metrics.recordStealReason("SORT", "merge_zone_proof_complete");
            verified = true;
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
            // A fully verified spool becomes a publisher-owned disposable intermediate. Deleting it
            // here would turn a harmless unlink failure into a pre-publication merge failure and
            // force an expensive replay. Proof or reader-close failures remain pre-publication and
            // clean their spools immediately.
            if (!verified || failure != null) {
                Set<Path> spools = new LinkedHashSet<>();
                for (RangeSummary summary : supplied) {
                    if (summary != null && summary.spool() != null) {
                        spools.add(summary.spool());
                    }
                }
                for (Path spool : spools) {
                    try {
                        PageRunProofSpool.delete(spool, stats);
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
                throw physicalZoneMismatch(plan.path(), zone, "range " + range
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

    private static SegmentCorruptionException physicalZoneMismatch(
            Path path, PageRunSeekPlan.Zone zone, String message, SortMetrics metrics) {
        if (zone.start().indexed() || zone.end().indexed()) {
            return indexMismatch(path, message, metrics);
        }
        return new SegmentCorruptionException(path,
                SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION, message);
    }

    private static IOException append(IOException first, IOException next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }
}
