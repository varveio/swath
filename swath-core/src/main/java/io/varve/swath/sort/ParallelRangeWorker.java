/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.LongConsumer;
import org.slf4j.Logger;

/** Executes exactly one key range; owns no executor, global proof, or fleet failure domain. */
final class ParallelRangeWorker {

    private final SortRun run;
    private final SortConfig config;
    private final Comparator<ListEntry> comparator;
    private final EqualKeyPolicy equalKeyPolicy;
    private final SortMetrics metrics;
    private final SortedFileWriterFactory finalWriterFactory;
    private final RangeMergeTimer rangeTimer;
    private final Logger log;

    ParallelRangeWorker(SortRun run, Logger log) {
        this.run = run;
        this.config = run.config();
        this.comparator = run.comparator();
        this.equalKeyPolicy = run.equalKeyPolicy();
        this.metrics = run.metrics();
        this.finalWriterFactory = run.finalWriterFactory();
        this.rangeTimer = run.rangeMergeTimer();
        this.log = log;
    }

    Callable<Result> task(int range, byte[] lo, byte[] hi, List<Path> stagingSegments,
            Path stagingDir, int perRangeFanIn, LongConsumer safeProgress,
            DuplicateHook safeHook, PartOpener partOpener,
            Map<Path, PageRunSegmentDescriptor> descriptorsByPath,
            PageRunSeekPlan seekPlan, PageRunProofSpool.Writer proofSpool,
            Path proofSpoolPath) {
        return () -> {
            try (PageRunZoneVerifier.RangeBuilder proofBuilder =
                         new PageRunZoneVerifier.RangeBuilder(
                                 proofSpool, proofSpoolPath, range, seekPlan.segments().size())) {
                SortedFileWriterFactory rangeWriterFactory =
                        finalWriterFactory.forOutputSequence();
                long startNanos = System.nanoTime();
                List<RangeScopedPageFrontier> pageFrontiers = new ArrayList<>();
                DuplicateHook rangeHook = (previous, duplicate) -> {
                    if (inRange(duplicate.key().rawUnsafe(), lo, hi)) {
                        safeHook.onDuplicate(previous, duplicate);
                    }
                };
                PageRunSegmentWriter pageRunWriter =
                        new PageRunSegmentWriter(comparator, rangeHook, metrics, config.segmentCodec());
                PageRunMergeIo io = new PageRunMergeIo(run, pageRunWriter, stagingDir,
                        "merge-r" + range + "-", new KeyRange(lo, hi), descriptorsByPath,
                        pageFrontiers::add, range, seekPlan, proofBuilder);
                KWayMerge<Path> merge =
                        new KWayMerge<>(comparator, perRangeFanIn, io, rangeHook, metrics);

                List<Path> tmpParts = new ArrayList<>();
                List<SortedFileWriter> parts = new ArrayList<>();
                long rows;
                try (SortedCursor merged = new RangeFilteredCursor(
                        merge.merge(stagingSegments, safeProgress), lo, hi)) {
                    rows = RolledPartWriter.drainOpen(merged, config.finalFileBytes(),
                            () -> partOpener.open(stagingDir, range, tmpParts, rangeWriterFactory),
                            safeProgress, metrics, equalKeyPolicy, comparator, parts, false);
                } catch (IOException | RuntimeException e) {
                    try {
                        RolledPartWriter.closeQuietly(parts);
                    } catch (IOException closeFailure) {
                        e.addSuppressed(closeFailure);
                    }
                    throw e;
                }
                for (Path intermediate : io.intermediates()) {
                    Files.deleteIfExists(intermediate);
                }
                metrics.recordStealReason("SORT", "merge_range_parallel");

                long pagesKept = 0;
                long pagesSkipped = 0;
                long pagesUnread = 0;
                long pagesSeekedOver = 0;
                long bytesRead = 0;
                long indexBytesRead = 0;
                for (RangeScopedPageFrontier frontier : pageFrontiers) {
                    pagesKept += frontier.pagesKept();
                    pagesSkipped += frontier.pagesSkipped();
                    pagesUnread += frontier.pagesUnread();
                    pagesSeekedOver += frontier.pagesSeekedOver();
                    bytesRead += frontier.bytesRead();
                    indexBytesRead += frontier.indexBytesRead();
                }
                if (pagesSeekedOver + pagesSkipped + pagesUnread > 0) {
                    metrics.recordStealReason("SORT", "merge_range_page_skipped");
                }
                long rangeNanos = System.nanoTime() - startNanos;
                rangeTimer.recordRangeMerge(rangeNanos);
                log.info("sort_merge_range range={} rows={} pages_kept={} pages_skipped={} "
                                + "pages_unread={} pages_seeked_over={} bytes_read={} "
                                + "index_bytes_read={} duration_ms={}",
                        range, rows, pagesKept, pagesSkipped, pagesUnread, pagesSeekedOver,
                        bytesRead, indexBytesRead, rangeNanos / 1_000_000L);
                PageRunZoneVerifier.RangeSummary zoneSummary = proofBuilder.finish();
                return new Result(tmpParts, parts, rows, merge.mergePasses(),
                        merge.cascadedPasses(), merge.fastPathEmissions(), zoneSummary);
            }
        };
    }

    private static boolean inRange(byte[] key, byte[] lo, byte[] hi) {
        return (lo == null || KeyBytes.compareUnsigned(key, lo) >= 0)
                && (hi == null || KeyBytes.compareUnsigned(key, hi) < 0);
    }

    record Result(List<Path> tmpParts, List<SortedFileWriter> writers, long rows,
                  long mergePasses, long cascadedPasses, long fastPathEmissions,
                  PageRunZoneVerifier.RangeSummary zoneSummary) {
    }

    @FunctionalInterface
    interface PartOpener {
        SortedFileWriter open(Path stagingDir, int range, List<Path> tmpParts,
                SortedFileWriterFactory rangeWriterFactory) throws IOException;
    }
}
