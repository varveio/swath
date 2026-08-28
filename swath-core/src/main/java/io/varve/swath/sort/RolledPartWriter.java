/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.LongConsumer;

/**
 * The shared publish-roll loop behind both merge paths' final streaming pass: drain a merged sorted
 * cursor into a rolled sequence of files, starting a fresh file after the current one reaches the
 * roll threshold <b>and</b> the next row begins a different raw-key group. Equal-key rows are an
 * indivisible final-file atom: a version cluster may therefore carry a file beyond the target, but
 * adjacent files always remain strictly key-disjoint. The loop also feeds merge-progress in batches
 * of {@link KWayMerge#PROGRESS_BATCH_ROWS} (never per-row — §3.2). The serial publish ({@link
 * SortTransform}) and the parallel range-merge ({@link ParallelRangeMerge}) share this identical
 * control loop, roll math, and progress cadence; they differ in exactly two ways, both parameters
 * here:
 *
 * <ul>
 *   <li>{@code fileFactory} — where each rolled file is opened (the serial path writes final
 *       {@code part-*} tmps; each parallel range writes its own range-local {@code prange-*} tmps).</li>
 *   <li>{@code markFinalOnLast} — whether this call can stamp global completeness itself. The serial
 *       path ({@code true}) knows its own last file, so it force-publishes one valid empty file for an
 *       empty listing and marks the last file final. A parallel range does not: its parts' positions in
 *       the output depend on the other ranges, so it uses {@link #drainOpen} instead and
 *       {@link SortTransform} stamps once every range has drained.</li>
 * </ul>
 */
final class RolledPartWriter {

    /** Opens the next rolled file, already registered with the caller's own output bookkeeping. */
    @FunctionalInterface
    interface FileFactory {
        SortedFileWriter open() throws IOException;
    }

    private RolledPartWriter() {
    }

    /**
     * Drain {@code merged} into rolled files opened by {@code fileFactory}, rolling to a fresh file
     * after the current one reaches {@code finalFileBytes} and the next row begins a distinct raw-key
     * group; returns the total row count. Feeds {@code progressCallback} one batch per {@link
     * KWayMerge#PROGRESS_BATCH_ROWS} rows written, with any remainder flushed once the cursor is fully
     * drained. {@code metrics} records one bounded deferral signal per equal-key group that crosses
     * the soft target.
     */
    static long drain(SortedCursor merged, long finalFileBytes, FileFactory fileFactory,
                      boolean markFinalOnLast, LongConsumer progressCallback, SortMetrics metrics,
                      EqualKeyPolicy equalKeyPolicy, Comparator<ListEntry> comparator)
            throws IOException {
        List<SortedFileWriter> open = new ArrayList<>();
        try {
            // closeRolledAway=true: this path knows each file's global index when it opens it, so only
            // the LAST writer has anything left to decide (markFinal). `open` therefore holds at most
            // one writer, exactly as before this loop was shared with the parallel path.
            long totalRows =
                    drainOpen(merged, finalFileBytes, fileFactory, progressCallback, metrics,
                            equalKeyPolicy, comparator, open, true);
            if (markFinalOnLast) {
                if (open.isEmpty()) {
                    // Empty listing: still publish one valid, self-describing empty sorted file.
                    open.add(fileFactory.open());
                }
                // The last writer here is genuinely the LAST file of this publish: no further roll
                // happens after this point. Mark it before close() (which is when the footer, and so
                // the stamp, is actually written). An exceptional exit below deliberately skips this:
                // an aborted run's last-written file is NOT the true final file and must not claim to be.
                open.get(open.size() - 1).markFinal();
            }
            closeInOrder(open);
            open.clear();
            return totalRows;
        } catch (IOException | RuntimeException e) {
            // Release whatever is left without letting a second failure replace the first. (These are
            // tmp files the caller has not renamed, so an abandoned one is never published whether or
            // not it got stamped -- see the publish path's note on that guarantee.)
            try {
                closeQuietly(open);
            } catch (IOException | RuntimeException releaseFailure) {
                e.addSuppressed(releaseFailure);
            }
            throw e;
        }
    }

    /**
     * The roll loop, leaving every writer it opened OPEN and appended to {@code out} in write order —
     * the caller owns closing them, and until it does it may still {@link SortedFileWriter#markFinal()}
     * or {@link SortedFileWriter#setFileIndex(int)} them.
     *
     * <p>This is what lets the parallel range merge stamp global completeness. A range cannot know its
     * parts' positions in the output's roll sequence while it is writing them — that depends on how
     * many parts the ranges BELOW it produce, which is only known once they finish. Deferring the
     * footer, rather than the data, keeps the cost small: a drained-but-unclosed Parquet writer has
     * already flushed its row groups and retains only their metadata plus at most one buffered row
     * group ({@code final-row-group-bytes}).
     *
     * <p>{@code closeRolledAway} bounds that cost for callers that do NOT need the deferral. A caller
     * that already knows each file's global index (the serial publish) leaves only its last file
     * undecided, so it closes each file as it rolls away and {@code out} never exceeds one entry.
     * Only the parallel path passes {@code false}, and only because every one of its parts is still
     * waiting to learn its index.
     */
    static long drainOpen(SortedCursor merged, long finalFileBytes, FileFactory fileFactory,
                          LongConsumer progressCallback, SortMetrics metrics,
                          EqualKeyPolicy equalKeyPolicy, Comparator<ListEntry> comparator,
                          List<SortedFileWriter> out,
                          boolean closeRolledAway) throws IOException {
        long totalRows = 0;
        long batchRows = 0;
        try {
            SortedFileWriter writer = null;
            byte[] previousKey = null;
            ListEntry previousEntry = null;
            boolean deferredForCurrentKey = false;
            while (merged.hasNext()) {
                MergeCancellation.check();
                ListEntry entry = merged.next();
                byte[] entryKey = entry.key().rawUnsafe();
                boolean rollReady = writer != null && shouldRoll(writer, finalFileBytes);
                // The equality check can scan a 1 KiB key. Keep it off the ordinary unique-key hot
                // path for ALLOW runs until the soft byte target has actually been reached. REJECT
                // is the fixture-only integrity policy and must inspect every adjacent pair.
                boolean sameKey = (equalKeyPolicy == EqualKeyPolicy.REJECT || rollReady)
                        && previousKey != null
                        && KeyBytes.compareUnsigned(previousKey, entryKey) == 0;
                if (equalKeyPolicy == EqualKeyPolicy.REJECT && sameKey) {
                    metrics.recordStealReason("SORT", "equal_key_rejected");
                    throw DuplicateKeyException.forAdjacentEntries(previousEntry, entry, comparator);
                }
                if (rollReady && sameKey) {
                    // A key is the future VERSIONS path's unsplittable atom. Record once for the
                    // whole deferred group, not once per version: a pathological million-version
                    // key must not turn this classification signal into a hot per-row metric call.
                    if (!deferredForCurrentKey) {
                        metrics.recordStealReason("SORT", "final_roll_equal_key_deferred");
                        deferredForCurrentKey = true;
                    }
                } else if (writer == null || rollReady) {
                    if (writer != null && closeRolledAway) {
                        // A rolled-away file is provably not the last one, so nothing about it is
                        // still undecided: close it NOW. Holding it would strand an fd and its
                        // buffered row group for the rest of the drain, turning peak open writers
                        // from 1 into one-per-part -- neither of which any budget accounts for.
                        writer.close();
                        out.remove(out.size() - 1);
                    }
                    writer = fileFactory.open();
                    out.add(writer);
                    deferredForCurrentKey = false;
                }
                MergeCancellation.check();
                writer.write(entry);
                // KeyBytes treats this hot-path array as immutable. Retain one no-copy reference:
                // final rolling uses O(1) extra state even for arbitrarily many versions of one key.
                previousKey = entryKey;
                previousEntry = entry;
                totalRows++;
                // §3.2: batched merge-progress feed (never per-row) — see KWayMerge.PROGRESS_BATCH_ROWS.
                if (++batchRows >= KWayMerge.PROGRESS_BATCH_ROWS) {
                    progressCallback.accept(batchRows);
                    batchRows = 0;
                }
            }
        } finally {
            if (batchRows > 0) {
                progressCallback.accept(batchRows);
            }
        }
        return totalRows;
    }

    /** Close in write order, so a failure leaves the earlier files complete rather than a hole. */
    static void closeInOrder(List<SortedFileWriter> writers) throws IOException {
        for (SortedFileWriter w : writers) {
            w.close();
        }
    }

    /**
     * Best-effort release on a failure path: every writer gets a close attempt, and the first failure
     * is rethrown with the rest suppressed, so one stuck file cannot strand the others' descriptors.
     */
    static void closeQuietly(List<SortedFileWriter> writers) throws IOException {
        IOException first = null;
        for (SortedFileWriter w : writers) {
            try {
                w.close();
            } catch (IOException | RuntimeException e) {
                if (first == null) {
                    first = e instanceof IOException io ? io : new IOException(e);
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        writers.clear();
        if (first != null) {
            throw first;
        }
    }

    private static boolean shouldRoll(SortedFileWriter writer, long finalFileBytes) {
        return finalFileBytes != Long.MAX_VALUE
                && writer.rows() > 0
                && writer.dataSize() >= finalFileBytes;
    }
}
