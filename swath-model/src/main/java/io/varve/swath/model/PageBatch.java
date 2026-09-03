/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import java.util.List;

/**
 * The pipeline granularity (contract §1.3): a page's worth of entries, tagged
 * with the producing node and a per-node monotonic page sequence. Passing
 * batches rather than single entries reduces queue contention and per-object
 * overhead.
 *
 * <p><b>Dual-form (pack-on-fetch).</b> A batch carries <b>exactly one</b> of two payloads:
 * <ul>
 *   <li>{@code entries} — a raw {@link ListEntry} list (the non-{@code --sort} text/parquet-direct
 *       pipelines, byte-for-byte unchanged); or</li>
 *   <li>{@code packed} — a {@link PackedPage} the fetch worker already packed ({@code --sort} mode),
 *       so the channel and the sort drain thread hold a compact packed page instead of the parsed
 *       entry objects (the in-flight-memory win) and packing runs on the fetch worker, not the single
 *       drain thread.</li>
 * </ul>
 * The unused form is {@code null}; {@link #isPacked()} distinguishes them.
 *
 * <p><b>Tally-on-build.</b> Every batch carries its {@link PageTally}, computed by the constructor
 * that built it — on the producing thread (a fetch worker), never on the single consumer stage,
 * which only merges it. The packed form reads the counts the packer already accumulated. The
 * canonical constructor rejects a tally whose row count disagrees with the payload (an O(1) check;
 * byte totals are trusted, since re-walking the entries would undo the single-tally design).
 */
public record PageBatch(
        long nodeId,
        long pageSeq,
        List<ListEntry> entries,
        PackedPage packed,
        boolean nodeCompleted,
        PageTally tally) {

    public PageBatch {
        if ((entries == null) == (packed == null)) {
            throw new IllegalArgumentException(
                    "PageBatch must carry exactly one of entries / packed (non-null)");
        }
        if (tally == null) {
            throw new IllegalArgumentException("PageBatch must carry its tally (non-null)");
        }
        // O(1) guard: every entry is classified into exactly one tally bucket, so the tally's row
        // count must equal the payload's entry count. This catches a foreign tally (e.g. EMPTY on a
        // non-empty page) without re-walking the entries on the producing thread.
        long rows = tally.rows();
        long count = packed != null ? packed.entryCount() : entries.size();
        if (rows != count) {
            throw new IllegalArgumentException(
                    "PageBatch tally rows (" + rows + ") must equal its entry count (" + count + ")");
        }
    }

    /** Compatibility constructor for an ordinary, non-terminal batch; tallies whichever form is present. */
    public PageBatch(long nodeId, long pageSeq, List<ListEntry> entries, PackedPage packed) {
        this(nodeId, pageSeq, entries, packed, false, tallyOf(entries, packed));
    }

    /** Raw-entries form (non-{@code --sort} pipelines): {@code packed} is {@code null}; tallies {@code entries} here. */
    public PageBatch(long nodeId, long pageSeq, List<ListEntry> entries) {
        this(nodeId, pageSeq, entries, null, false, PageTally.of(entries));
    }

    /** Raw-entries form carrying the producing node's completion signal; tallies {@code entries} here. */
    public PageBatch(long nodeId, long pageSeq, List<ListEntry> entries, boolean nodeCompleted) {
        this(nodeId, pageSeq, entries, null, nodeCompleted, PageTally.of(entries));
    }

    /** Packed form ({@code --sort} mode): {@code entries} is {@code null}. */
    public static PageBatch ofPacked(long nodeId, long pageSeq, PackedPage packed) {
        return ofPacked(nodeId, pageSeq, packed, false);
    }

    /** Packed form carrying the producing node's completion signal. */
    public static PageBatch ofPacked(
            long nodeId, long pageSeq, PackedPage packed, boolean nodeCompleted) {
        return new PageBatch(nodeId, pageSeq, null, packed, nodeCompleted, PageTally.of(packed));
    }

    /** Zero-row control batch used when a completed node's terminal page retained no rows. */
    public static PageBatch completion(long nodeId, long pageSeq) {
        return new PageBatch(nodeId, pageSeq, List.of(), null, true, PageTally.EMPTY);
    }

    private static PageTally tallyOf(List<ListEntry> entries, PackedPage packed) {
        if (entries != null) {
            return PageTally.of(entries);
        }
        return packed != null ? PageTally.of(packed) : PageTally.EMPTY;   // the compact constructor rejects both-null
    }

    /** True iff this batch carries a pre-{@link #packed} page (sort mode) rather than raw {@link #entries}. */
    public boolean isPacked() {
        return packed != null;
    }

    /** Weight of this batch for the {@code --object-listing-queue-size} entry budget (I11). */
    public long entryCount() {
        return packed != null ? packed.entryCount() : entries.size();
    }

    /** A completion marker is control state but still consumes one bounded channel-weight unit. */
    public long channelWeight() {
        return completionOnly() ? 1L : entryCount();
    }

    /** True for a node-completion control batch with no output rows. */
    public boolean completionOnly() {
        return nodeCompleted && entryCount() == 0;
    }
}
