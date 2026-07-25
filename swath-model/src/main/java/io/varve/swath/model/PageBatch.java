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
 */
public record PageBatch(long nodeId, long pageSeq, List<ListEntry> entries, PackedPage packed) {

    public PageBatch {
        if ((entries == null) == (packed == null)) {
            throw new IllegalArgumentException(
                    "PageBatch must carry exactly one of entries / packed (non-null)");
        }
    }

    /** Raw-entries form (non-{@code --sort} pipelines): {@code packed} is {@code null}. */
    public PageBatch(long nodeId, long pageSeq, List<ListEntry> entries) {
        this(nodeId, pageSeq, entries, null);
    }

    /** Packed form ({@code --sort} mode): {@code entries} is {@code null}. */
    public static PageBatch ofPacked(long nodeId, long pageSeq, PackedPage packed) {
        return new PageBatch(nodeId, pageSeq, null, packed);
    }

    /** True iff this batch carries a pre-{@link #packed} page (sort mode) rather than raw {@link #entries}. */
    public boolean isPacked() {
        return packed != null;
    }

    /** Weight of this batch for the {@code --object-listing-queue-size} entry budget (I11). */
    public long entryCount() {
        return packed != null ? packed.entryCount() : entries.size();
    }
}
