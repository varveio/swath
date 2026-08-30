/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

/**
 * A page's entries after they have been packed into the sort lane's compact in-memory form
 * (pack-on-fetch). Carried by {@link PageBatch} in {@code --sort} mode <b>instead of</b> a
 * {@link java.util.List}&lt;{@link ListEntry}&gt;, so the pipeline channel and the sort consumer hold a
 * compact packed page rather than the parsed entry objects (the in-flight-memory win) and the packing
 * cost runs on the fetch worker that built the page, not on the single pipeline drain thread.
 *
 * <p>This is a {@code swath-model} seam: the concrete packed representation (a {@code PageBlock}) lives
 * in {@code swath-core}'s {@code io.varve.swath.sort} package, which {@code swath-model} (a leaf module)
 * cannot reference. The interface exposes only what the drain-thread consumer needs without decoding the
 * payload — the per-row-type tallies and the object-size sum {@code SortOutputStage} would otherwise
 * recompute by iterating the entries — while the sort package downcasts to recover the packed block.
 */
public interface PackedPage {

    /** Total entries packed into this page (the {@code --object-listing-queue-size} budget weight). */
    long entryCount();

    /** How many of the packed entries are {@link ObjectEntry}s. */
    long objectCount();

    /** How many of the packed entries are {@link CommonPrefixEntry}s. */
    long commonPrefixCount();

    /** How many of the packed entries are {@link DeleteMarkerEntry}s. */
    long deleteMarkerCount();

    /** Sum of {@link ObjectEntry#size()} across the packed objects (the estimated-bytes tally). */
    long totalObjectSize();

    /** First raw key in source-page order; returned defensively for invariant checks. */
    byte[] firstKey();

    /** Last raw key in source-page order; returned defensively for invariant checks. */
    byte[] lastKey();
}
