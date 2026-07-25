/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import java.util.List;

/**
 * Strategy that packs a page's entries into a {@link PackedPage} on the fetch worker thread
 * (pack-on-fetch). {@code null} in every non-{@code --sort} run — the engine only holds a
 * packer when the run stages sorted output, so the text/parquet-direct pipelines keep carrying raw
 * {@link ListEntry} lists byte-for-byte unchanged.
 *
 * <p>Implemented in {@code swath-core}'s {@code io.varve.swath.sort} package (where the packed
 * {@code PageBlock} representation and the configured payload codec live); declared here in the leaf
 * {@code swath-model} module so {@code io.varve.swath.engine}'s {@code WorkStealingScan} can hold and
 * invoke it without a module cycle. Called concurrently from many fetch workers — implementations must
 * be thread-safe (the packer holds no per-call mutable state; each {@code pack} builds a fresh block).
 */
@FunctionalInterface
public interface PagePacker {

    /**
     * Pack {@code entries} (a non-empty, already-filtered page) into a {@link PackedPage}. The entry
     * order is preserved — packing never reorders a page.
     */
    PackedPage pack(List<ListEntry> entries);
}
