/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@code n<nodeId>/key-<i>} {@link PageBatch} builder shared across the parquet
 * writer-pool/rotation/idle-lane tests (swath-core) and the CLI's crash-state test
 * (swath-cli) — was duplicated byte-identically in both before this consolidation.
 */
public final class PageBatches {

    private PageBatches() {
    }

    /** Build a batch of {@code to - from} entries keyed {@code n<nodeId>/key-<i>} for {@code i} in {@code [from, to)}. */
    public static PageBatch batch(long nodeId, long seq, int from, int to) {
        List<ListEntry> entries = new ArrayList<>();
        for (int i = from; i < to; i++) {
            entries.add(ObjectEntry.withoutOwnerDisplayNameAndChecksumType(
                    KeyBytes.ofUtf8(String.format("n%d/key-%08d", nodeId, i)),
                    100, 1_700_000_000_000_000L, "etag", "STANDARD", null, true, null, null));
        }
        return new PageBatch(nodeId, seq, entries);
    }
}
