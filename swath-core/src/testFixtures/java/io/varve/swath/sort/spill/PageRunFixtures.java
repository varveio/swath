/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Test-only writer for page-run staging segments, in-package under {@code io.varve.swath.sort} so
 * it can reach the package-private {@link PageRunSegmentWriter}/{@link SortBuffer}. Exposed publicly
 * (testFixtures) so tests in other modules — notably the {@code swath dump-run} CLI test in
 * {@code swath-cli} — can produce a real segment file to inspect without a Parquet or S3 round-trip.
 */
public final class PageRunFixtures {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    private PageRunFixtures() {
    }

    /**
     * Write {@code keys} (as plain objects, one node run) as a single-page page-run segment at
     * {@code path}. A handful of keys seals into exactly one page → one framed record, so a caller can
     * assert on a deterministic record layout.
     */
    public static void writeSinglePageSegment(Path path, List<String> keys) throws IOException {
        SortBuffer buffer = new SortBuffer(SortConfig.fromSystemProperties(), CMP);
        List<ListEntry> entries = new ArrayList<>(keys.size());
        for (String k : keys) {
            entries.add(new ObjectEntry(KeyBytes.ofUtf8(k), 1L, 0L, null, null, null,
                    false, null, null, null, null));
        }
        buffer.admit(1L, entries);
        new PageRunSegmentWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
    }
}
