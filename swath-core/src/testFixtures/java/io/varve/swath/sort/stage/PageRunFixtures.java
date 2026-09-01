/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.stage;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.spill.PageCompression;
import io.varve.swath.sort.spill.PageRunWriter;
import io.varve.swath.sort.spill.SealTrigger;
import io.varve.swath.sort.spill.SealedBuffer;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Test-fixture writer for listing-time page-run staging segments. It stays in {@code sort.stage}
 * to reach package-private {@link SortBuffer}; {@link PageRunWriter} is the public spill-format seam.
 * The fixture is public only because tests in other modules — notably the {@code swath dump-run} CLI test in
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
        new PageRunWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCompression.NONE)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
    }

    public static Path writePages(Path path, List<? extends List<? extends ListEntry>> pages,
            SortMode orderingMode,
            PageCompression codec) throws IOException {
        Buffer buffer = buffer(SortConfigs.base().withSegmentCodec(codec), CMP);
        for (int page = 0; page < pages.size(); page++) {
            buffer.admit(page, new ArrayList<>(pages.get(page)));
        }
        new PageRunWriter(CMP, DuplicateHook.NO_OP, SortMetrics.NO_OP, codec, orderingMode)
                .flush(buffer.seal(SealTrigger.DRAIN), path);
        return path;
    }

    public static Buffer buffer(SortConfig config, java.util.Comparator<ListEntry> comparator) {
        return new Buffer(new SortBuffer(config, comparator));
    }

    public static final class Buffer {
        private final SortBuffer delegate;

        private Buffer(SortBuffer delegate) {
            this.delegate = delegate;
        }

        public void admit(long nodeId, List<ListEntry> page) {
            delegate.admit(nodeId, page);
        }

        public SealedBuffer seal(SealTrigger trigger) {
            return delegate.seal(trigger);
        }

        public boolean isEmpty() {
            return delegate.isEmpty();
        }
    }
}
