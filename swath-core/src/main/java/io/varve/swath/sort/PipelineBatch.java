/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.util.List;

/** One bounded, ordinal-stamped transfer from the merge router to a part encoder. */
record PipelineBatch(long sequence, int partOrdinal, boolean partFirst, boolean partLast,
                     boolean mergeEnd, Payload payload) {

    sealed interface Payload permits WholePage, Rows, Empty {
        long logicalBytes();

        long rowCount();

        byte[] firstKey();

        byte[] lastKey();
    }

    enum Empty implements Payload {
        INSTANCE;

        @Override
        public long logicalBytes() {
            return 0;
        }

        @Override
        public long rowCount() {
            return 0;
        }

        @Override
        public byte[] firstKey() {
            return null;
        }

        @Override
        public byte[] lastKey() {
            return null;
        }
    }

    record WholePage(PageBlock page) implements Payload {
        @Override
        public long logicalBytes() {
            return page.estimatedBytes();
        }

        @Override
        public long rowCount() {
            return page.count();
        }

        @Override
        public byte[] firstKey() {
            return page.firstKeyUnsafe();
        }

        @Override
        public byte[] lastKey() {
            return page.lastKeyUnsafe();
        }
    }

    /** Router-owned list transferred without copying; the router never mutates it after construction. */
    record Rows(List<ListEntry> entries, long logicalBytes) implements Payload {
        Rows {
            if (entries.isEmpty() || logicalBytes <= 0) {
                throw new IllegalArgumentException("row batches must be non-empty with positive logical bytes");
            }
        }

        @Override
        public long rowCount() {
            return entries.size();
        }

        @Override
        public byte[] firstKey() {
            return entries.getFirst().key().rawUnsafe();
        }

        @Override
        public byte[] lastKey() {
            return entries.getLast().key().rawUnsafe();
        }
    }
}
