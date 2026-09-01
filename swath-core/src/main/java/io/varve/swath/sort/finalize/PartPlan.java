/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.sort.spill.PageRef;
import java.io.IOException;
import java.util.List;

/** One complete, contiguous final part assigned by the router to one encoder. */
record PartPlan(int ordinal, List<Item> items, boolean mergeEnd, long logicalBytes, long rows) {
    PartPlan {
        items = List.copyOf(items);
        if (ordinal < 0 || logicalBytes < 0 || rows < 0) {
            throw new IllegalArgumentException("invalid complete part plan");
        }
        if (items.isEmpty() != (rows == 0) || (rows == 0 && ordinal != 0)) {
            throw new IllegalArgumentException("only part zero may carry an empty plan");
        }
    }

    /** Release the staging references of every item this plan owns. Safe to repeat. */
    void discard() throws IOException {
        for (Item item : items) {
            item.discard();
        }
    }

    /**
     * Routing geometry an item exposes without re-reading its references, so an overlap component
     * whose references live in staging is measured exactly once, while the router closes it.
     */
    sealed interface Item permits Page, Cluster {
        /** Routing minimum of the item's first reference. */
        byte[] firstKey();

        /** True high bound over every reference the item owns. */
        byte[] lastKey();

        long refCount();

        long rows();

        long logicalBytes();

        /** Release any staging file backing this item's references. Safe to repeat. */
        void discard() throws IOException;
    }

    record Page(PageRef ref) implements Item {
        @Override
        public byte[] firstKey() {
            return ref.minKey();
        }

        @Override
        public byte[] lastKey() {
            return ref.maxKey();
        }

        @Override
        public long refCount() {
            return 1;
        }

        @Override
        public long rows() {
            return ref.count();
        }

        @Override
        public long logicalBytes() {
            return ref.rawPayloadLength();
        }

        @Override
        public void discard() {
        }
    }

    /**
     * One indivisible transitive overlap component together with the geometry the router measured
     * while closing it. The references are a one-shot ordered stream because a component larger
     * than the plan reference cap lives in staging rather than in heap.
     */
    record Cluster(ClusterRefs refs, byte[] firstKey, byte[] lastKey, long refCount, long rows,
                   long logicalBytes) implements Item {
        Cluster {
            if (refCount < 2 || rows < refCount || logicalBytes < 1) {
                throw new IllegalArgumentException("invalid overlap cluster geometry: refs="
                        + refCount + " rows=" + rows + " logical_bytes=" + logicalBytes);
            }
        }

        @Override
        public void discard() throws IOException {
            refs.discard();
        }
    }
}
