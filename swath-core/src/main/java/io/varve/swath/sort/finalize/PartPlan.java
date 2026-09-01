/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.sort.spill.PageRef;
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

    sealed interface Item permits Page, Cluster {
        List<PageRef> refs();
    }

    record Page(PageRef ref) implements Item {
        @Override
        public List<PageRef> refs() {
            return List.of(ref);
        }
    }

    record Cluster(List<PageRef> refs) implements Item {
        Cluster {
            refs = List.copyOf(refs);
            if (refs.size() < 2) {
                throw new IllegalArgumentException("overlap cluster needs at least two page refs");
            }
        }
    }
}
