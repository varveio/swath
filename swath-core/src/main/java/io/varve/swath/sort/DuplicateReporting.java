/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.util.Comparator;

/** Reports adjacent comparator-equal rows from one merged cursor without changing multiplicity. */
final class DuplicateReporting implements SortedCursor, LogicalMergeCompletion {

    private final SortedCursor inner;
    private final Comparator<ListEntry> comparator;
    private final DuplicateHook hook;
    private ListEntry previous;
    private boolean closed;

    DuplicateReporting(SortedCursor inner, Comparator<ListEntry> comparator, DuplicateHook hook) {
        this.inner = inner;
        this.comparator = comparator;
        this.hook = hook;
    }

    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    @Override
    public ListEntry next() {
        ListEntry current = inner.next();
        if (previous != null) {
            int order = comparator.compare(previous, current);
            if (order > 0) {
                throw new SortOrderException("merged output order regressed from key "
                        + previous.key() + " to " + current.key());
            }
            if (order == 0) {
                hook.onDuplicate(previous, current);
            }
        }
        previous = current;
        return current;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            inner.close();
        }
    }

    @Override
    public void completeLogicalMerge() {
        LogicalMergeCompletion.complete(inner);
    }
}
