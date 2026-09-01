/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.model.ListEntry;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortedEntryCursor;
import java.util.Comparator;

/** Reports adjacent comparator-equal rows from one merged cursor without changing multiplicity. */
final class DuplicateReportingCursor implements SortedEntryCursor, LogicalMergeCompletion {

    private final SortedEntryCursor inner;
    private final OrderedEntryGuard guard;
    private boolean closed;

    DuplicateReportingCursor(SortedEntryCursor inner, Comparator<ListEntry> comparator, DuplicateHook hook) {
        this.inner = inner;
        this.guard = new OrderedEntryGuard(comparator, hook, EqualKeyPolicy.ALLOW,
                SortMetrics.NO_OP, "merged");
    }

    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    @Override
    public ListEntry next() {
        ListEntry current = inner.next();
        guard.accept(current);
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
