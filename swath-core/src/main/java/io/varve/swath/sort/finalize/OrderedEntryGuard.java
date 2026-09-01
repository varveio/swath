/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.DuplicateKeyException;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortOrderException;
import java.util.Comparator;

/** Shared adjacent-row order, duplicate-reporting, and raw-key uniqueness policy. */
final class OrderedEntryGuard {
    private final Comparator<ListEntry> comparator;
    private final DuplicateHook hook;
    private final EqualKeyPolicy equalKeyPolicy;
    private final SortMetrics metrics;
    private final String streamName;
    private ListEntry previous;

    OrderedEntryGuard(Comparator<ListEntry> comparator, DuplicateHook hook,
            EqualKeyPolicy equalKeyPolicy, SortMetrics metrics, String streamName) {
        this.comparator = comparator;
        this.hook = hook;
        this.equalKeyPolicy = equalKeyPolicy;
        this.metrics = metrics;
        this.streamName = streamName;
    }

    void accept(ListEntry current) {
        if (previous != null) {
            int order = comparator.compare(previous, current);
            if (order > 0) {
                throw new SortOrderException(streamName + " output order regressed from key "
                        + previous.key() + " to " + current.key());
            }
            if (order == 0) {
                hook.onDuplicate(previous, current);
            }
            if (equalKeyPolicy == EqualKeyPolicy.REJECT && sameRawKey(previous, current)) {
                rejectEqualKey(previous, current, comparator, metrics);
            }
        }
        previous = current;
    }

    void reset() {
        previous = null;
    }

    static boolean sameRawKey(ListEntry previous, ListEntry current) {
        return KeyBytes.compareUnsigned(
                previous.key().rawUnsafe(), current.key().rawUnsafe()) == 0;
    }

    static void rejectEqualKey(ListEntry previous, ListEntry current,
            Comparator<ListEntry> comparator, SortMetrics metrics) {
        metrics.recordStealReason("SORT", "equal_key_rejected");
        throw DuplicateKeyException.forAdjacentEntries(previous, current, comparator);
    }
}
