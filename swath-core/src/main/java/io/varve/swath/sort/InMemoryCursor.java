/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * {@link SortedCursor} over an already-sorted in-memory list. The fixture sorter uses it to stream a
 * bounded, locally sorted chunk into page-run staging. Applies the {@link DuplicateHook} on adjacent
 * equal entries so behavior matches the streaming merge path.
 */
final class InMemoryCursor implements SortedCursor {

    private final Iterator<ListEntry> it;
    private final Comparator<ListEntry> comparator;
    private final DuplicateHook hook;
    private ListEntry previous;

    InMemoryCursor(List<ListEntry> sorted, Comparator<ListEntry> comparator, DuplicateHook hook) {
        this.it = sorted.iterator();
        this.comparator = comparator;
        this.hook = hook;
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public ListEntry next() {
        if (!it.hasNext()) {
            throw new NoSuchElementException();
        }
        ListEntry e = it.next();
        if (previous != null && comparator.compare(previous, e) == 0) {
            hook.onDuplicate(previous, e);
        }
        previous = e;
        return e;
    }

    @Override
    public void close() {
        // nothing to release — the data is on-heap
    }
}
