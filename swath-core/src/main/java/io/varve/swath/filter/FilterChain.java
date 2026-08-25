/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.filter;

import io.varve.swath.error.ListingException;
import io.varve.swath.model.ListEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Conjunction of {@link Filter}s, evaluated cheapest-first (size/mtime/
 * storage-class before regex) with short-circuit. An entry is kept iff every
 * filter matches.
 */
public final class FilterChain {

    public static final FilterChain EMPTY = new FilterChain(List.of());

    private final Filter[] filters;

    private FilterChain(List<Filter> filters) {
        List<Filter> sorted = new ArrayList<>(filters);
        sorted.sort(Comparator.comparingInt(Filter::cost));
        this.filters = sorted.toArray(new Filter[0]);
    }

    public static FilterChain of(List<Filter> filters) {
        return filters.isEmpty() ? EMPTY : new FilterChain(filters);
    }

    public boolean isEmpty() {
        return filters.length == 0;
    }

    /** The cost-sorted evaluation order (visible for testing cheap-before-regex). */
    List<Filter> orderedForTest() {
        return List.of(filters);
    }

    public boolean matches(ListEntry e) throws ListingException {
        for (Filter f : filters) {
            if (!f.matches(e)) {
                return false;
            }
        }
        return true;
    }

    /** Apply the chain to a batch, returning the kept entries (the batch itself when empty chain). */
    public List<ListEntry> apply(List<ListEntry> batch) throws ListingException {
        if (filters.length == 0) {
            return batch;
        }
        List<ListEntry> kept = new ArrayList<>(batch.size());
        for (ListEntry e : batch) {
            if (matches(e)) {
                kept.add(e);
            }
        }
        return kept;
    }
}
