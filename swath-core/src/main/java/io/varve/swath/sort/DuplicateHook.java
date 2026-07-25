/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;

/**
 * Callback invoked with {@code (previous, current)} whenever two adjacent emitted entries
 * compare equal under {@link ListEntryComparator}. The default {@link #NO_OP} does nothing:
 * swath {@code --sort} never drops user entries (it is a sorter, not a deduper).
 * Consumers that must reject duplicates (e.g. the replay {@code sort-fixture} fail-fast) supply
 * their own hook.
 */
@FunctionalInterface
public interface DuplicateHook {

    /** No-op default: duplicates are kept, not reported. */
    DuplicateHook NO_OP = (previous, current) -> { };

    void onDuplicate(ListEntry previous, ListEntry current);
}
