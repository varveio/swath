/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.util.Comparator;

/** Thrown when a key-unique sort run encounters two adjacent rows with the same raw key. */
public final class DuplicateKeyException extends RuntimeException {

    public static final String ERROR_CLASS = "sort_duplicate_key";

    public DuplicateKeyException(String message) {
        super(message);
    }

    public String errorClass() {
        return ERROR_CLASS;
    }

    public static DuplicateKeyException forAdjacentEntries(ListEntry previous, ListEntry entry,
                                                    Comparator<ListEntry> comparator) {
        String key = entry.key().asString();
        if (comparator.compare(previous, entry) == 0) {
            return new DuplicateKeyException(
                    "sort-fixture found a duplicate key (adjacent-equal under the sort order): '"
                            + key + "'");
        }
        return new DuplicateKeyException(
                "sort-fixture found a duplicate key across row types "
                        + "(adjacent-equal key bytes regardless of row_type): '" + key + "'");
    }
}
