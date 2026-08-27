/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;

/** Thrown when a key-unique sort run encounters two adjacent rows with the same raw key. */
public final class DuplicateKeyException extends RuntimeException {

    public DuplicateKeyException(String message) {
        super(message);
    }

    static DuplicateKeyException forEntry(ListEntry entry) {
        return new DuplicateKeyException(
                "sort-fixture found a duplicate key: '" + entry.key().asString() + "'");
    }
}
