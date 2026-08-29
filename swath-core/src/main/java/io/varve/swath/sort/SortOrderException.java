/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Fatal invariant failure: a merge emitted rows in decreasing comparator order. */
public final class SortOrderException extends IllegalStateException {

    public static final String ERROR_CLASS = "sort_output_order_regression";

    public SortOrderException(String message) {
        super(message);
    }

    /** Stable terminal-summary fingerprint for this deterministic invariant failure. */
    public String errorClass() {
        return ERROR_CLASS;
    }
}
