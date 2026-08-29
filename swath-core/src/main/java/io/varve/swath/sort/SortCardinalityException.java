/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;

/** Fatal invariant failure: source, drained, and durable final-row totals disagree. */
public final class SortCardinalityException extends IOException {

    public static final String ERROR_CLASS = "sort_output_cardinality_mismatch";

    public SortCardinalityException(String message) {
        super(message);
    }

    public String errorClass() {
        return ERROR_CLASS;
    }
}
