/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import java.io.IOException;

/** Resumable refusal when the fd or memory budget cannot open even a minimum two-stream cascade. */
public final class CascadeCapacityExhaustedException extends IOException {

    public static final String ERROR_CLASS = "sort_cascade_capacity_exhausted";

    CascadeCapacityExhaustedException(String message) {
        super(message);
    }

    public String errorClass() {
        return ERROR_CLASS;
    }
}
