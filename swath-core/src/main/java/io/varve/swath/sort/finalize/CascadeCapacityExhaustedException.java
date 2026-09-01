/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

/** Resumable refusal when the fd or memory budget cannot open even a minimum two-stream cascade. */
public final class CascadeCapacityExhaustedException extends FinalizationCapacityException {

    public static final String ERROR_CLASS = "sort_cascade_capacity_exhausted";

    private static final String DEFERRAL = "sort merge deferred because the cascade cannot open two "
            + "input streams; raise the open-file limit or swath.sort.merge-budget-bytes and resume";

    CascadeCapacityExhaustedException(String message) {
        super(ERROR_CLASS, DEFERRAL, message);
    }
}
