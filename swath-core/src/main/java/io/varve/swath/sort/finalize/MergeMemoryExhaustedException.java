/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

/** Resumable refusal when decoded page residency cannot fit the configured merge budget. */
public final class MergeMemoryExhaustedException extends FinalizationCapacityException {

    public static final String ERROR_CLASS = "sort_merge_memory_exhausted";

    private static final String DEFERRAL = "sort merge deferred because decoded pages do not fit "
            + "the merge budget; raise swath.sort.merge-budget-bytes and resume";

    MergeMemoryExhaustedException(String message) {
        super(ERROR_CLASS, DEFERRAL, message);
    }
}
