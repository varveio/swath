/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;

/** Resumable refusal when decoded page residency cannot fit the configured merge budget. */
public final class MergeMemoryExhaustedException extends IOException {

    public static final String ERROR_CLASS = "sort_merge_memory_exhausted";

    MergeMemoryExhaustedException(String message) {
        super(message);
    }

    public String errorClass() {
        return ERROR_CLASS;
    }
}
