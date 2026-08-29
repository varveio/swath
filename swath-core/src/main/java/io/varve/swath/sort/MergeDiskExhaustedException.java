/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;

/** Resumable merge-start refusal raised before any proof or output allocation begins. */
public final class MergeDiskExhaustedException extends IOException {

    public static final String ERROR_CLASS = "sort_disk_exhausted";

    MergeDiskExhaustedException(String message) {
        super(message);
    }

    public String errorClass() {
        return ERROR_CLASS;
    }
}
