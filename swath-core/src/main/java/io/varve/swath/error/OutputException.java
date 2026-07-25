/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/** Sink write / Parquet / disk-full failure. Exit 1. */
public final class OutputException extends SwathException {
    public OutputException(String message) {
        super(message);
    }

    public OutputException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public int exitCode() {
        return 1;
    }
}
