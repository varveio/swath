/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/** SQLite / checkpoint corruption or write failure. Exit 1. */
public final class CheckpointException extends SwathException {
    public CheckpointException(String message) {
        super(message);
    }

    public CheckpointException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public int exitCode() {
        return 1;
    }
}
