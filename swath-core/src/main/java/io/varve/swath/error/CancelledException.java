/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/** SIGINT / cooperative cancellation. Exit 130 (standard for Ctrl+C). */
public final class CancelledException extends SwathException {
    public CancelledException(String message) {
        super(message);
    }

    public CancelledException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public int exitCode() {
        return 130;
    }
}
