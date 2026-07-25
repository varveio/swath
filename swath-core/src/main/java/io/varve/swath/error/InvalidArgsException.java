/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/** Bad flag combination (mutual-constraint violation, args_hash mismatch). Exit 2. */
public final class InvalidArgsException extends SwathException {
    public InvalidArgsException(String message) {
        super(message);
    }

    public InvalidArgsException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public int exitCode() {
        return 2;
    }
}
