/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/** Malformed or unsupported store URI. Exit 2 (bad input). */
public final class InvalidUriException extends SwathException {
    public InvalidUriException(String message) {
        super(message);
    }

    @Override
    public int exitCode() {
        return 2;
    }
}
