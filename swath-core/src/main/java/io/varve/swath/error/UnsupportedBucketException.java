/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/** A recognized bucket class whose listing contract is not safely supported. Exit 2. */
public final class UnsupportedBucketException extends SwathException {

    public UnsupportedBucketException(String message) {
        super(message);
    }

    @Override
    public int exitCode() {
        return 2;
    }
}
