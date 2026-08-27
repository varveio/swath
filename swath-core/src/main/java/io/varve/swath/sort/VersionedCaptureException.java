/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Thrown when a capture supplied to the non-versioned sort-fixture path contains a versioned row. */
public final class VersionedCaptureException extends RuntimeException {

    public VersionedCaptureException(String message) {
        super(message);
    }
}
