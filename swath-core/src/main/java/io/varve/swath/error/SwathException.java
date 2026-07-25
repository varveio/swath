/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/**
 * Root of swath's sealed error hierarchy. Each concrete subtype carries the
 * process exit code it maps to; the sealed permits list makes the
 * error→exit-code mapping compiler-checked for exhaustiveness (UNIT-exit).
 */
public sealed abstract class SwathException extends Exception
        permits CancelledException, InvalidUriException, InvalidConfigException,
                InvalidArgsException, ListingException, OutputException,
                CheckpointException, UnsupportedBucketException {

    protected SwathException(String message) {
        super(message);
    }

    protected SwathException(String message, Throwable cause) {
        super(message, cause);
    }

    /** The process exit code for this error class. */
    public abstract int exitCode();
}
