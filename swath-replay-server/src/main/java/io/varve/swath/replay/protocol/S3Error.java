/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

/** A protocol-level S3 error carrying an HTTP status and an S3 error code, rendered as XML. */
public final class S3Error extends RuntimeException {

    private final int status;
    private final String code;

    public S3Error(int status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public int status() {
        return status;
    }

    public String code() {
        return code;
    }
}
