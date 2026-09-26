/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

/** A provider request failure to map through its native error envelope. */
public final class ReplayRequestException extends RuntimeException {
    private final ReplayFailure failure;

    public ReplayRequestException(ReplayFailure failure) {
        super(failure.message());
        this.failure = failure;
    }

    public ReplayFailure failure() { return failure; }
}
