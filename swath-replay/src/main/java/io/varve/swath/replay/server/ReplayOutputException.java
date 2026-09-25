/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

/** Rendering stopped before headers because its byte bound was reached. */
public final class ReplayOutputException extends RuntimeException {
    private final boolean responseTooLarge;
    private final String reason;

    ReplayOutputException(boolean responseTooLarge) {
        this(responseTooLarge, responseTooLarge ? "response_too_large" : "response_budget_exhausted");
    }

    ReplayOutputException(boolean responseTooLarge, String reason) {
        super(responseTooLarge ? "response exceeded configured per-response cap"
                : "response buffer budget exhausted");
        this.responseTooLarge = responseTooLarge;
        this.reason = reason;
    }

    public boolean responseTooLarge() { return responseTooLarge; }
    public String reason() { return reason; }
}
