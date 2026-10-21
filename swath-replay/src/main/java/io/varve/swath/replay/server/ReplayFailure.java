/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

/** Shared failure classification; each protocol maps it to its own wire envelope. */
public record ReplayFailure(Kind kind, String reason, String message) {
    public static final String REASON_HEADER = "x-swath-replay-error";
    public enum Kind {
        MALFORMED, UNSUPPORTED, NOT_FOUND, WRONG_METHOD, FIXTURE_INCOMPATIBLE,
        FIXTURE_DISORDERED, OVERLOAD, RESPONSE_TOO_LARGE, INTERNAL
    }
}
