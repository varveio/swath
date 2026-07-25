/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/**
 * Classification of a {@link ThrottleException}, threaded through to
 * {@code swath.throttle.events{type}} (RunMetrics) so the dashboard can tell a real S3
 * throttle signal apart from a self-inflicted client-side timeout. Tag spellings match the
 * {@code steal_reason} reasons ({@code "THROTTLE"} outcome) exactly, so summary/harvest
 * consumers see the same vocabulary.
 */
public enum ThrottleType {
    /** A per-attempt/per-call SDK client timeout — self-inflicted, not a store signal. */
    ATTEMPT_TIMEOUT("attempt_timeout"),
    /** A real S3 {@code 503 SlowDown} / {@code ServiceUnavailable}. */
    SLOWDOWN("slowdown"),
    /** A transient 5xx S3-side server error (not SlowDown). */
    SERVER_5XX("server5xx"),
    /** A client-local network fault (connection reset, socket timeout, DNS, TLS, ...) surviving SDK retries. */
    NETWORK("network");

    private final String tag;

    ThrottleType(String tag) {
        this.tag = tag;
    }

    /** The {@code type} tag value, matching the {@code steal_reason} reason spelling. */
    public String tag() {
        return tag;
    }
}
