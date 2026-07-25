/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/**
 * A transient, retryable store-side/client-side signal that surfaced as an
 * exception <b>after</b> the SDK's own {@code RetryStrategy} retries were
 * exhausted (algorithms.md §5). Covers four {@link Kind}s: a real S3
 * {@code 503 SlowDown} / {@code ServiceUnavailable}, a 5xx server error, a
 * client-side per-attempt/per-call timeout, and an exhausted network fault.
 *
 * <p>Unlike a plain {@link ListingException} (which is fatal — exit 1), a
 * {@code ThrottleException} must NOT abort the scan. The engine's gauge-aware
 * fetcher wrappers catch it, back off, and retry the SAME fetch — the page/probe
 * is never lost. It extends {@link ListingException} only so it flows through the
 * existing {@code throws ListingException} fetcher signatures; if a throttle ever
 * escapes the retry wrappers as itself — the count-bounded no-token/embedded path, not a
 * token-wired {@code RIDE_OUT} (unbounded) or {@code BOUNDED} (converts to a resumable STUCK) path —
 * it degrades to the fatal {@code ListingException} contract (exit 1).
 *
 * <p><b>Why {@link Kind} exists.</b> Only a real store <i>backpressure</i>
 * signal — a 503 {@code SlowDown} or a 5xx server response — should drive the AIMD
 * {@code ConcurrencyGauge} down. A client-side attempt-timeout or an exhausted
 * network fault is NOT S3 asking us to slow down; it is a local read stall. Folding
 * those into the AIMD vote is exactly what strangled concurrency to 1
 * (a hung-read tail read as a throttle storm). So the retry wrapper consults
 * {@link Kind#votesAimdDown()}: {@link Kind#SLOWDOWN}/{@link Kind#SERVER_5XX} vote
 * the target down; {@link Kind#ATTEMPT_TIMEOUT}/{@link Kind#NETWORK} are retried
 * without a concurrency vote (they instead feed the growth-freeze hedge).
 *
 * <p>{@link Kind} is a purely behavioral classifier (does it vote AIMD down); the
 * exported {@code swath.throttle.events{type}} observability dimension lives on the 1:1
 * name-mapped {@link ThrottleType} (see {@link #type()}). The two are kept distinct so a
 * dimension rename never perturbs the AIMD-voting contract, and vice versa.
 */
public final class ThrottleException extends ListingException {

    /** The transient cause, and whether it is a genuine AIMD backpressure signal. */
    public enum Kind {
        /** Real S3 {@code 503 SlowDown} / {@code ServiceUnavailable} — genuine backpressure. */
        SLOWDOWN(true),
        /** 5xx server error (500 InternalError etc.) — server-side, treated as backpressure. */
        SERVER_5XX(true),
        /** Client-side per-attempt / per-call SDK timeout — a local read stall, NOT backpressure. */
        ATTEMPT_TIMEOUT(false),
        /** Exhausted client-local network fault (connection reset, socket timeout, DNS, TLS). */
        NETWORK(false);

        private final boolean votesAimdDown;

        Kind(boolean votesAimdDown) {
            this.votesAimdDown = votesAimdDown;
        }

        /** Whether this kind is a genuine store backpressure signal that should drive AIMD down. */
        public boolean votesAimdDown() {
            return votesAimdDown;
        }
    }

    private final Kind kind;

    private ThrottleException(String message, Throwable cause, boolean causeSupplied, Kind kind) {
        super(message);
        if (causeSupplied) {
            initCause(cause);
        }
        this.kind = kind;
    }

    /** A real S3 503 {@code SlowDown} / {@code ServiceUnavailable} backpressure signal. */
    public static ThrottleException slowDown(String message) {
        return new ThrottleException(message, null, false, Kind.SLOWDOWN);
    }

    /** A real S3 503 {@code SlowDown} / {@code ServiceUnavailable} wrapping {@code cause}. */
    public static ThrottleException slowDown(String message, Throwable cause) {
        return new ThrottleException(message, cause, true, Kind.SLOWDOWN);
    }

    /** A client-side per-attempt or per-call timeout, which does not vote AIMD down. */
    public static ThrottleException attemptTimeout(String message) {
        return new ThrottleException(message, null, false, Kind.ATTEMPT_TIMEOUT);
    }

    /** A client-side per-attempt or per-call timeout wrapping {@code cause}. */
    public static ThrottleException attemptTimeout(String message, Throwable cause) {
        return new ThrottleException(message, cause, true, Kind.ATTEMPT_TIMEOUT);
    }

    /** An exhausted client-local network fault, which does not vote AIMD down. */
    public static ThrottleException networkExhaustion(String message) {
        return new ThrottleException(message, null, false, Kind.NETWORK);
    }

    /** An exhausted client-local network fault wrapping {@code cause}. */
    public static ThrottleException networkExhaustion(String message, Throwable cause) {
        return new ThrottleException(message, cause, true, Kind.NETWORK);
    }

    /** A retryable 5xx server response that is treated as store backpressure. */
    public static ThrottleException serverError(String message) {
        return new ThrottleException(message, null, false, Kind.SERVER_5XX);
    }

    /** A retryable 5xx server response wrapping {@code cause}. */
    public static ThrottleException serverError(String message, Throwable cause) {
        return new ThrottleException(message, cause, true, Kind.SERVER_5XX);
    }

    /** A test/script-supplied transient classification selected at runtime. */
    public static ThrottleException classifiedTransient(String message, Kind kind) {
        return new ThrottleException(message, null, false, kind);
    }

    /**
     * A transient classification supplied through its observability {@link ThrottleType} view.
     * {@link Kind} and {@link ThrottleType} share member names 1:1.
     */
    public static ThrottleException classifiedTransient(String message, Throwable cause, ThrottleType type) {
        return new ThrottleException(message, cause, true, Kind.valueOf(type.name()));
    }

    /** The transient cause; drives whether the retry wrapper votes AIMD down. */
    public Kind kind() {
        return kind;
    }

    /**
     * The {@link ThrottleType} view of this throttle ({@code swath.throttle.events{type}}
     * observability), derived 1:1 from {@link #kind()} — the two enums share member names.
     */
    public ThrottleType type() {
        return ThrottleType.valueOf(kind.name());
    }
}
