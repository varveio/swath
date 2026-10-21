/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

import java.util.EnumMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Transport-level allowance shared by every request a capture makes, including SDK retries,
 * failed attempts, credential refreshes and transport resends that can be observed below the
 * SDK. An attempt is refused before it is sent; response bytes are charged as they are read.
 */
public final class AttemptGate {
    /** Hard per-request timeout ceiling for live campaigns. */
    public static final int MAX_REQUEST_TIMEOUT_SECONDS = 30;

    public enum Kind { STORAGE, AUTH }

    private final int maxAttempts;
    private final long deadlineEpochMs;
    private final int requestTimeoutSeconds;
    private final long maxTotalResponseBytes;
    private final LongSupplier clock;
    private final Map<Kind, Integer> attempts = new EnumMap<>(Kind.class);
    private final Map<Kind, Integer> transmissions = new EnumMap<>(Kind.class);
    private int total;
    private int resends;
    private long responseBytes;

    public AttemptGate(int maxAttempts, long deadlineEpochMs, int requestTimeoutSeconds,
                       long maxTotalResponseBytes) {
        this(maxAttempts, deadlineEpochMs, requestTimeoutSeconds, maxTotalResponseBytes,
                System::currentTimeMillis);
    }

    AttemptGate(int maxAttempts, long deadlineEpochMs, int requestTimeoutSeconds, long maxTotalResponseBytes,
                LongSupplier clock) {
        if (maxAttempts < 0 || maxTotalResponseBytes < 0) {
            throw new IllegalArgumentException("attempt and response-byte allowances must be nonnegative");
        }
        if (requestTimeoutSeconds < 1 || requestTimeoutSeconds > MAX_REQUEST_TIMEOUT_SECONDS) {
            throw new IllegalArgumentException("request timeout must be 1.." + MAX_REQUEST_TIMEOUT_SECONDS + " seconds");
        }
        this.maxAttempts = maxAttempts;
        this.deadlineEpochMs = deadlineEpochMs;
        this.requestTimeoutSeconds = requestTimeoutSeconds;
        this.maxTotalResponseBytes = maxTotalResponseBytes;
        this.clock = clock;
        for (Kind kind : Kind.values()) {
            attempts.put(kind, 0);
            transmissions.put(kind, 0);
        }
    }

    /** Counts one attempt or throws {@link CaptureBoundException} without counting it. */
    public synchronized void acquire(Kind kind) {
        if (clock.getAsLong() >= deadlineEpochMs) {
            throw new CaptureBoundException("deadline");
        }
        if (total >= maxAttempts) {
            throw new CaptureBoundException("attempt_allowance");
        }
        total++;
        attempts.merge(kind, 1, Integer::sum);
    }

    /**
     * Records one connection opened to send a request. A connection beyond the attempts already
     * counted is a resend the transport made on its own, so it is counted and gated here.
     */
    public synchronized void transmission(Kind kind) {
        int sent = transmissions.merge(kind, 1, Integer::sum);
        if (sent > attempts.get(kind)) {
            acquire(kind);
            resends++;
        }
    }

    /** Charges response bytes as they arrive; the aggregate cap is a bound, not a truncation. */
    public synchronized void chargeBytes(long count) {
        if (responseBytes + count > maxTotalResponseBytes) {
            responseBytes = maxTotalResponseBytes;
            throw new CaptureBoundException("response_bytes");
        }
        responseBytes += count;
    }

    /** The timeout for an attempt starting now: the request cap, shortened by the deadline. */
    public int timeoutMillisNow() {
        long remaining = deadlineEpochMs - clock.getAsLong();
        if (remaining <= 0) {
            throw new CaptureBoundException("deadline");
        }
        return (int) Math.min(requestTimeoutMillis(), remaining);
    }

    public boolean pastDeadline() {
        return clock.getAsLong() > deadlineEpochMs;
    }

    /** An attempt that finished after the deadline is late evidence and stops the walk. */
    public void requireWithinDeadline() {
        if (pastDeadline()) {
            throw new CaptureBoundException("deadline");
        }
    }

    public synchronized int attempts(Kind kind) {
        return attempts.get(kind);
    }

    public synchronized int totalAttempts() {
        return total;
    }

    public synchronized int resends() {
        return resends;
    }

    public synchronized long responseBytes() {
        return responseBytes;
    }

    public int requestTimeoutSeconds() {
        return requestTimeoutSeconds;
    }

    public int requestTimeoutMillis() {
        return requestTimeoutSeconds * 1000;
    }
}
