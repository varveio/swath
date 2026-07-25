/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import io.varve.swath.model.ListingMode;

/**
 * A single page request. For S3 OBJECTS, pagination is purely
 * {@code startAfter = last emitted key} — {@code continuationToken} is unused
 * (kept for marker/opaque stores). {@code endBefore} is an optional upper range
 * bound for range-param stores (GCS); null for S3. Versioned pagination uses
 * {@code keyMarker} + {@code versionIdMarker}.
 *
 * <p>Byte-array fields are treated as immutable; callers must not mutate them.
 *
 * @param attemptTimeoutEscalationLevel how many times the per-attempt timeout budget for THIS
 *     attempt has been escalated: {@code 0} = the store's own base budget for this request's call
 *     class, {@code n} = the n-th escalation rung. Set by the retry loops
 *     ({@code TransientRetryFetcher}, {@code GaugedFetcher}) via
 *     {@link #withAttemptTimeoutEscalationLevel} on consecutive attempt-timeout faults of the SAME
 *     logical fetch, so a genuinely-slow tail page can eventually complete under
 *     {@code maxAttempts=1} instead of retrying forever at a budget it can never beat.
 *
 *     <p><b>A level, deliberately not a duration.</b> Retry POLICY (how many rungs exist, and when
 *     to climb one) is the engine's; the DURATION a rung costs is the store's, because only the
 *     store knows what a given call class's base budget is — and call classes differ by more than a
 *     constant factor (a point lookup and a scan are not the same call). An engine that authored
 *     absolute wall-clock here would be encoding a value it cannot know: it did, and the resulting
 *     mismatch is documented in {@code docs/internals/probe-budgets.md} §3. The store maps level to
 *     duration (for S3: {@code base(callClass) * 2^level}); the overall {@code apiCallTimeout}
 *     ceiling is never overridden here.
 */
public record PageRequest(
        ListingMode mode,
        int maxKeys,
        byte[] prefix,
        byte[] delimiter,
        byte[] startAfter,
        byte[] endBefore,
        String continuationToken,
        byte[] keyMarker,
        String versionIdMarker,
        int attemptTimeoutEscalationLevel) {

    /**
     * 9-arg convenience constructor: {@link #attemptTimeoutEscalationLevel} defaults to
     * {@code 0} (the store's base budget) — the record's own 10-arg constructor above is the
     * canonical one (a Java record does not offer additive-constructor binary compatibility; this
     * overload only helps a caller that recompiles against it). An out-of-tree store adapter/test
     * built against the 9-arg shape should call THIS constructor (or use {@link
     * #withAttemptTimeoutEscalationLevel}) rather than the 10-arg canonical one.
     */
    public PageRequest(ListingMode mode, int maxKeys, byte[] prefix, byte[] delimiter, byte[] startAfter,
                        byte[] endBefore, String continuationToken, byte[] keyMarker, String versionIdMarker) {
        this(mode, maxKeys, prefix, delimiter, startAfter, endBefore, continuationToken, keyMarker,
                versionIdMarker, 0);
    }

    /** A plain OBJECTS request paginating by {@code startAfter}. */
    public static PageRequest objects(byte[] prefix, byte[] startAfter, int maxKeys) {
        return new PageRequest(ListingMode.OBJECTS, maxKeys, prefix, null, startAfter, null, null, null, null, 0);
    }

    /** An OBJECTS request with a delimiter (folder listing / seed probe). */
    public static PageRequest objectsDelimited(byte[] prefix, byte[] delimiter, byte[] startAfter, int maxKeys) {
        return new PageRequest(
                ListingMode.OBJECTS, maxKeys, prefix, delimiter, startAfter, null, null, null, null, 0);
    }

    /**
     * A copy of this request with {@link #attemptTimeoutEscalationLevel} set/replaced —
     * used by the retry loops to escalate the per-attempt timeout of a retried logical fetch
     * without disturbing any other field (pagination cursor, prefix, mode, ...).
     */
    public PageRequest withAttemptTimeoutEscalationLevel(int level) {
        return new PageRequest(mode, maxKeys, prefix, delimiter, startAfter, endBefore,
                continuationToken, keyMarker, versionIdMarker, level);
    }
}
