/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import io.varve.swath.model.ListingMode;
import java.time.Duration;

/**
 * A single page request. For S3 OBJECTS, pagination is purely
 * {@code startAfter = last emitted key} — {@code continuationToken} is unused
 * (kept for marker/opaque stores). {@code endBefore} is an optional upper range
 * bound for range-param stores (GCS); null for S3. Versioned pagination uses
 * {@code keyMarker} + {@code versionIdMarker}.
 *
 * <p>Byte-array fields are treated as immutable; callers must not mutate them.
 *
 * @param apiCallAttemptTimeoutOverride an optional per-request override of the SDK
 *     per-attempt timeout for THIS attempt only (applied by a store's fetcher — currently
 *     {@code S3PageFetcher} via the AWS SDK's per-request {@code overrideConfiguration}).
 *     {@code null} = use the store's configured base ({@code S3Config.DEFAULT_ATTEMPT_TIMEOUT},
 *     10 s); the overall {@code apiCallTimeout} ceiling (60 s) is never overridden here. Set by
 *     the retry loops ({@code TransientRetryFetcher}, {@code GaugedFetcher}) via
 *     {@link #withApiCallAttemptTimeoutOverride} to escalate a genuinely-slow tail page's timeout
 *     on consecutive attempt-timeout faults of the SAME logical fetch, so it can eventually
 *     complete under {@code maxAttempts=1} instead of retrying forever at a budget it can never
 *     beat.
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
        Duration apiCallAttemptTimeoutOverride) {

    /**
     * 9-arg convenience constructor: {@link #apiCallAttemptTimeoutOverride} defaults to
     * {@code null} (no per-attempt override) — the record's own 10-arg constructor above is the
     * canonical one (a Java record does not offer additive-constructor binary compatibility; this
     * overload only helps a caller that recompiles against it). An out-of-tree store adapter/test
     * built against the 9-arg shape should call THIS constructor (or use {@link
     * #withApiCallAttemptTimeoutOverride}) rather than the 10-arg canonical one.
     */
    public PageRequest(ListingMode mode, int maxKeys, byte[] prefix, byte[] delimiter, byte[] startAfter,
                        byte[] endBefore, String continuationToken, byte[] keyMarker, String versionIdMarker) {
        this(mode, maxKeys, prefix, delimiter, startAfter, endBefore, continuationToken, keyMarker,
                versionIdMarker, null);
    }

    /** A plain OBJECTS request paginating by {@code startAfter}. */
    public static PageRequest objects(byte[] prefix, byte[] startAfter, int maxKeys) {
        return new PageRequest(ListingMode.OBJECTS, maxKeys, prefix, null, startAfter, null, null, null, null, null);
    }

    /** An OBJECTS request with a delimiter (folder listing / seed probe). */
    public static PageRequest objectsDelimited(byte[] prefix, byte[] delimiter, byte[] startAfter, int maxKeys) {
        return new PageRequest(
                ListingMode.OBJECTS, maxKeys, prefix, delimiter, startAfter, null, null, null, null, null);
    }

    /**
     * A copy of this request with {@link #apiCallAttemptTimeoutOverride} set/replaced —
     * used by the retry loops to escalate the per-attempt timeout of a retried logical fetch
     * without disturbing any other field (pagination cursor, prefix, mode, ...).
     */
    public PageRequest withApiCallAttemptTimeoutOverride(Duration override) {
        return new PageRequest(mode, maxKeys, prefix, delimiter, startAfter, endBefore,
                continuationToken, keyMarker, versionIdMarker, override);
    }
}
