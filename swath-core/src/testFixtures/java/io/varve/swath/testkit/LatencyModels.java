/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.error.ListingException;
import io.varve.swath.store.PageRequest;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.function.Supplier;

/**
 * Canned {@link LatencyModel} profiles. Every profile is deterministic: for a given seed,
 * the delay for a given request is a pure function of that request's own coordinates (mode,
 * prefix, delimiter, {@code startAfter}, {@code maxKeys}) via a fresh {@link SplittableRandom}
 * keyed off {@code seed ^ hash(coordinates)} — NOT off {@link LatencyModel.Context#callIndex()} or
 * any shared mutable RNG state. That makes the SAME logical request draw the SAME delay no matter
 * when/in-what-order it lands relative to other calls, so a replay under a different worker/steal
 * interleaving still reproduces per-request timing where it matters (the {@code warm}/{@code cold}
 * classification itself is inherently order-dependent — it depends on what this fetcher instance
 * has served so far — so "order-independent" is a per-request property, not a whole-run one).
 * {@link #stormBurst} is the deliberate exception: a storm is a property of WHEN a call lands in
 * the sequence, so it is keyed off {@link LatencyModel.Context#callIndex()} instead.
 */
public final class LatencyModels {

    private LatencyModels() {
    }

    /** Uniform low-ms latency for every call, independent of call class. */
    public static LatencyModel uniformFast(long seed, Duration min, Duration max) {
        requireOrdered(min, max);
        return ctx -> uniform(seed, ctx.request(), min, max);
    }

    /**
     * The dense-tail-collapse shape: warm sequential pages stay fast
     * ({@code [warmMin, warmMax]}), but a cold/far jump — exactly what a pivot or structure probe
     * issues — is drawn from {@code [coldMin, coldMax]}, which must exceed
     * {@code probeTimeoutThreshold} (enforced at construction time below).
     *
     * <p>This model injects latency only — it never throws. {@code probeTimeoutThreshold} exists
     * so a composed test can (a) configure the ENGINE's attempt-timeout budget for the fixture run
     * at/below this value (scale the timeout DOWN to ms-scale rather than sleeping seconds) and
     * (b) layer a {@code
     * PageInterceptor} that inspects {@code request.attemptTimeoutEscalationLevel()} the way {@code
     * GaugedFetcherAttemptTimeoutEscalationTest} does, to turn an over-threshold cold delay into an
     * actual {@code ThrottleException(Kind.ATTEMPT_TIMEOUT)} — reproducing the probe-timeout
     * spiral end to end. That composition is deliberately left to the caller (the big Hive-keyspace
     * acceptance experiment); this profile only guarantees the timing shape is available.
     */
    public static LatencyModel denseColdProbes(long seed, Duration warmMin, Duration warmMax,
                                                Duration probeTimeoutThreshold, Duration coldMin, Duration coldMax) {
        requireOrdered(warmMin, warmMax);
        requireOrdered(coldMin, coldMax);
        Objects.requireNonNull(probeTimeoutThreshold, "probeTimeoutThreshold");
        if (probeTimeoutThreshold.isNegative()) {
            throw new IllegalArgumentException("probeTimeoutThreshold must be >= 0: " + probeTimeoutThreshold);
        }
        if (coldMin.compareTo(probeTimeoutThreshold) <= 0) {
            throw new IllegalArgumentException(
                    "denseColdProbes requires coldMin > probeTimeoutThreshold (cold jumps must exceed the "
                            + "configured probe-timeout budget to reproduce the spiral), got coldMin=" + coldMin
                            + " probeTimeoutThreshold=" + probeTimeoutThreshold);
        }
        return ctx -> {
            Duration lo = ctx.cold() ? coldMin : warmMin;
            Duration hi = ctx.cold() ? coldMax : warmMax;
            return uniform(seed, ctx.request(), lo, hi);
        };
    }

    /**
     * A scripted call-index window of elevated latency ({@code [burstMin, burstMax]}) layered over
     * {@code base} outside the window — models a transient storm (e.g. a throttling event) rather
     * than a steady-state regime. Deliberately {@code callIndex}-keyed (see the class doc): a
     * storm is about WHEN a call lands, not what it asks for, so unlike {@link #uniformFast}/
     * {@link #denseColdProbes} this is order-DEPENDENT under concurrency. Pair with {@link
     * #stormFaultWindow} (same window) to also model the "failures" half of a storm.
     */
    public static LatencyModel stormBurst(LatencyModel base, long seed, int windowStartCallIndex,
                                           int windowLength, Duration burstMin, Duration burstMax) {
        Objects.requireNonNull(base, "base");
        requireOrdered(burstMin, burstMax);
        if (windowStartCallIndex < 0) {
            throw new IllegalArgumentException("windowStartCallIndex must be >= 0: " + windowStartCallIndex);
        }
        if (windowLength < 0) {
            throw new IllegalArgumentException("windowLength must be >= 0: " + windowLength);
        }
        return ctx -> inWindow(ctx.callIndex(), windowStartCallIndex, windowLength)
                ? uniform(seed, ctx.request(), burstMin, burstMax)
                : base.delayFor(ctx);
    }

    /**
     * The "failures" half of a {@link #stormBurst} window: a {@link MockPageFetcher.PageInterceptor}
     * that throws {@code fault.get()} for every call whose index falls in
     * {@code [windowStartCallIndex, windowStartCallIndex + windowLength)}, and passes every other
     * call through unchanged. Compose with {@link #stormBurst} on the SAME window via {@code
     * MockPageFetcher.Builder#latencyModel}/{@code #interceptor} to script a combined
     * latency+failure storm.
     */
    public static MockPageFetcher.PageInterceptor stormFaultWindow(int windowStartCallIndex, int windowLength,
                                                                     Supplier<ListingException> fault) {
        Objects.requireNonNull(fault, "fault");
        if (windowStartCallIndex < 0) {
            throw new IllegalArgumentException("windowStartCallIndex must be >= 0: " + windowStartCallIndex);
        }
        if (windowLength < 0) {
            throw new IllegalArgumentException("windowLength must be >= 0: " + windowLength);
        }
        return (req, idx, page) -> {
            if (inWindow(idx, windowStartCallIndex, windowLength)) {
                throw fault.get();
            }
            return page;
        };
    }

    private static boolean inWindow(int callIndex, int start, int length) {
        return callIndex >= start && callIndex < start + length;
    }

    private static Duration uniform(long seed, PageRequest request, Duration min, Duration max) {
        long loNanos = min.toNanos();
        long hiNanos = max.toNanos();
        if (loNanos == hiNanos) {
            return min;
        }
        SplittableRandom rng = new SplittableRandom(seed ^ coordinateHash(request));
        return Duration.ofNanos(rng.nextLong(loNanos, hiNanos + 1));
    }

    /**
     * A stable, content-based hash of the request's coordinates — deliberately NOT
     * {@code request.hashCode()}, whose {@code record}-generated implementation hashes the
     * {@code byte[]} fields by REFERENCE (identity), not content, so two byte-identical requests
     * built from separate arrays would otherwise land in different RNG streams.
     */
    private static long coordinateHash(PageRequest request) {
        long h = 1_125_899_906_842_597L;   // FNV-ish odd seed constant
        h = 31 * h + Arrays.hashCode(request.prefix());
        h = 31 * h + Arrays.hashCode(request.startAfter());
        h = 31 * h + Arrays.hashCode(request.delimiter());
        h = 31 * h + Arrays.hashCode(request.keyMarker());
        h = 31 * h + Objects.hashCode(request.continuationToken());
        h = 31 * h + Objects.hashCode(request.versionIdMarker());
        h = 31 * h + request.maxKeys();
        h = 31 * h + (request.mode() == null ? 0 : request.mode().ordinal());
        return h;
    }

    private static void requireOrdered(Duration min, Duration max) {
        Objects.requireNonNull(min, "min");
        Objects.requireNonNull(max, "max");
        if (min.isNegative()) {
            throw new IllegalArgumentException("min must be >= 0: " + min);
        }
        if (max.compareTo(min) < 0) {
            throw new IllegalArgumentException("max must be >= min: min=" + min + " max=" + max);
        }
    }
}
