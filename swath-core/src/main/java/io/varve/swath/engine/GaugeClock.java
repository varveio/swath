/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Test seam: the {@link ConcurrencyGauge}'s time source, threaded through
 * {@link WorkStealingScan}'s constructor so a regime/composition test can COMPRESS the gauge's
 * real wall-clock adaptive windows (the sustained-timeout shed window, the 10&nbsp;s
 * clean/recovery window, the growth-freeze window, the +1 growth pace, the
 * latency-baseline decay) down to the ~100&nbsp;ms scale a fast offline run can actually span. It
 * rides the constructor the way {@link RetryConfig} threads the retry policy — one value, not two
 * loose parameters.
 *
 * <p><b>Default-preserving.</b> A {@code null} {@code GaugeClock} means "production": the engine
 * builds the gauge with its own {@code System::nanoTime} + jittered-window defaults. Only a test that
 * passes a non-null {@code GaugeClock} changes anything; every constructor overload defaults it to
 * {@code null}.
 *
 * <p><b>Relationship-preserving.</b> Because ConcurrencyGauge routes <i>all</i> of its nano reads
 * through {@link #nanoClock()}, injecting a single {@link #scaled scaled clock} compresses every
 * window by the SAME factor, preserving their RELATIVE timing — the adaptive loops still race in the
 * correct order, just faster. {@link #shedWindowNanosSupplier()} is offered separately so a test can
 * also pin the shed window to a fixed length (production jitters it) for determinism.
 *
 * @param nanoClock the source for ALL of the gauge's {@code nanoTime} reads (production
 *                  {@code System::nanoTime})
 * @param shedWindowNanosSupplier the length of each shed window in the {@code nanoClock}'s units
 *                                (production draws a fresh jitter in [25&nbsp;s,40&nbsp;s] per window)
 */
public record GaugeClock(LongSupplier nanoClock, LongSupplier shedWindowNanosSupplier) {

    public GaugeClock {
        Objects.requireNonNull(nanoClock, "nanoClock");
        Objects.requireNonNull(shedWindowNanosSupplier, "shedWindowNanosSupplier");
    }

    /**
     * A gauge clock that runs {@code scale}× faster than real wall-clock time (so a 10&nbsp;s window
     * elapses in {@code 10s/scale} real time), with a fixed shed-window length of
     * {@code shedWindowNanos} in the compressed (virtual) time base. All of the gauge's other windows
     * (clean/recovery, growth-freeze, +1 pace, latency decay) compress by the same {@code scale}.
     *
     * @param scale time-acceleration factor (&ge; 1); e.g. 300 turns the 10&nbsp;s clean window into
     *              ~33&nbsp;ms and a 30&nbsp;s shed window into ~100&nbsp;ms of real time
     * @param shedWindowNanos fixed shed-window length in the VIRTUAL (scaled) time base — i.e. compare
     *                        it against the same units {@link #nanoClock()} returns
     */
    public static GaugeClock scaled(long scale, long shedWindowNanos) {
        if (scale < 1L) {
            throw new IllegalArgumentException("scale must be >= 1: " + scale);
        }
        long origin = System.nanoTime();
        LongSupplier fast = () -> origin + (System.nanoTime() - origin) * scale;
        return new GaugeClock(fast, () -> shedWindowNanos);
    }
}
