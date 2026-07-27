/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import java.util.concurrent.TimeUnit;

/**
 * The engine's timeouts, pacing windows and probe budget, restated as <b>declared inputs of a
 * simulated run</b> rather than inherited from whatever the engine's compiled-in constants happen to
 * be.
 *
 * <p><b>Why they are declared and not imported.</b> Every one of these is a fixed duration, and a
 * fixed duration only means something relative to the latencies around it. Under a real-time
 * experiment those ratios move with the host and with any latency scaling applied to the workload,
 * which is exactly how a timeout pathology disappears from a scaled-down reproduction: the budget
 * stayed at three seconds while the call it bounds got ten times faster. Virtual time can state the
 * ratio exactly — but only if the budget is an input a scenario can set, so a sweep can vary the
 * ratio deliberately instead of discovering it. Reading the engine's constants directly would
 * reintroduce the silent coupling this record exists to break.
 *
 * <p>{@link #engineDefaults()} carries the values the shipped engine currently uses, so a scenario
 * that wants "today's engine" says so explicitly and a scenario that wants a different ratio states
 * the difference against a written reference.
 *
 * <p><b>Not here, deliberately:</b> the per-victim futility-pacing cooldown. It is counted in
 * steal-selection <em>skips</em>, not nanoseconds, so it is not a time budget at all and belongs to
 * the policy configuration a later phase wires up.
 *
 * @param seedProbeBudget            probes the seed descent may spend, in calls
 * @param probeAttemptTimeoutNanos   per-attempt timeout for a pivot/structure probe
 * @param workerAttemptTimeoutNanos  per-attempt timeout for a worker page fetch
 * @param idleStealBaseParkNanos     the idle thief's shortest park, and its backoff's base step
 * @param idleStealBackoffCapNanos   the cap the idle-steal backoff grows to
 * @param idleStealAttemptParkNanos  the park of a worker denied the fleet-wide steal-attempt slot
 * @param concurrencyCleanWindowNanos the clean window an additive concurrency increase must observe
 * @param maxDurationNanos           the whole run's ceiling; {@code 0} means unbounded
 */
public record EngineTimeBudgets(
        int seedProbeBudget,
        long probeAttemptTimeoutNanos,
        long workerAttemptTimeoutNanos,
        long idleStealBaseParkNanos,
        long idleStealBackoffCapNanos,
        long idleStealAttemptParkNanos,
        long concurrencyCleanWindowNanos,
        long maxDurationNanos) {

    /** {@link #maxDurationNanos()} for a run with no ceiling — the engine's own default. */
    public static final long UNBOUNDED_DURATION = 0L;

    /** The seed descent's probe budget at the engine's default concurrency ceiling (see below). */
    private static final int DEFAULT_SEED_PROBE_BUDGET = 256;

    public EngineTimeBudgets {
        requirePositive("seedProbeBudget", seedProbeBudget);
        requirePositive("probeAttemptTimeoutNanos", probeAttemptTimeoutNanos);
        requirePositive("workerAttemptTimeoutNanos", workerAttemptTimeoutNanos);
        requirePositive("idleStealBaseParkNanos", idleStealBaseParkNanos);
        requirePositive("idleStealBackoffCapNanos", idleStealBackoffCapNanos);
        requirePositive("idleStealAttemptParkNanos", idleStealAttemptParkNanos);
        requirePositive("concurrencyCleanWindowNanos", concurrencyCleanWindowNanos);
        if (maxDurationNanos < 0) {
            throw new IllegalArgumentException("maxDurationNanos must be >= 0 (0 = unbounded), got "
                    + maxDurationNanos);
        }
        if (idleStealBackoffCapNanos < idleStealBaseParkNanos) {
            throw new IllegalArgumentException("idleStealBackoffCapNanos (" + idleStealBackoffCapNanos
                    + ") must be >= idleStealBaseParkNanos (" + idleStealBaseParkNanos + ")");
        }
    }

    /**
     * The values the shipped engine uses today, as the reference point a scenario varies from.
     *
     * <ul>
     *   <li><b>seed probe budget 256</b> — the descent bounds itself at
     *       {@code min(256, max(1, min(1000, 4 x concurrency)))}, which saturates at the default
     *       concurrency ceiling of 64. A scenario at a lower concurrency sets the smaller budget its
     *       own worker count would produce.</li>
     *   <li><b>probe attempt timeout 3 s</b> — the pivot/structure-probe attempt override.</li>
     *   <li><b>worker attempt timeout 10 s</b> — the client-level per-attempt default worker pages
     *       keep.</li>
     *   <li><b>idle-steal base park 5 ms / backoff cap 50 ms / attempt park 1 s</b> — the fleet-wide
     *       idle-steal pacing windows.</li>
     *   <li><b>clean window 10 s</b> — the interval of clean responses an additive concurrency
     *       increase requires.</li>
     *   <li><b>max duration unbounded</b> — the engine ships with no run ceiling set.</li>
     * </ul>
     */
    public static EngineTimeBudgets engineDefaults() {
        return new EngineTimeBudgets(
                DEFAULT_SEED_PROBE_BUDGET,
                TimeUnit.SECONDS.toNanos(3),
                TimeUnit.SECONDS.toNanos(10),
                TimeUnit.MILLISECONDS.toNanos(5),
                TimeUnit.MILLISECONDS.toNanos(50),
                TimeUnit.SECONDS.toNanos(1),
                TimeUnit.SECONDS.toNanos(10),
                UNBOUNDED_DURATION);
    }

    /** This budget set with {@code maxDurationNanos} replaced — the knob a liveness scenario varies. */
    public EngineTimeBudgets withMaxDuration(long newMaxDurationNanos) {
        return new EngineTimeBudgets(seedProbeBudget, probeAttemptTimeoutNanos, workerAttemptTimeoutNanos,
                idleStealBaseParkNanos, idleStealBackoffCapNanos, idleStealAttemptParkNanos,
                concurrencyCleanWindowNanos, newMaxDurationNanos);
    }

    private static void requirePositive(String name, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive, got " + value);
        }
    }
}
