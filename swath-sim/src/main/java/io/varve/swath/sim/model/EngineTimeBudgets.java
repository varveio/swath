/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import java.util.concurrent.TimeUnit;

/**
 * Declared virtual-time budgets for a simulated engine run.
 *
 * <p>{@link #engineDefaults()} is an approximate reference, not an exact copy of production. In
 * particular, this model has one 3 s probe-attempt field for seed, pivot, and structure calls;
 * production uses 3 s for point-class pivot probes and 10 s for scan-class seed and structure calls.
 *
 * <p>Durations are nanoseconds. Construction requires all counts and every duration except the
 * maximum duration to be positive, both maxima to be at least their minima, the AIMD budgets to be
 * present, and the maximum duration to be nonnegative. The futility cooldown is excluded because it
 * is measured in selection skips, not time.
 *
 * @param seedProbeBudget            probes the seed descent may spend, in calls
 * @param probeAttemptTimeoutNanos   per-attempt timeout for every simulated probe class; unlike
 *                                   production, the model cannot express its 3 s point / 10 s scan
 *                                   split
 * @param workerAttemptTimeoutNanos  per-attempt timeout for worker page fetches; the reference default
 *                                   is 10 s
 * @param workerAttemptRetryCap      worker retry threshold; under simulated {@code BOUNDED},
 *                                   exhaustion stops the run as stuck, while simulated
 *                                   {@code RIDE_OUT} ignores it. Production's default
 *                                   {@code RIDE_OUT} uses the reference value to shape backoff,
 *                                   not as a stopping ceiling
 * @param probeAttemptRetryCap       transient retries before an exhausted seed or thief probe
 *                                   abandons its descent or steal attempt
 * @param transientRetryBackoffNanos flat simulated delay between retries; production uses full-jitter
 *                                   exponential backoff
 * @param idleStealBaseParkNanos     the idle thief's shortest park, and its backoff's base step
 * @param idleStealBackoffCapNanos   the cap the idle-steal backoff grows to
 * @param idleStealAttemptParkNanos  the park of a worker denied the fleet-wide steal-attempt slot
 * @param concurrencyCleanWindowNanos the clean window an additive concurrency increase must observe
 * @param aimd                       the adaptive-concurrency controller's own windows
 * @param maxDurationNanos           the whole run's ceiling; {@code 0} means unbounded
 */
public record EngineTimeBudgets(
        int seedProbeBudget,
        long probeAttemptTimeoutNanos,
        long workerAttemptTimeoutNanos,
        int workerAttemptRetryCap,
        int probeAttemptRetryCap,
        long transientRetryBackoffNanos,
        long idleStealBaseParkNanos,
        long idleStealBackoffCapNanos,
        long idleStealAttemptParkNanos,
        long concurrencyCleanWindowNanos,
        AimdBudgets aimd,
        long maxDurationNanos) {

    /**
     * Virtual-time windows used by the simulated adaptive-concurrency controller.
     *
     * @param growthPaceNanos          minimum interval between successive additive {@code +1} steps
     * @param transientTimeoutWindowNanos trailing window over which worker timeouts are counted for the
     *                                 growth freeze
     * @param shedWindowMinNanos       shortest jittered sustained-timeout shed window
     * @param shedWindowMaxNanos       longest jittered sustained-timeout shed window (inclusive)
     * @param latencyBaselineDecayNanos how long the latency-freeze rung's rolling-minimum baseline is
     *                                 held before it is re-floored to the elapsed window's minimum
     * @param valvePaceNanos           minimum interval between valve-admitted {@code +1} steps while
     *                                 latency-frozen
     */
    public record AimdBudgets(
            long growthPaceNanos,
            long transientTimeoutWindowNanos,
            long shedWindowMinNanos,
            long shedWindowMaxNanos,
            long latencyBaselineDecayNanos,
            long valvePaceNanos) {

        public AimdBudgets {
            requirePositive("growthPaceNanos", growthPaceNanos);
            requirePositive("transientTimeoutWindowNanos", transientTimeoutWindowNanos);
            requirePositive("shedWindowMinNanos", shedWindowMinNanos);
            requirePositive("shedWindowMaxNanos", shedWindowMaxNanos);
            requirePositive("latencyBaselineDecayNanos", latencyBaselineDecayNanos);
            requirePositive("valvePaceNanos", valvePaceNanos);
            if (shedWindowMaxNanos < shedWindowMinNanos) {
                throw new IllegalArgumentException("shedWindowMaxNanos (" + shedWindowMaxNanos + ") must "
                        + "be >= shedWindowMinNanos (" + shedWindowMinNanos + ")");
            }
        }

        /**
         * The windows the shipped controller uses today: a 1 s growth pace, a 10 s transient-timeout
         * window, a shed window jittered in [25 s, 40 s], a 60 s latency-baseline decay, and a valve
         * paced at the shed window's 30 s nominal length.
         */
        public static AimdBudgets engineDefaults() {
            return new AimdBudgets(
                    TimeUnit.SECONDS.toNanos(1),
                    TimeUnit.SECONDS.toNanos(10),
                    TimeUnit.SECONDS.toNanos(25),
                    TimeUnit.SECONDS.toNanos(40),
                    TimeUnit.SECONDS.toNanos(60),
                    TimeUnit.SECONDS.toNanos(30));
        }

        /** These windows with both shed-window bounds pinned to {@code nanos} — a jitter-free run. */
        public AimdBudgets withFixedShedWindow(long nanos) {
            return new AimdBudgets(growthPaceNanos, transientTimeoutWindowNanos, nanos, nanos,
                    latencyBaselineDecayNanos, valvePaceNanos);
        }
    }

    /** {@link #maxDurationNanos()} for a run with no ceiling — the engine's own default. */
    public static final long UNBOUNDED_DURATION = 0L;

    /** Seed budget at concurrency 64: {@code min(256, max(1, min(1000, 4 * 64)))}. */
    private static final int DEFAULT_SEED_PROBE_BUDGET = 256;

    /** Reference worker retry threshold; it is a hard ceiling only under simulated {@code BOUNDED}. */
    private static final int DEFAULT_WORKER_ATTEMPT_RETRY_CAP = 8;

    /** Reference probe retry ceiling; exhaustion abandons the descent or steal. */
    private static final int DEFAULT_PROBE_ATTEMPT_RETRY_CAP = 1;

    public EngineTimeBudgets {
        requirePositive("seedProbeBudget", seedProbeBudget);
        requirePositive("probeAttemptTimeoutNanos", probeAttemptTimeoutNanos);
        requirePositive("workerAttemptTimeoutNanos", workerAttemptTimeoutNanos);
        requirePositive("workerAttemptRetryCap", workerAttemptRetryCap);
        requirePositive("probeAttemptRetryCap", probeAttemptRetryCap);
        requirePositive("transientRetryBackoffNanos", transientRetryBackoffNanos);
        if (aimd == null) {
            throw new MissingSimDependencyException("adaptive-concurrency time windows (growth pace, "
                    + "timeout windows, shed window, latency-baseline decay, valve pace)");
        }
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
     * Returns the simulator's production-reference defaults.
     *
     * <p>The seed budget is 256 at concurrency 64; the worker retry threshold is 8 (a hard cap only
     * under simulated {@code BOUNDED}) and the probe retry cap is 1; retry backoff is a flat 100 ms;
     * idle-steal pacing is 5 ms base, 50 ms cap, and 1 s attempt park; the clean window is 10 s; and
     * duration is unbounded. The worker timeout is 10 s.
     *
     * <p>This is only approximate for probes: the returned single 3 s timeout applies to seed, pivot,
     * and structure calls, whereas production uses 3 s for point pivots and 10 s for scan-class seed
     * and structure calls.
     */
    public static EngineTimeBudgets engineDefaults() {
        return new EngineTimeBudgets(
                DEFAULT_SEED_PROBE_BUDGET,
                TimeUnit.SECONDS.toNanos(3),
                TimeUnit.SECONDS.toNanos(10),
                DEFAULT_WORKER_ATTEMPT_RETRY_CAP,
                DEFAULT_PROBE_ATTEMPT_RETRY_CAP,
                TimeUnit.MILLISECONDS.toNanos(100),
                TimeUnit.MILLISECONDS.toNanos(5),
                TimeUnit.MILLISECONDS.toNanos(50),
                TimeUnit.SECONDS.toNanos(1),
                TimeUnit.SECONDS.toNanos(10),
                AimdBudgets.engineDefaults(),
                UNBOUNDED_DURATION);
    }

    /** This budget set with {@code maxDurationNanos} replaced — the knob a liveness scenario varies. */
    public EngineTimeBudgets withMaxDuration(long newMaxDurationNanos) {
        return new EngineTimeBudgets(seedProbeBudget, probeAttemptTimeoutNanos, workerAttemptTimeoutNanos,
                workerAttemptRetryCap, probeAttemptRetryCap, transientRetryBackoffNanos,
                idleStealBaseParkNanos, idleStealBackoffCapNanos, idleStealAttemptParkNanos,
                concurrencyCleanWindowNanos, aimd, newMaxDurationNanos);
    }

    /** This budget set with {@code aimd} replaced — the knob an AIMD-shape scenario varies. */
    public EngineTimeBudgets withAimd(AimdBudgets newAimd) {
        return new EngineTimeBudgets(seedProbeBudget, probeAttemptTimeoutNanos, workerAttemptTimeoutNanos,
                workerAttemptRetryCap, probeAttemptRetryCap, transientRetryBackoffNanos,
                idleStealBaseParkNanos, idleStealBackoffCapNanos, idleStealAttemptParkNanos,
                concurrencyCleanWindowNanos, newAimd, maxDurationNanos);
    }

    /** Returns these budgets with different probe and worker attempt timeouts. */
    public EngineTimeBudgets withAttemptTimeouts(long newProbeTimeoutNanos, long newWorkerTimeoutNanos) {
        return new EngineTimeBudgets(seedProbeBudget, newProbeTimeoutNanos, newWorkerTimeoutNanos,
                workerAttemptRetryCap, probeAttemptRetryCap, transientRetryBackoffNanos,
                idleStealBaseParkNanos, idleStealBackoffCapNanos, idleStealAttemptParkNanos,
                concurrencyCleanWindowNanos, aimd, maxDurationNanos);
    }

    /** Returns these budgets with a different seed/thief probe retry ceiling. */
    public EngineTimeBudgets withProbeAttemptRetryCap(int newProbeAttemptRetryCap) {
        return new EngineTimeBudgets(seedProbeBudget, probeAttemptTimeoutNanos, workerAttemptTimeoutNanos,
                workerAttemptRetryCap, newProbeAttemptRetryCap, transientRetryBackoffNanos,
                idleStealBaseParkNanos, idleStealBackoffCapNanos, idleStealAttemptParkNanos,
                concurrencyCleanWindowNanos, aimd, maxDurationNanos);
    }

    private static void requirePositive(String name, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive, got " + value);
        }
    }
}
