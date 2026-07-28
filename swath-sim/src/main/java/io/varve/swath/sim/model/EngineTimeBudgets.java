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
 * @param workerAttemptRetryCap      transient retries a worker page fetch may spend before the run is
 *                                   declared stuck (the engine's bounded transient-retry policy)
 * @param probeAttemptRetryCap       transient retries a thief probe may spend before the whole steal
 *                                   attempt fails fast — deliberately far smaller than the worker cap
 * @param transientRetryBackoffNanos the delay charged between transient retries. The engine draws a
 *                                   full-jitter exponential backoff here; a scenario declares one flat
 *                                   interval instead, because jitter's live purpose is desynchronising
 *                                   a fleet of separate processes, which a single-threaded kernel has
 *                                   no analogue of — what the model needs is the ratio of the backoff
 *                                   to the latencies around it, and that is exactly what this states
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
     * The adaptive-concurrency controller's own time windows, grouped so the surrounding record stays
     * readable: they are one mechanism's parameters, they are varied together, and every one of them is
     * meaningless except relative to the others.
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

    /** The seed descent's probe budget at the engine's default concurrency ceiling (see below). */
    private static final int DEFAULT_SEED_PROBE_BUDGET = 256;

    /** The engine's bounded transient-retry ceiling for a slot-gated worker page fetch. */
    private static final int DEFAULT_WORKER_ATTEMPT_RETRY_CAP = 8;

    /** The engine's probe fail-fast ceiling: a probe rides out nothing. */
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
     *   <li><b>worker retry cap 8 / probe retry cap 1</b> — the transient-retry ceilings the engine's
     *       bounded retry policy and its probe fail-fast apply. A probe that keeps timing out costs the
     *       whole steal attempt rather than riding out a storm; a worker page that exhausts its cap
     *       ends the run as stuck.</li>
     *   <li><b>transient retry backoff 100 ms</b> — one flat interval standing in for the engine's
     *       full-jitter exponential schedule (see the component's own note).</li>
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

    /**
     * This budget set with both per-attempt timeouts replaced — the ratio a timeout-pathology scenario
     * varies against a fixed latency model (C0's lesson: a budget only means something relative to the
     * latencies it bounds).
     */
    public EngineTimeBudgets withAttemptTimeouts(long newProbeTimeoutNanos, long newWorkerTimeoutNanos) {
        return new EngineTimeBudgets(seedProbeBudget, newProbeTimeoutNanos, newWorkerTimeoutNanos,
                workerAttemptRetryCap, probeAttemptRetryCap, transientRetryBackoffNanos,
                idleStealBaseParkNanos, idleStealBackoffCapNanos, idleStealAttemptParkNanos,
                concurrencyCleanWindowNanos, aimd, maxDurationNanos);
    }

    /**
     * This budget set with the thief's probe retry ceiling replaced — the knob a scenario varies to ask
     * how much a steal is willing to spend riding out a store that is not answering. A declared input
     * like every other here: the executor honours whatever this says rather than the default it
     * happens to agree with.
     */
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
