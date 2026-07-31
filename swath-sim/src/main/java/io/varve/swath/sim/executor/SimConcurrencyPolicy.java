/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.policy.ConcurrencyPolicy;
import io.varve.swath.sim.kernel.SimRng;
import io.varve.swath.sim.model.EngineTimeBudgets;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Single-threaded, virtual-time behavioral port of the production adaptive-concurrency controller.
 * It is not a shared implementation, and its tests do not prove equivalence under production
 * concurrency. Callers supply every instant and the shed-window random stream; this class reads no
 * ambient clock or randomness. Virtual time starts at zero, so timestamps use the explicit
 * {@link #UNARMED} sentinel rather than treating zero as "never".
 *
 * <p>The port preserves these controller contracts:
 * <ul>
 *   <li>growth starts at {@code min(4, tMax)}, doubles until the first congestion signal, then grows
 *       additively and at a configured pace;</li>
 *   <li>throttles reduce the target to 70%, while starvation-gated sustained worker timeouts reduce it
 *       to 50% at most once per jittered window; both decreases floor at one;</li>
 *   <li>worker-timeout and successful-latency freezes do not decrease the target; the latency freeze
 *       uses a rolling-minimum baseline and permits a paced progress-gated valve step;</li>
 *   <li>probe-class transients are excluded from congestion, freeze, and shed decisions but retained
 *       for attribution, and latency samples represent successful attempts only;</li>
 *   <li>decrease signals pause steals, including floor no-ops; only a numeric reduction monotonically
 *       re-arms the clean-window cooldown. A floor no-op may resume on the next eligible success.</li>
 * </ul>
 * Shed jitter belongs to the supplied fleet stream, and engagement counters are accumulated here for
 * the executor to fold into the run via {@link #counters()}.
 */
public final class SimConcurrencyPolicy implements ConcurrencyPolicy {

    /** Multiplicative decrease factor on a store throttle. */
    static final double DECREASE_FACTOR = 0.7;
    /** Additive increase step, once slow start has been latched off. */
    static final int INCREASE_STEP = 1;
    /** Slow start's initial target, clamped to the ceiling. */
    static final int SLOW_START_INITIAL_T = 4;
    /** Worker timeouts inside the trailing window that freeze growth. */
    static final int TRANSIENT_FREEZE_THRESHOLD = 3;
    /** Absolute minimum timeouts in a shed window (dominates the fraction-of-T gate at small T). */
    static final int SHED_K = 3;
    /** Fraction-of-T timeout gate: a shed needs {@code timeouts >= ceil(ALPHA * T)}, or {@link #SHED_K}. */
    static final double SHED_ALPHA = 0.3;
    /** Multiplicative decrease factor on a sustained-timeout shed. */
    static final double SHED_FACTOR = 0.5;
    /** Starvation gate divisor: a shed needs {@code successes <= max(1, T / this)}. */
    static final int SHED_SUCCESS_DIVISOR = 32;
    /** Growth freezes once the successful-attempt latency EWMA exceeds this multiple of the baseline. */
    static final double LATENCY_FREEZE_FACTOR = 2.0;
    /** Smoothing factor of the trailing EWMA of successful-attempt latency. */
    static final double LATENCY_EWMA_ALPHA = 0.2;

    /** Unarmed timestamp; zero is a valid virtual instant. Checked before timestamp arithmetic. */
    private static final long UNARMED = Long.MIN_VALUE;

    private final int tMax;
    private final EngineTimeBudgets budgets;
    private final SimRng shedJitter;
    private final SortedMap<String, Long> counters = new TreeMap<>();

    private int effectiveT;
    private boolean congestionSeen;
    private boolean stealingAllowed = true;
    private long lastThrottleNanos = UNARMED;
    private long lastGrowthNanos = UNARMED;
    private long lastValveGrowthNanos = UNARMED;

    private long transientWindowStartNanos = UNARMED;
    private int transientWindowCount;

    private long shedWindowStartNanos = UNARMED;
    private long shedWindowLengthNanos;
    private int shedWindowTimeouts;
    private int shedWindowSuccesses;
    private int shedWindowWorkerTimeouts;
    private int shedWindowProbeTimeouts;
    /** Start of the window that last shed, or {@link #UNARMED}; reads follow window rolling. */
    private long shedFiredWindowNanos = UNARMED;

    private long latencyBaselineNanos;
    private long latencyWindowMinNanos;
    private long latencyDecayWindowStartNanos = UNARMED;
    private long latencyEwmaNanos;

    /**
     * @param tMax       the configured concurrency ceiling
     * @param budgets    the run's declared windows (clean window, growth pace, timeout windows, shed
     *                   window bounds, latency decay, valve pace)
     * @param shedJitter the fleet-owned shed-window stream, independent of the worker that rolls it
     */
    public SimConcurrencyPolicy(int tMax, EngineTimeBudgets budgets, SimRng shedJitter) {
        if (tMax <= 0) {
            throw new IllegalArgumentException("tMax must be positive, got " + tMax);
        }
        this.tMax = tMax;
        this.budgets = budgets;
        this.shedJitter = shedJitter;
        this.effectiveT = Math.min(SLOW_START_INITIAL_T, tMax);
        // Production pre-draws to prevent a concurrent re-roll. This single-threaded port leaves the
        // window unarmed so the first roll owns the first jitter draw.
    }

    @Override
    public void onSuccess(long atNanos) {
        rollShedWindowIfElapsed(atNanos);
        shedWindowSuccesses++;
        if (effectiveT >= tMax) {
            stealingAllowed = true;
            return;
        }
        if (lastThrottleNanos != UNARMED && atNanos - lastThrottleNanos < budgets.concurrencyCleanWindowNanos()) {
            count("AIMD.growth_blocked_cooldown");
            return;
        }
        // The pause tracks decreases, not either growth freeze.
        stealingAllowed = true;
        boolean latencyFrozen = latencyFrozen();
        if (latencyFrozen) {
            count("FREEZE.latency_inflation");
        }
        boolean growthFrozen = growthFrozen(atNanos);
        if (growthFrozen) {
            count("FREEZE.transient_timeouts");
        }
        if (growthFrozen) {
            return;   // The timeout freeze is hard; the latency valve cannot relax it.
        }
        if (latencyFrozen) {
            growThroughValve(atNanos);
            return;
        }
        if (lastGrowthNanos != UNARMED && atNanos - lastGrowthNanos < budgets.aimd().growthPaceNanos()) {
            return;   // Growth is paced, not per-success.
        }
        lastGrowthNanos = atNanos;
        int next = congestionSeen
                ? Math.min(tMax, effectiveT + INCREASE_STEP)
                : Math.min(tMax, effectiveT * 2);
        if (next > effectiveT && !congestionSeen) {
            count("AIMD.slow_start_double");
        }
        effectiveT = next;
    }

    @Override
    public void onThrottle(long atNanos) {
        multiplicativeDecrease(atNanos, DECREASE_FACTOR, true);
    }

    @Override
    public void onTransientTimeout(long atNanos, boolean workerClass) {
        if (!workerClass) {
            // Probe-class transients affect attribution, not congestion or the freeze/shed gates.
            rollShedWindowIfElapsed(atNanos);
            shedWindowProbeTimeouts++;
            count("GROWTH.probe_timeout_excluded");
            return;
        }
        markCongestion();
        if (transientWindowStartNanos == UNARMED
                || atNanos - transientWindowStartNanos > budgets.aimd().transientTimeoutWindowNanos()) {
            transientWindowStartNanos = atNanos;
            transientWindowCount = 1;
        } else {
            transientWindowCount++;
        }
        rollShedWindowIfElapsed(atNanos);
        shedWindowTimeouts++;
        shedWindowWorkerTimeouts++;
        maybeShed(atNanos);
    }

    @Override
    public void onAttemptLatency(long atNanos, long latencyNanos) {
        if (latencyNanos <= 0L) {
            return;   // A non-positive latency carries no signal.
        }
        updateLatencyBaseline(atNanos, latencyNanos);
        latencyEwmaNanos = latencyEwmaNanos == 0L
                ? latencyNanos
                : (long) (latencyEwmaNanos + LATENCY_EWMA_ALPHA * (latencyNanos - latencyEwmaNanos));
    }

    @Override
    public int effectiveT() {
        return effectiveT;
    }

    @Override
    public boolean isStealingAllowed() {
        return stealingAllowed;
    }

    /** Returns engagement counters in name order for folding into the run's counters. */
    public SortedMap<String, Long> counters() {
        return Collections.unmodifiableSortedMap(new TreeMap<>(counters));
    }

    /** Rolling-minimum latency baseline, or zero before the first sample. */
    long latencyBaselineNanos() {
        return latencyBaselineNanos;
    }

    /** Current jittered shed-window length, or zero before the first roll. */
    long shedWindowLengthNanos() {
        return shedWindowLengthNanos;
    }

    private void growThroughValve(long atNanos) {
        // The latency valve admits one paced step only while progress exceeds the shed starvation gate.
        int successGate = Math.max(1, effectiveT / SHED_SUCCESS_DIVISOR);
        if (shedWindowSuccesses <= successGate) {
            return;
        }
        if (lastValveGrowthNanos != UNARMED && atNanos - lastValveGrowthNanos < budgets.aimd().valvePaceNanos()) {
            return;
        }
        lastValveGrowthNanos = atNanos;
        int next = Math.min(tMax, effectiveT + INCREASE_STEP);
        if (next > effectiveT) {
            count("GROWTH.frozen_growth_valve");
        }
        effectiveT = next;
    }

    private void markCongestion() {
        if (!congestionSeen) {
            congestionSeen = true;
            count("AIMD.slow_start_exit_congestion");
        }
    }

    private void multiplicativeDecrease(long atNanos, double factor, boolean throttle) {
        markCongestion();
        if (throttle) {
            count("AIMD.votes");
        } else {
            count("AIMD.timeout_shed");
            count("SHED.timeout_storm");
        }
        stealingAllowed = false;
        if (effectiveT <= 2) {
            count("AIMD.decrease_at_floor");
        } else if (effectiveT <= 8) {
            count("AIMD.decrease_low_t");
        } else if (effectiveT <= 32) {
            count("AIMD.decrease_mid_t");
        } else {
            count("AIMD.decrease_high_t");
        }
        int next = Math.max(1, (int) (effectiveT * factor));
        if (next >= effectiveT) {
            // A no-op decrease buys no new cooldown, so do not re-arm it.
            count("AIMD.floor_noop_rearm");
            count("AIMD.floor_rearm_suppressed");
            return;
        }
        effectiveT = next;
        // A late signal must not move an existing cooldown backwards.
        lastThrottleNanos = Math.max(lastThrottleNanos, atNanos);
    }

    private void maybeShed(long atNanos) {
        int timeoutGate = Math.max(SHED_K, (int) Math.ceil(SHED_ALPHA * effectiveT));
        int successGate = Math.max(1, effectiveT / SHED_SUCCESS_DIVISOR);
        if (shedWindowTimeouts >= timeoutGate && shedWindowSuccesses <= successGate
                && shedFiredWindowNanos != shedWindowStartNanos) {
            shedFiredWindowNanos = shedWindowStartNanos;
            count("SHED.timeout_storm_worker_fed", shedWindowWorkerTimeouts);
            count("SHED.timeout_storm_probe_fed", shedWindowProbeTimeouts);
            multiplicativeDecrease(atNanos, SHED_FACTOR, false);
        }
    }

    private void rollShedWindowIfElapsed(long atNanos) {
        if (shedWindowStartNanos != UNARMED && atNanos - shedWindowStartNanos <= shedWindowLengthNanos) {
            return;
        }
        shedWindowStartNanos = atNanos;
        shedWindowLengthNanos = drawShedWindow();
        shedWindowTimeouts = 0;
        shedWindowSuccesses = 0;
        shedWindowWorkerTimeouts = 0;
        shedWindowProbeTimeouts = 0;
    }

    /** Draws one bounded window length from the fleet-owned deterministic stream. */
    private long drawShedWindow() {
        long min = budgets.aimd().shedWindowMinNanos();
        long span = budgets.aimd().shedWindowMaxNanos() - min + 1L;
        return min + Math.floorMod(shedJitter.nextLong(), span);
    }

    private boolean growthFrozen(long atNanos) {
        if (transientWindowStartNanos == UNARMED
                || atNanos - transientWindowStartNanos > budgets.aimd().transientTimeoutWindowNanos()) {
            return false;   // Unarmed or elapsed without fresh timeouts: thaw.
        }
        return transientWindowCount >= TRANSIENT_FREEZE_THRESHOLD;
    }

    private boolean latencyFrozen() {
        if (latencyBaselineNanos <= 0L) {
            return false;
        }
        return (double) latencyEwmaNanos > LATENCY_FREEZE_FACTOR * (double) latencyBaselineNanos;
    }

    /** Updates the immediate low-water mark and the decay-window minimum. */
    private void updateLatencyBaseline(long atNanos, long sample) {
        if (latencyBaselineNanos == 0L) {
            latencyBaselineNanos = sample;
            latencyWindowMinNanos = sample;
            latencyDecayWindowStartNanos = atNanos;
            return;
        }
        latencyWindowMinNanos = Math.min(latencyWindowMinNanos, sample);
        if (sample < latencyBaselineNanos) {
            latencyBaselineNanos = sample;
        }
        if (latencyDecayWindowStartNanos != UNARMED
                && atNanos - latencyDecayWindowStartNanos > budgets.aimd().latencyBaselineDecayNanos()) {
            latencyDecayWindowStartNanos = atNanos;
            latencyBaselineNanos = latencyWindowMinNanos;
            latencyWindowMinNanos = sample;
        }
    }

    private void count(String name) {
        count(name, 1);
    }

    private void count(String name, long delta) {
        counters.merge(name, delta, Long::sum);
    }
}
