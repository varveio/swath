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
 * The simulator's own faithful implementation of the adaptive-concurrency controller — the one
 * mechanism the policy seam defines as a <b>port</b> rather than an extraction, because the shipped
 * controller is a lock-free machine of CAS loops, jittered windows and racing timestamps whose whole
 * shape exists to survive concurrency the simulator does not have.
 *
 * <p>Every reactive signal arrives with its own instant, so this class reads no clock; the shed
 * window's jitter is drawn from a supplied stream, so it reads no ambient randomness either. Both
 * substitutions are forced: a virtual-time run has no wall clock to read and no thread-local generator
 * that would be reproducible if it did.
 *
 * <p><b>A third difference, and it is not cosmetic.</b> The shipped controller uses {@code 0} as the
 * "never happened yet" value for its pacing timestamps, which works only because it reads a clock whose
 * values are enormous: a zero there is unreachably far in the past, so the first growth step after a
 * cool-down fires immediately, exactly as its own comment intends. A virtual run starts at zero. Reusing
 * that sentinel here would invert its meaning — the first success of every run would be paced out, and a
 * relaxation valve paced at thirty seconds could never open in a run shorter than thirty virtual
 * seconds, which is precisely the length of the runs that exercise it. Every timestamp here therefore
 * carries an explicit {@link #UNARMED} value that is never confused with an instant, and every read of
 * one checks for it rather than doing arithmetic against it.
 *
 * <h2>The behaviour this reproduces</h2>
 * <ul>
 *   <li><b>Slow start, then additive growth.</b> A run begins at four workers (or the ceiling, if
 *       lower), and growth doubles until the first congestion signal of the run — a worker attempt
 *       timeout, a throttle, or a shed — latches it permanently to {@code +1}. Growth is paced, not
 *       per-success, so recovery ramps rather than rebounding into the condition it just left.</li>
 *   <li><b>Exactly two ways down, both multiplicative, both floored at one.</b> A store throttle takes
 *       the target to 70% of itself; a sustained-timeout shed takes it to half, at most once per
 *       jittered window, and only when a timeout-volume gate <em>and</em> a starved-progress gate both
 *       hold. Nothing else ever lowers the target.</li>
 *   <li><b>Two freezes, neither of which is a decrease.</b> A high worker-timeout rate in the trailing
 *       window suppresses growth outright. An inflated successful-attempt latency (against a rolling
 *       minimum baseline) is the softer one: it holds growth too, but a valve admits a single paced
 *       step while the run is demonstrably making progress and is not also timeout-frozen.</li>
 *   <li><b>Probe timeouts are excluded</b> from both the shed gate and the growth freeze — a probe
 *       carries no backpressure signal, and a pure probe storm that froze growth could never trip the
 *       shed that would end it. They are counted, for attribution only.</li>
 *   <li><b>Latency samples come only from successful attempts.</b> A timed-out attempt is a censored
 *       observation; feeding it to the baseline would poison exactly the signal the freeze reads.</li>
 *   <li><b>Steal pausing tracks the decrease paths, not the freezes.</b> Steals pause on a real
 *       decrease and resume when the clean window elapses, whether or not growth is still frozen.</li>
 * </ul>
 *
 * <h2>What a reader may not conclude from this class being green</h2>
 * None of the above is mechanically checkable against the shipped controller — the two share no code
 * by design, and the purity walk that guards the engine's decision path never reaches an
 * implementation of this interface. Its correctness rests on review against the controller's own
 * documented guarantees and on the shape tests beside it, and it is a reimplementation, so a change to
 * the shipped controller is a change that has to be made here too.
 *
 * <p>Counters are accumulated internally, in name order, rather than written through a context: the
 * interface hands this class an instant and nothing else, and threading a mutable context in purely to
 * count would be the ambient coupling every other seam here removes. The executor folds
 * {@link #counters()} into the run's own at the end.
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

    /**
     * The "this has never happened" value for every timestamp below. Explicit, and deliberately not
     * {@code 0}: zero is a perfectly ordinary instant on a virtual clock — it is the instant every run
     * begins at — so a zero sentinel would make "never" and "at the very start" the same value, and the
     * two have opposite meanings for every window and pace this class keeps. Never used in arithmetic;
     * every site tests for it first, which also avoids the overflow that subtracting it would cause.
     */
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
    /** The window a shed most recently fired in; {@link #UNARMED} until one has. Every path that reads
     *  it has already rolled the window, so it can never be compared against an unarmed start. */
    private long shedFiredWindowNanos = UNARMED;

    private long latencyBaselineNanos;
    private long latencyWindowMinNanos;
    private long latencyDecayWindowStartNanos = UNARMED;
    private long latencyEwmaNanos;

    /**
     * @param tMax       the configured concurrency ceiling
     * @param budgets    the run's declared windows (clean window, growth pace, timeout windows, shed
     *                   window bounds, latency decay, valve pace)
     * @param shedJitter the fleet's shed-window draw stream — one instrument, one tape, drawn on the
     *                   reserved fleet actor so a window's length never depends on which worker
     *                   happened to roll it
     */
    public SimConcurrencyPolicy(int tMax, EngineTimeBudgets budgets, SimRng shedJitter) {
        if (tMax <= 0) {
            throw new IllegalArgumentException("tMax must be positive, got " + tMax);
        }
        this.tMax = tMax;
        this.budgets = budgets;
        this.shedJitter = shedJitter;
        this.effectiveT = Math.min(SLOW_START_INITIAL_T, tMax);
        // Seeded up front, exactly as the shipped controller seeds it, so the first window roll never
        // measures against a zero length.
        this.shedWindowLengthNanos = drawShedWindow();
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
        // Backpressure has cleared, whatever the freezes below say: the flag tracks throttling, and
        // re-coupling it to a timeout-shaped freeze would keep a healthy fleet under-parallelised.
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
            return;   // a worker-timeout storm is a hard freeze; the valve never relaxes it
        }
        if (latencyFrozen) {
            growThroughValve(atNanos);
            return;
        }
        if (lastGrowthNanos != UNARMED && atNanos - lastGrowthNanos < budgets.aimd().growthPaceNanos()) {
            return;   // paced
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
            // Excluded from the congestion latch and the growth freeze entirely; it still rolls the
            // window and feeds the attribution split, so a mixed storm's probe share is never lost.
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
            return;   // a non-positive latency carries no signal
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

    /** This controller's engagement counters, in name order, for folding into the run's own. */
    public SortedMap<String, Long> counters() {
        return Collections.unmodifiableSortedMap(new TreeMap<>(counters));
    }

    /** The rolling-minimum latency baseline the freeze rung measures against; zero until a sample lands. */
    long latencyBaselineNanos() {
        return latencyBaselineNanos;
    }

    /**
     * The jittered length drawn for the current shed window. Package-private: the draw is a property a
     * test has to be able to read directly, since a controller that always drew the same length would
     * behave identically to a correct one on every other observable.
     */
    long shedWindowLengthNanos() {
        return shedWindowLengthNanos;
    }

    private void growThroughValve(long atNanos) {
        // A latency-inflation-only freeze is a damper, not a latch: one paced step, and only while the
        // run is demonstrably progressing (the exact complement of the shed's starvation gate).
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
            // At the floor, or rounding produced no change: removing zero concurrency buys no fresh
            // cool-down, so the recovery window is deliberately NOT re-armed here.
            // Both names, as the shipped controller records them: one says a decrease removed nothing,
            // the other that it therefore bought no fresh recovery window. A comparison against a real
            // run reads whichever it knows, and a missing name reads as a silent zero.
            count("AIMD.floor_noop_rearm");
            count("AIMD.floor_rearm_suppressed");
            return;
        }
        effectiveT = next;
        // Monotonic, so a late signal carrying an older instant cannot walk a real decrease's recovery
        // window backwards. UNARMED loses that comparison by construction.
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

    /**
     * One jittered window length in the declared bounds. The live controller draws this to desynchronise
     * a fleet of separate processes; here it keeps a single run's windows from lining up with any
     * periodicity the workload happens to have, and it comes off the fleet's own tape so its values do
     * not depend on which worker rolled the window.
     */
    private long drawShedWindow() {
        long min = budgets.aimd().shedWindowMinNanos();
        long span = budgets.aimd().shedWindowMaxNanos() - min + 1L;
        return min + Math.floorMod(shedJitter.nextLong(), span);
    }

    private boolean growthFrozen(long atNanos) {
        if (transientWindowStartNanos == UNARMED
                || atNanos - transientWindowStartNanos > budgets.aimd().transientTimeoutWindowNanos()) {
            return false;   // never armed, or elapsed with no fresh timeouts: thaw
        }
        return transientWindowCount >= TRANSIENT_FREEZE_THRESHOLD;
    }

    private boolean latencyFrozen() {
        if (latencyBaselineNanos <= 0L) {
            return false;
        }
        return (double) latencyEwmaNanos > LATENCY_FREEZE_FACTOR * (double) latencyBaselineNanos;
    }

    /**
     * The rolling minimum: a new low floors it at once, and at each decay boundary it is re-floored to
     * the elapsed window's minimum, so a baseline rises only if the environment's true minimum has.
     */
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
