/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.observability.RunMetrics;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AIMD concurrency gauge: bounds how many workers may issue a page fetch concurrently, adapting the
 * target {@code T} to the store's observed health. This class is the control loop; the full policy,
 * the tuning derivations, and the rationale live in {@code docs/internals/algorithms.md} §5.
 *
 * <p>A worker acquires a permit before the HTTP call and releases it immediately after the call
 * returns — the permit is never held across a commit-await or channel-send that could stall
 * quiescence.
 *
 * <p>Reactive signals drive {@code T}:
 * <ul>
 *   <li><b>Decrease on stress</b> — a {@link #SLOWDOWN_STATUS 503 SlowDown} multiplies {@code T} down
 *       by {@link #DECREASE_FACTOR}. Concurrent observers each CAS; the floor of 1 bounds the collapse
 *       so at least one worker always makes progress. New steals pause ({@link #isStealingAllowed()}).
 *   <li><b>Slow-start ramp</b> — a fresh gauge starts at {@link #SLOW_START_INITIAL_T} (clamped to
 *       {@code Tmax}), not at {@code Tmax}, so a fleet launch does not storm a shared endpoint from
 *       {@code t=0}. Until the first congestion signal ({@link #congestionSeen}) growth is
 *       multiplicative; that signal latches it off for the run and growth reverts to additive {@code +1}.
 *   <li><b>Increase on health</b> — after a {@link #CLEAN_WINDOW_NANOS clean window} a success grows
 *       {@code T}, paced to ~one step per {@link #GROWTH_PACE_NANOS}, so recovery ramps gradually
 *       rather than rebounding to {@code Tmax} and re-storming.
 *   <li><b>Sustained-timeout shed</b> — a client attempt-timeout casts no AIMD vote, but a sustained
 *       timeout storm with starved progress has no other path down (the growth-freeze is a no-op at
 *       {@code Tmax}); {@link #maybeShed()} multiplies {@code T} down by {@link #SHED_FACTOR}, at most
 *       once per window, via the shared {@link #multiplicativeDecrease} under its own counter.
 *   <li><b>Growth freezes</b> — a high transient-timeout rate ({@link #growthFrozen()}) or an inflated
 *       success-latency EWMA ({@link #latencyFrozen()}) suppresses the {@code +1} without ever
 *       multiplying {@code T} down; the shed owns every decrease.
 * </ul>
 *
 * <p><b>Permit mechanics.</b> {@link ResizableSemaphore#reduceAvailable} reduces the available permit
 * count (potentially below zero) without blocking — workers already holding permits finish their
 * current page normally; new acquires are deferred until enough permits are returned to reach zero.
 */
public final class ConcurrencyGauge {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyGauge.class);

    /** Multiplicative decrease factor on 503 SlowDown (algorithms.md §5). */
    static final double DECREASE_FACTOR = 0.7;
    /** Additive increase step per pace interval (algorithms.md §5). */
    static final int INCREASE_STEP = 1;
    /** Slow-start: initial effective {@code T} (clamped to {@code Tmax}); a healthy run doubles out of
     *  it in a handful of paced steps (see the class javadoc, {@link #congestionSeen}). */
    static final int SLOW_START_INITIAL_T = 4;
    /** Throttle-free interval that triggers a +1 recovery (algorithms.md §5: "e.g. 10 s"). */
    static final long CLEAN_WINDOW_NANOS = 10_000_000_000L;   // 10 s
    /**
     * Regrowth pace: minimum interval, on the injected clock, between successive additive-increase
     * steps — the {@code +1} is throttled to at most one step per second, NOT one per raw success, so
     * recovery ramps slowly enough that the growth-freeze and the sustained-timeout shed can hold a
     * sustainable width instead of the pure-timeout regime rebounding to Tmax and re-storming. The
     * first {@code +1} after a cool-down fires immediately (a stale/0 {@code lastGrowthNs}). Pacing
     * recovery here rather than per-success is deliberate, not a defect.
     */
    static final long GROWTH_PACE_NANOS = 1_000_000_000L;   // 1 s
    /** HTTP status that signals S3 SlowDown / ServiceUnavailable (algorithms.md §5). */
    static final int SLOWDOWN_STATUS = 503;

    // ---- growth-freeze -------------------------------------------------------------
    // A high transient-timeout rate suppresses the +1 recovery so concurrency does not expand into a
    // sick network — but never multiplies T down and never raises the floor above 1 (a floor >1 would
    // blunt the correct AIMD response to a real 503 storm). Approximate windowed rate: fixed 10 s
    // window, freeze at >=3. See docs/internals/algorithms.md §5.
    /** Trailing window over which transient timeouts are counted for the growth-freeze. */
    static final long TRANSIENT_WINDOW_NANOS = 10_000_000_000L;   // 10 s
    /** Transient timeouts within the window that freeze the +1 recovery (rate-high threshold). */
    static final int TRANSIENT_FREEZE_THRESHOLD = 3;

    // ---- sustained-timeout SHED ----------------------------------------------------
    // A progress-gated multiplicative decrease for a sustained attempt-timeout storm — the adaptive
    // path down that the growth-freeze cannot provide at Tmax, distinct from the 503 decrease. The
    // starvation gate (successes must be low) means a tail of timeouts that coexists with real page
    // commits clears the gate and never sheds. Thresholds derived in docs/internals/algorithms.md §5.
    /** Absolute minimum timeouts-in-window to shed (dominates {@code ceil(ALPHA*T)} at small T). */
    static final int SHED_K = 3;
    /** Fraction-of-T timeout threshold: shed needs {@code timeouts >= ceil(ALPHA*T)} (or {@link #SHED_K}). */
    static final double SHED_ALPHA = 0.3;
    /** Multiplicative decrease factor on a sustained-timeout shed (RTO-like: later/costlier than the 0.7 503). */
    static final double SHED_FACTOR = 0.5;
    /** Starvation gate divisor: shed needs {@code successes <= max(1, floor(T/SHED_SUCCESS_DIVISOR))}. */
    static final int SHED_SUCCESS_DIVISOR = 32;
    /**
     * Shed-window NOMINAL length (= 3× the 10 s attempt timeout, so {@link #SHED_K} is reachable at small
     * T — a permit yields ≤1 timeout per 10 s window). Production jitters around this per window
     * ([25s,40s]); the deterministic tests inject exactly this as the fixed window via the clock seam.
     */
    static final long SHED_WINDOW_BASE_NANOS = 30_000_000_000L;   // 30 s nominal
    /** Shed-window jitter lower bound (RED-style per-window desync so a fleet does not shed in lockstep). */
    static final long SHED_WINDOW_MIN_NANOS = 25_000_000_000L;    // 25 s
    /** Shed-window jitter upper bound. */
    static final long SHED_WINDOW_MAX_NANOS = 40_000_000_000L;    // 40 s

    // ---- latency-freeze rung (Vegas-style early warning) ---------------------------
    // A GROWTH GATE, not a decrease (it NEVER decreases T — the shed owns every decrease). It reacts
    // earlier than the shed: when the latency of SUCCESSFUL attempts inflates past a healthy baseline,
    // freeze the +1 so T stops expanding into a developing degradation before reads hit the
    // attempt-timeout wall. Latency is sampled ONLY from successful attempts — a timed-out attempt is
    // a CENSORED (>=10 s) observation that would poison the baseline/EWMA. This catches the
    // HEALTHY->DEGRADING transition, not a run born uniformly-degraded (that is the shed's job). See
    // docs/internals/algorithms.md §5.
    /** Freeze +1 growth when the recent successful-attempt latency EWMA exceeds this multiple of baseline. */
    static final double LATENCY_FREEZE_FACTOR = 2.0;
    /**
     * Baseline decay window: the Vegas rolling minimum is re-floored to the just-elapsed window's
     * minimum at each boundary, so the baseline rises (slowly) if the environment's TRUE minimum
     * genuinely rises — it never locks in one anomalously-fast sample forever, nor a degraded first
     * window. Chosen at 6× the attempt timeout so it decays much slower than per-request latencies.
     */
    static final long LATENCY_BASELINE_DECAY_NANOS = 60_000_000_000L;   // 60 s
    /** Smoothing factor for the short trailing EWMA of successful-attempt latency (the freeze numerator). */
    static final double LATENCY_EWMA_ALPHA = 0.2;

    /**
     * Valve cool-down — minimum interval, on the injected clock, between successive valve-admitted
     * {@code +1} steps while latency-frozen. At the shed-window scale (~30 s) the valve relaxes the
     * latency freeze into a slow damper, admitting at most one worker per shed window so the
     * 503/timeout decrease paths always dominate under real congestion while a dense tail still
     * ratchets out of the floor.
     */
    static final long VALVE_PACE_NANOS = SHED_WINDOW_BASE_NANOS;   // 30 s

    private final int tMax;
    private final AtomicInteger effectiveT;
    private final ResizableSemaphore semaphore;
    private final RunMetrics metrics;
    /** Nanos timestamp of the last 503 observation; 0 = never throttled. */
    private final AtomicLong lastThrottleNs = new AtomicLong(0L);
    /** Nanos timestamp of the last +1 growth step; 0 = none yet. Paces the additive increase to ~1/s. */
    private final AtomicLong lastGrowthNs = new AtomicLong(0L);
    /** Nanos timestamp of the last valve-admitted +1 (latency-freeze relaxation); 0 = none yet.
     *  Paces the valve to ~1 step per VALVE_PACE_NANOS, INDEPENDENT of lastGrowthNs (the normal +1 pace). */
    private final AtomicLong lastValveGrowthNs = new AtomicLong(0L);
    /**
     * Slow-start latch: false until the FIRST congestion signal of the run (an attempt-timeout, an
     * AIMD 503 down-vote, or a sustained-timeout shed). While false {@link #onSuccess()} doubles
     * {@code T}; once latched (for the rest of the run) growth reverts to additive {@code +1}. An
     * {@link AtomicBoolean} so the transition records its {@code slow_start_exit_congestion} counter
     * exactly once under concurrent signals (see {@link #markCongestion()}).
     */
    private final AtomicBoolean congestionSeen = new AtomicBoolean(false);
    /**
     * Serializes the slow-start double-vs-{@code +1} DECISION against the congestion latch.
     * {@link #markCongestion()} and the grow-step (decision + CAS + release) both run under this
     * monitor, so once {@link #congestionSeen} is latched no later success can DOUBLE: {@link
     * #onSuccess()} re-reads the latch inside the monitor and takes the additive branch (happens-before
     * via the monitor). Contention is nil — growth is already paced to <=1 step/s by the lock-free
     * {@link #lastGrowthNs} CAS before entering here.
     *
     * <p>The monitor deliberately does NOT cover the whole congestion path ({@link #lastThrottleNs} and
     * the decrease CAS are set outside it), so at the single transition instant one success that already
     * cleared the lock-free gates may take ONE growth step concurrent with the run's first congestion
     * signal — a bounded, one-time slip that is a legal serialization (it doubled before congestion was
     * observed) and self-corrects on the next decrease. It cannot repeat: once the latch and cool-down
     * are set, all later growth is additive. Holding the monitor across the whole decrease was rejected
     * (it is a double-vs-congestion serializer, not a whole-controller lock). Accepted as benign.
     */
    private final Object growthLock = new Object();
    /** Growth-freeze: start of the current transient-timeout counting window; 0 = none yet. */
    private final AtomicLong transientWindowStartNs = new AtomicLong(0L);
    /** Growth-freeze: transient timeouts observed in the current window (approximate; races are benign). */
    private final AtomicInteger transientWindowCount = new AtomicInteger(0);
    /** Injected nanoTime seam (production {@code System::nanoTime}); routes ALL nano reads. */
    private final LongSupplier nanoClock;
    /** Per-window shed-length source (production draws jitter in [25s,40s]; tests inject a fixed value). */
    private final LongSupplier shedWindowNanosSupplier;
    /** Start of the current shed window; 0 = none yet (approximate; races are benign). */
    private final AtomicLong shedWindowStartNs = new AtomicLong(0L);
    /** The jittered length drawn for the current shed window. */
    private final AtomicLong shedWindowLengthNs = new AtomicLong(0L);
    /** Attempt-timeouts observed in the current shed window. */
    private final AtomicInteger shedWindowTimeouts = new AtomicInteger(0);
    /** Real page completions observed in the current shed window (the starvation-gate numerator). */
    private final AtomicInteger shedWindowSuccesses = new AtomicInteger(0);
    /**
     * The WORKER-timeout half of the call-class split. Worker timeouts are the only ones that feed the
     * shed gate, so this equals {@link #shedWindowTimeouts}; it is tracked separately so the terminal
     * {@code SHED.timeout_storm_worker_fed} publish need not re-derive it. Reset alongside
     * {@link #shedWindowTimeouts} in {@link #rollShedWindowIfElapsed}.
     */
    private final AtomicInteger shedWindowWorkerTimeouts = new AtomicInteger(0);
    /**
     * The probe-fetch half of the split — VISIBILITY-ONLY. Probe timeouts never feed
     * {@link #shedWindowTimeouts} and never gate {@link #maybeShed()} (a probe carries no
     * S3-backpressure signal); this counter increments only so a worker-storm shed can publish the
     * {@code SHED.timeout_storm_probe_fed} mix that coexisted with it. See
     * {@link #onTransientTimeout(boolean)}.
     */
    private final AtomicInteger shedWindowProbeTimeouts = new AtomicInteger(0);
    /**
     * Generation-stamped one-shed-per-window latch — holds the {@link #shedWindowStartNs} value of the
     * window in which a shed most recently fired ({@code Long.MIN_VALUE} = never, never a real start).
     * A shed may fire again only once the current window's start differs from this stamp. Because the
     * stamp is only advanced, never cleared, it is immune to counter-reset ordering: a losing rollover
     * thread cannot clobber it back and let a second shed fire inside the same real window.
     */
    private final AtomicLong shedFiredWindowNs = new AtomicLong(Long.MIN_VALUE);
    /** Vegas rolling-minimum healthy-latency baseline (nanos); 0 = no successful sample yet. */
    private final AtomicLong latencyBaselineNs = new AtomicLong(0L);
    /** Minimum successful latency observed within the current decay window (nanos). */
    private final AtomicLong latencyWindowMinNs = new AtomicLong(0L);
    /** Start of the current baseline-decay window; 0 = none yet. */
    private final AtomicLong latencyDecayWindowStartNs = new AtomicLong(0L);
    /** Short trailing EWMA of successful-attempt latency (nanos); the freeze numerator. */
    private final AtomicLong latencyEwmaNs = new AtomicLong(0L);
    /**
     * Whether stealing is currently permitted. Volatile — written by any observing thread
     * (503 or recovery), read cheaply on the hot idle loop.
     */
    private volatile boolean stealingAllowed = true;

    /**
     * The public construction path.
     *
     * @param tMax    the configured max-parallel-listings (Tmax, the ceiling); initial effective T =
     *                {@code min(}{@link #SLOW_START_INITIAL_T}{@code , Tmax)} (slow-start)
     * @param metrics per-run metrics holder for the effective-T gauge
     */
    public ConcurrencyGauge(int tMax, RunMetrics metrics) {
        this(tMax, metrics, System::nanoTime, ConcurrencyGauge::defaultShedWindowNanos, defaultInitialT(tMax));
    }

    /**
     * The package-private construction path: injects the nanoTime clock and the shed-window length so
     * the window logic is deterministically testable with no real sleep, plus the initial effective
     * {@code T} so a shed/AIMD unit test can start the gauge at a chosen T — the state a healthy run
     * reaches by the slow-start ramp — without driving the whole ramp first (the ramp has its own guard,
     * {@code SlowStartConcurrencyGaugeContractTest}). Production passes {@link #defaultInitialT} explicitly;
     * {@code initialT} is clamped to {@code [1, Tmax]}.
     *
     * @param nanoClock source for ALL of the gauge's nanoTime reads (freeze window AND shed window)
     * @param shedWindowNanosSupplier length of each shed window (production jitters [25s,40s]; a test injects a fixed value)
     */
    ConcurrencyGauge(int tMax, RunMetrics metrics, LongSupplier nanoClock, LongSupplier shedWindowNanosSupplier,
                     int initialT) {
        this.tMax = Math.max(1, tMax);
        int clampedInitial = Math.max(1, Math.min(initialT, this.tMax));
        this.effectiveT = new AtomicInteger(clampedInitial);
        this.semaphore = new ResizableSemaphore(clampedInitial);
        this.metrics = metrics;
        this.nanoClock = nanoClock;
        this.shedWindowNanosSupplier = shedWindowNanosSupplier;
        // Seed a real (non-zero) length up front so the very first rollShedWindowIfElapsed call never
        // sees length == 0: with a 0 length, a co-runner
        // computing `now2 - start <= 0` on the freshly-published start could fail the fast-path and
        // re-roll a second time before the winner's own reset of shedWindowLengthNs is visible.
        this.shedWindowLengthNs.set(shedWindowNanosSupplier.getAsLong());
        this.metrics.setConcurrencyTarget(clampedInitial);
    }

    /**
     * The slow-start initial effective {@code T} for a given {@code tMax} — {@code
     * min(}{@link #SLOW_START_INITIAL_T}{@code , max(1, tMax))}. Production and every caller that
     * wants the plain slow-start ramp (rather than isolating from it — see the test-seam
     * constructor's javadoc) pass this explicitly.
     */
    static int defaultInitialT(int tMax) {
        return Math.min(SLOW_START_INITIAL_T, Math.max(1, tMax));
    }

    /**
     * Latch the FIRST congestion signal of the run (an attempt-timeout, an AIMD 503 down-vote, or a
     * sustained-timeout shed). Flips {@link #congestionSeen} once — after which {@link #onSuccess()} grows
     * additively rather than doubling — and records the {@code AIMD/slow_start_exit_congestion} engagement
     * counter EXACTLY once (the CAS guards against a duplicate under concurrent signals).
     */
    private void markCongestion() {
        // Latch under growthLock so the flip is atomic w.r.t. a concurrent slow-start
        // grow-step's decision+CAS — never interleaves between its congestionSeen read and effectiveT CAS.
        synchronized (growthLock) {
            if (congestionSeen.compareAndSet(false, true)) {
                metrics.recordStealReason("AIMD", "slow_start_exit_congestion");
            }
        }
    }

    /** Production shed-window length: a fresh per-window jittered draw in [25s, 40s] (RED-style fleet desync). */
    private static long defaultShedWindowNanos() {
        return ThreadLocalRandom.current().nextLong(SHED_WINDOW_MIN_NANOS, SHED_WINDOW_MAX_NANOS + 1L);
    }

    /**
     * Acquire a permit before issuing a page fetch. Blocks (interruptibly) until a slot is
     * available. The floor of 1 guarantees at least one worker can always proceed.
     *
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public void acquireSlot() throws InterruptedException {
        semaphore.acquire();
    }

    /**
     * Release a permit after a page fetch completes (success or error). Must be called in
     * a {@code finally} block to prevent permit leaks that would permanently reduce
     * concurrency.
     */
    public void releaseSlot() {
        semaphore.release();
    }

    /**
     * Feed one completed page's HTTP status into the AIMD policy. Call this after the
     * fetch returns (and after {@link #releaseSlot()} — the permit need not be held).
     *
     * @param httpStatus the {@link io.varve.swath.store.ListPage#httpStatus()} of the just-completed page
     */
    public void reportStatus(int httpStatus) {
        if (httpStatus == SLOWDOWN_STATUS) {
            // A real 503 page is a genuine store SlowDown.
            onThrottle();
        } else {
            onSuccess();
        }
    }

    /**
     * Feed one completed attempt into the controller as an ordered event. For a successful page,
     * apply its uncensored latency sample before its status can claim a paced growth opportunity;
     * otherwise a slow completion could double {@code T} using the preceding EWMA and only publish
     * its own evidence after that decision. Returned 503s retain the existing throttle path and do
     * not contaminate the successful-latency baseline.
     */
    CompletionSnapshot reportCompletedAttempt(int httpStatus, long latencyNanos) {
        boolean latencySampled = httpStatus != SLOWDOWN_STATUS && latencyNanos > 0L;
        if (latencySampled) {
            onAttemptLatency(latencyNanos);
        }
        boolean latencyInflated = latencySampled && latencyFrozen();
        reportStatus(httpStatus);
        return new CompletionSnapshot(effectiveT.get(), latencyBaselineNs.get(), latencyEwmaNs.get(),
                latencyInflated, latencySampled);
    }

    /** Coherent-enough observation captured at the ordered successful-attempt controller seam. */
    record CompletionSnapshot(int effectiveT, long latencyBaselineNanos, long latencyEwmaNanos,
                              boolean latencyInflated, boolean latencySampled) {
    }

    /**
     * A client-side attempt-timeout / exhausted network fault observed by the retry loop. Unlike a
     * 503 through {@link #reportStatus}, a single timeout is NOT an AIMD down-vote (it never multiplies
     * {@code T} by {@link #DECREASE_FACTOR} nor records a {@code swath.aimd.votes} vote — a hung local
     * read is not S3 backpressure). Its effects are (1) feeding the growth-freeze so {@link #onSuccess}
     * suppresses the {@code +1} while the network is visibly sick, and (2) — as a WORKER-class timeout
     * (see {@link #onTransientTimeout(boolean)}) — feeding the shed window, so a sustained storm can
     * still trip {@link #maybeShed()}. Classification is documented in docs/internals/algorithms.md §5 and
     * docs/internals/metrics-internals.md §5a.
     */
    public void onTransientTimeout() {
        // Test-only entry point — every production caller goes through the 1-arg overload with its own
        // slotGated flag. This no-arg form defaults to WORKER, which drives the full path (feeds
        // shedWindowTimeouts, reaches maybeShed(), ends slow-start, feeds the growth-freeze), not merely
        // the visibility split — so gauge-only unit tests exercise the whole worker-timeout path.
        onTransientTimeout(true);
    }

    /**
     * As {@link #onTransientTimeout()}, additionally attributing this timeout to a call class
     * (slot-gated WORKER page fetch vs a thief's slot-free probe fetch) so a shed that fires can report
     * the {@code SHED.timeout_storm_worker_fed}/{@code _probe_fed} mix that fed its window.
     *
     * <p>A probe timeout carries no S3-backpressure signal, so it must never gate the shed nor feed the
     * growth-freeze: were a pure probe-timeout storm to freeze growth, nothing could trip the shed that
     * would end it (a probe can never satisfy the shed's worker gate). The probe branch is therefore
     * checked FIRST and returns after only rolling the shed window and incrementing the
     * {@link #shedWindowProbeTimeouts} visibility split; {@code markCongestion()}, the growth-freeze
     * feed, and {@link #shedWindowTimeouts} are reached ONLY by {@code worker == true}. The flag
     * distinguishes call class, not fault kind — a probe-class transient of either kind
     * (attempt-timeout or network fault) is excluded here.
     *
     * @param worker {@code true} for a {@code slotGated=true} worker range fetch, {@code false} for a
     *               thief's {@code slotGated=false} probe fetch (1-key pivot probe or structure probe)
     */
    public void onTransientTimeout(boolean worker) {
        long now = nanoClock.getAsLong();
        if (!worker) {
            // A probe timeout carries no S3-backpressure signal -- fully excluded from
            // markCongestion() and the transient growth-freeze feed. Still rolls the shed window and
            // feeds the shed-side visibility split so a mixed storm's probe count is never lost.
            rollShedWindowIfElapsed(now);
            shedWindowProbeTimeouts.incrementAndGet();
            metrics.recordStealReason("GROWTH", "probe_timeout_excluded");
            return;
        }
        markCongestion();   // The first WORKER timeout ends slow-start (growth reverts to additive +1).
        // Feed the 10 s growth-freeze window.
        long start = transientWindowStartNs.get();
        if (start == 0L || now - start > TRANSIENT_WINDOW_NANOS) {
            transientWindowStartNs.set(now);
            transientWindowCount.set(1);
        } else {
            transientWindowCount.incrementAndGet();
        }
        // Feed the sustained-timeout SHED window.
        rollShedWindowIfElapsed(now);
        shedWindowTimeouts.incrementAndGet();
        shedWindowWorkerTimeouts.incrementAndGet();
        maybeShed();
    }

    /**
     * Feed the latency of ONE SUCCESSFUL attempt into the latency-freeze rung. Called from
     * {@code GaugedFetcher} on the success path for slot-gated worker fetches only (like
     * {@link #reportStatus}) — NEVER for a timed-out attempt (a censored ≥10 s observation that would
     * poison the baseline) and NEVER for thief probes. Updates the Vegas rolling-minimum baseline and
     * the short trailing EWMA; the freeze decision itself is read in {@link #onSuccess()}.
     *
     * @param latencyNanos the just-completed successful page's {@link io.varve.swath.store.ListPage#latency()}, in nanos
     */
    public void onAttemptLatency(long latencyNanos) {
        if (latencyNanos <= 0L) {
            return;   // defensive: a non-positive latency carries no signal
        }
        long now = nanoClock.getAsLong();
        updateLatencyBaseline(now, latencyNanos);
        updateLatencyEwma(latencyNanos);
    }

    /**
     * Maintain the Vegas rolling-minimum baseline. A new minimum floors it immediately
     * (fast-down); at each {@link #LATENCY_BASELINE_DECAY_NANOS decay boundary} the baseline is
     * re-floored to the just-elapsed window's minimum, so it rises (slowly) only if the environment's
     * TRUE minimum genuinely rose. Lock-free (atomics); approximate under races (benign, matching the
     * gauge's window-tracking style).
     */
    private void updateLatencyBaseline(long now, long sample) {
        long base = latencyBaselineNs.get();
        if (base == 0L) {
            // First successful sample seeds both the baseline and the decay window.
            latencyBaselineNs.compareAndSet(0L, sample);
            latencyWindowMinNs.set(sample);
            latencyDecayWindowStartNs.set(now);
            publishBaseline();
            return;
        }
        atomicMin(latencyWindowMinNs, sample);
        if (sample < base) {
            atomicMin(latencyBaselineNs, sample);   // fast-down: a genuine new floor
            publishBaseline();
        }
        long start = latencyDecayWindowStartNs.get();
        if (now - start > LATENCY_BASELINE_DECAY_NANOS
                && latencyDecayWindowStartNs.compareAndSet(start, now)) {
            // Slow upward decay: re-floor to the elapsed window's min, then restart the window at
            // the current sample. Only the thread that won the CAS rolls the window.
            long windowMin = latencyWindowMinNs.getAndSet(sample);
            latencyBaselineNs.set(windowMin);
            publishBaseline();
        }
    }

    /** Update the short trailing EWMA (relaxed set; races benign — an approximate signal). */
    private void updateLatencyEwma(long sample) {
        long cur = latencyEwmaNs.get();
        long next = (cur == 0L) ? sample : (long) (cur + LATENCY_EWMA_ALPHA * (sample - cur));
        latencyEwmaNs.set(next);
    }

    /** CAS the reference down to {@code candidate} if it is currently larger. */
    private static void atomicMin(AtomicLong ref, long candidate) {
        long cur;
        while ((cur = ref.get()) > candidate) {
            if (ref.compareAndSet(cur, candidate)) {
                return;
            }
        }
    }

    /** Republish the baseline gauge (ms) for post-hoc analysis after any baseline change. */
    private void publishBaseline() {
        metrics.setLatencyBaselineMillis(latencyBaselineNs.get() / 1_000_000L);
    }

    /**
     * True when the recent successful-attempt latency EWMA has inflated past
     * {@link #LATENCY_FREEZE_FACTOR} × the rolling-minimum baseline — the HEALTHY→DEGRADING signal
     * that freezes +1 growth. Returns {@code false} until a baseline exists.
     */
    private boolean latencyFrozen() {
        long base = latencyBaselineNs.get();
        if (base <= 0L) {
            return false;
        }
        long recent = latencyEwmaNs.get();
        return (double) recent > LATENCY_FREEZE_FACTOR * (double) base;
    }

    /**
     * If the sustained-timeout window is met AND progress is starved, shed {@code T} once
     * (multiplicatively, factor {@link #SHED_FACTOR}). At most one shed per real-time window, elected
     * via a {@code compareAndSet(prev, w)} on the {@link #shedFiredWindowNs} generation stamp (see that
     * field for why a stamp, not a boolean latch). Gates are read against the LIVE {@code effectiveT},
     * so the thresholds shrink as the target sheds down (scale-invariant across T = 2 … 256).
     */
    private void maybeShed() {
        int t = effectiveT.get();
        int timeoutGate = Math.max(SHED_K, (int) Math.ceil(SHED_ALPHA * t));
        int successGate = Math.max(1, t / SHED_SUCCESS_DIVISOR);   // floor(T / 32)
        if (shedWindowTimeouts.get() >= timeoutGate && shedWindowSuccesses.get() <= successGate) {
            long w = shedWindowStartNs.get();
            long prev = shedFiredWindowNs.get();
            if (prev != w && shedFiredWindowNs.compareAndSet(prev, w)) {
                multiplicativeDecrease(SHED_FACTOR, DecreaseKind.TIMEOUT_SHED);
            }
        }
    }

    /**
     * Roll the shed window if the current one has elapsed on the injected clock — redraw the jittered
     * length and clear the timeout/success counts. Under concurrent callers at a boundary the roll is
     * elected by a single {@code compareAndSet} on {@link #shedWindowStartNs} (mirroring
     * {@link #updateLatencyBaseline}'s decay-window roller): only the CAS winner resets the counters. It
     * touches no shed latch — {@link #shedFiredWindowNs} is advanced solely by {@link #maybeShed}. The
     * gate is approximate by design: after the CAS wins, a brief interval holds {@code start == now} with
     * the old counters (benign, like the latency-baseline sibling); the decrease itself is CAS-guarded.
     */
    private void rollShedWindowIfElapsed(long now) {
        long start = shedWindowStartNs.get();
        if (start != 0L && now - start <= shedWindowLengthNs.get()) {
            return;   // still inside the current window
        }
        if (!shedWindowStartNs.compareAndSet(start, now)) {
            return;   // another thread won this boundary's roll (start == 0L first call rolls here too)
        }
        // Only the CAS winner rolls: redraw the jittered length and clear the counts.
        shedWindowLengthNs.set(shedWindowNanosSupplier.getAsLong());
        shedWindowTimeouts.set(0);
        shedWindowSuccesses.set(0);
        // Reset alongside the total they split -- see the fields' javadoc.
        shedWindowWorkerTimeouts.set(0);
        shedWindowProbeTimeouts.set(0);
    }

    /** True while the recent-window transient-timeout rate is high enough to freeze +1 growth. */
    private boolean growthFrozen() {
        long start = transientWindowStartNs.get();
        if (start == 0L || nanoClock.getAsLong() - start > TRANSIENT_WINDOW_NANOS) {
            return false;   // window elapsed with no fresh timeouts → thaw, growth resumes
        }
        return transientWindowCount.get() >= TRANSIENT_FREEZE_THRESHOLD;
    }

    /**
     * Whether the thief driver should attempt a steal right now. Returns {@code false} while
     * in throttle backoff — creating new nodes doesn't help when the store is already
     * throttling, and new nodes consume probes that add more load. Returns {@code true} once
     * the clean window elapses and T has started recovering.
     */
    public boolean isStealingAllowed() {
        return stealingAllowed;
    }

    /** Current effective concurrency target (between 1 and Tmax, inclusive). */
    public int effectiveT() {
        return effectiveT.get();
    }

    // ---- private AIMD policy ----------------------------------------------------

    /** Distinguishes the two callers of {@link #multiplicativeDecrease} so the shared machinery records the right vote/metric. */
    private enum DecreaseKind {
        /** Real 503 SlowDown: casts an honest AIMD down-vote ({@code swath.aimd.votes}). */
        THROTTLE,
        /** Sustained-timeout shed: records {@code swath.aimd.timeout_shed}, NEVER an AIMD vote. */
        TIMEOUT_SHED
    }

    private void onThrottle() {
        // A genuine store backpressure signal (real 503 SlowDown). The gauge owns only the AIMD reaction
        // (the down-vote + the multiplicative decrease); the throttle EVENT is recorded once elsewhere,
        // at its classification point in S3PageFetcher.
        multiplicativeDecrease(DECREASE_FACTOR, DecreaseKind.THROTTLE);
    }

    /**
     * The single multiplicative-decrease implementation, shared by {@link #onThrottle()} (factor
     * {@link #DECREASE_FACTOR 0.7}, a real-503 down-vote) and the {@linkplain #maybeShed()
     * sustained-timeout shed} (factor {@link #SHED_FACTOR 0.5}). Permit-conserving: every CAS step
     * reduces the permit pool by exactly {@code cur - next}.
     *
     * <p><b>Vote accounting is per-kind.</b> Only {@link DecreaseKind#THROTTLE} casts an AIMD vote
     * ({@code swath.aimd.votes}, guarded by {@code AimdAttemptTimeoutSignalContractTest}); the shed records
     * a distinct {@code swath.aimd.timeout_shed} + a {@code SHED/timeout_storm} steal_reason.
     *
     * <p><b>Both re-arm the recovery cool-down ({@code lastThrottleNs}), but ONLY on a real
     * reduction.</b> That timestamp gates only the {@code +1} clean-window recovery in
     * {@link #onSuccess()} (never the vote accounting), so a shed setting it paces regrowth without
     * conflating a shed with a 503. A floor no-op ({@code floor(factor*cur) >= cur}) removes zero
     * concurrency and does NOT re-arm (see docs/internals/metrics-internals.md §5a). The re-arm write is
     * MONOTONIC ({@code accumulateAndGet(now, max)}): {@code now} is captured once at entry, and never
     * moving {@code lastThrottleNs} backward stops a delayed thread's stale entry timestamp from
     * clobbering a later real decrease's correct re-arm. Guarded by
     * {@code ConcurrencyGaugeFloorRearmSuppressionTest}.
     *
     * <p>This admits the same ACCEPTED one-step race as {@link #onSuccess()}'s valve block: a concurrent
     * success can observe the reduced {@code effectiveT} before the re-armed {@code lastThrottleNs} is
     * visible and take one growth step inside a nominally-fresh cool-down. The next decrease
     * self-corrects it and no permit is lost ({@code effectiveT} is re-read under the CAS / growthLock).
     */
    private void multiplicativeDecrease(double factor, DecreaseKind kind) {
        long now = nanoClock.getAsLong();
        markCongestion();   // An AIMD down-vote OR a shed ends slow-start (growth reverts to +1).
        if (kind == DecreaseKind.THROTTLE) {
            metrics.recordAimdVote();        // This path IS a genuine AIMD down-vote
        } else {
            metrics.recordTimeoutShed();     // Distinct counter, NOT an AIMD vote
            metrics.recordStealReason("SHED", "timeout_storm");
            // The call-class mix that fed THIS window, read at fire time (before the next
            // rollShedWindowIfElapsed clears it) -- see shedWindowWorkerTimeouts/shedWindowProbeTimeouts'
            // javadoc. A benign race with a concurrent window roll is possible here (the same
            // approximate-under-races tolerance every other window read in this class already accepts).
            metrics.recordShedCallClassMix(shedWindowWorkerTimeouts.get(), shedWindowProbeTimeouts.get());
        }
        stealingAllowed = false;
        // T-band engagement, read ONCE at entry so a CAS-retry never double-counts; kind-agnostic,
        // counter-only. See docs/internals/metrics-internals.md §5a.
        int tAtEntry = effectiveT.get();
        if (tAtEntry <= 2) {
            metrics.recordStealReason("AIMD", "decrease_at_floor");
        } else if (tAtEntry <= 8) {
            metrics.recordStealReason("AIMD", "decrease_low_t");
        } else if (tAtEntry <= 32) {
            metrics.recordStealReason("AIMD", "decrease_mid_t");
        } else {
            metrics.recordStealReason("AIMD", "decrease_high_t");
        }
        // CAS loop: T := max(1, floor(factor * T)). Multiple threads may observe concurrent
        // signals; each attempts the decrease. The floor of 1 makes the loop terminate in at
        // most a handful of iterations regardless of concurrency.
        while (true) {
            int cur = effectiveT.get();
            int next = Math.max(1, (int) (cur * factor));
            if (next >= cur) {
                // At the floor (or rounding produced no change): no CAS, no permit change, and no
                // cool-down re-arm -- the key observable is that a floor-pinned 503/shed removing zero
                // concurrency buys no fresh cool-down. The stealing/vote/T-band side effects above
                // already fired unconditionally.
                metrics.recordStealReason("AIMD", "floor_noop_rearm");
                metrics.recordStealReason("AIMD", "floor_rearm_suppressed");
                break;   // already at floor, or rounding produced no change
            }
            if (effectiveT.compareAndSet(cur, next)) {
                // Re-arm the cool-down ONLY on a real reduction, with the entry `now`, monotonically
                // (see this method's javadoc).
                lastThrottleNs.accumulateAndGet(now, Math::max);
                // Reduce the permit pool by (cur - next). Workers currently holding permits
                // finish their in-flight page; future acquires deferred until permits return.
                semaphore.reduceAvailable(cur - next);
                metrics.setConcurrencyTarget(next);
                metrics.recordAimdTargetReduction();
                log.debug("concurrency_target_decreased from={} to={} reason={}", cur, next,
                        kind == DecreaseKind.THROTTLE ? "throttle" : "timeout_shed");
                break;
            }
            // Another thread already decreased T; re-read the new value and retry.
        }
    }

    private void onSuccess() {
        long now = nanoClock.getAsLong();
        // A real completed page is forward progress — count it toward the shed window's
        // starvation gate BEFORE any recovery early-return, so even an at-Tmax run keeps the gate fed.
        rollShedWindowIfElapsed(now);
        shedWindowSuccesses.incrementAndGet();
        int cur = effectiveT.get();
        if (cur >= tMax) {
            stealingAllowed = true;   // fully recovered; ensure the flag is set
            return;
        }
        long last = lastThrottleNs.get();
        if (last != 0L && (now - last) < CLEAN_WINDOW_NANOS) {
            // This success's growth opportunity (the +1 and the valve, both downstream of this gate) was
            // eaten by a decrease-armed cool-down; counter-only. See docs/internals/metrics-internals.md §5a.
            metrics.recordStealReason("AIMD", "growth_blocked_cooldown");
            return;   // still within the throttle cool-down window; do not recover yet
        }
        // Unpause stealing HERE, before the freeze checks below. Once the real-503 cool-down elapses the
        // store is healthy from a backpressure perspective (what stealingAllowed tracks); the narrower
        // growth-freeze must not re-couple to pausing steals, or a timeout-heavy period would keep the
        // fleet under-parallelized even after 503-backpressure cleared. The two concerns stay independent.
        stealingAllowed = true;
        // The freeze counters' denominator: every early return above this line is a success that
        // could never have frozen, so only successes reaching here are comparable trials.
        metrics.recordFreezeGateCheck();
        // Two independent growth-gate rungs (both leave T untouched; the shed owns all decreases). Each
        // records its own attribution counter so post-hoc can tell WHICH rung suppressed a growth step;
        // both may fire on the same step. We are past the cool-down and below Tmax, so a fired rung is
        // holding a genuine growth step.
        boolean latFrozen = latencyFrozen();
        if (latFrozen) {
            metrics.recordLatencyFreeze();
            metrics.recordStealReason("FREEZE", "latency_inflation");
        }
        boolean growthFroz = growthFrozen();
        if (growthFroz) {
            metrics.recordGrowthFreeze();
            metrics.recordStealReason("FREEZE", "transient_timeouts");
        }
        if (growthFroz) {
            // Worker-timeout storm: HARD freeze preserved. The valve NEVER relaxes a worker-storm
            // freeze (it answers a different, stronger question than latency inflation). T untouched.
            return;
        }
        if (latFrozen) {
            // Valve: a latency-inflation-ONLY freeze is demoted from a latch to a damper. Admit ONE
            // paced additive +1 iff the run is making progress (NOT starved -- the exact complement of
            // the shed starvation gate) and the valve cool-down has elapsed; otherwise hold the freeze.
            int successGate = Math.max(1, cur / SHED_SUCCESS_DIVISOR);   // == the shed's starvation gate
            if (shedWindowSuccesses.get() <= successGate) {
                return;   // starved: no recent progress -> keep the freeze hard (defense-in-depth w/ growthFroz)
            }
            long lastValve = lastValveGrowthNs.get();
            if (now - lastValve < VALVE_PACE_NANOS) {
                return;   // valve paced (~1 step / shed window): not yet time for the next relaxation step
            }
            if (!lastValveGrowthNs.compareAndSet(lastValve, now)) {
                return;   // another concurrent success already claimed this valve pace slot
            }
            // Admit exactly one additive +1, NEVER doubling: relaxing out of a freeze must be gentle
            // (doubling would be the rebound-storm the damper exists to prevent). ACCEPTED one-step race:
            // the lastThrottleNs cool-down gate above is not re-checked under growthLock, so a concurrent
            // 503 can land between that gate and this block, arm a fresh cool-down, and reduce effectiveT
            // after this thread cleared the gate -- this +1 then rides in on the fresh cool-down. Bounded
            // and self-correcting: the decrease is never lost (curNow is re-read under the lock, so the +1
            // composes with the reduced value), and it is at most one step, never a doubling or a repeat.
            // Guarded by ConcurrencyGaugeFrozenGrowthValveTest.
            synchronized (growthLock) {
                int curNow = effectiveT.get();
                int next = Math.min(tMax, curNow + INCREASE_STEP);
                if (effectiveT.compareAndSet(curNow, next)) {
                    int delta = next - curNow;
                    if (delta > 0) {
                        semaphore.release(delta);
                        // Engagement counter: fired once per admitted valve step.
                        metrics.recordStealReason("GROWTH", "frozen_growth_valve");
                    }
                    metrics.setConcurrencyTarget(next);
                    log.debug("concurrency_target_valve_increased from={} to={} reason=frozen_growth_valve",
                            curNow, next);
                }
            }
            return;
        }
        // Clean window (or never throttled), growth not frozen: a growth step paced to at most one per
        // GROWTH_PACE_NANOS (see that constant for why pacing, not one step per raw success). The first
        // step after a cool-down fires immediately (stale/0 lastGrowthNs).
        long lastGrowth = lastGrowthNs.get();
        if (now - lastGrowth < GROWTH_PACE_NANOS) {
            return;   // paced: not yet time for another +1 step
        }
        // Claim the pace slot via CAS so a burst of concurrent successes at one instant grants AT MOST
        // one +1 (the losers return here and re-pace). A benign race with a concurrent decrease may
        // consume this slot without an increment (the effectiveT CAS below then no-ops) — acceptable,
        // matching the gauge's other window trackers.
        if (!lastGrowthNs.compareAndSet(lastGrowth, now)) {
            return;   // another concurrent success already claimed this pace slot
        }
        // Slow-start doubles while no congestion has been seen, else grows additively. The
        // double-vs-+1 decision, the effectiveT CAS (re-read inside the monitor, since a concurrent
        // decrease may have shrunk T), and the release run as one step under growthLock so a concurrent
        // timeout/shed cannot flip congestionSeen mid-decision (see growthLock's javadoc).
        synchronized (growthLock) {
            boolean slowStart = !congestionSeen.get();
            int curNow = effectiveT.get();
            int next = slowStart ? Math.min(tMax, curNow * 2) : Math.min(tMax, curNow + INCREASE_STEP);
            if (effectiveT.compareAndSet(curNow, next)) {
                int delta = next - curNow;
                if (delta > 0) {
                    // Release the EXACT delta of permits (doubling releases next-cur; additive releases 1).
                    semaphore.release(delta);
                    if (slowStart) {
                        // Engagement counter: every new algo path stays observable post-hoc.
                        metrics.recordStealReason("AIMD", "slow_start_double");
                    }
                }
                metrics.setConcurrencyTarget(next);
                log.debug("concurrency_target_increased from={} to={} reason={} mode={}", curNow, next,
                        "clean_window", slowStart ? "slow_start_double" : "additive");
            }
        }
    }

    // ---- package-visible test seam -----------------------------------------------

    /**
     * Reset the throttle timestamp to zero, simulating a state where no 503 was ever
     * observed. The next {@link #reportStatus} call with a non-503 status will then
     * satisfy the clean-window condition and trigger a +1 recovery step.
     *
     * <p>Package-private — for unit tests only.
     */
    void forceCleanWindow() {
        lastThrottleNs.set(0L);
    }

    /**
     * Currently-available permits in the resizable semaphore — the live permit pool the multiplicative
     * decrease shrinks. Equals {@link #effectiveT()} when no worker holds a permit. Package-private —
     * for unit tests asserting permit conservation after a shed.
     */
    int availablePermits() {
        return semaphore.availablePermits();
    }

    // ---- inner ResizableSemaphore ------------------------------------------------

    /**
     * A {@link Semaphore} subclass that exposes the protected {@link #reducePermits(int)}
     * method as {@link #reduceAvailable}. We subclass rather than maintaining a separate
     * counter to keep the AQS internal permit state as the single source of truth —
     * decoupling them would create a TOCTOU race between the counter and the actual
     * semaphore state.
     *
     * <p>Fair mode ({@code fair = true}) ensures FIFO ordering under sustained throttle
     * contention and prevents starvation of workers waiting for a recovered slot.
     */
    static final class ResizableSemaphore extends Semaphore {

        ResizableSemaphore(int permits) {
            super(permits, true);   // fair = true: FIFO acquisition under throttle pressure
        }

        /**
         * Reduce the available permit count by {@code reduction}. Does not block —
         * in-flight holders are unaffected; future acquires are deferred until the
         * reduction is absorbed by returning permits.
         */
        void reduceAvailable(int reduction) {
            reducePermits(reduction);
        }
    }
}
