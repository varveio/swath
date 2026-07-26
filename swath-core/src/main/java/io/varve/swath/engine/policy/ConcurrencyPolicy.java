/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * <b>The policy seam</b> (contracts.md §2.1): the AIMD concurrency
 * controller's PORT, not its extraction. {@code ConcurrencyGauge} (engine; algorithms.md §5) remains
 * the only implementation this repo ships, unchanged by this interface — nothing in production
 * constructs, holds, or calls a {@code ConcurrencyPolicy}. AIMD is the most timing-coupled mechanism
 * in the engine (clean-window cooldowns, a jittered shed window, a paced relaxation valve, and a
 * decaying latency baseline all racing under CAS), and the feasibility study judged a simulator-side
 * reimplementation's divergence risk low rather than justify
 * carving the real controller out from under its concurrent callers. So instead of an extraction, this
 * defines the shape a simulator's OWN faithful port carries — derived from {@code ConcurrencyGauge}'s
 * current behavior and documented here for whoever writes that port.
 *
 * <h3>Inputs — every reactive signal {@code ConcurrencyGauge} reacts to, each timestamped explicitly</h3>
 * Every method below takes {@code atNanos} as its own parameter rather than reaching for a clock —
 * contrast {@code ConcurrencyGauge} itself, which still reads its injected {@code nanoClock} (a
 * {@link java.util.function.LongSupplier}, production {@code System::nanoTime}, threaded through
 * {@code GaugeClock}) internally at each of these call sites. An implementation of THIS interface
 * never does: the timestamp always arrives with the signal, so a discrete-event simulator can drive it
 * from its own virtual clock with no ambient read to fake. This mirrors the "inject rather than reach"
 * discipline {@link DecisionRng} already applies elsewhere in this package (an injected interface a
 * policy calls for a value, never the ambient default itself) and matches the idiom
 * {@link IdleStealPacingPolicy} already uses (an explicit {@code nowNanos} parameter on every method —
 * no clock held, none called) — not a step beyond either. {@link DecisionClock} is not the right
 * comparison here: it is an executor-only seam held as a field only by {@code IdleStealBackoff}: no
 * policy-package class, including {@code IdleStealPacingPolicy}, ever holds or calls one.
 *
 * <ul>
 *   <li>{@link #onSuccess(long)} — a completed page fetch that was NOT an S3 SlowDown ({@code
 *       ConcurrencyGauge#onSuccess}, driven by {@code reportStatus} on any non-503 status). Feeds the
 *       clean-window recovery pace and the shed window's starvation-gate numerator.</li>
 *   <li>{@link #onThrottle(long)} — a genuine store 503 SlowDown/ServiceUnavailable ({@code
 *       ConcurrencyGauge#onThrottle}), algorithms.md §5's REQUIRED decrease trigger. Deliberately NOT
 *       one {@code reportStatus(int httpStatus)} entry point: the real gauge dispatches on the raw HTTP
 *       status itself, but that classification is executor work ({@code S3PageFetcher}/{@code
 *       GaugedFetcher} already do it) — folding it in here would cross a protocol detail into this
 *       source-agnostic package, the convention every other view/outcome type here already follows
 *       (contracts.md §2.1).</li>
 *   <li>{@link #onTransientTimeout(long, boolean)} — a client-side attempt-timeout / exhausted network
 *       fault ({@code ConcurrencyGauge#onTransientTimeout(boolean)}). NEVER an AIMD down-vote by
 *       itself. {@code workerClass} distinguishes a slot-gated WORKER page fetch ({@code true}) from a
 *       thief's slot-free probe fetch ({@code false}) — algorithms.md §5's probe-timeout carve-out: a
 *       probe timeout carries no store-backpressure signal and must never feed the sustained-timeout
 *       shed gate or the transient growth-freeze, only a worker timeout may.</li>
 *   <li>{@link #onAttemptLatency(long, long)} — the latency of ONE successful attempt ({@code
 *       ConcurrencyGauge#onAttemptLatency}). Feeds the latency-freeze rung only. A timed-out attempt is
 *       a censored (&ge;10 s) observation and must never reach this method — the same discipline
 *       {@code GaugedFetcher} already applies to the real gauge.</li>
 * </ul>
 *
 * <h3>Outputs</h3>
 * <ul>
 *   <li>{@link #effectiveT()} — the live concurrency target, mirroring {@code
 *       ConcurrencyGauge#effectiveT()}.</li>
 *   <li>{@link #isStealingAllowed()} — whether new steals may proceed right now, mirroring {@code
 *       ConcurrencyGauge#isStealingAllowed()}.</li>
 * </ul>
 * These are two independent queries, not one combined snapshot: production reads them at different
 * call sites for different reasons ({@code WorkStealingScan}'s idle-steal gate reads only {@code
 * isStealingAllowed()}; {@code OwnerSelfSplit} reads only {@code effectiveT()}, "AT USE TIME" per its
 * own field's javadoc) — bundling them into one record here would imply an atomic joint read neither
 * caller needs nor production supplies.
 *
 * <h3>Deliberately excluded</h3>
 * {@code acquireSlot()}/{@code releaseSlot()} — the semaphore that actually bounds live concurrency —
 * has no place here. Blocking a thread on a permit is executor mechanism, not a policy decision (the
 * same split contracts.md §2.1 draws between code that "touches a lock, a clock, or the checkpoint CAS"
 * and code that "decides"); a discrete-event simulator has no thread to block on a permit in the first
 * place.
 *
 * <h3>The state a faithful implementation must own</h3>
 * (As found in {@code ConcurrencyGauge} as of this writing — the code, not this list, is authoritative
 * if the two drift.)
 * <ul>
 *   <li>{@code tMax} (the ceiling) and the current {@code effectiveT}, initialized by the slow-start
 *       clamp ({@code min(4, tMax)}), plus the ONE-SHOT congestion latch that flips growth from
 *       multiplicative doubling to additive {@code +1} on the run's first congestion signal.</li>
 *   <li>The last-throttle timestamp that re-arms the 10&nbsp;s clean-window recovery — advanced
 *       monotonically, and ONLY on a real T reduction (a floor no-op re-arms nothing).</li>
 *   <li>The additive-growth pace timestamp (~1 step/s) and the latency-valve pace timestamp (~1
 *       step/shed-window), paced independently of each other.</li>
 *   <li>The transient-timeout growth-freeze window: a trailing count over a fixed 10&nbsp;s window.</li>
 *   <li>The sustained-timeout shed window: a jittered [25&nbsp;s, 40&nbsp;s] window's start/length, its
 *       timeout/success counts (plus the worker/probe split, for post-hoc attribution only), and the
 *       one-shed-per-window generation stamp that admits at most one shed per real window.</li>
 *   <li>The latency-freeze rung: a Vegas-style rolling-minimum baseline with a 60&nbsp;s decay window,
 *       and a short trailing EWMA (&alpha;=0.2) of successful-attempt latency.</li>
 *   <li>The {@code stealingAllowed} pause flag itself.</li>
 * </ul>
 *
 * <h3>Guarantees a faithful implementation must uphold</h3>
 * The full derivation is algorithms.md §5; the load-bearing shape a port must reproduce:
 * <ul>
 *   <li><b>Growth</b> is multiplicative ({@code T := min(Tmax, T*2)}) until the FIRST congestion signal
 *       of the run (a worker attempt-timeout, a 503 down-vote, or a shed) latches it, for the rest of
 *       the run, to additive {@code T := min(Tmax, T+1)}, paced.</li>
 *   <li><b>Decrease</b> has exactly two triggers, both multiplicative with a floor of 1: a 503 ({@code
 *       T := max(1, floor(0.7T))}) and a starvation-gated sustained-timeout shed ({@code T := max(1,
 *       floor(0.5T))}, at most once per jittered window, requiring BOTH a timeout-volume gate and a
 *       starved-progress gate).</li>
 *   <li><b>The growth-freeze is a hard gate, never a decrease</b>: a high worker-timeout rate
 *       suppresses the {@code +1}/doubling step entirely without ever lowering T.</li>
 *   <li><b>The latency-freeze rung is a softer damper</b>: it also never decreases T, but where the
 *       growth-freeze is a hard latch, an inflated successful-attempt latency EWMA (vs. the rolling
 *       minimum) admits one paced {@code +1} at most per valve interval, and only while NOT also
 *       growth-frozen and NOT starved.</li>
 *   <li><b>Probe-class timeouts are excluded</b> from the shed gate and the growth-freeze entirely —
 *       only a worker-class timeout counts toward either.</li>
 *   <li><b>Latency samples are successful-attempt-only</b> — a timed-out attempt must never reach
 *       {@link #onAttemptLatency(long, long)}, or it poisons the baseline with a censored value.</li>
 *   <li><b>Steal-pausing tracks the 503/timeout decrease paths, not the growth-freeze/latency-freeze
 *       rungs</b>: new steals pause on a real decrease and resume once the clean window elapses,
 *       independent of whether growth itself is still frozen.</li>
 * </ul>
 *
 * <h3>What is, and is not, mechanically checked</h3>
 * None of the above. {@code DecisionPathPurityTest}'s ambient-state/ambient-clock closure walk starts
 * from every class actually IN {@code io.varve.swath.engine.policy} plus whatever a scanned class holds
 * as a FIELD — and nothing in this codebase holds a {@code ConcurrencyPolicy}-typed field (this port is
 * unwired), so an implementation of this interface is never reached by that walk at all, let alone
 * checked for the guarantees above. That test's own "Known gaps" javadoc names this interface for
 * exactly this reason. A correct implementation is a matter of whoever writes it getting the arithmetic
 * right and having it reviewed against algorithms.md §5 and the bullets above — not something this
 * repository can verify mechanically, the same caveat {@link DecisionRng}/{@link DecisionClock}'s own
 * defaults carry, sharpened here because AIMD is the most timing-coupled mechanism in the engine.
 */
public interface ConcurrencyPolicy {

    /**
     * A completed page fetch that was NOT a 503 SlowDown ({@code ConcurrencyGauge#onSuccess}).
     *
     * @param atNanos the instant this success was observed, on whatever clock base the caller uses
     *                consistently for every call to this interface
     */
    void onSuccess(long atNanos);

    /**
     * A genuine store 503 SlowDown/ServiceUnavailable ({@code ConcurrencyGauge#onThrottle}) — the
     * required AIMD decrease trigger (algorithms.md §5).
     *
     * @param atNanos the instant this throttle was observed
     */
    void onThrottle(long atNanos);

    /**
     * A client-side attempt-timeout / exhausted network fault ({@code
     * ConcurrencyGauge#onTransientTimeout(boolean)}). Never itself an AIMD down-vote.
     *
     * @param atNanos     the instant this timeout was observed
     * @param workerClass {@code true} for a slot-gated WORKER page fetch, {@code false} for a thief's
     *                    slot-free probe fetch — a probe timeout must never feed the shed gate or the
     *                    growth-freeze (algorithms.md §5)
     */
    void onTransientTimeout(long atNanos, boolean workerClass);

    /**
     * The latency of ONE successful attempt ({@code ConcurrencyGauge#onAttemptLatency}), feeding the
     * latency-freeze rung only. Never called for a timed-out attempt.
     *
     * @param atNanos      the instant this attempt completed
     * @param latencyNanos the attempt's observed latency, in nanoseconds
     */
    void onAttemptLatency(long atNanos, long latencyNanos);

    /** The live concurrency target, mirroring {@code ConcurrencyGauge#effectiveT()}. */
    int effectiveT();

    /** Whether new steals may proceed right now, mirroring {@code ConcurrencyGauge#isStealingAllowed()}. */
    boolean isStealingAllowed();
}
