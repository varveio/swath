/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.micrometer.core.instrument.Timer;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.StopReason;
import io.varve.swath.runtime.CancelSource;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import java.util.function.Supplier;

/**
 * Gauge-aware {@link PageFetcher} wrapper that ties a fetch into the AIMD policy on BOTH
 * the success and the throttle path (algorithms.md §5).
 *
 * <p><b>Slot gating (worker fetches).</b> When {@code slotGated}, acquires a permit
 * immediately before each HTTP attempt and releases it immediately after (in a
 * {@code finally}), so the permit is never held across a commit-await, a channel-send, or
 * the backoff sleep. When {@code reportSuccess}, the returned page's HTTP status is fed to
 * the gauge after the slot is released (the permit need not be held for AIMD).
 *
 * <p><b>Throttle path.</b> A {@link ThrottleException} is never fatal here: the same fetch is
 * retried after a bounded, jittered, cancellation-aware backoff, so the page is never lost (a
 * probe's own, much smaller fail-fast cap is the one exception — see the next paragraph). The
 * AIMD gauge is nudged down (multiplicative decrease of {@code T} + pause steals,
 * {@link ConcurrencyGauge#SLOWDOWN_STATUS}) <b>only</b> when the throttle is a genuine store
 * backpressure signal — {@link ThrottleException.Kind#votesAimdDown()} true, i.e. a real 503
 * {@code SlowDown} or 5xx. A client-side attempt-timeout / network fault
 * ({@code votesAimdDown()} false) is retried but does NOT vote {@code T} down (a hung local read
 * is not S3 rate-limiting); it feeds {@link ConcurrencyGauge#onTransientTimeout(boolean)} instead
 * — the worker growth-freeze and sustained-timeout shed window, never AIMD. Cap-exhaustion
 * disposition under {@link RetryPolicy#RIDE_OUT} vs {@link RetryPolicy#BOUNDED} (which one owns a
 * never-healing storm's death, and why {@link #MAX_TRANSIENT_RETRIES} is a backoff-shaping
 * threshold under ride-out but a hard give-up bound otherwise) is algorithms.md §5. The
 * AIMD-voting kinds stay retry-until-cancel (UNBOUNDED): AIMD paces
 * them (T collapses toward 1) and cancellation/max-duration bounds them, preserving the
 * sustained-throttle liveness contract (THR-1). Genuinely fatal {@link ListingException}s
 * propagate unchanged.
 *
 * <p>Worker page fetches use {@code (slotGated=true, reportSuccess=true)}. The {@link Thief}
 * uses {@code (slotGated=false, reportSuccess=false)}: probes stay off the concurrency gate
 * (rare 1-key calls; stealing is paused separately via
 * {@link ConcurrencyGauge#isStealingAllowed()}) and a healthy probe must not nudge the AIMD
 * recovery. For a genuine VOTING throttle (503/5xx) a probe still drives the decrease and
 * retries UNBOUNDED, exactly like a worker. For a NON-voting transient
 * (attempt-timeout / network fault) a probe fails fast on {@link #PROBE_TRANSIENT_RETRY_CAP} —
 * independent of {@link #policy} (RIDE_OUT/BOUNDED govern the worker path only) — rather than
 * riding out: see {@link #PROBE_TRANSIENT_RETRY_CAP}'s javadoc and {@link Thief#steal} for how
 * the resulting {@link ThrottleException} re-enters the thief's ordinary non-productive flow.
 */
final class GaugedFetcher implements PageFetcher {

    /**
     * The transient-retry cap-shaping threshold, shared with {@link TransientRetryFetcher} as one
     * source of truth for the backoff schedule. Crossing it engages storm ride-out (raised backoff
     * ceiling, {@code storm_ride_out} recorded) under {@link RetryPolicy#RIDE_OUT}, or a hard
     * give-up under {@link RetryPolicy#BOUNDED} / the token-less path — see algorithms.md §5 for
     * the full disposition.
     */
    private static final int MAX_TRANSIENT_RETRIES = TransientRetryFetcher.MAX_TRANSIENT_RETRIES;

    /**
     * The thief's {@code slotGated=false} probe fetches get their OWN, much smaller retry cap —
     * independent of {@link #policy} (RIDE_OUT/BOUNDED govern the WORKER path only; see
     * {@link Thief#steal} for where a probe's cap-exhaustion {@link ThrottleException} re-enters
     * the thief's ordinary non-productive-steal flow). Riding out the worker's schedule lets ONE
     * camping probe consume the majority of a storm's request volume, since it holds the sole
     * in-flight steal slot for the whole storm — see algorithms.md §5 for the measured evidence.
     * Exactly ONE retry is kept (not zero): a single transient blip gets one chance to self-heal
     * at the escalated 20 s budget, so a probe is not needlessly lost to a one-off network hiccup
     * — but a SECOND retry is not granted, since by then the probe has already spent as long as a
     * worker's own page fetch would, defeating the point of bounding it.
     */
    static final int PROBE_TRANSIENT_RETRY_CAP = 1;

    private final PageFetcher delegate;
    private final ConcurrencyGauge gauge;
    private final boolean slotGated;
    private final boolean reportSuccess;
    private final RunMetrics metrics;
    /** The run's cancellation token: consulted between throttle retries to abort promptly. */
    private final Supplier<CancellationToken> cancellation;
    /** The backoff sleep, injectable so storm ride-out is testable without real sleeps. */
    private final TransientRetryFetcher.Sleeper sleeper;
    /**
     * How cap exhaustion is disposed with a token wired — {@link RetryPolicy#RIDE_OUT}
     * (watchdog owns storm death; retry indefinitely) or {@link RetryPolicy#BOUNDED} (no watchdog
     * armed; cap exhaustion cancels resumable STUCK). The token-less path is bounded regardless.
     */
    private final RetryPolicy policy;

    GaugedFetcher(PageFetcher delegate, ConcurrencyGauge gauge, boolean slotGated,
                  boolean reportSuccess, RunMetrics metrics,
                  Supplier<CancellationToken> cancellation) {
        this(delegate, gauge, slotGated, reportSuccess, metrics, cancellation,
                TransientRetryFetcher.DEFAULT_SLEEPER, RetryPolicy.RIDE_OUT);
    }

    GaugedFetcher(PageFetcher delegate, ConcurrencyGauge gauge, boolean slotGated,
                  boolean reportSuccess, RunMetrics metrics,
                  Supplier<CancellationToken> cancellation, TransientRetryFetcher.Sleeper sleeper,
                  RetryPolicy policy) {
        this.delegate = delegate;
        this.gauge = gauge;
        this.slotGated = slotGated;
        this.reportSuccess = reportSuccess;
        this.metrics = metrics;
        this.cancellation = cancellation;
        this.sleeper = sleeper;
        this.policy = policy;
    }

    @Override
    public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
        int attempt = 0;
        int transientRetries = 0;
        // Consecutive Kind.ATTEMPT_TIMEOUT count for THIS logical fetch — drives the
        // per-attempt timeout escalation (base -> 20s -> 40s), independent of the cap-shaping
        // transientRetries above. Local to this call, so it is implicitly reset on success.
        int consecutiveAttemptTimeouts = 0;
        // Per-kind fault tally within the SAME reset window as transientRetries (both reset on a
        // voting throttle) — cap-exhaustion can therefore only be a run of consecutive non-voting
        // faults, so votingFaults is 0 at trip time by construction; kept anyway so
        // RunMetrics#classify's dominance rule stays honest if that invariant ever changes. See
        // RunMetrics#recordTransientRetryCapExhaustion.
        int attemptTimeoutFaults = 0;
        int votingFaults = 0;
        // A floor, never a starting point -- see TransientRetryFetcher#fetchPage's identical
        // incomingLevel for why: this loop's own streak restarting at 0 must never step a
        // pre-escalated incoming request DOWN to a smaller budget.
        int incomingLevel = req.attemptTimeoutEscalationLevel();
        while (true) {
            // Check cancellation before each attempt and again after each throttle (a flag-only
            // cancel does not interrupt the backoff sleep), so a persistently-throttled/timing-out
            // bucket cannot spin past a --max-duration deadline or a SIGTERM; abort by throwing
            // InterruptedException, which the engine maps to CancelledException so the run
            // unwinds gracefully.
            throwIfRunCancelled();
            int level = Math.max(incomingLevel, TransientRetryFetcher.escalationLevel(consecutiveAttemptTimeouts));
            PageRequest attemptReq = level == incomingLevel ? req : req.withAttemptTimeoutEscalationLevel(level);
            try {
                ListPage page = fetchOnce(attemptReq);
                if (reportSuccess) {
                    // One ordered controller event: the successful attempt's latency must be visible
                    // before that same completion is allowed to claim a paced growth step. A returned
                    // 503 is store backpressure, not a successful-latency observation; a timed-out
                    // attempt never reaches here at all (it throws).
                    long latencyNanos = page.latency() == null ? 0L : page.latency().toNanos();
                    ConcurrencyGauge.CompletionSnapshot controller =
                            gauge.reportCompletedAttempt(page.httpStatus(), latencyNanos);
                    if (page.httpStatus() != ConcurrencyGauge.SLOWDOWN_STATUS) {
                        metrics.recordAimdWorkerSuccess(page.entries().size(), latencyNanos,
                                controller.effectiveT(), controller.latencyBaselineNanos(),
                                controller.latencyEwmaNanos(), controller.latencyInflated(),
                                controller.latencySampled());
                    }
                }
                if (level > 0) {
                    // A page that only completes at an escalated level is post-hoc proof the
                    // tail page genuinely needed longer than the base budget.
                    metrics.recordStealReason("TRANSIENT", "page_completed_at_" + level);
                }
                return page;
            } catch (ThrottleException te) {
                if (te.kind().votesAimdDown()) {
                    // Genuine store backpressure (real 503 SlowDown / 5xx): drive AIMD down. This
                    // path stays retry-until-cancel (UNBOUNDED) — AIMD itself paces it (T collapses
                    // toward 1) and cancellation/max-duration bounds it, preserving the THR-1
                    // sustained-throttle liveness contract.
                    gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);
                    // Reset every non-voting-transient counter here: the STUCK cap bounds a RUN of
                    // consecutive client-side transients (a genuinely wedged read), never a count
                    // spread across an otherwise-healthy, AIMD-paced 503 stretch — so a mixed-fault
                    // bucket must not accumulate transientRetries across a voting throttle.
                    transientRetries = 0;
                    consecutiveAttemptTimeouts = 0;
                    attemptTimeoutFaults = 0;
                    votingFaults = 0;
                } else {
                    // Client-side attempt-timeout / network fault: retry it, but do
                    // NOT vote T down — a hung local read is not S3 rate-limiting. Feed only the
                    // growth-freeze so we don't expand concurrency into a sick network.
                    gauge.onTransientTimeout(slotGated);   // Attribute to worker vs probe class
                    transientRetries++;
                    if (te.kind() == ThrottleException.Kind.ATTEMPT_TIMEOUT) {
                        // Attribute EVERY attempt-timeout retry to its source — worker range fetch
                        // vs the thief's probe fetch — unconditionally (not gated on the escalation
                        // level, so it measures raw request cadence), so post-run analysis can tell from
                        // the metrics alone what fraction of tail-storm timeouts is probe pressure
                        // vs a genuinely slow tail page hit by every worker.
                        metrics.recordStealReason("TRANSIENT",
                                slotGated ? "attempt_timeout_worker" : "attempt_timeout_probe");
                        consecutiveAttemptTimeouts++;
                        attemptTimeoutFaults++;
                        int nextLevel = Math.max(incomingLevel,
                                TransientRetryFetcher.escalationLevel(consecutiveAttemptTimeouts));
                        if (nextLevel > 0) {
                            metrics.recordStealReason("TRANSIENT", "attempt_timeout_escalated_" + nextLevel);
                        }
                    } else {
                        // NETWORK (connection reset / DNS / TLS): not a timeout signal, so it
                        // does not carry a "the read hung" implication — escalating the SDK's
                        // per-attempt timeout would not address it. Breaks the streak.
                        consecutiveAttemptTimeouts = 0;
                    }
                    if (!slotGated && transientRetries > PROBE_TRANSIENT_RETRY_CAP) {
                        // Probe fail-fast (see PROBE_TRANSIENT_RETRY_CAP's javadoc): never touches
                        // the CancellationToken; Thief.steal() folds this into its ordinary
                        // non-productive-steal RETRY flow, freeing the sole in-flight steal slot
                        // immediately instead of camping on it for the whole storm.
                        metrics.recordStealReason("TRANSIENT", "probe_retry_cap_failfast");
                        throw te;
                    }
                    // PROBE_TRANSIENT_RETRY_CAP < MAX_TRANSIENT_RETRIES, so a probe (slotGated=false)
                    // has already failed fast above by the time transientRetries could reach this
                    // threshold — everything below is reachable for WORKER fetches only.
                    if (transientRetries > MAX_TRANSIENT_RETRIES) {
                        CancellationToken token = cancellation.get();
                        if (token == null) {
                            // Degenerate/embedded wiring (no run to attribute a stop to, no
                            // watchdog armed): keep the legacy bound rather than loop forever.
                            throw te;
                        }
                        if (policy == RetryPolicy.BOUNDED) {
                            // No watchdog is armed, so cap exhaustion is the only thing that can
                            // stop an unbounded ride-out (algorithms.md §5): cancel STUCK and unwind
                            // via InterruptedException, the SAME resumable exit-75 disposition the
                            // watchdog uses; the engine's produce() maps it to CancelledException.
                            metrics.recordStealReason("TRANSIENT", "retry_cap_stuck");
                            // Record the fault tally only if this cancel WINS the token's
                            // first-writer-wins attribution — two ranges can wedge near-
                            // simultaneously, and a losing recorder's history must never overwrite
                            // the winner's (that would pair stop_source with the wrong error_class).
                            boolean wonAttribution =
                                    token.cancel(StopReason.STUCK, CancelSource.TRANSIENT_RETRY_CAP);
                            if (wonAttribution) {
                                metrics.recordTransientRetryCapExhaustion(attemptTimeoutFaults, votingFaults);
                            }
                            throw new InterruptedException(
                                    "fetch aborted: transient-retry cap exhausted (stuck)");
                        }
                        // Storm ride-out (algorithms.md §5): do not cancel the run; keep retrying
                        // with a raised backoff ceiling and record the engagement counter. Real
                        // 503s are still retried unbounded above; the reset there keeps a mixed
                        // 503/timeout bucket from tripping this deep needlessly. Worker-only — a
                        // probe never reaches ride-out (see the invariant note above).
                        metrics.recordStealReason("TRANSIENT", "storm_ride_out");
                        metrics.recordStealReason("TRANSIENT", "storm_ride_out_worker");
                    }
                }
                throwIfRunCancelled();
                long cap = transientRetries > MAX_TRANSIENT_RETRIES
                        ? TransientRetryFetcher.STORM_BACKOFF_CAP_MILLIS
                        : TransientRetryFetcher.BACKOFF_CAP_MILLIS;
                sleeper.sleep(TransientRetryFetcher.backoffDelayMillis(++attempt, cap));
            }
        }
    }

    private void throwIfRunCancelled() throws InterruptedException {
        CancellationToken token = cancellation.get();
        if (token != null && token.isCancelled()) {
            throw new InterruptedException("fetch aborted: run cancelled");
        }
    }

    private ListPage fetchOnce(PageRequest req) throws ListingException, InterruptedException {
        if (!slotGated) {
            return delegate.fetchPage(req);
        }
        Timer.Sample rateLimitWait = metrics.startRateLimitWaitTimer();
        gauge.acquireSlot();
        metrics.recordRateLimitWait(rateLimitWait);
        metrics.incrementInFlight();
        try {
            return delegate.fetchPage(req);
        } finally {
            metrics.decrementInFlight();
            gauge.releaseSlot();
        }
    }

    @Override
    public StoreCapabilities capabilities() {
        return delegate.capabilities();
    }
}
