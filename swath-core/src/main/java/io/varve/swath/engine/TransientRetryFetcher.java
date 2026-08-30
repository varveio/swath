/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

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
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Bounded, jittered, cancellation-aware transient-retry decorator for the fetch paths that do
 * <b>not</b> go through the engine's gauge-wrapped worker/thief fetcher — the seed structure probe
 * ({@link SeedStep}) and the sequential (non-work-stealing / {@code --checkpoint none}) listing path
 * ({@code ListRunner}). With SDK {@code maxAttempts=1} (swath is the sole retrier) the SDK does not
 * absorb a transient 503 / attempt-timeout / connection reset on these paths itself, so this
 * decorator mirrors {@code GaugedFetcher}'s transient handling: retry the SAME fetch after a bounded,
 * full-jitter exponential backoff, checking cancellation between attempts so a
 * {@code --max-duration}/signal abort unwinds promptly.
 *
 * <p>Cap-exhaustion disposition is a {@link RetryPolicy} resolved once at CLI wiring time (merely
 * wiring a {@link CancellationToken} does not select {@link RetryPolicy#RIDE_OUT} — the token-accepting
 * 2/3/4-arg constructors default to {@link RetryPolicy#BOUNDED}; callers with an armed watchdog thread
 * the resolved policy through the 5-arg constructor). Under {@link RetryPolicy#RIDE_OUT},
 * {@link #MAX_TRANSIENT_RETRIES} is a backoff-shaping threshold only: crossing it raises the full-jitter
 * ceiling from {@link #BACKOFF_CAP_MILLIS 5 s} to {@link #STORM_BACKOFF_CAP_MILLIS 15 s} and records a
 * {@code TRANSIENT/storm_ride_out} counter. Under {@link RetryPolicy#BOUNDED}, cap exhaustion cancels the
 * run {@code STUCK} (attributing {@link CancelSource#TRANSIENT_RETRY_CAP}) and unwinds via
 * {@link InterruptedException} — the same resumable exit-75 disposition the watchdog uses. With no
 * {@link CancellationToken} wired at all, the retry stays count-bounded regardless of policy and the
 * {@link ThrottleException} escapes as the fatal {@link ListingException} contract. See
 * {@code docs/internals/algorithms.md} §5 ("Per-fetch transient-retry cap disposition") for the full
 * mechanism and why exactly one policy, never a cap racing the watchdog, owns liveness death. The shared
 * {@linkplain #backoffDelayMillis backoff schedule} keeps {@code GaugedFetcher} and this decorator on
 * one source of truth.
 */
public final class TransientRetryFetcher implements PageFetcher {

    /** Per-fetch transient-retry cap — see the class doc for cap-exhaustion disposition per {@link RetryPolicy}. */
    static final int MAX_TRANSIENT_RETRIES = 8;
    static final long BACKOFF_BASE_MILLIS = 100L;
    static final long BACKOFF_CAP_MILLIS = 5_000L;
    /**
     * Raised full-jitter ceiling used once the retry count exceeds {@link #MAX_TRANSIENT_RETRIES}
     * (storm ride-out) — a longer backoff reduces the TLS-handshake churn of a self-amplifying storm.
     */
    static final long STORM_BACKOFF_CAP_MILLIS = 15_000L;

    /**
     * How many per-attempt-timeout escalation rungs exist above the base budget, applied to the SAME
     * logical fetch on CONSECUTIVE {@link ThrottleException.Kind#ATTEMPT_TIMEOUT} faults. Escalation
     * exists because ride-out alone would retry a genuinely-slow tail page forever at the SAME budget
     * under {@code maxAttempts=1} without ever completing it; buying the attempt more room is what
     * lets it finish. Shared with {@code GaugedFetcher} (one source of truth, like the backoff
     * schedule above).
     *
     * <p><b>Rung COUNT only — never rung durations.</b> How long a rung is worth is the STORE's to
     * decide, because only the store knows a given call class's base budget, and call classes differ
     * by more than a constant factor (a point lookup and a scan are not the same call). The engine
     * publishes the level on {@link PageRequest#attemptTimeoutEscalationLevel()} and the store maps it
     * to a duration (for S3: {@code base(callClass) * 2^level}). This file previously held absolute
     * durations authored against one store's base; see {@code docs/internals/probe-budgets.md} §3 for
     * the mismatch that produced.
     */
    static final int MAX_ATTEMPT_TIMEOUT_ESCALATION_LEVEL = 2;

    /**
     * The 0-based escalation level (0 = the store's base budget) implied by {@code
     * consecutiveAttemptTimeouts}, capped at {@link #MAX_ATTEMPT_TIMEOUT_ESCALATION_LEVEL} — used for
     * both {@link PageRequest#withAttemptTimeoutEscalationLevel} and the {@code
     * attempt_timeout_escalated_<n>} / {@code page_completed_at_<n>} engagement counters: a page that
     * completes only at a non-zero level is post-hoc proof it needed the escalated budget.
     */
    static int escalationLevel(int consecutiveAttemptTimeouts) {
        return Math.min(Math.max(consecutiveAttemptTimeouts, 0), MAX_ATTEMPT_TIMEOUT_ESCALATION_LEVEL);
    }

    /**
     * Test seam: the backoff sleep, injectable so storm scenarios run without real multi-second
     * sleeps. Public so the CLI ({@code ListCommand.backoffSleeperOverride}, a different package) and the
     * runtime ({@code ListRunner}, threading it into the engine) can reference the type when plumbing the
     * override — mirroring the {@code fetcherOverride} seam.
     */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    /** Production sleeper — {@link Thread#sleep(long)}, used when no override is threaded through the seam. */
    static final Sleeper DEFAULT_SLEEPER = Thread::sleep;

    private final PageFetcher delegate;
    /** The run's cancellation token: consulted between retries to abort promptly. Nullable. */
    private final CancellationToken cancellation;
    /** Per-run metrics for the {@code storm_ride_out} engagement counter. Nullable (no-op when absent). */
    private final RunMetrics metrics;
    private final Sleeper sleeper;
    /** Cap-exhaustion disposition when a {@link CancellationToken} is wired — see the class doc for {@link RetryPolicy}'s two dispositions. */
    private final RetryPolicy policy;
    /** True only for the fresh-run seed decorator created by {@link #forSeed}. */
    private final boolean failFastUnreachableEndpoint;

    /**
     * Defaults to {@link RetryPolicy#BOUNDED}: {@link RetryPolicy#RIDE_OUT} is only safe when a real
     * watchdog is armed to own storm death (see the class doc), and a bare no-policy constructor has
     * no way to know that — leaving it RIDE_OUT here would be an ownerless infinite-retry route for
     * direct/embedded construction. Callers with an armed watchdog pass the resolved policy explicitly
     * via the 5-arg constructor below.
     */
    public TransientRetryFetcher(PageFetcher delegate, CancellationToken cancellation) {
        this(delegate, cancellation, null, DEFAULT_SLEEPER, RetryPolicy.BOUNDED);
    }

    /** See the 2-arg constructor's javadoc — defaults to {@link RetryPolicy#BOUNDED}. */
    public TransientRetryFetcher(PageFetcher delegate, CancellationToken cancellation, RunMetrics metrics) {
        this(delegate, cancellation, metrics, DEFAULT_SLEEPER, RetryPolicy.BOUNDED);
    }

    /**
     * Test seam: inject a {@link Sleeper} so cap-crossing storm scenarios run without real sleeps.
     * See the 2-arg constructor's javadoc — defaults to {@link RetryPolicy#BOUNDED}.
     */
    TransientRetryFetcher(PageFetcher delegate, CancellationToken cancellation, RunMetrics metrics,
                          Sleeper sleeper) {
        this(delegate, cancellation, metrics, sleeper, RetryPolicy.BOUNDED);
    }

    /**
     * Full seam: the seed path threads its resolved {@link RetryPolicy} and (test-only)
     * {@link Sleeper} override here from {@code ListCommand}.
     */
    public TransientRetryFetcher(PageFetcher delegate, CancellationToken cancellation, RunMetrics metrics,
                                 Sleeper sleeper, RetryPolicy policy) {
        this(delegate, cancellation, metrics, sleeper, policy, false);
    }

    /**
     * Build the fresh-run seed decorator whose endpoint identity is known from wiring rather than
     * inferred from request fields also used by mid-run structure probes.
     */
    public static TransientRetryFetcher forSeed(PageFetcher delegate,
            CancellationToken cancellation, RunMetrics metrics, Sleeper sleeper,
            RetryPolicy policy) {
        return new TransientRetryFetcher(
                delegate, cancellation, metrics, sleeper, policy, true);
    }

    private TransientRetryFetcher(PageFetcher delegate, CancellationToken cancellation,
            RunMetrics metrics, Sleeper sleeper, RetryPolicy policy,
            boolean failFastUnreachableEndpoint) {
        this.delegate = delegate;
        this.cancellation = cancellation;
        this.metrics = metrics;
        this.sleeper = sleeper;
        this.policy = policy;
        this.failFastUnreachableEndpoint = failFastUnreachableEndpoint;
    }

    @Override
    public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
        int attempt = 0;
        int transientRetries = 0;
        // Consecutive Kind.ATTEMPT_TIMEOUT count for THIS logical fetch — drives the
        // per-attempt timeout escalation (base -> 20s -> 40s), independent of the cap-shaping
        // transientRetries above. Local to this call, so it is implicitly reset on success.
        int consecutiveAttemptTimeouts = 0;
        // Per-kind fault counts within the SAME window as transientRetries
        // above (never reset here — the cap counts every ThrottleException kind, so these mirror that),
        // so a cap-exhaustion can classify itself from ITS OWN local fault history — see
        // RunMetrics#recordTransientRetryCapExhaustion.
        int attemptTimeoutFaults = 0;
        int votingFaults = 0;
        // A floor, never a starting point: if req already arrived carrying an escalation level (a
        // caller retrying an already-escalated logical fetch), the locally-derived level below must
        // never step BELOW it -- escalation only ever buys room (PageRequest#withAttemptTimeoutEscalationLevel's
        // javadoc), so a request entering at level 2 must not be handed a level-1 (halved) budget on
        // its very next attempt just because this loop's own streak restarted at 0.
        int incomingLevel = req.attemptTimeoutEscalationLevel();
        while (true) {
            throwIfRunCancelled();
            int level = Math.max(incomingLevel, escalationLevel(consecutiveAttemptTimeouts));
            PageRequest attemptReq = level == incomingLevel ? req : req.withAttemptTimeoutEscalationLevel(level);
            try {
                ListPage page = delegate.fetchPage(attemptReq);
                if (level > 0 && metrics != null) {
                    // Proof the page needed the escalated budget (see escalationLevel's javadoc).
                    metrics.recordStealReason("TRANSIENT", "page_completed_at_" + level);
                }
                return page;
            } catch (ThrottleException te) {
                if (failFastUnreachableEndpoint && isUnreachableEndpoint(te)) {
                    if (metrics != null) {
                        metrics.recordStealReason("FATAL", "seed_endpoint_unreachable");
                    }
                    throw new ListingException(
                            "seed probe failed because the object-store endpoint is unreachable", te);
                }
                transientRetries++;
                if (te.kind() == ThrottleException.Kind.ATTEMPT_TIMEOUT) {
                    consecutiveAttemptTimeouts++;
                    attemptTimeoutFaults++;
                    int nextLevel = Math.max(incomingLevel, escalationLevel(consecutiveAttemptTimeouts));
                    if (nextLevel > 0 && metrics != null) {
                        metrics.recordStealReason("TRANSIENT", "attempt_timeout_escalated_" + nextLevel);
                    }
                } else {
                    // A voting throttle (real 503/5xx) or a NETWORK fault breaks the CONSECUTIVE
                    // attempt-timeout streak this escalation tracks — reset to the base budget.
                    consecutiveAttemptTimeouts = 0;
                    if (te.kind().votesAimdDown()) {
                        votingFaults++;
                    }
                }
                if (transientRetries > MAX_TRANSIENT_RETRIES) {
                    if (cancellation == null) {
                        // No token wired: keep the count-bound and escape as the fatal
                        // ListingException contract (class doc).
                        throw te;
                    }
                    if (policy == RetryPolicy.BOUNDED) {
                        // BOUNDED: no watchdog could stop an unbounded ride-out, so cancel STUCK
                        // (resumable exit-75).
                        if (metrics != null) {
                            metrics.recordStealReason("TRANSIENT", "retry_cap_stuck");
                        }
                        // Only the fetch whose cancel WINS the token's
                        // first-writer-wins attribution may record its local fault tally — two ranges can
                        // wedge near-simultaneously, and a losing recorder's history must never overwrite
                        // the winner's (that would leave stop_source paired with the wrong error_class).
                        // Recording only after a won cancel makes this structural, not a benign race.
                        boolean wonAttribution = cancellation.cancel(StopReason.STUCK, CancelSource.TRANSIENT_RETRY_CAP);
                        if (wonAttribution && metrics != null) {
                            metrics.recordTransientRetryCapExhaustion(attemptTimeoutFaults, votingFaults);
                        }
                        throw new InterruptedException(
                                "fetch aborted: transient-retry cap exhausted (stuck)");
                    }
                    // RIDE_OUT: the watchdog owns liveness death, not this cap — keep retrying with
                    // the raised backoff ceiling; the counter's magnitude is how deep into ride-out we went.
                    if (metrics != null) {
                        metrics.recordStealReason("TRANSIENT", "storm_ride_out");
                    }
                }
                throwIfRunCancelled();
                backoff(++attempt, transientRetries);
            }
        }
    }

    /** DNS failure and connection refusal are permanent for this invocation's seed configuration. */
    private static boolean isUnreachableEndpoint(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = failure;
        while (current != null && seen.add(current)) {
            if (current instanceof ConnectException || current instanceof UnknownHostException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private void throwIfRunCancelled() throws InterruptedException {
        if (cancellation != null && cancellation.isCancelled()) {
            throw new InterruptedException("fetch aborted: run cancelled");
        }
    }

    /** Full-jitter backoff sleep, using the raised {@link #STORM_BACKOFF_CAP_MILLIS} ceiling once past the cap. */
    private void backoff(int attempt, int transientRetries) throws InterruptedException {
        long cap = transientRetries > MAX_TRANSIENT_RETRIES ? STORM_BACKOFF_CAP_MILLIS : BACKOFF_CAP_MILLIS;
        sleeper.sleep(backoffDelayMillis(attempt, cap));
    }

    /**
     * Full-jitter (AWS "Exponential Backoff And Jitter") delay in {@code [0, ceiling]} where the ceiling
     * grows exponentially up to {@code capMillis}. Shared by {@code GaugedFetcher} so the two retriers
     * keep one backoff schedule; the caller supplies {@link #BACKOFF_CAP_MILLIS} normally and
     * {@link #STORM_BACKOFF_CAP_MILLIS} during storm ride-out.
     */
    static long backoffDelayMillis(int attempt, long capMillis) {
        int shift = Math.min(attempt - 1, 16);
        long ceiling = Math.min(capMillis, BACKOFF_BASE_MILLIS << shift);
        return ThreadLocalRandom.current().nextLong(ceiling + 1);
    }

    @Override
    public StoreCapabilities capabilities() {
        return delegate.capabilities();
    }
}
