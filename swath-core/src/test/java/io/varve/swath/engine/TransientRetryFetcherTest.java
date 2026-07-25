/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.StopReason;
import io.varve.swath.runtime.CancelSource;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The bounded, cancellation-aware transient-retry decorator that shields the non-gauge fetch paths
 * (the seed structure probe and the sequential {@code --checkpoint none} listing) from a single
 * transient error under SDK {@code maxAttempts=1}. Without it, those paths call the raw fetcher
 * directly, so a fresh run's seed or a sequential run dies on the first 503 / attempt-timeout / reset.
 */
// Tagged at METHOD granularity, not class-level. The FAST, deterministic methods stay per-commit:
// cancellationAbortsTheRetryLoop (no backoff — aborts immediately on a cancelled token),
// seedStepOverARetryingFetcherSurvivesATransientTail (one 100ms-ceiling backoff), and
// retriesRealSlowdownsThenSucceeds (three sub-second backoffs). The cap-CROSSING methods
// (overCapWithToken_ridesOut_..., capExhaustionRethrowsTheThrottleWhenNoCancellationToken,
// overCapBackoff_usesTheRaisedStormCeiling) are FAST too — each injects the no-op Sleeper seam, so
// exhausting the cap costs no real backoff sleeps. Only retriesTransientThrottlesThenSucceeds (a real
// sub-cap retry tail) stays @Tag("deep").
final class TransientRetryFetcherTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** No-op sleeper: cap-crossing storm scenarios run without real multi-second backoff sleeps. */
    private static final TransientRetryFetcher.Sleeper NO_SLEEP = ms -> { };

    private static double steal(RunMetrics m, String outcome, String reason) {
        Counter c = m.registry().find("swath.steal_reason").tag("outcome", outcome).tag("reason", reason)
                .counter();
        return c == null ? 0.0 : c.count();
    }

    /** A transient page fetch that throws {@code failures} transient throttles, then succeeds, must NOT fail. */
    @Tag("deep")   // slow: five real backoff sleeps (up to ~3s). Retry-then-succeed is pinned per-commit
                   // by retriesRealSlowdownsThenSucceeds + seedStep... + SequentialPathTransientRetryTest.
    @Test
    @Timeout(30)
    void retriesTransientThrottlesThenSucceeds() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a"), b("data/b")))
                .interceptor((req, idx, page) -> {
                    // Fail the first 5 attempts (< the cap of 8), then serve the page.
                    if (calls.getAndIncrement() < 5) {
                        throw ThrottleException.attemptTimeout("injected transient");
                    }
                    return page;
                })
                .build();
        TransientRetryFetcher fetcher = new TransientRetryFetcher(delegate, new CancellationToken());

        ListPage page = fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000));

        assertThat(page).as("the retried fetch is never lost").isNotNull();
        assertThat(calls.get()).as("it retried past the transient failures").isGreaterThan(5);
    }

    /** A real 503 too (SLOWDOWN) is retried the same way — both kinds are bounded here (no AIMD to pace). */
    @Test
    @Timeout(30)
    void retriesRealSlowdownsThenSucceeds() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    if (calls.getAndIncrement() < 3) {
                        throw ThrottleException.slowDown("injected 503");
                    }
                    return page;
                })
                .build();
        TransientRetryFetcher fetcher = new TransientRetryFetcher(delegate, new CancellationToken());

        assertThat(fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000))).isNotNull();
    }

    /**
     * Storm ride-out: with a live {@link CancellationToken} wired (the real seed/sequential wiring),
     * crossing {@link TransientRetryFetcher#MAX_TRANSIENT_RETRIES} does not cancel the run — a
     * never-healing storm's death is owned SOLELY by the {@code LivenessWatchdog}. So the retrier keeps
     * going: an over-cap transient storm that eventually clears completes normally, the run is NEVER
     * cancelled, and each over-cap retry records a {@code TRANSIENT/storm_ride_out} engagement counter.
     * FAST via the injected no-op {@link TransientRetryFetcher.Sleeper} (no real backoff sleeps).
     */
    @Test
    @Timeout(30)
    void overCapWithToken_ridesOut_completes_neverCancels_recordsStormRideOut() throws Exception {
        CancellationToken token = new CancellationToken();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    // Fail 10 times (> the cap of 8, crossing into ride-out), then serve the page.
                    if (calls.getAndIncrement() < 10) {
                        throw ThrottleException.attemptTimeout("storm");
                    }
                    return page;
                })
                .build();
        // The no-policy constructors default to BOUNDED; this test exercises RIDE_OUT specifically,
        // so it passes the policy explicitly.
        TransientRetryFetcher fetcher =
                new TransientRetryFetcher(delegate, token, metrics, NO_SLEEP, RetryPolicy.RIDE_OUT);

        ListPage page = fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000));

        assertThat(page).as("cap crossing engages ride-out; the fetch is never lost").isNotNull();
        assertThat(token.isCancelled())
                .as("ride-out NEVER cancels the run (the watchdog owns storm death, not this cap)")
                .isFalse();
        assertThat(steal(metrics, "TRANSIENT", "storm_ride_out"))
                .as("each over-cap retry records a storm_ride_out engagement counter (retries 9 and 10)")
                .isGreaterThanOrEqualTo(1.0);
    }

    /**
     * The never-heals guard under {@link RetryPolicy#BOUNDED}, reachable only when NO watchdog is
     * armed: a never-healing storm past the cap must NOT ride out forever (no backstop exists). It
     * cancels the run resumably {@code STUCK} — attributing {@link CancelSource#TRANSIENT_RETRY_CAP} —
     * and unwinds via {@link InterruptedException}, matching the {@code STUCK} exit-75 disposition.
     * Bounded at exactly {@code MAX_TRANSIENT_RETRIES + 1} attempts; records {@code retry_cap_stuck},
     * never {@code storm_ride_out}. FAST via the injected no-op sleeper.
     */
    @Test
    @Timeout(30)
    void overCapWithToken_boundedPolicy_cancelsResumableStuck_recordsRetryCapStuck() {
        CancellationToken token = new CancellationToken();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    calls.incrementAndGet();
                    throw ThrottleException.attemptTimeout("never heals");
                })
                .build();
        TransientRetryFetcher fetcher =
                new TransientRetryFetcher(delegate, token, metrics, NO_SLEEP, RetryPolicy.BOUNDED);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                .as("BOUNDED cap exhaustion unwinds as a resumable InterruptedException, not an infinite ride-out")
                .isInstanceOf(InterruptedException.class);
        assertThat(token.isCancelled()).as("the run is cancelled resumably STUCK").isTrue();
        assertThat(token.stopReason()).as("attributed STUCK (exit-75 tempfail)").isEqualTo(StopReason.STUCK);
        assertThat(token.source())
                .as("the retry cap is named as the cancel source")
                .isEqualTo(CancelSource.TRANSIENT_RETRY_CAP);
        assertThat(calls.get())
                .as("bounded at exactly MAX_TRANSIENT_RETRIES+1 attempts — no indefinite ride-out")
                .isEqualTo(TransientRetryFetcher.MAX_TRANSIENT_RETRIES + 1);
        assertThat(steal(metrics, "TRANSIENT", "retry_cap_stuck"))
                .as("the BOUNDED cap-stuck path records its engagement counter")
                .isGreaterThanOrEqualTo(1.0);
        assertThat(steal(metrics, "TRANSIENT", "storm_ride_out"))
                .as("BOUNDED never enters ride-out").isEqualTo(0.0);
    }

    /**
     * Two ranges wedge near-simultaneously under {@code BOUNDED} — fetcher A (an ATTEMPT_TIMEOUT
     * storm) and fetcher B (a SLOWDOWN storm) share ONE token. Ordered deterministically (no real
     * thread race needed) by nesting B's OWN full cap-exhaustion INSIDE A's final throttle callback,
     * so B's {@code cancel()} reaches the shared token strictly BEFORE A's — B wins the token's
     * first-writer-wins attribution, A loses.
     *
     * <p>{@code recordTransientRetryCapExhaustion} must not run unconditionally on every cap-tripping
     * fetch: a plain last-write-wins {@code .set()} would let A — despite LOSING the token
     * attribution — overwrite B's recorded class with its own, pairing the winner's
     * {@code stop_source=transient_retry_cap} with the LOSER's {@code error_class}. This asserts the
     * fix: only B's (the WINNER's) mix survives.
     */
    @Test
    @Timeout(30)
    void losingCapExhaustionDoesNotOverwriteTheWinningFetchersErrorClass() {
        CancellationToken token = new CancellationToken();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        // Fetcher B: a pure real-503 SLOWDOWN storm.
        MockPageFetcher delegateB = MockPageFetcher.builder()
                .keys(List.of(b("data/b")))
                .interceptor((req, idx, page) -> {
                    throw ThrottleException.slowDown("B: real backpressure storm");
                })
                .build();
        TransientRetryFetcher fetcherB =
                new TransientRetryFetcher(delegateB, token, metrics, NO_SLEEP, RetryPolicy.BOUNDED);

        // Fetcher A: a pure ATTEMPT_TIMEOUT storm. On its FINAL (cap-exhausting) call, it first drives
        // B all the way through ITS OWN cap exhaustion — deterministically landing B's cancel() before
        // A's own cap-exhaustion branch calls cancel().
        MockPageFetcher delegateA = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    if (idx == TransientRetryFetcher.MAX_TRANSIENT_RETRIES) {
                        assertThatThrownBy(() -> fetcherB.fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                                .as("B's own BOUNDED cap-exhaustion unwind, nested inside A's last call")
                                .isInstanceOf(InterruptedException.class);
                    }
                    throw ThrottleException.attemptTimeout("A: never heals");
                })
                .build();
        TransientRetryFetcher fetcherA =
                new TransientRetryFetcher(delegateA, token, metrics, NO_SLEEP, RetryPolicy.BOUNDED);

        assertThatThrownBy(() -> fetcherA.fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                .as("A's own BOUNDED cap-exhaustion unwind, whether it won or lost the attribution")
                .isInstanceOf(InterruptedException.class);

        assertThat(token.source())
                .as("attributed regardless of which fetcher's cap tripped")
                .isEqualTo(CancelSource.TRANSIENT_RETRY_CAP);
        assertThat(metrics.transientRetryCapErrorClass())
                .as("B (the WINNER) recorded its own mix; A's (the loser's) different mix must not have "
                        + "overwritten it")
                .isEqualTo("stuck_throttle");
    }

    /**
     * Degenerate/embedded wiring with no token to attribute a stop to (and no watchdog armed): the
     * count-bound is preserved — cap exhaustion falls back to the fatal {@link ThrottleException}/{@link
     * ListingException} escape rather than spinning forever. FAST via the injected no-op sleeper.
     */
    @Test
    @Timeout(30)
    void capExhaustionRethrowsTheThrottleWhenNoCancellationToken() {
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    throw ThrottleException.attemptTimeout("always wedged");
                })
                .build();
        TransientRetryFetcher fetcher = new TransientRetryFetcher(delegate, null, null, NO_SLEEP);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                .as("with no token to attribute the stop, cap exhaustion keeps the legacy bound and falls "
                        + "back to the fatal ListingException contract (seed failure)")
                .isInstanceOf(ListingException.class);
    }

    /**
     * Once the retry count exceeds the cap, backoff uses the RAISED {@link
     * TransientRetryFetcher#STORM_BACKOFF_CAP_MILLIS 15 s} ceiling (fewer TLS handshakes during ride-out)
     * rather than the normal {@link TransientRetryFetcher#BACKOFF_CAP_MILLIS 5 s} ceiling. Pure delay
     * computation — no sleep.
     */
    @Test
    void overCapBackoff_usesTheRaisedStormCeiling() {
        long normalMax = 0L;
        long stormMax = 0L;
        // A high attempt makes the exponential term exceed both caps, so the full-jitter draw is bounded
        // by the ceiling; over many draws the observed max approaches each ceiling.
        for (int i = 0; i < 5000; i++) {
            normalMax = Math.max(normalMax,
                    TransientRetryFetcher.backoffDelayMillis(20, TransientRetryFetcher.BACKOFF_CAP_MILLIS));
            stormMax = Math.max(stormMax,
                    TransientRetryFetcher.backoffDelayMillis(20, TransientRetryFetcher.STORM_BACKOFF_CAP_MILLIS));
        }
        assertThat(normalMax).as("normal ceiling never exceeds 5 s")
                .isLessThanOrEqualTo(TransientRetryFetcher.BACKOFF_CAP_MILLIS);
        assertThat(stormMax).as("storm ride-out draws exceed the 5 s normal ceiling")
                .isGreaterThan(TransientRetryFetcher.BACKOFF_CAP_MILLIS);
        assertThat(stormMax).as("storm ceiling is bounded by 15 s")
                .isLessThanOrEqualTo(TransientRetryFetcher.STORM_BACKOFF_CAP_MILLIS);
    }

    /** A cancelled run aborts the retry loop promptly rather than exhausting the cap. */
    @Test
    @Timeout(30)
    void cancellationAbortsTheRetryLoop() {
        CancellationToken token = new CancellationToken();
        token.cancel();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    throw ThrottleException.attemptTimeout("wedged");
                })
                .build();
        TransientRetryFetcher fetcher = new TransientRetryFetcher(delegate, token);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                .isInstanceOf(InterruptedException.class);
    }

    /**
     * Seed-path wiring: a {@link SeedStep} whose fetcher is wrapped by {@link TransientRetryFetcher}
     * survives a transient tail on its probes and still produces seed specs.
     */
    @Test
    @Timeout(30)
    void seedStepOverARetryingFetcherSurvivesATransientTail() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("a/1"), b("b/1"), b("c/1")))
                .interceptor((req, idx, page) -> {
                    // Fail the very first probe attempt, then serve normally: pre-fix this killed the seed.
                    if (calls.getAndIncrement() == 0) {
                        throw ThrottleException.networkExhaustion("first probe transient");
                    }
                    return page;
                })
                .build();
        SeedStep seedStep = SeedSteps.of(new TransientRetryFetcher(delegate, new CancellationToken()),
                new byte[0], 4);

        List<NodeSpec> specs = seedStep.seedSpecs(1L, SeedMode.SHALLOW);

        assertThat(specs).as("the seed survives the transient probe and tiles the keyspace").isNotEmpty();
    }

    // ---- attempt-timeout escalation -----------------------------------------------------

    /**
     * A page interceptor that records the {@code attemptTimeoutEscalationLevel} seen on every call
     * (MockPageFetcher forwards the request unchanged — this is how "MockPageFetcher sees the
     * escalation" is exercised) and only stops throwing {@link ThrottleException.Kind#ATTEMPT_TIMEOUT}
     * once the level reaches {@code needsLevel}. Level 0 (the base budget) never satisfies a positive
     * {@code needsLevel}, modeling a genuinely-slow tail page that cannot complete without escalation.
     */
    private static MockPageFetcher.PageInterceptor slowPageInterceptor(int needsLevel, List<Integer> seenLevels) {
        return (req, idx, page) -> {
            seenLevels.add(req.attemptTimeoutEscalationLevel());
            if (req.attemptTimeoutEscalationLevel() < needsLevel) {
                throw ThrottleException.attemptTimeout("page needs escalation level " + needsLevel);
            }
            return page;
        };
    }

    /**
     * The core property: a page that genuinely needs 20 s completes once the retry loop
     * escalates to the level-1 (20 s) override — and, precisely, the FIRST attempt (fixed base, no
     * override) fails: it never completes "under fixed 10 s" (the base budget). Also asserts the
     * {@code attempt_timeout_escalated_1} / {@code page_completed_at_1} engagement counters (the §4
     * discriminator: a page completing only at a non-zero level is post-hoc proof of the slow-page
     * pathology).
     */
    @Test
    @Timeout(30)
    void escalation_pageThatNeeds20s_neverCompletesAtBase_completesAtLevel1() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<Integer> seenLevels = new ArrayList<>();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor(slowPageInterceptor(1, seenLevels))
                .build();
        TransientRetryFetcher fetcher =
                new TransientRetryFetcher(delegate, new CancellationToken(), metrics, NO_SLEEP);

        ListPage page = fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000));

        assertThat(page).as("the escalated attempt completes the fetch").isNotNull();
        assertThat(seenLevels)
                .as("attempt 1 used the fixed base budget (no override) and failed; attempt 2 escalated to 20s")
                .containsExactly(0, 1);
        assertThat(steal(metrics, "TRANSIENT", "attempt_timeout_escalated_1"))
                .as("the escalation to level 1 is recorded")
                .isEqualTo(1.0);
        assertThat(steal(metrics, "TRANSIENT", "page_completed_at_1"))
                .as("the success is attributed to the escalated level it completed at")
                .isEqualTo(1.0);
        assertThat(steal(metrics, "TRANSIENT", "attempt_timeout_escalated_2"))
                .as("never escalated past level 1 for a page that only needed 20s")
                .isEqualTo(0.0);
    }

    /** Escalation is capped at level 2 (40 s) — a page needing exactly the cap still completes. */
    @Test
    @Timeout(30)
    void escalation_pageThatNeeds40s_completesAtCapLevel2() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<Integer> seenLevels = new ArrayList<>();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor(slowPageInterceptor(2, seenLevels))
                .build();
        TransientRetryFetcher fetcher =
                new TransientRetryFetcher(delegate, new CancellationToken(), metrics, NO_SLEEP);

        ListPage page = fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000));

        assertThat(page).as("the level-2-capped attempt completes the fetch").isNotNull();
        assertThat(seenLevels)
                .as("base -> 20s -> 40s, in order")
                .containsExactly(0, 1, 2);
        assertThat(steal(metrics, "TRANSIENT", "attempt_timeout_escalated_2"))
                .as("escalated all the way to the level-2 cap")
                .isGreaterThanOrEqualTo(1.0);
        assertThat(steal(metrics, "TRANSIENT", "page_completed_at_2")).isEqualTo(1.0);
    }

    /**
     * Escalation is per-LOGICAL-fetch, not carried across separate {@code fetchPage} calls: a second,
     * independent fetch on the SAME fetcher instance starts fresh at the base budget (level 0), even
     * though the previous fetch escalated to level 1.
     */
    @Test
    @Timeout(30)
    void escalation_isLocalToOneFetchPageCall_doesNotBleedIntoTheNextFetch() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<Integer> seenLevels = new ArrayList<>();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a"), b("data/b")))
                .interceptor((req, idx, page) -> {
                    seenLevels.add(req.attemptTimeoutEscalationLevel());
                    // Call 0 (first logical fetch, attempt 1 @ base): fail so it escalates.
                    // Call 1 (first logical fetch, attempt 2 @ level 1): succeed.
                    // Call 2 (SECOND logical fetch, a fresh fetchPage() below): must ALSO be @ base
                    // (no override) if escalation state does not bleed across calls — succeed either way.
                    if (idx == 0) {
                        throw ThrottleException.attemptTimeout("first fetch needs escalation");
                    }
                    return page;
                })
                .build();
        TransientRetryFetcher fetcher =
                new TransientRetryFetcher(delegate, new CancellationToken(), metrics, NO_SLEEP);

        ListPage firstPage = fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000));
        assertThat(firstPage).isNotNull();

        ListPage secondPage = fetcher.fetchPage(PageRequest.objects(b("data/a"), null, 1000));
        assertThat(secondPage).isNotNull();

        assertThat(seenLevels)
                .as("fetch 1: base (fails), then escalated to level 1 (succeeds); fetch 2 on the SAME "
                        + "fetcher instance starts fresh at base — no escalation state carried over")
                .containsExactly(0, 1, 0);
    }

    /**
     * The escalation streak is keyed strictly on {@code Kind.ATTEMPT_TIMEOUT}, not on
     * "any non-fatal throttle": a {@link ThrottleException.Kind#NETWORK} fault must break the
     * CONSECUTIVE attempt-timeout streak (like a voting throttle would on the {@code GaugedFetcher}
     * path) and must never itself emit an {@code attempt_timeout_escalated_<n>} counter. A mutant
     * that escalated on any {@code ThrottleException} regardless of kind would let a NETWORK fault
     * keep (or advance) the streak; this guards against exactly that on the seed/sequential path.
     */
    @Test
    @Timeout(30)
    void escalation_resetsOnNetworkFault_breaksTheConsecutiveStreak() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<Integer> seenLevels = new ArrayList<>();
        List<ThrottleException.Kind> script = List.of(
                ThrottleException.Kind.ATTEMPT_TIMEOUT,   // call 0: base -> fails, escalates to level 1
                ThrottleException.Kind.NETWORK,           // call 1: @ level 1 override, but NOT a timeout
                                                           // -> must still reset the streak
                ThrottleException.Kind.ATTEMPT_TIMEOUT);  // call 2: back @ base -> fails, escalates to level 1 again
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    seenLevels.add(req.attemptTimeoutEscalationLevel());
                    if (idx < script.size()) {
                        ThrottleException.Kind kind = script.get(idx);
                        throw ThrottleException.classifiedTransient("scripted " + kind, kind);
                    }
                    return page;   // call 3: succeeds
                })
                .build();
        TransientRetryFetcher fetcher =
                new TransientRetryFetcher(delegate, new CancellationToken(), metrics, NO_SLEEP);

        ListPage page = fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000));

        assertThat(page).isNotNull();
        assertThat(seenLevels)
                .as("base, level-1 (interrupted by the NETWORK fault), base again, level-1 again")
                .containsExactly(0, 1, 0, 1);
        assertThat(steal(metrics, "TRANSIENT", "attempt_timeout_escalated_2"))
                .as("the intervening NETWORK fault reset the streak — level 2 is never reached")
                .isEqualTo(0.0);
        assertThat(steal(metrics, "TRANSIENT", "attempt_timeout_escalated_1"))
                .as("level 1 is recorded twice — once per independent climb; the NETWORK call itself "
                        + "never emits an escalation counter").isEqualTo(2.0);
        assertThat(steal(metrics, "TRANSIENT", "page_completed_at_1")).isEqualTo(1.0);
    }

    /**
     * REGRESSION: a request that ARRIVES already carrying an escalation level (level 2 here) must
     * never be retried at a LOWER level than it came in at. The old bug computed the retry's level
     * purely from this loop's own local streak (which restarts at 0 on every {@code fetchPage} call),
     * so the first ATTEMPT_TIMEOUT on this call would step the request from its incoming level 2 DOWN
     * to level 1 — halving the budget on the very next attempt. Fixed: the incoming level is a floor,
     * never a starting point.
     */
    @Test
    @Timeout(30)
    void escalation_neverStepsAnIncomingPreEscalatedLevelDown() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<Integer> seenLevels = new ArrayList<>();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    seenLevels.add(req.attemptTimeoutEscalationLevel());
                    // Fail exactly once (whatever level it lands at) so the loop retries; that retry
                    // is where the old bug would drop the incoming level 2 down to 1.
                    if (idx == 0) {
                        throw ThrottleException.attemptTimeout("forced retry to exercise the floor");
                    }
                    return page;
                })
                .build();
        TransientRetryFetcher fetcher =
                new TransientRetryFetcher(delegate, new CancellationToken(), metrics, NO_SLEEP);

        ListPage page = fetcher.fetchPage(
                PageRequest.objects(new byte[0], null, 1000).withAttemptTimeoutEscalationLevel(2));

        assertThat(page).isNotNull();
        assertThat(seenLevels)
                .as("both attempts stay AT LEAST the incoming level 2 -- the retry must never see level 1")
                .containsExactly(2, 2);
    }
}
