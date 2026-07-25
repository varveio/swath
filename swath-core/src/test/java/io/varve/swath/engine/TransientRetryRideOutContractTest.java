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
import io.varve.swath.error.ThrottleException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.StopReason;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * <b>The contract.</b> With a cancellation token wired, an over-cap transient no longer cancels
 * the run — the fetch retries INDEFINITELY (full-jitter backoff, ceiling 5 s through retry 8, then
 * {@link TransientRetryFetcher#STORM_BACKOFF_CAP_MILLIS 15 s}), recording {@code
 * recordStealReason("TRANSIENT","storm_ride_out")} per over-cap retry, until the store heals (a later
 * fetch returns a page) or the LivenessWatchdog no-progress backstop ends the run. That death is
 * owned SOLELY by the watchdog, never by this per-fetch cap. Two invariants are preserved: a cooperative
 * cancel still unwinds promptly mid-backoff, and <b>with NO token</b> (embedded use) the bounded
 * behavior stands — the {@link ThrottleException} is thrown after the cap.
 *
 * <p>These are FAST unit tests over the {@code TransientRetryFetcher} seam directly: an injected
 * <b>backoff sleeper</b> (a no-op / scripted {@link TransientRetryFetcher.Sleeper}) removes the
 * multi-second real sleeps, so a &gt;8-retry storm runs in microseconds and stays deterministic.
 *
 * <p>These tests reach directly into {@link TransientRetryFetcher}'s package-private surface — the
 * {@code Sleeper} functional interface, its 5-arg ride-out constructor, and {@code
 * TransientRetryFetcher.STORM_BACKOFF_CAP_MILLIS}/{@code TransientRetryFetcher.MAX_TRANSIENT_RETRIES}
 * — whose contracts are documented there. A rename of any of them must be reflected here too.
 */
final class TransientRetryRideOutContractTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A no-op backoff sleeper: removes the real multi-second storm backoffs so the ride-out runs fast. */
    private static final TransientRetryFetcher.Sleeper NOOP_SLEEPER = millis -> { /* deterministic: no real sleep */ };

    private static double stormRideOut(RunMetrics m) {
        Counter c = m.registry().find("swath.steal_reason")
                .tag("outcome", "TRANSIENT").tag("reason", "storm_ride_out").counter();
        return c == null ? 0.0 : c.count();
    }

    /**
     * WITH A TOKEN: a run of {@code >8} consecutive transients does NOT cancel STUCK — the fetch rides
     * out well past the cap and returns the page once the store heals.
     */
    @Test
    @Timeout(30)
    void withToken_stormPastTheCap_ridesOut_doesNotCancel_thenReturnsHealedPage() throws Exception {
        int healAfter = 12;   // 12 consecutive transients (>> the cap of 8), then the store heals
        AtomicInteger thrown = new AtomicInteger();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a/1"), b("data/a/2"), b("data/b/1")))
                .interceptor((req, idx, page) -> {
                    // Increment ONLY when throwing, so thrown.get() is the exact count of transients
                    // endured (not inflated by the final healed call) — matches the CLI sibling's shape.
                    if (thrown.get() < healAfter) {
                        thrown.incrementAndGet();
                        throw ThrottleException.attemptTimeout("seed storm (t=0 fleet launch)");
                    }
                    return page;   // healed
                })
                .build();
        CancellationToken token = new CancellationToken();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // The no-policy ctor defaults to BOUNDED — this test exercises RIDE_OUT, so it must pass
        // the policy explicitly.
        TransientRetryFetcher fetcher =
                new TransientRetryFetcher(delegate, token, metrics, NOOP_SLEEPER, RetryPolicy.RIDE_OUT);

        ListPage page = fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000));

        assertThat(page).as("the ride-out never loses the fetch — the healed page is returned").isNotNull();
        assertThat(token.isCancelled())
                .as("an over-cap transient storm must NOT cancel the run (the watchdog owns death)")
                .isFalse();
        assertThat(thrown.get())
                .as("the fetch rode out WELL past the legacy 9-attempt cap")
                .isEqualTo(healAfter);
        assertThat(stormRideOut(metrics))
                .as("each over-cap retry records the storm_ride_out engagement counter")
                .isGreaterThan(0.0);
    }

    /**
     * WITHOUT A TOKEN (embedded use): the bounded contract STANDS — cap exhaustion throws the
     * {@link ThrottleException} after exactly {@code MAX_TRANSIENT_RETRIES + 1} attempts, it does NOT ride
     * out forever. Kills a mutant that made ride-out unconditional (ignoring token presence) — with no
     * token that would spin forever and this bounded assertion / {@code @Timeout} would fail.
     */
    @Test
    @Timeout(30)
    void withoutToken_capExhaustion_throwsThrottle_afterExactlyTheCap_noRideOut() {
        AtomicInteger thrown = new AtomicInteger();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    thrown.incrementAndGet();
                    throw ThrottleException.attemptTimeout("always wedged");
                })
                .build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        TransientRetryFetcher fetcher =
                new TransientRetryFetcher(delegate, null, metrics, NOOP_SLEEPER);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                .as("no token to attribute a stop: cap exhaustion falls back to the fatal throttle escape")
                .isInstanceOf(ThrottleException.class);
        assertThat(thrown.get())
                .as("bounded at exactly MAX_TRANSIENT_RETRIES+1 — no indefinite ride-out without a token")
                .isEqualTo(TransientRetryFetcher.MAX_TRANSIENT_RETRIES + 1);
        assertThat(stormRideOut(metrics))
                .as("the no-token path never enters ride-out, so it records no storm_ride_out")
                .isEqualTo(0.0);
    }

    /**
     * A cooperative cancel landing DURING an over-cap backoff unwinds promptly: the ride-out is not a
     * black hole. We script the sleeper to trip the run's cancellation on an over-cap backoff (sleep #11,
     * well past the cap of 8); the very next loop guard must abort via {@link InterruptedException} with
     * the token cancelled — the fetch does not swallow the cancel and keep riding out. Kills a ride-out
     * that stopped consulting cancellation between retries (a Ctrl-C / max-duration black hole).
     */
    @Test
    @Timeout(30)
    void cooperativeCancelDuringOverCapBackoff_unwindsPromptly() {
        CancellationToken token = new CancellationToken();
        AtomicInteger sleeps = new AtomicInteger();
        TransientRetryFetcher.Sleeper cancellingSleeper = millis -> {
            if (sleeps.incrementAndGet() >= 11) {   // an OVER-cap backoff (> the cap of 8)
                token.cancel(StopReason.SIGNAL);
            }
        };
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    throw ThrottleException.attemptTimeout("wedged");
                })
                .build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // Explicit RIDE_OUT — see class doc.
        TransientRetryFetcher fetcher =
                new TransientRetryFetcher(delegate, token, metrics, cancellingSleeper, RetryPolicy.RIDE_OUT);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                .as("a cancel mid-ride-out unwinds promptly as InterruptedException, not a black hole")
                .isInstanceOf(InterruptedException.class);
        assertThat(token.isCancelled()).as("the cooperative cancel is honored").isTrue();
    }

    /**
     * The ride-out's backoff DELAYS actually delivered to the sleeper are bounded by the raised 15 s storm
     * ceiling — and DO exceed the normal 5 s cap, proving the ceiling was genuinely raised past
     * {@code BACKOFF_CAP_MILLIS} once the retry count crosses {@code MAX_TRANSIENT_RETRIES}. We drive a
     * permanent stream of transients with a capturing sleeper that records every requested delay and
     * cancels after enough over-cap retries to sample the raised ceiling, then assert on the observed
     * distribution (full-jitter
     * draws land in {@code [0, ceiling]}). Kills: a mutant that forgot to raise the ceiling (max delay
     * could never exceed 5 s) and one that raised it unbounded / to the wrong value (max delay would
     * exceed 15 s).
     */
    @Test
    @Timeout(30)
    void overCapBackoffDelays_areBoundedByTheRaisedStormCeiling_andExceedTheNormalCap() {
        CancellationToken token = new CancellationToken();
        List<Long> requested = Collections.synchronizedList(new ArrayList<>());
        int stopAfter = 200;   // deep into ride-out (>> the cap of 8): most samples are at the raised ceiling
        TransientRetryFetcher.Sleeper capturing = millis -> {
            requested.add(millis);
            if (requested.size() >= stopAfter) {
                token.cancel(StopReason.SIGNAL);   // end the indefinite ride-out
            }
        };
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    throw ThrottleException.attemptTimeout("permanent storm");
                })
                .build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // Explicit RIDE_OUT — see class doc.
        TransientRetryFetcher fetcher =
                new TransientRetryFetcher(delegate, token, metrics, capturing, RetryPolicy.RIDE_OUT);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                .as("the ride-out ends on the cooperative cancel the sleeper trips, not a black hole")
                .isInstanceOf(InterruptedException.class);

        long maxDelay = requested.stream().mapToLong(Long::longValue).max().orElse(0L);
        assertThat(maxDelay)
                .as("no ride-out backoff exceeds the raised 15 s storm ceiling")
                .isLessThanOrEqualTo(15_000L);
        assertThat(maxDelay)
                .as("ride-out DID raise the ceiling past the normal 5 s cap (else no delay could exceed 5000)")
                .isGreaterThan(5_000L);
    }
}
