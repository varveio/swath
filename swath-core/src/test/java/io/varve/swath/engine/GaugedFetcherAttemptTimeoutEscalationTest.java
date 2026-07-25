/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * {@link GaugedFetcher} (the worker/thief retry loop) escalates the
 * per-attempt SDK timeout on CONSECUTIVE {@link ThrottleException.Kind#ATTEMPT_TIMEOUT} faults of
 * the SAME logical fetch — level 0 (the store's own base budget) -> level 1 -> level 2 (cap) — so a
 * genuinely-slow tail page can eventually complete under
 * {@code maxAttempts=1} instead of retrying forever at a budget it can never beat (ride-out
 * alone cannot fix this — it just retries the SAME fixed budget indefinitely).
 *
 * <p>Direct unit tests against {@link GaugedFetcher} (package-private,
 * constructed directly here — no full engine run needed for this retry-loop-local behavior).
 */
final class GaugedFetcherAttemptTimeoutEscalationTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final TransientRetryFetcher.Sleeper NO_SLEEP = ms -> { };

    private static double steal(RunMetrics m, String outcome, String reason) {
        Counter c = m.registry().find("swath.steal_reason").tag("outcome", outcome).tag("reason", reason)
                .counter();
        return c == null ? 0.0 : c.count();
    }

    private static GaugedFetcher workerFetcher(
            MockPageFetcher delegate, RunMetrics metrics, CancellationToken token) {
        ConcurrencyGauge gauge = new ConcurrencyGauge(4, metrics);
        return new GaugedFetcher(delegate, gauge, true, true, metrics,
                () -> token, NO_SLEEP, RetryPolicy.RIDE_OUT);
    }

    /**
     * A page interceptor that records the {@code attemptTimeoutEscalationLevel} seen on every call
     * and only stops throwing {@link ThrottleException.Kind#ATTEMPT_TIMEOUT} once the level reaches
     * {@code needsLevel}. Level 0 (the base budget) never satisfies a positive {@code needsLevel} —
     * a page that genuinely needs more room than the base can NEVER complete without escalation.
     *
     * <p>The engine only ever sees levels; what a level is WORTH in wall-clock is the store's
     * business ({@code S3PageFetcher#attemptTimeoutForLevel}), which is why this models the rung and
     * not a duration.
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
     * The core property on the worker/thief path: a page that genuinely needs 20 s completes
     * once escalated to level 1 — and NEVER at the fixed base budget (attempt 1, no override,
     * fails). Also asserts the {@code attempt_timeout_escalated_1} / {@code page_completed_at_1}
     * engagement counters (the §4 discriminator).
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
        GaugedFetcher fetcher = workerFetcher(delegate, metrics, new CancellationToken());

        ListPage page = fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000));

        assertThat(page).as("the escalated attempt completes the fetch").isNotNull();
        assertThat(seenLevels)
                .as("attempt 1 used the fixed base budget (no override) and failed; attempt 2 escalated to 20s")
                .containsExactly(0, 1);
        assertThat(steal(metrics, "TRANSIENT", "attempt_timeout_escalated_1"))
                .as("the escalation to level 1 is recorded").isEqualTo(1.0);
        assertThat(steal(metrics, "TRANSIENT", "page_completed_at_1"))
                .as("the success is attributed to the escalated level it completed at").isEqualTo(1.0);
        assertThat(steal(metrics, "TRANSIENT", "attempt_timeout_escalated_2"))
                .as("never escalated past level 1 for a page that only needed 20s").isEqualTo(0.0);
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
        GaugedFetcher fetcher = workerFetcher(delegate, metrics, new CancellationToken());

        ListPage page = fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000));

        assertThat(page).as("the level-2-capped attempt completes the fetch").isNotNull();
        assertThat(seenLevels).as("base -> level 1 -> level 2, in order")
                .containsExactly(0, 1, 2);
        assertThat(steal(metrics, "TRANSIENT", "attempt_timeout_escalated_2"))
                .as("escalated all the way to the level-2 cap").isGreaterThanOrEqualTo(1.0);
        assertThat(steal(metrics, "TRANSIENT", "page_completed_at_2")).isEqualTo(1.0);
    }

    /**
     * A genuinely-slow page that NEVER reaches the base+cap escalation ceiling (needs MORE than
     * 40 s) never completes — escalation has a hard cap; it does not turn every page into a
     * guaranteed success, only pages within the escalated budget. Bounded run (RetryPolicy.BOUNDED,
     * no token) so the test terminates instead of riding out forever.
     */
    @Test
    @Timeout(30)
    void escalation_pageNeedingMoreThanTheCap_stillNeverCompletes() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<Integer> seenLevels = new ArrayList<>();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor(slowPageInterceptor(99, seenLevels))
                .build();
        ConcurrencyGauge gauge = new ConcurrencyGauge(4, metrics);
        CancellationToken token = new CancellationToken();
        GaugedFetcher fetcher = new GaugedFetcher(
                delegate, gauge, true, true, metrics, () -> token, NO_SLEEP, RetryPolicy.BOUNDED);

        Assertions.assertThatThrownBy(
                        () -> fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                .as("BOUNDED cap exhaustion still applies — escalation caps at 40s, so a page needing "
                        + "more never completes and the bounded policy resumably STUCKs the run")
                .isInstanceOf(InterruptedException.class);
        assertThat(seenLevels).as("every attempt past level 2 stays pinned at the level-2 cap")
                .filteredOn(d -> d != null)
                .allSatisfy(l -> assertThat(l).isLessThanOrEqualTo(2));
    }

    /**
     * The {@code GaugedFetcher} reset (mixed-fault): a REAL voting throttle (503 SlowDown)
     * between two attempt-timeouts resets the CONSECUTIVE escalation streak (same reset as
     * {@code transientRetries}) — so a page that sees ATTEMPT_TIMEOUT, then SLOWDOWN, then
     * ATTEMPT_TIMEOUT again never reaches level 2; it must re-climb from the base.
     */
    @Test
    @Timeout(30)
    void escalation_resetsOnVotingThrottle_breaksTheConsecutiveStreak() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<Integer> seenLevels = new ArrayList<>();
        List<ThrottleException.Kind> script = List.of(
                ThrottleException.Kind.ATTEMPT_TIMEOUT,   // call 0: base -> fails, escalates to level 1
                ThrottleException.Kind.SLOWDOWN,          // call 1: @ level 1 override, but VOTING -> resets streak
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
        GaugedFetcher fetcher = workerFetcher(delegate, metrics, new CancellationToken());

        ListPage page = fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000));

        assertThat(page).isNotNull();
        assertThat(seenLevels)
                .as("base, level-1 (interrupted by the voting throttle), base again, level-1 again")
                .containsExactly(0, 1, 0, 1);
        assertThat(steal(metrics, "TRANSIENT", "attempt_timeout_escalated_2"))
                .as("the intervening SLOWDOWN reset the streak — level 2 is never reached")
                .isEqualTo(0.0);
        assertThat(steal(metrics, "TRANSIENT", "attempt_timeout_escalated_1"))
                .as("level 1 is recorded twice — once per independent climb").isEqualTo(2.0);
        assertThat(steal(metrics, "TRANSIENT", "page_completed_at_1")).isEqualTo(1.0);
    }

    /**
     * The escalation streak is keyed strictly on {@code Kind.ATTEMPT_TIMEOUT}, not on
     * {@code !votesAimdDown()} — a {@link ThrottleException.Kind#NETWORK} fault is ALSO non-voting
     * (retried, feeds only the growth-freeze) but is NOT a timeout signal, so it must break the
     * CONSECUTIVE attempt-timeout streak exactly like a voting throttle does, and must never itself
     * emit an {@code attempt_timeout_escalated_<n>} counter. A mutant that gated escalation on
     * {@code !votesAimdDown()} instead of {@code == ATTEMPT_TIMEOUT} would let a NETWORK fault keep
     * (or advance) the streak; this guards against exactly that.
     */
    @Test
    @Timeout(30)
    void escalation_resetsOnNetworkFault_breaksTheConsecutiveStreak() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<Integer> seenLevels = new ArrayList<>();
        List<ThrottleException.Kind> script = List.of(
                ThrottleException.Kind.ATTEMPT_TIMEOUT,   // call 0: base -> fails, escalates to level 1
                ThrottleException.Kind.NETWORK,           // call 1: @ level 1 override, non-voting but NOT a
                                                           // timeout -> must still reset the streak
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
        GaugedFetcher fetcher = workerFetcher(delegate, metrics, new CancellationToken());

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
     * REGRESSION (mirrors {@code TransientRetryFetcherTest}'s identical case): a request that
     * ARRIVES already carrying an escalation level (level 2 here) must never be retried at a LOWER
     * level than it came in at. The old bug derived the retry level purely from this loop's own
     * local streak (which restarts at 0 on every {@code fetchPage} call), so the first
     * ATTEMPT_TIMEOUT would step the request from its incoming level 2 DOWN to level 1 — halving
     * the budget on the very next attempt. Fixed: the incoming level is a floor, never a starting
     * point.
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
        GaugedFetcher fetcher = workerFetcher(delegate, metrics, new CancellationToken());

        ListPage page = fetcher.fetchPage(
                PageRequest.objects(new byte[0], null, 1000).withAttemptTimeoutEscalationLevel(2));

        assertThat(page).isNotNull();
        assertThat(seenLevels)
                .as("both attempts stay AT LEAST the incoming level 2 -- the retry must never see level 1")
                .containsExactly(2, 2);
    }
}
