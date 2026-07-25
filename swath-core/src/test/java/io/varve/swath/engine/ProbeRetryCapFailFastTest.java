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
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.StubCheckpointStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Direct unit coverage of {@link GaugedFetcher#PROBE_TRANSIENT_RETRY_CAP} and how
 * its cap-exhaustion {@link ThrottleException} re-enters {@link Thief#steal}'s existing
 * non-productive-steal flow. The
 * anti-camping CONTRACT itself (full engine, narrow tail, probe share collapsing under the cap, the
 * run still completing) lives in {@link NarrowTailProbeVsWorkerAttributionTest} — this file is the
 * mechanism-local guard: the cap boundary, and that it is completely independent of {@link
 * RetryPolicy} (which governs the WORKER path only).
 */
final class ProbeRetryCapFailFastTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final TransientRetryFetcher.Sleeper NO_SLEEP = ms -> { };

    private static double steal(RunMetrics m, String outcome, String reason) {
        Counter c = m.registry().find("swath.steal_reason").tag("outcome", outcome).tag("reason", reason)
                .counter();
        return c == null ? 0.0 : c.count();
    }

    private static GaugedFetcher probeFetcher(
            MockPageFetcher delegate, RunMetrics metrics, CancellationToken token, RetryPolicy policy) {
        ConcurrencyGauge gauge = new ConcurrencyGauge(4, metrics);
        return new GaugedFetcher(delegate, gauge, false, false, metrics,
                () -> token, NO_SLEEP, policy);
    }

    private static GaugedFetcher workerFetcher(
            MockPageFetcher delegate, RunMetrics metrics, CancellationToken token, RetryPolicy policy,
            TransientRetryFetcher.Sleeper sleeper) {
        ConcurrencyGauge gauge = new ConcurrencyGauge(4, metrics);
        return new GaugedFetcher(delegate, gauge, true, true, metrics,
                () -> token, sleeper, policy);
    }

    // -------------------------------------------------------------------------
    // The cap boundary, at the GaugedFetcher level.
    // -------------------------------------------------------------------------

    /**
     * A permanently-wedged probe fails fast at EXACTLY {@code PROBE_TRANSIENT_RETRY_CAP + 1 = 2}
     * total attempts (the base attempt + one retry) — never rides out, never touches the
     * cancellation token — under {@link RetryPolicy#RIDE_OUT} (the production default, where a
     * worker in the same situation would ride out indefinitely).
     */
    @Test
    @Timeout(30)
    void probeFailsFastAtExactlyTheCapBoundary_doesNotCancel_recordsCounter() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        CancellationToken token = new CancellationToken();
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    calls.incrementAndGet();
                    throw ThrottleException.attemptTimeout("permanently wedged probe");
                })
                .build();
        GaugedFetcher fetcher = probeFetcher(delegate, metrics, token, RetryPolicy.RIDE_OUT);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1)))
                .as("a probe never rides out — it fails fast with the ORIGINAL ThrottleException, "
                        + "not an InterruptedException/cancel")
                .isInstanceOf(ThrottleException.class);

        assertThat(calls.get())
                .as("exactly cap+1 attempts: the base attempt plus ONE retry, then give up")
                .isEqualTo(GaugedFetcher.PROBE_TRANSIENT_RETRY_CAP + 1);
        assertThat(steal(metrics, "TRANSIENT", "probe_retry_cap_failfast"))
                .as("the fail-fast engagement counter fires exactly once").isEqualTo(1.0);
        assertThat(token.isCancelled())
                .as("a probe's cap exhaustion NEVER cancels the run — single-liveness-owner intact")
                .isFalse();
    }

    /** Policy independence: the SAME permanently-wedged probe fails fast identically under BOUNDED. */
    @Test
    @Timeout(30)
    void probeFailsFast_identicallyUnderBoundedPolicy_policyIsIrrelevantToProbes() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        CancellationToken token = new CancellationToken();
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    calls.incrementAndGet();
                    throw ThrottleException.attemptTimeout("permanently wedged probe");
                })
                .build();
        GaugedFetcher fetcher = probeFetcher(delegate, metrics, token, RetryPolicy.BOUNDED);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1)))
                .as("BOUNDED does not change a probe's disposition — it still fails fast with the "
                        + "ORIGINAL ThrottleException, never cancelling STUCK the way a worker's BOUNDED "
                        + "cap-exhaustion would")
                .isInstanceOf(ThrottleException.class);

        assertThat(calls.get()).isEqualTo(GaugedFetcher.PROBE_TRANSIENT_RETRY_CAP + 1);
        assertThat(token.isCancelled())
                .as("BOUNDED's cancel-STUCK disposition is WORKER-only — a probe never reaches it")
                .isFalse();
        assertThat(steal(metrics, "TRANSIENT", "retry_cap_stuck"))
                .as("the worker-only retry_cap_stuck counter never fires for a probe").isEqualTo(0.0);
    }

    /** A probe that heals within its one grace retry still succeeds — the cap is not zero-tolerance. */
    @Test
    @Timeout(30)
    void probeSucceeds_ifTheStormHealsWithinTheOneGraceRetry() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        CancellationToken token = new CancellationToken();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    if (idx == 0) {
                        throw ThrottleException.attemptTimeout("one-off blip");
                    }
                    return page;   // healed on the retry
                })
                .build();
        GaugedFetcher fetcher = probeFetcher(delegate, metrics, token, RetryPolicy.RIDE_OUT);

        ListPage page = fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1));

        assertThat(page).as("a single transient blip does not lose the probe — the grace retry absorbs it")
                .isNotNull();
        assertThat(steal(metrics, "TRANSIENT", "probe_retry_cap_failfast"))
                .as("no fail-fast — the probe succeeded within its cap").isEqualTo(0.0);
    }

    /**
     * Policy isolation: a WORKER fetch (slotGated=true) in the IDENTICAL permanently-wedged
     * situation is completely unaffected by the new probe cap — it keeps retrying past the point
     * where a probe would already have failed fast, riding out under {@link RetryPolicy#RIDE_OUT}
     * exactly as before this unit.
     */
    @Test
    @Timeout(30)
    void workerFetches_areUnaffectedByTheProbeCap_stillRideOutPastIt() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        CancellationToken token = new CancellationToken();
        int probeCapPlusOne = GaugedFetcher.PROBE_TRANSIENT_RETRY_CAP + 1;
        int healAfter = probeCapPlusOne + 10;   // well past where a probe would already have failed fast
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    if (calls.getAndIncrement() < healAfter) {
                        throw ThrottleException.attemptTimeout("wedged then heals");
                    }
                    return page;
                })
                .build();
        GaugedFetcher fetcher =
                workerFetcher(delegate, metrics, token, RetryPolicy.RIDE_OUT, NO_SLEEP);

        ListPage page = fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000));

        assertThat(page).as("the worker fetch rode out well past the probe's cap and completed").isNotNull();
        assertThat(calls.get()).isEqualTo(healAfter + 1);
        assertThat(steal(metrics, "TRANSIENT", "probe_retry_cap_failfast"))
                .as("the probe-only fail-fast counter never fires for a slotGated=true worker fetch")
                .isEqualTo(0.0);
        assertThat(token.isCancelled()).as("ride-out never cancels the run").isFalse();
    }

    // -------------------------------------------------------------------------
    // How the fail-fast re-enters Thief's flow.
    // -------------------------------------------------------------------------

    private static final class RecordingSink implements Thief.ChildSink {
        @Override public void accept(long childNodeId, byte[] childLo, byte[] childHi) { }
    }

    /**
     * {@link Thief#steal} catches the probe's cap-exhaustion {@link ThrottleException} and folds it
     * into the SAME {@link Thief.Outcome#RETRY} non-productive-steal outcome an ordinary retry
     * takes — no exception propagates out of {@code steal()}, and the thief can be invoked again
     * immediately (the sole in-flight steal slot is freed, not camped on).
     */
    @Test
    @Timeout(30)
    void thiefStealCatchesProbeFailFast_returnsRetry_andCanBeCalledAgainImmediately() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        CancellationToken token = new CancellationToken();
        // A parent-empty-sliver-free wide bounded range with a deep cursor so the pivot computation
        // reaches a real probe fetch (probeNonEmpty) rather than resolving via an early RETRY/
        // UNSPLITTABLE outcome that never touches the fetcher at all.
        WorkerState victim = WorkerStates.of(1, b("a"), b("a"), b("z"));
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("m")))
                .interceptor((req, idx, page) -> {
                    throw ThrottleException.attemptTimeout("permanently wedged victim");
                })
                .build();
        GaugedFetcher probeFetcher =
                probeFetcher(delegate, metrics, token, RetryPolicy.RIDE_OUT);
        Thief thief = Thiefs.of(new StubCheckpointStore(), probeFetcher, 1L, new byte[0], ListingMode.OBJECTS,
                new RecordingSink(), metrics);

        Thief.Outcome first = thief.steal(List.of(victim));
        assertThat(first).as("the probe's cap exhaustion is caught and folded into RETRY, never propagates")
                .isEqualTo(Thief.Outcome.RETRY);
        assertThat(steal(metrics, "RETRY", "probe_retry_cap_failfast"))
                .as("the RETRY-side attribution of the fail-fast fires").isEqualTo(1.0);

        // The thief moves on: a second attempt against the SAME still-wedged victim behaves
        // identically (no leaked state, no camped slot, no exception the second time either).
        Thief.Outcome second = thief.steal(List.of(victim));
        assertThat(second).as("the thief is immediately usable again — no camped state").isEqualTo(Thief.Outcome.RETRY);
        assertThat(steal(metrics, "RETRY", "probe_retry_cap_failfast")).isEqualTo(2.0);
        assertThat(token.isCancelled()).as("neither attempt ever cancelled the run").isFalse();
    }
}
