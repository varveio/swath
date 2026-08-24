/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Adversarial guards for the ordering of the two controller observations produced by one
 * successful {@link GaugedFetcher} attempt: publish the attempt's uncensored latency before its
 * HTTP-success vote can claim a paced growth step.
 *
 * <p>These tests deliberately exercise {@link GaugedFetcher}, rather than calling the two gauge
 * methods in the desired order. That pins the production integration seam where the former
 * status-before-latency ordering lived.
 */
final class GaugedFetcherSuccessLatencyOrderingTest {

    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;
    private static final TransientRetryFetcher.Sleeper NO_SLEEP = millis -> { };

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static GaugedFetcher worker(
            MockPageFetcher delegate, ConcurrencyGauge gauge, RunMetrics metrics) {
        return new GaugedFetcher(delegate, gauge, true, true, metrics,
                CancellationToken::new, NO_SLEEP, RetryPolicy.RIDE_OUT);
    }

    private static ListPage withStatusAndLatency(ListPage page, int status, Duration latency) {
        return new ListPage(page.entries(), page.commonPrefixes(), page.truncated(),
                page.nextContinuationToken(), page.nextKeyMarker(), page.nextVersionIdMarker(),
                status, latency);
    }

    /**
     * The just-completed slow page must freeze its OWN growth opportunity. A preceding returned 503
     * moves the gauge out of slow-start, then a 100 ms success establishes the baseline. The next
     * 1 s page moves the EWMA to 280 ms, above the 2x freeze threshold. The shed success gate is
     * intentionally still starved at that instant, so the latency valve cannot obscure the ordering.
     *
     * <p><b>Mutant caught:</b> under the old {@code reportStatus(200)} then
     * {@code onAttemptLatency(1s)} ordering, the slow page sees the stale 100 ms EWMA, consumes the
     * next paced additive step, and changes {@code T} from 71 to 72 before publishing the evidence
     * that should have frozen it. The ordered path keeps {@code T == 71}.
     */
    @Test
    void slowSuccess_appliesItsLatencyBeforeItsOwnGrowthVote() throws Exception {
        AtomicLong now = new AtomicLong(1_000_000_000_000L);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = new ConcurrencyGauge(256, metrics, now::get, () -> WINDOW, 100);
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((request, call, page) -> switch (call) {
                    case 0 -> withStatusAndLatency(page, 503, Duration.ofSeconds(10));
                    case 1 -> page.withLatency(Duration.ofMillis(100));
                    case 2 -> page.withLatency(Duration.ofSeconds(1));
                    default -> throw new AssertionError("unexpected fetch call " + call);
                })
                .build();
        GaugedFetcher fetcher = worker(delegate, gauge, metrics);
        PageRequest request = PageRequest.objects(new byte[0], null, 1000);

        ListPage returnedThrottle = fetcher.fetchPage(request);

        assertThat(returnedThrottle.httpStatus()).isEqualTo(503);
        assertThat(gauge.effectiveT())
                .as("a returned 503 retains its 0.7 AIMD decrease: 100 -> 70")
                .isEqualTo(70);
        assertThat(metrics.registry().get("swath.aimd.latency_baseline_ms").gauge().value())
                .as("returned-503 latency is not a successful-attempt sample")
                .isZero();
        gauge.forceCleanWindow();
        fetcher.fetchPage(request);

        assertThat(gauge.effectiveT())
                .as("the fast baseline page takes one additive recovery step: 70 -> 71")
                .isEqualTo(71);
        assertThat(metrics.registry().get("swath.aimd.latency_baseline_ms").gauge().value())
                .isEqualTo(100.0);

        now.addAndGet(ConcurrencyGauge.GROWTH_PACE_NANOS + 1L);
        fetcher.fetchPage(request);

        assertThat(gauge.effectiveT())
                .as("the 1s page's own latency freezes its otherwise-eligible paced growth step")
                .isEqualTo(71);
        assertThat(metrics.registry().get("swath.aimd.latency_freeze").counter().count())
                .as("the ordering-sensitive page engaged the latency freeze")
                .isEqualTo(1.0);
        assertThat(metrics.registry().get("swath.steal_reason")
                .tags("outcome", "FREEZE", "reason", "latency_inflation")
                .counter().count()).isEqualTo(1.0);
    }

    /**
     * An attempt-timeout remains a censored observation: it ends slow-start and feeds the transient
     * controls, but creates no successful-latency baseline. Only the healing retry establishes one;
     * a later success continues from that uncensored baseline.
     */
    @Test
    void timedOutAttempt_remainsExcludedAndHealingSuccessEstablishesBaseline() throws Exception {
        AtomicLong now = new AtomicLong(1_000_000_000_000L);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = new ConcurrencyGauge(64, metrics, now::get, () -> WINDOW, 4);
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((request, call, page) -> {
                    if (call == 0) {
                        throw ThrottleException.attemptTimeout("scripted censored attempt");
                    }
                    if (call == 1) {
                        assertThat(gauge.effectiveT())
                                .as("a timeout is not an AIMD down-vote")
                                .isEqualTo(4);
                        assertThat(metrics.registry().get("swath.aimd.latency_baseline_ms")
                                .gauge().value())
                                .as("the timed-out attempt supplied no latency sample")
                                .isZero();
                    }
                    return page.withLatency(Duration.ofMillis(100));
                })
                .build();
        GaugedFetcher fetcher = worker(delegate, gauge, metrics);
        PageRequest request = PageRequest.objects(new byte[0], null, 1000);

        fetcher.fetchPage(request);

        assertThat(gauge.effectiveT())
                .as("the timeout ended slow-start, so the healing success grows additively: 4 -> 5")
                .isEqualTo(5);
        assertThat(metrics.registry().get("swath.steal_reason")
                .tags("outcome", "TRANSIENT", "reason", "attempt_timeout_worker")
                .counter().count()).isEqualTo(1.0);
        assertThat(metrics.registry().get("swath.aimd.latency_baseline_ms").gauge().value())
                .isEqualTo(100.0);

        now.addAndGet(ConcurrencyGauge.GROWTH_PACE_NANOS + 1L);
        fetcher.fetchPage(request);

        assertThat(gauge.effectiveT()).isEqualTo(6);
        assertThat(metrics.registry().get("swath.aimd.latency_baseline_ms").gauge().value())
                .isEqualTo(100.0);
    }
}
