/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ProtocolViolationException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The response-side entry bound through {@link GaugedFetcher} — the retry loop every WORKER page
 * fetch actually rides, and the one the readahead/probe fetches ride off-gauge. A page carrying more
 * entries than the request's max-keys bound is refused on the first response in both shapes: it is
 * not a {@code ThrottleException}, so it never enters the ride-out loop, and it never reaches the
 * cancel-STUCK give-up path. The end-to-end refusal against a real over-serving endpoint (and the
 * {@code TransientRetryFetcher} decorator) lives in the replay server's {@code
 * OversizedPageRefusalIT}.
 */
final class GaugedFetcherProtocolViolationRefusalTest {

    private static final TransientRetryFetcher.Sleeper NO_SLEEP = ms -> { };

    private static MockPageFetcher overServing(AtomicInteger calls) {
        return MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8)))
                .interceptor((req, idx, page) -> {
                    calls.incrementAndGet();
                    throw ProtocolViolationException.oversizedPage("bucket", req.maxKeys(),
                            req.maxKeys() + 1, 0);
                })
                .build();
    }

    private static GaugedFetcher fetcher(MockPageFetcher delegate, RunMetrics metrics,
                                         CancellationToken token, boolean slotGated) {
        return new GaugedFetcher(delegate, new ConcurrencyGauge(4, metrics), slotGated, slotGated,
                metrics, () -> token, NO_SLEEP, RetryPolicy.RIDE_OUT);
    }

    @Test
    @Timeout(30)
    void workerFetchRefusesTheOversizedPageOnTheFirstResponse() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        CancellationToken token = new CancellationToken();
        AtomicInteger calls = new AtomicInteger();
        GaugedFetcher fetcher = fetcher(overServing(calls), metrics, token, true);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                .isInstanceOf(ProtocolViolationException.class)
                .hasMessageContaining("oversized_page");

        assertThat(calls.get()).as("one request reached the endpoint — the refusal is not retried")
                .isEqualTo(1);
        assertThat(token.isCancelled())
                .as("the refusal propagates as itself, never through the cancel-STUCK give-up path")
                .isFalse();
    }

    /** The off-gauge shape ({@code slotGated=false}) the thief's probe and speculative readahead ride. */
    @Test
    @Timeout(30)
    void offGaugeFetchRefusesTheOversizedPageOnTheFirstResponse() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        CancellationToken token = new CancellationToken();
        AtomicInteger calls = new AtomicInteger();
        GaugedFetcher fetcher = fetcher(overServing(calls), metrics, token, false);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                .isInstanceOf(ProtocolViolationException.class);

        assertThat(calls.get()).isEqualTo(1);
        assertThat(token.isCancelled()).isFalse();
    }
}
