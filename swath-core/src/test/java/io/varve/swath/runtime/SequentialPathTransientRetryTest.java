/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.error.ThrottleException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The sequential (non-work-stealing / {@code --checkpoint none}) listing path
 * calls the raw fetcher outside the engine's gauge-wrapped retrying fetcher, so with SDK
 * {@code maxAttempts=1} a single transient 503 / attempt-timeout / reset would kill the run. The
 * {@code observedSequentialFetcher} wraps the delegate in the shared bounded transient-retry
 * decorator — a transient tail must be retried, not fatal.
 */
final class SequentialPathTransientRetryTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static ListRunner.Spec jsonl(int maxKeys) {
        return new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, maxKeys, FilterChain.EMPTY, null, null);
    }

    @Test
    @Timeout(30)
    void sequentialRunSurvivesATransientTailAndListsEveryKey() throws Exception {
        List<byte[]> keys = List.of(b("a"), b("b"), b("c"));
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keys)
                .interceptor((req, idx, page) -> {
                    // The first 4 fetch attempts throw (< the cap of 8); without the retry decorator
                    // the run would die on the first.
                    if (calls.getAndIncrement() < 4) {
                        throw ThrottleException.attemptTimeout("injected transient");
                    }
                    return page;
                })
                .build();

        StringWriter out = new StringWriter();
        new ListRunner().run(RunContext.create(), fetcher, out, jsonl(10));

        String[] lines = out.toString().strip().split("\n");
        assertThat(lines).as("every key is listed despite the transient tail (the run did not fail)")
                .hasSize(3);
        assertThat(calls.get()).as("the fetch was retried past the transient failures").isGreaterThan(4);
    }
}
