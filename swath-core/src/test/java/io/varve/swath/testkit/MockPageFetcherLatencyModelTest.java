/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * {@link MockPageFetcher}'s {@link LatencyModel} wiring: default-off, additive with the
 * pre-existing {@code latency}/{@code pageDelay} knobs, deterministic across independently-built
 * fetcher instances, and the {@code warm}/{@code cold} classification it derives from its own
 * call history. The big shape-regression acceptance experiment (dense-tail collapse reproduction)
 * is a different, adversarial test — this file only proves the wiring itself is correct.
 */
class MockPageFetcherLatencyModelTest {

    @Test
    void defaultFetcherInjectsZeroLatency() throws Exception {
        MockPageFetcher f = MockPageFetcher.builder().keys(Keyspaces.exactly(50)).build();

        ListPage page = f.fetchPage(PageRequest.objects(new byte[0], null, 1000));

        assertThat(page.latency()).isEqualTo(Duration.ZERO);
    }

    @Test
    void latencyModelIsDeterministicAcrossIndependentFetcherInstances() throws Exception {
        LatencyModel model = LatencyModels.uniformFast(99L, Duration.ofMillis(1), Duration.ofMillis(4));
        List<byte[]> keys = Keyspaces.exactly(20);

        MockPageFetcher a = MockPageFetcher.builder().keys(keys).latencyModel(model).build();
        MockPageFetcher b = MockPageFetcher.builder().keys(keys).latencyModel(model).build();

        byte[] startAfter = null;
        List<Duration> delaysA = new ArrayList<>();
        List<Duration> delaysB = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ListPage pa = a.fetchPage(PageRequest.objects(new byte[0], startAfter, 4));
            ListPage pb = b.fetchPage(PageRequest.objects(new byte[0], startAfter, 4));
            delaysA.add(pa.latency());
            delaysB.add(pb.latency());
            startAfter = pa.entries().getLast().key().raw();
            if (!pa.truncated()) {
                break;
            }
        }

        assertThat(delaysA).isEqualTo(delaysB);
    }

    @Test
    void latencyModelIsAdditiveWithThePreExistingPageDelayKnob() throws Exception {
        Duration fixed = Duration.ofMillis(3);
        LatencyModel model = LatencyModels.uniformFast(1L, Duration.ofMillis(2), Duration.ofMillis(2));
        MockPageFetcher f = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(5))
                .pageDelay(fixed)
                .latencyModel(model)
                .build();

        ListPage page = f.fetchPage(PageRequest.objects(new byte[0], null, 1000));

        assertThat(page.latency()).isEqualTo(fixed.plus(Duration.ofMillis(2)));
    }

    @Test
    void sequentialContinuationIsWarmAndAFreshJumpIsCold() throws Exception {
        List<LatencyModel.Context> seen = new CopyOnWriteArrayList<>();
        LatencyModel recording = ctx -> {
            seen.add(ctx);
            return Duration.ZERO;
        };
        MockPageFetcher f = MockPageFetcher.builder()
                .keys(Keyspaces.exactly(30))
                .latencyModel(recording)
                .build();

        // First page: startAfter == null -- always cold (no prior boundary to continue from).
        ListPage first = f.fetchPage(PageRequest.objects(new byte[0], null, 10));
        assertThat(first.truncated()).isTrue();
        byte[] boundary = first.entries().getLast().key().raw();

        // Immediate continuation of the page just served: warm.
        ListPage second = f.fetchPage(PageRequest.objects(new byte[0], boundary, 10));

        // A jump to a position never served as a page boundary (a pivot/structure-probe shape): cold.
        byte[] farAway = Keyspaces.exactly(30).get(2);
        ListPage probe = f.fetchPage(PageRequest.objects(new byte[0], farAway, 1));

        assertThat(seen).hasSize(3);
        assertThat(seen.get(0).warm()).as("root call has no prior boundary").isFalse();
        assertThat(seen.get(1).warm()).as("immediate continuation of the just-served page").isTrue();
        assertThat(seen.get(2).cold()).as("far jump never served as a boundary").isTrue();
        assertThat(seen.get(2).probe()).as("maxKeys<=1 is the probe convention").isTrue();
        assertThat(second.entries()).isNotEmpty();
        assertThat(probe.entries()).isNotEmpty();
    }
}
