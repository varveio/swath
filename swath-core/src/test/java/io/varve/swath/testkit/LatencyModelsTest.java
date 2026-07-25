/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.ListingException;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Self-test of the {@link LatencyModels} canned profiles in isolation, without going
 * through {@link MockPageFetcher}: determinism keyed off request coordinates (not call order or
 * call-class), the warm/cold split of {@code denseColdProbes}, the {@code stormBurst} call-index
 * window, and the {@code stormFaultWindow} companion fault.
 */
class LatencyModelsTest {

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static LatencyModel.Context ctx(PageRequest req, int callIndex, boolean warm) {
        boolean probe = req.maxKeys() <= 1;
        boolean delimiterVisible = req.delimiter() != null && req.delimiter().length > 0;
        return new LatencyModel.Context(req, callIndex, warm, probe, delimiterVisible);
    }

    @Test
    void uniformFastIsDeterministicByRequestCoordinatesNotCallIndexOrWarmth() {
        LatencyModel model = LatencyModels.uniformFast(42L, Duration.ofMillis(1), Duration.ofMillis(50));
        PageRequest req = PageRequest.objects(utf8("a/"), utf8("a/5"), 1000);

        Duration first = model.delayFor(ctx(req, 0, false));
        Duration replayedLaterIndexWarm = model.delayFor(ctx(req, 999, true));

        assertThat(replayedLaterIndexWarm).isEqualTo(first);
    }

    @Test
    void uniformFastVariesByRequestCoordinates() {
        LatencyModel model = LatencyModels.uniformFast(42L, Duration.ofMillis(1), Duration.ofMillis(500_000));
        PageRequest a = PageRequest.objects(utf8("a/"), utf8("a/1"), 1000);
        PageRequest b = PageRequest.objects(utf8("a/"), utf8("a/2"), 1000);

        assertThat(model.delayFor(ctx(a, 0, false))).isNotEqualTo(model.delayFor(ctx(b, 0, false)));
    }

    @Test
    void uniformFastStaysWithinBounds() {
        Duration min = Duration.ofMillis(3);
        Duration max = Duration.ofMillis(9);
        LatencyModel model = LatencyModels.uniformFast(7L, min, max);
        for (int i = 0; i < 200; i++) {
            PageRequest req = PageRequest.objects(utf8("a/"), utf8("a/" + i), 1000);
            Duration d = model.delayFor(ctx(req, i, false));
            assertThat(d).isBetween(min, max);
        }
    }

    @Test
    void denseColdProbesSplitsWarmAndColdRanges() {
        Duration warmMin = Duration.ofMillis(1);
        Duration warmMax = Duration.ofMillis(5);
        Duration threshold = Duration.ofMillis(10);
        Duration coldMin = Duration.ofMillis(20);
        Duration coldMax = Duration.ofMillis(40);
        LatencyModel model = LatencyModels.denseColdProbes(1L, warmMin, warmMax, threshold, coldMin, coldMax);

        for (int i = 0; i < 100; i++) {
            PageRequest req = PageRequest.objects(utf8("a/"), utf8("a/" + i), 1000);
            Duration warmDelay = model.delayFor(ctx(req, i, true));
            Duration coldDelay = model.delayFor(ctx(req, i, false));
            assertThat(warmDelay).isBetween(warmMin, warmMax);
            assertThat(coldDelay).isBetween(coldMin, coldMax);
            assertThat(coldDelay).isGreaterThan(threshold);
        }
    }

    @Test
    void denseColdProbesRejectsColdRangeAtOrBelowThreshold() {
        Duration warm = Duration.ofMillis(1);
        Duration threshold = Duration.ofMillis(10);
        assertThatThrownBy(() -> LatencyModels.denseColdProbes(1L, warm, warm, threshold, threshold, threshold))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("probeTimeoutThreshold");
    }

    @Test
    void stormBurstAppliesElevatedLatencyOnlyInsideTheCallIndexWindow() {
        LatencyModel base = LatencyModels.uniformFast(1L, Duration.ofMillis(1), Duration.ofMillis(1));
        LatencyModel storm = LatencyModels.stormBurst(base, 2L, 10, 5,
                Duration.ofMillis(100), Duration.ofMillis(100));
        PageRequest req = PageRequest.objects(utf8("a/"), null, 1000);

        assertThat(storm.delayFor(ctx(req, 9, false))).isEqualTo(Duration.ofMillis(1));
        for (int idx = 10; idx < 15; idx++) {
            assertThat(storm.delayFor(ctx(req, idx, false))).isEqualTo(Duration.ofMillis(100));
        }
        assertThat(storm.delayFor(ctx(req, 15, false))).isEqualTo(Duration.ofMillis(1));
    }

    @Test
    void stormFaultWindowThrowsOnlyInsideTheWindow() throws Exception {
        MockPageFetcher.PageInterceptor interceptor = LatencyModels.stormFaultWindow(2, 3,
                () -> new ListingException("storm"));
        ListPage page = new ListPage(List.of(), List.of(), false, null, null, null, 200, Duration.ZERO);
        PageRequest req = PageRequest.objects(utf8("a/"), null, 1000);

        assertThat(interceptor.intercept(req, 0, page)).isSameAs(page);
        assertThat(interceptor.intercept(req, 1, page)).isSameAs(page);
        assertThatThrownBy(() -> interceptor.intercept(req, 2, page)).isInstanceOf(ListingException.class);
        assertThatThrownBy(() -> interceptor.intercept(req, 4, page)).isInstanceOf(ListingException.class);
        assertThat(interceptor.intercept(req, 5, page)).isSameAs(page);
    }
}
