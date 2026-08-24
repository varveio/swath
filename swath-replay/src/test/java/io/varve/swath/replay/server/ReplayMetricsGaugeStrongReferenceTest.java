/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Regression for Micrometer weakly referencing the method-reference object behind a gauge. */
final class ReplayMetricsGaugeStrongReferenceTest {

    private static final int MAX_GC_ATTEMPTS = 50;

    @Test
    void liveCacheGaugesSurviveGarbageCollection() throws InterruptedException {
        ReplayMetrics metrics = new ReplayMetrics();
        registerAndDropGaugeSources(metrics);

        // Do not merely hope System.gc() ran: wait until an unrelated weak canary proves it did.
        WeakReference<Object> canary = new WeakReference<>(new Object());
        for (int i = 0; i < MAX_GC_ATTEMPTS && canary.get() != null; i++) {
            System.gc();
            Thread.sleep(10L);
        }
        assertThat(canary.get())
                .as("a GC that clears weak references must run within %d attempts", MAX_GC_ATTEMPTS)
                .isNull();

        assertThat(metrics.registry().find("swath.replay.prefetch.windows.live").gauge().value())
                .isEqualTo(3.0);
        assertThat(metrics.registry().find("swath.replay.prefetch.anchors.live").gauge().value())
                .isEqualTo(7.0);
    }

    /** Leave the bound method references reachable only through {@link ReplayMetrics}. */
    private static void registerAndDropGaugeSources(ReplayMetrics metrics) {
        AtomicInteger windows = new AtomicInteger(3);
        AtomicInteger anchors = new AtomicInteger(7);
        metrics.registerPrefetchCacheGauges(windows::get, anchors::get);
    }
}
