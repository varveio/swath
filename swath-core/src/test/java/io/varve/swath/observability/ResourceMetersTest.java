/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

/** The pull-based {@code swath.process.*} resource meters. */
final class ResourceMetersTest {

    @Test
    void rssAndHeapGaugesAreRegisteredWithCurrentAndPeakKindTags() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new RunMetrics(registry);

        assertThat(registry.find("swath.process.memory.rss.bytes").tags("kind", "current").gauge()).isNotNull();
        assertThat(registry.find("swath.process.memory.rss.bytes").tags("kind", "peak").gauge()).isNotNull();
        assertThat(registry.find("swath.process.memory.heap.bytes").tags("kind", "current").gauge()).isNotNull();
        assertThat(registry.find("swath.process.memory.heap.bytes").tags("kind", "peak").gauge()).isNotNull();
    }

    @Test
    void cpuTimeFunctionCounterIsRegisteredWhenCpuTimeIsAvailable() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new RunMetrics(registry);

        // On this Linux box the com.sun OS bean is expected to be present; skip cleanly if not.
        assumeTrue(ResourceMetrics.processCpuTimeNanos() >= 0, "CPU-time probe not available on this platform");
        assertThat(registry.find("swath.process.cpu.time").functionCounter()).isNotNull();
        assertThat(registry.find("swath.process.cpu.time").functionCounter().count()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void rssAndHeapGaugeValuesArePositiveFiniteOnThisLinuxBox() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new RunMetrics(registry);

        // On this Linux box /proc/self/status is expected to be present; skip cleanly if not.
        assumeTrue(ResourceMetrics.currentRssBytes() >= 0, "RSS probe not available on this platform");
        Gauge rssCurrent = registry.find("swath.process.memory.rss.bytes").tags("kind", "current").gauge();
        Gauge heapCurrent = registry.find("swath.process.memory.heap.bytes").tags("kind", "current").gauge();

        assertThat(rssCurrent.value()).isFinite().isPositive();
        assertThat(heapCurrent.value()).isFinite().isPositive();
    }

    @Test
    void cpuTimeCounterIsMonotonicAcrossTwoReads() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new RunMetrics(registry);
        assumeTrue(ResourceMetrics.processCpuTimeNanos() >= 0, "CPU-time probe not available on this platform");
        double first = registry.find("swath.process.cpu.time").functionCounter().count();
        // Burn a little CPU so the second read is >= the first (monotonic, never decreases).
        long sink = 0;
        for (int i = 0; i < 2_000_000; i++) {
            sink += i;
        }
        assertThat(sink).isGreaterThanOrEqualTo(0L);
        double second = registry.find("swath.process.cpu.time").functionCounter().count();
        assertThat(second).isGreaterThanOrEqualTo(first);
    }

    @Test
    void nanIfUnavailableMapsSentinelToNanAndPassesThroughRealValues() {
        assertThat(RunMetrics.nanIfUnavailable(-1L)).isNaN();
        assertThat(RunMetrics.nanIfUnavailable(0L)).isEqualTo(0.0);
        assertThat(RunMetrics.nanIfUnavailable(12345L)).isEqualTo(12345.0);
    }

    @Test
    void currentRssAndHeapBytesReturnPositiveValuesAndDoNotThrow() {
        assertThat(ResourceMetrics.currentHeapBytes()).isPositive();

        // RSS is read from /proc/self/status, which platforms other than Linux do not publish;
        // the sentinel is the documented "unavailable" answer, not a failure.
        assumeTrue(ResourceMetrics.currentRssBytes() >= 0, "RSS probe not available on this platform");
        assertThat(ResourceMetrics.currentRssBytes()).isPositive();
    }
}
