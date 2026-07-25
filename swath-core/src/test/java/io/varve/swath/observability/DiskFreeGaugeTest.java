/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** §3.3: the pull-based {@code swath.disk.free_bytes} gauge, guarding against disk exhaustion. */
final class DiskFreeGaugeTest {

    @Test
    void registerDiskFreeGaugeRegistersAPositiveUsableSpaceGauge(@TempDir Path tempDir) {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.registerDiskFreeGauge(tempDir);

        Gauge gauge = registry.find("swath.disk.free_bytes").gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isGreaterThan(0.0);
    }

    @Test
    void registerDiskFreeGaugeWithNullPathRegistersNothing() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.registerDiskFreeGauge(null);

        assertThat(registry.find("swath.disk.free_bytes").gauge()).isNull();
    }

    @Test
    void freshRunMetricsWithoutRegisteringAPathRegistersNothing() {
        // The many unit tests across the suite build a RunMetrics without ever knowing an output
        // path — registerDiskFreeGauge must simply never be called there, and never throw either.
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new RunMetrics(registry);

        assertThat(registry.find("swath.disk.free_bytes").gauge()).isNull();
    }

    @Test
    void secondRegistrationAfterAFirstSuccessfulOneIsANoOp(@TempDir Path tempDir, @TempDir Path otherDir) {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        metrics.registerDiskFreeGauge(tempDir);
        metrics.registerDiskFreeGauge(otherDir);   // idempotent: no-op, does not throw or duplicate

        assertThat(registry.find("swath.disk.free_bytes").gauges()).hasSize(1);
    }
}
