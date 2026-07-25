/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the two new RunMetrics surfaces built for the probe-timeout-storm
 * decomposition —
 * <ul>
 *   <li>{@link RunMetrics#recordCallClassLatency}: the per-{@code call_class}/{@code phase} Timer,
 *       read back as {@link RunSummary#callClassLatency()};</li>
 *   <li>{@link RunMetrics#recordDemandGatedConcurrency}: the {@code OWNER_SPLIT.demand_gated}
 *       T-vs-Tmax snapshot, read back as {@link RunSummary#demandGate()} and the companion
 *       {@code swath.owner_split.demand_gated_t}/{@code _t_min} gauges;</li>
 *   <li>{@link RunMetrics#recordShedCallClassMix}: the {@code SHED.timeout_storm_worker_fed}/{@code
 *       _probe_fed} steal-reason pair.</li>
 * </ul>
 */
final class RunMetricsCallClassAndDemandGateTest {

    private static RunMetrics newMetrics() {
        return new RunMetrics(new SimpleMeterRegistry());
    }

    @Test
    void negativeNanosAreSkippedNotFabricatedAsAZeroSample() {
        RunMetrics metrics = newMetrics();
        metrics.recordCallClassLatency(RunMetrics.CALL_CLASS_WORKER_PAGE, RunMetrics.LATENCY_PHASE_TOTAL, -1L);

        RunSummary summary = metrics.summary(Duration.ofSeconds(1), "work_stealing", 0, 0);
        assertThat(summary.callClassLatency()).isEmpty();
    }

    @Test
    void callClassLatencyPercentilesReadBackWithCorrectCountAndBounds() {
        RunMetrics metrics = newMetrics();
        metrics.recordCallClassLatency(RunMetrics.CALL_CLASS_PIVOT_PROBE, RunMetrics.LATENCY_PHASE_TTFB,
                TimeUnit.MILLISECONDS.toNanos(10));
        metrics.recordCallClassLatency(RunMetrics.CALL_CLASS_PIVOT_PROBE, RunMetrics.LATENCY_PHASE_TTFB,
                TimeUnit.MILLISECONDS.toNanos(20));
        metrics.recordCallClassLatency(RunMetrics.CALL_CLASS_PIVOT_PROBE, RunMetrics.LATENCY_PHASE_TTFB,
                TimeUnit.MILLISECONDS.toNanos(30));

        RunSummary summary = metrics.summary(Duration.ofSeconds(1), "work_stealing", 0, 0);
        List<RunSummary.CallClassLatencySummary> rows = summary.callClassLatency();
        assertThat(rows).hasSize(1);
        RunSummary.CallClassLatencySummary row = rows.getFirst();
        assertThat(row.callClass()).isEqualTo(RunMetrics.CALL_CLASS_PIVOT_PROBE);
        assertThat(row.phase()).isEqualTo(RunMetrics.LATENCY_PHASE_TTFB);
        assertThat(row.count()).isEqualTo(3L);
        assertThat(row.p50Ms()).isNotNull();
        assertThat(row.p90Ms()).isNotNull();
        assertThat(row.p99Ms()).isNotNull();
        assertThat(row.maxMs()).isCloseTo(30.0, Offset.offset(1.0));
    }

    @Test
    void demandGateIsAbsentWhenNeverEngaged() {
        RunMetrics metrics = newMetrics();
        RunSummary summary = metrics.summary(Duration.ofSeconds(1), "work_stealing", 0, 0);
        assertThat(summary.demandGate()).isNull();

        Gauge lastT = metrics.registry().find("swath.owner_split.demand_gated_t").gauge();
        assertThat(lastT).isNotNull();
        assertThat(lastT.value()).isNaN();
    }

    @Test
    void demandGateTracksLastAndMinTAcrossMultipleEvents() {
        RunMetrics metrics = newMetrics();
        metrics.recordDemandGatedConcurrency(60, 64);
        metrics.recordDemandGatedConcurrency(12, 64);   // a shed-shrunken T at a later gate event
        metrics.recordDemandGatedConcurrency(30, 64);

        RunSummary summary = metrics.summary(Duration.ofSeconds(1), "work_stealing", 0, 0);
        RunSummary.DemandGateSummary dg = summary.demandGate();
        assertThat(dg).isNotNull();
        assertThat(dg.events()).isEqualTo(3L);
        assertThat(dg.lastT()).isEqualTo(30);
        assertThat(dg.minT()).isEqualTo(12);
        assertThat(dg.tMax()).isEqualTo(64);

        assertThat(metrics.registry().find("swath.owner_split.demand_gated_t").gauge().value()).isEqualTo(30.0);
        assertThat(metrics.registry().find("swath.owner_split.demand_gated_t_min").gauge().value()).isEqualTo(12.0);
    }

    @Test
    void shedCallClassMixIncrementsByObservedMagnitudeNotJustPlusOne() {
        RunMetrics metrics = newMetrics();
        metrics.recordShedCallClassMix(5, 2);
        metrics.recordShedCallClassMix(3, 0);

        assertThat(metrics.registry().find("swath.steal_reason")
                .tags("outcome", "SHED", "reason", "timeout_storm_worker_fed").counter().count())
                .isEqualTo(8.0);
        assertThat(metrics.registry().find("swath.steal_reason")
                .tags("outcome", "SHED", "reason", "timeout_storm_probe_fed").counter().count())
                .isEqualTo(2.0);
    }

    @Test
    void shedCallClassMixSkipsRegisteringAZeroCounter() {
        RunMetrics metrics = newMetrics();
        metrics.recordShedCallClassMix(0, 0);

        assertThat(metrics.registry().find("swath.steal_reason")
                .tags("outcome", "SHED", "reason", "timeout_storm_worker_fed").counter()).isNull();
        assertThat(metrics.registry().find("swath.steal_reason")
                .tags("outcome", "SHED", "reason", "timeout_storm_probe_fed").counter()).isNull();
    }
}
