/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * {@link RunMetrics#recordApiCall()} tags {@code swath.api.calls} by the run's CURRENT {@code
 * strategy} value at the instant of the call (a mutable field, default {@code "unknown"}, only
 * overwritten when {@link RunMetrics#setStrategy} is called) — the only {@code RunMetrics} counter
 * tagged from mutable run state rather than a call-site parameter (every other tagged counter —
 * {@code swath.errors{type}}, {@code swath.steals{result}}, {@code swath.parquet.*}, {@code
 * swath.output.*} — takes its tag value as a method argument, so it can never fragment this way).
 * A call recorded before {@code setStrategy} runs lands on a {@code strategy="unknown"} series
 * distinct from every call recorded after — two separate Micrometer {@code Counter} instances /
 * OTLP data points for the one run. {@link RunMetrics#counterTotal} sums BOTH series (so {@code
 * cost.api_calls} stays correct), but a consumer reading a single raw OTLP series (e.g. a
 * dashboard panel or alert keyed on one label combination) sees only the calls recorded before the
 * strategy was set — plausibly single digits to a few dozen for seed probing (bounded, see {@code
 * SeedStep}'s own probe cap) — against a run whose real total can be millions to billions. {@code
 * ListCommand.runWithCheckpoint} calls {@code ctx.metrics().setStrategy(STRATEGY_WORK_STEALING)}
 * before {@code seedFreshRun}, so every API call for the run — probe and engine alike — lands on
 * the ONE canonical series.
 *
 * <p>This test exercises the underlying {@link RunMetrics}/OTLP-export mechanism directly,
 * reproducing the ordering where API calls are recorded before {@code setStrategy} to prove the
 * fragmentation mechanism, and the ordering where {@code setStrategy} runs before any call to
 * guard the invariant {@code ListCommand} relies on. The real {@code
 * ExportMetricsServiceRequest}/{@code NumberDataPoint} wire types (io.opentelemetry.proto, a
 * transitive runtime dep of {@code micrometer-registry-otlp}) let the assertions read the actual
 * exported numeric value, not just series presence.
 */
final class ApiCallsOtlpStrategyTagOrderingTest {

    private static OtlpMeterRegistry captor(AtomicReference<List<byte[]>> captured) {
        captured.set(new ArrayList<>());
        OtlpConfig config = MeterRegistries.buildOtlpConfig(
                "http://localhost:4318/v1/metrics", Duration.ofSeconds(5), Map.of());
        return OtlpMeterRegistry.builder(config)
                .metricsSender(request -> captured.get().add(request.getMetricsData()))
                .build();
    }

    private static List<NumberDataPoint> apiCallsDataPoints(List<byte[]> payloads) throws Exception {
        List<NumberDataPoint> points = new ArrayList<>();
        for (byte[] payload : payloads) {
            ExportMetricsServiceRequest req = ExportMetricsServiceRequest.parseFrom(payload);
            for (var rm : req.getResourceMetricsList()) {
                for (var sm : rm.getScopeMetricsList()) {
                    for (Metric m : sm.getMetricsList()) {
                        if (m.getName().equals("swath.api.calls")) {
                            points.addAll(m.getSum().getDataPointsList());
                        }
                    }
                }
            }
        }
        return points;
    }

    /**
     * Recording API calls before {@code setStrategy} runs fragments {@code swath.api.calls} into
     * two OTLP series. {@code cost.api_calls} (the sum) stays correct, but neither individual
     * series alone equals it — the {@code strategy="unknown"} series is what a single-series
     * reader would see instead of the run's true total.
     */
    @Test
    void recordingApiCallsBeforeSetStrategyFragmentsTheOtlpSeries() throws Exception {
        AtomicReference<List<byte[]>> captured = new AtomicReference<>();
        OtlpMeterRegistry registry = captor(captured);
        RunMetrics metrics = new RunMetrics(registry);

        for (int i = 0; i < 3; i++) {
            metrics.recordApiCall();   // seed probes, recorded before setStrategy — strategy still the "unknown" default
        }
        metrics.setStrategy("WORK_STEALING");   // set only once the engine is constructed
        for (int i = 0; i < 7; i++) {
            metrics.recordApiCall();   // the engine's bulk calls
        }

        long costApiCalls = metrics.summary(Duration.ofSeconds(1), "WORK_STEALING", 1L, 0L).apiCalls();
        registry.close();

        List<NumberDataPoint> points = apiCallsDataPoints(captured.get());
        assertThat(costApiCalls).as("summary.json cost.api_calls stays correct (sums every series)").isEqualTo(10);
        assertThat(points).as("the bug: two disjoint OTLP series for the one run").hasSize(2);
        for (NumberDataPoint dp : points) {
            assertThat(dp.getAsDouble())
                    .as("neither individual series alone equals the run's true total — a reader "
                            + "of just one series undercounts")
                    .isNotEqualTo((double) costApiCalls);
        }
    }

    /**
     * The invariant: calling {@code setStrategy} before the FIRST {@code recordApiCall()} (what
     * {@code ListCommand.runWithCheckpoint} does, ahead of {@code seedFreshRun}) keeps every API
     * call for the run on ONE canonical OTLP series, whose cumulative exported value equals
     * {@code cost.api_calls} exactly.
     */
    @Test
    void settingStrategyBeforeAnyApiCallKeepsOneOtlpSeriesMatchingCost() throws Exception {
        AtomicReference<List<byte[]>> captured = new AtomicReference<>();
        OtlpMeterRegistry registry = captor(captured);
        RunMetrics metrics = new RunMetrics(registry);

        metrics.setStrategy("WORK_STEALING");   // set before ANY api call, incl. seeding
        for (int i = 0; i < 10; i++) {
            metrics.recordApiCall();
        }

        long costApiCalls = metrics.summary(Duration.ofSeconds(1), "WORK_STEALING", 1L, 0L).apiCalls();
        registry.close();

        List<NumberDataPoint> points = apiCallsDataPoints(captured.get());
        assertThat(points).as("exactly one series for the whole run").hasSize(1);
        assertThat(points.get(0).getAsDouble()).isEqualTo((double) costApiCalls).isEqualTo(10.0);
    }
}
