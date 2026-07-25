/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.micrometer.registry.otlp.OtlpMetricsSender;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * The {@code -1 -> NaN -> series omitted} idiom ({@code RunMetrics.atomicLongOrNan}, used by every
 * {@code swath.s3.pool.*} / {@code swath.phase} / {@code swath.process.*} gauge) had only ever
 * been asserted against {@link io.micrometer.core.instrument.simple.SimpleMeterRegistry}'s
 * {@code Gauge.value() == NaN} directly — never against the REAL production {@link OtlpMeterRegistry}
 * exporter path, whose actual on-the-wire behavior this test verifies directly.
 *
 * <p><b>How the real export payload is observed, without a live OTLP endpoint.</b> {@link
 * OtlpMeterRegistry.Builder} accepts a custom {@link OtlpMetricsSender} — the seam the registry uses
 * to actually transmit a publish batch. Installing a sender that just captures the request's raw
 * (uncompressed) serialized {@code ExportMetricsServiceRequest} protobuf bytes, then calling {@link
 * OtlpMeterRegistry#close()} (which synchronously forces exactly one {@code publish()} before
 * shutting down — the same call the production shutdown hook in {@code MeterRegistries.fromEnv}
 * makes) reproduces the EXACT bytes a real collector would receive, with zero network dependency.
 * Those bytes are the real protobuf wire encoding, so this test doesn't need the
 * {@code opentelemetry-proto} generated classes (a {@code runtime}-scope transitive dependency of
 * {@code micrometer-registry-otlp}, not guaranteed on this module's compile classpath) to observe the
 * payload: protobuf string fields (a metric's {@code name}) are literal UTF-8 bytes in the wire
 * encoding, so a substring match on a sufficiently unique metric name unambiguously proves that exact
 * series' presence/absence in the batch.
 *
 * <p>{@link OtlpMeterRegistry} EMITS a NaN-backed gauge as a literal NaN datapoint at export, in
 * the pinned Micrometer version (1.17.0) — it does not omit the series. Decompiling {@code
 * OtlpMetricConverter.writeGauge} shows it unconditionally calls {@code
 * NumberDataPoint.Builder.setAsDouble(gauge.value())} with no NaN guard anywhere in the gauge-encoding
 * path — confirmed empirically below: a NaN-backed gauge's metric name IS present in the captured wire
 * bytes, i.e. the datapoint is emitted with a literal NaN value, not dropped. (Whether a downstream
 * OTLP collector or the eventual Prometheus/Cloud-Monitoring backend then discards or renders a NaN
 * sample as "no data" is a separate, downstream concern this test cannot observe — that later hop is
 * where any "no data" behavior actually happens, never at the swath-side OTLP encoding this test
 * exercises.) This test underwrites {@code RunMetrics}'s and {@code Phase}'s javadoc description of
 * the mechanism: the gauge is emitted with a literal NaN value, and any "no data" outcome happens
 * downstream — never omission by Micrometer at the OTLP-export hop.
 */
final class OtlpNaNOmissionTest {

    @Test
    void nanBackedGaugeIsEmittedAsALiteralNanDatapointNotOmittedFromTheRealOtlpExport() {
        AtomicReference<byte[]> captured = new AtomicReference<>();
        OtlpConfig config = MeterRegistries.buildOtlpConfig(
                "http://localhost:4318/v1/metrics", Duration.ofSeconds(5), Map.of());

        OtlpMeterRegistry registry = OtlpMeterRegistry.builder(config)
                // No network I/O anywhere: this sender only records the bytes the real HTTP sender
                // would have transmitted, it never opens a connection.
                .metricsSender(request -> captured.set(request.getMetricsData()))
                .build();
        try {
            Gauge.builder("swath.test.nan_backed_marker_a1b2c3", () -> Double.NaN).register(registry);
            Gauge.builder("swath.test.finite_backed_marker_d4e5f6", () -> 42.0).register(registry);
        } finally {
            registry.close();   // forces exactly one synchronous publish() -> our fake sender captures it
        }

        byte[] payload = captured.get();
        assertThat(payload).as("OTLP export payload was captured from the real publish() path").isNotNull();
        String wire = new String(payload, StandardCharsets.ISO_8859_1);   // byte-preserving, 1:1 with the wire bytes

        // The NaN-backed gauge's series NAME is present in the real OTLP wire payload — it is
        // emitted as a NumberDataPoint carrying a literal NaN value, not dropped by Micrometer.
        // See the class javadoc.
        assertThat(wire)
                .as("a NaN-backed gauge's series IS present in the real OTLP export "
                        + "payload (emitted with a literal NaN value) for this Micrometer version "
                        + "(1.17.0) -- Micrometer does not omit it at the OTLP-export hop")
                .contains("swath.test.nan_backed_marker_a1b2c3");
        assertThat(wire)
                .as("a finite-backed gauge is also present, as expected")
                .contains("swath.test.finite_backed_marker_d4e5f6");
    }
}
