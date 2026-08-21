/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Renders a whole {@link MeterRegistry} as one JSON document — the payload {@code serve}'s metrics
 * endpoint answers with.
 *
 * <p>It walks {@link MeterRegistry#getMeters()} rather than naming meters one by one, so a meter
 * added anywhere in {@code swath.replay.*} appears in the payload without this class being touched.
 * That matters for its consumer: a benchmark harness reads a scrape at the start of a measured
 * window and another at the end, and a meter missing from the payload is a signal the harness can
 * never recover, because the server it was scraping is gone by the time anyone notices.
 *
 * <p>Every timer field is milliseconds. {@code p50_ms}/{@code p99_ms} come from the timer's own
 * published percentiles and are {@code null} on a timer that publishes none — the same values
 * {@code bench} reports, read the same way, so a scrape and a bench report of the same run agree.
 *
 * <p>Hand-rolled rather than Jackson-backed, matching {@code TokenWalkBenchmark.Report#toJson()};
 * this module carries no JSON dependency and one small writer is cheaper than adding one.
 */
final class MetricsSnapshotJson {

    private MetricsSnapshotJson() {
    }

    /**
     * @param registry     the server's registry
     * @param servingMode  the resolved serving path, so a scrape is self-describing about which
     *                     store produced the latencies in it
     * @param uptimeMillis how long the server has been up, so two scrapes bound an interval without
     *                     the reader having to trust its own clock against the server's
     * @param sampledAtEpochMillis wall-clock stamp of this scrape
     */
    static String render(MeterRegistry registry, String servingMode, long uptimeMillis,
                         long sampledAtEpochMillis) {
        StringBuilder json = new StringBuilder(1024);
        json.append("{\"schema_version\":1");
        json.append(",\"serving_mode\":");
        appendString(json, servingMode);
        json.append(",\"uptime_ms\":").append(uptimeMillis);
        json.append(",\"sampled_at_epoch_ms\":").append(sampledAtEpochMillis);
        json.append(",\"meters\":[");
        List<Meter> meters = new ArrayList<>();
        registry.getMeters().forEach(meters::add);
        // Stable order: a reader diffing two scrapes should see the same meter in the same place,
        // and a registry's iteration order is not promised to be either.
        meters.sort(Comparator.comparing((Meter m) -> m.getId().getName())
                .thenComparing(m -> m.getId().getTags().toString()));
        boolean first = true;
        for (Meter meter : meters) {
            if (isPercentileGauge(meter)) {
                continue;
            }
            String rendered = renderMeter(meter);
            if (rendered == null) {
                continue;
            }
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append(rendered);
        }
        json.append("]}");
        return json.toString();
    }

    /**
     * Micrometer auto-registers a {@code <timer>.percentile} gauge per published percentile, in the
     * registry's base unit (seconds) rather than the milliseconds every other field here uses. The
     * timer already carries those same values as {@code p50_ms}/{@code p99_ms}, so emitting the
     * gauges too would only offer a reader a second copy in a different unit to misread.
     */
    private static boolean isPercentileGauge(Meter meter) {
        return meter instanceof Gauge && meter.getId().getName().endsWith(".percentile");
    }

    private static String renderMeter(Meter meter) {
        StringBuilder json = new StringBuilder(256);
        Meter.Id id = meter.getId();
        switch (meter) {
            case Timer timer -> {
                HistogramSnapshot snapshot = timer.takeSnapshot();
                openMeter(json, "timer", id);
                json.append(",\"count\":").append(timer.count());
                appendNumber(json, "sum_ms", timer.totalTime(TimeUnit.MILLISECONDS));
                appendNumber(json, "mean_ms", timer.mean(TimeUnit.MILLISECONDS));
                appendNumber(json, "max_ms", timer.max(TimeUnit.MILLISECONDS));
                appendPercentile(json, "p50_ms", snapshot, 0.50);
                appendPercentile(json, "p99_ms", snapshot, 0.99);
                json.append('}');
            }
            case Counter counter -> {
                openMeter(json, "counter", id);
                appendNumber(json, "count", counter.count());
                json.append('}');
            }
            case DistributionSummary summary -> {
                openMeter(json, "distribution", id);
                json.append(",\"count\":").append(summary.count());
                appendNumber(json, "total", summary.totalAmount());
                appendNumber(json, "mean", summary.mean());
                appendNumber(json, "max", summary.max());
                json.append('}');
            }
            case Gauge gauge -> {
                openMeter(json, "gauge", id);
                appendNumber(json, "value", gauge.value());
                json.append('}');
            }
            default -> {
                return null;
            }
        }
        return json.toString();
    }

    private static void openMeter(StringBuilder json, String type, Meter.Id id) {
        json.append("{\"name\":");
        appendString(json, id.getName());
        json.append(",\"type\":");
        appendString(json, type);
        json.append(",\"tags\":{");
        boolean first = true;
        for (Tag tag : id.getTags()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            appendString(json, tag.getKey());
            json.append(':');
            appendString(json, tag.getValue());
        }
        json.append('}');
    }

    private static void appendPercentile(StringBuilder json, String field, HistogramSnapshot snapshot,
                                         double percentile) {
        for (ValueAtPercentile value : snapshot.percentileValues()) {
            if (Math.abs(value.percentile() - percentile) < 1e-9) {
                appendNumber(json, field, value.value(TimeUnit.MILLISECONDS));
                return;
            }
        }
        json.append(",\"").append(field).append("\":null");
    }

    private static void appendNumber(StringBuilder json, String field, double value) {
        json.append(",\"").append(field).append("\":");
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            // A mean over zero samples is NaN, which is not JSON. Say "no value" rather than
            // emit a token no parser accepts.
            json.append("null");
        } else {
            json.append(String.format(Locale.ROOT, "%.6f", value));
        }
    }

    private static void appendString(StringBuilder json, String value) {
        json.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (c < 0x20) {
                        json.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        json.append(c);
                    }
                }
            }
        }
        json.append('"');
    }
}
