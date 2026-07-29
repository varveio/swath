/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Metric;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Characterization: the FROZEN OTLP-mode meter identity of {@link RunMetrics}, plus the
 * swath-owned properties of what that registry actually puts on the wire.
 *
 * <p><b>Why this exists separately from {@link RunMetricsSimpleRegistrySeriesIdentityTest}.</b>
 * {@code MeterRegistries} builds a {@code SimpleMeterRegistry} by default and an {@link
 * OtlpMeterRegistry} when {@code SWATH_OTLP_ENDPOINT}/{@code --metrics-export=otlp} is set, and the
 * two do NOT register the same meters: {@code SimpleMeterRegistry.newTimer} registers {@code
 * HistogramGauges} — 54 extra {@code *.percentile{phi}} GAUGE meters for the 18
 * percentile-publishing timers — that {@code OtlpMeterRegistry.newTimer} never creates, because
 * OTLP encodes those percentiles as {@code SUMMARY} quantiles on the wire instead. So the same run
 * is {@value RunMetricsSimpleRegistrySeriesIdentityTest#EXPECTED_SIMPLE_METER_COUNT} Simple meters
 * and {@value #EXPECTED_OTLP_METER_COUNT} OTLP meters.
 *
 * <h2>What is pinned, and what is deliberately NOT</h2>
 * <p><b>Pinned exhaustively: the OTLP METER SET</b> ({@link #EXPECTED_OTLP_METER_IDS}) — name, type
 * and full tag set. That set is decided by {@code RunMetrics}' own registrations, so it is
 * swath-owned, environment-independent, and precisely what an extraction can break.
 *
 * <p><b>Deliberately NOT pinned: a full literal snapshot of the exported protobuf payload.</b> An
 * earlier draft froze all 134 exported data points. That is a worse guard than it looks, because
 * most of what it pins is Micrometer exporter behaviour rather than swath behaviour:
 * <ul>
 *   <li>The bucket layout and datapoint encoding a histogram produces are Micrometer exporter
 *       behaviour, not swath's, and a version bump can legitimately move them. (The FLAVOUR itself
 *       is no longer environment-dependent: {@code MeterRegistries.buildOtlpConfig} now pins {@code
 *       OtlpConfig.histogramFlavor()} to explicit-bucket {@code HISTOGRAM}, so it no longer
 *       follows the ambient {@code OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION} env
 *       var — see {@code MeterRegistriesTest}. This unit drives that same config, so it sees the
 *       pinned flavour.)</li>
 *   <li>The synthesized {@code <name>.max} companion gauges and the NaN encoding are
 *       Micrometer-version behaviour that a version bump legitimately moves.</li>
 * </ul>
 * What IS asserted about the wire below is the swath-owned subset: percentile timers arrive as
 * {@code SUMMARY} — asserted per attribute set, one series per call_class/phase — every distribution
 * gets a {@code .max} companion carrying the SAME attribute set, {@code swath.api.calls} is a single
 * {@code WORK_STEALING} series, no {@code *.percentile} gauge reaches the wire, and the
 * always-registered memory gauges are present.
 *
 * <p><b>Known coverage gap (shared with the Simple sibling).</b> In OTLP mode {@code ListRunner}
 * (~:1334) passes {@code ctx.metrics().registry()} — this {@link OtlpMeterRegistry} — to {@code
 * JsonRunSummaryWriter}, so the JSON run-summary's generic {@code meters[]} block is rendered from
 * the OTLP meter set, not the Simple one. The meter set it reads from IS pinned here; how {@code
 * JsonRunSummaryWriter} walks an OTLP registry is covered by neither guard.
 *
 * <p>FROZEN in the same sense as its sibling: {@link #EXPECTED_OTLP_METER_IDS} is updated
 * deliberately, in the same commit as an intended metric change, and shipped in the Varve-facing
 * change note — never silently re-captured to turn a red test green.
 */
final class RunMetricsOtlpSeriesIdentityTest {

    /** Micrometer-side meter count under an OTLP registry — no {@code *.percentile} gauges. */
    static final int EXPECTED_OTLP_METER_COUNT = 125;

    /**
     * {@code swath.process.cpu.time} is the ONLY platform-conditional meter: it is a {@code
     * FunctionCounter}, which cannot honestly report {@code NaN}, so {@link RunMetrics} registers it
     * only when the CPU bean is available. Every other platform-sensitive meter — notably {@code
     * swath.process.memory.*} — is ALWAYS registered and is exported as a literal {@code NaN}
     * datapoint when the reading is unavailable rather than being dropped (proven against the real
     * exporter by {@code OtlpNaNOmissionTest}). Those must therefore NOT be filtered: conditioning
     * on the platform would both fail where the meters do exist and mask their real removal.
     */
    private static final String CPU_TIME_METER_ID = "COUNTER|swath.process.cpu.time|{}";

    /** Every meter registered on an OTLP registry, as {@code TYPE|name|{sorted tags}}. FROZEN. */
    private static final List<String> EXPECTED_OTLP_METER_IDS = List.of(
            "COUNTER|swath.aimd.growth_freeze|{}",
            "COUNTER|swath.aimd.latency_freeze|{}",
            "COUNTER|swath.aimd.target_reductions|{}",
            "COUNTER|swath.aimd.timeout_shed|{}",
            "COUNTER|swath.aimd.votes|{}",
            "COUNTER|swath.api.calls|{strategy=WORK_STEALING}",
            "COUNTER|swath.bytes.estimated|{}",
            "COUNTER|swath.entries.emitted|{}",
            "COUNTER|swath.errors|{type=accessdenied}",
            "COUNTER|swath.errors|{type=nosuchbucket}",
            "COUNTER|swath.errors|{type=throttle}",
            "COUNTER|swath.idle_backoff.resets|{}",
            "COUNTER|swath.idle_backoff.slot_denied|{}",
            "COUNTER|swath.output.broken_pipe|{}",
            "COUNTER|swath.output.bytes|{format=jsonl}",
            "COUNTER|swath.output.bytes|{format=parquet}",
            "COUNTER|swath.output.files|{format=jsonl,outcome=written}",
            "COUNTER|swath.output.files|{format=parquet,outcome=written}",
            "COUNTER|swath.page.raw_count|{}",
            "COUNTER|swath.page.raw_keys|{}",
            "COUNTER|swath.page.short_truncated|{}",
            "COUNTER|swath.parquet.parts|{outcome=discarded}",
            "COUNTER|swath.parquet.parts|{outcome=finalize_failed}",
            "COUNTER|swath.parquet.parts|{outcome=finalized}",
            "COUNTER|swath.parquet.rotation|{trigger=rows}",
            "COUNTER|swath.parquet.rotation|{trigger=size}",
            "COUNTER|swath.parquet.rotation|{trigger=time}",
            "COUNTER|swath.probe.empty_upper_bisections|{}",
            "COUNTER|swath.probe.fetches|{}",
            "COUNTER|swath.probe.structure_fetches|{}",
            CPU_TIME_METER_ID,
            "COUNTER|swath.progress.units|{}",
            "COUNTER|swath.s3.pool.connection_aborted|{}",
            "COUNTER|swath.s3.pool.handshakes|{}",
            "COUNTER|swath.s3.socket_closure_recovered|{}",
            "COUNTER|swath.sort.entries|{}",
            "COUNTER|swath.sort.merge.passes|{}",
            "COUNTER|swath.sort.segment.bytes|{}",
            "COUNTER|swath.sort.segments.written|{}",
            "COUNTER|swath.split.guard_aborts|{}",
            "COUNTER|swath.split.unsplittable_victims|{}",
            "COUNTER|swath.steal_reason|{outcome=CHILD_CREATED,reason=split_committed}",
            "COUNTER|swath.steal_reason|{outcome=CHILD_MASS,reason=empty}",
            "COUNTER|swath.steal_reason|{outcome=CHILD_MASS,reason=large}",
            "COUNTER|swath.steal_reason|{outcome=CHILD_MASS,reason=small}",
            "COUNTER|swath.steal_reason|{outcome=CHILD_MASS,reason=tiny}",
            "COUNTER|swath.steal_reason|{outcome=NO_VICTIM,reason=no_splittable_victim}",
            "COUNTER|swath.steal_reason|{outcome=OWNER_SPLIT,reason=self_published}",
            "COUNTER|swath.steal_reason|{outcome=PIVOT_BYTE,reason=dead_zone}",
            "COUNTER|swath.steal_reason|{outcome=PIVOT_BYTE,reason=hex_alpha}",
            "COUNTER|swath.steal_reason|{outcome=PIVOT_BYTE,reason=hex_digit}",
            "COUNTER|swath.steal_reason|{outcome=PIVOT_BYTE,reason=other}",
            "COUNTER|swath.steal_reason|{outcome=RESUME,reason=args_hash_refused}",
            "COUNTER|swath.steal_reason|{outcome=RESUME,reason=durable_cursor_lag}",
            "COUNTER|swath.steal_reason|{outcome=RESUME,reason=nodes_reopened}",
            "COUNTER|swath.steal_reason|{outcome=RETRY,reason=split_aborted}",
            "COUNTER|swath.steal_reason|{outcome=SEED,reason=radix_bands}",
            "COUNTER|swath.steal_reason|{outcome=SHED,reason=timeout_storm_probe_fed}",
            "COUNTER|swath.steal_reason|{outcome=SHED,reason=timeout_storm_worker_fed}",
            "COUNTER|swath.steal_reason|{outcome=STEAL,reason=attempted}",
            "COUNTER|swath.steal_reason|{outcome=UNSPLITTABLE,reason=no_pivot}",
            "COUNTER|swath.steals|{result=CHILD_CREATED}",
            "COUNTER|swath.steals|{result=NO_VICTIM}",
            "COUNTER|swath.throttle.events|{type=attempt_timeout}",
            "COUNTER|swath.throttle.events|{type=network}",
            "COUNTER|swath.throttle.events|{type=server5xx}",
            "COUNTER|swath.throttle.events|{type=slowdown}",
            "DISTRIBUTION_SUMMARY|swath.checkpoint.commit_batch_size|{}",
            "DISTRIBUTION_SUMMARY|swath.sort.page_runs_per_buffer|{}",
            "GAUGE|swath.aimd.latency_baseline_ms|{}",
            "GAUGE|swath.aimd.target_low_water|{}",
            "GAUGE|swath.checkpoint.queue.depth|{}",
            "GAUGE|swath.disk.free_bytes|{}",
            "GAUGE|swath.idle_backoff.level|{}",
            "GAUGE|swath.in_flight.avg|{}",
            "GAUGE|swath.owner_split.demand_gated_t_min|{}",
            "GAUGE|swath.owner_split.demand_gated_t|{}",
            "GAUGE|swath.phase|{}",
            "GAUGE|swath.process.memory.heap.bytes|{kind=current}",
            "GAUGE|swath.process.memory.heap.bytes|{kind=peak}",
            "GAUGE|swath.process.memory.rss.bytes|{kind=current}",
            "GAUGE|swath.process.memory.rss.bytes|{kind=peak}",
            "GAUGE|swath.run.throughput|{}",
            "GAUGE|swath.s3.pool.idle_available|{}",
            "GAUGE|swath.s3.pool.leased|{}",
            "GAUGE|swath.s3.pool.max|{}",
            "GAUGE|swath.s3.pool.pending_acquisition|{}",
            "GAUGE|swath.sort.handoff.queue.depth.peak|{}",
            "GAUGE|swath.sort.off_thread.buffers.peak|{}",
            "GAUGE|swath.sort.staging.bytes.peak|{}",
            "GAUGE|swath.tail_occupancy.avg_in_flight|{pct=10}",
            "GAUGE|swath.tail_occupancy.avg_in_flight|{pct=5}",
            "GAUGE|swath.tail_occupancy.wall_share|{pct=10}",
            "GAUGE|swath.tail_occupancy.wall_share|{pct=5}",
            "GAUGE|swath.workers.active|{}",
            "TIMER|swath.api.latency|{op=listObjectsV2}",
            "TIMER|swath.checkpoint.commit.latency|{}",
            "TIMER|swath.checkpoint.commit.wait|{}",
            "TIMER|swath.checkpoint.queue.wait|{}",
            "TIMER|swath.emit.latency|{}",
            "TIMER|swath.fetch.latency.phase|{call_class=pivot_probe,phase=connect_acquire}",
            "TIMER|swath.fetch.latency.phase|{call_class=pivot_probe,phase=response_parse}",
            "TIMER|swath.fetch.latency.phase|{call_class=pivot_probe,phase=sdk_unmarshal}",
            "TIMER|swath.fetch.latency.phase|{call_class=pivot_probe,phase=total}",
            "TIMER|swath.fetch.latency.phase|{call_class=pivot_probe,phase=ttfb}",
            "TIMER|swath.fetch.latency.phase|{call_class=structure_probe,phase=connect_acquire}",
            "TIMER|swath.fetch.latency.phase|{call_class=structure_probe,phase=response_parse}",
            "TIMER|swath.fetch.latency.phase|{call_class=structure_probe,phase=sdk_unmarshal}",
            "TIMER|swath.fetch.latency.phase|{call_class=structure_probe,phase=total}",
            "TIMER|swath.fetch.latency.phase|{call_class=structure_probe,phase=ttfb}",
            "TIMER|swath.fetch.latency.phase|{call_class=worker_page,phase=connect_acquire}",
            "TIMER|swath.fetch.latency.phase|{call_class=worker_page,phase=response_parse}",
            "TIMER|swath.fetch.latency.phase|{call_class=worker_page,phase=sdk_unmarshal}",
            "TIMER|swath.fetch.latency.phase|{call_class=worker_page,phase=total}",
            "TIMER|swath.fetch.latency.phase|{call_class=worker_page,phase=ttfb}",
            "TIMER|swath.idle_backoff.park_time|{}",
            "TIMER|swath.parquet.finalize.latency|{}",
            "TIMER|swath.parquet.write.latency|{}",
            "TIMER|swath.queue.wait|{}",
            "TIMER|swath.rate_limit.api_wait|{}",
            "TIMER|swath.rate_limit.wait|{}",
            "TIMER|swath.run.duration|{}",
            "TIMER|swath.sort.backpressure.wait|{}",
            "TIMER|swath.sort.merge.latency|{}",
            "TIMER|swath.sort.merge.range.latency|{}");

    /**
     * The 22 percentile-timer {@code SUMMARY} series OTLP must export, one per attribute set: the
     * single {@code swath.api.latency} op series, all fifteen {@code swath.fetch.latency.phase}
     * call_class/phase distributions, and the six client-service-cost spans. Asserting the EXACT
     * set (not just count + allowed names) catches one attribute set being duplicated while another
     * silently disappears.
     */
    private static final List<String> EXPECTED_SUMMARY_SERIES = List.of(
            "SUMMARY|swath.api.latency|{op=listObjectsV2}",
            "SUMMARY|swath.checkpoint.commit.latency|{}",
            "SUMMARY|swath.checkpoint.commit.wait|{}",
            "SUMMARY|swath.checkpoint.queue.wait|{}",
            "SUMMARY|swath.emit.latency|{}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=pivot_probe,phase=connect_acquire}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=pivot_probe,phase=response_parse}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=pivot_probe,phase=sdk_unmarshal}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=pivot_probe,phase=total}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=pivot_probe,phase=ttfb}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=structure_probe,phase=connect_acquire}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=structure_probe,phase=response_parse}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=structure_probe,phase=sdk_unmarshal}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=structure_probe,phase=total}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=structure_probe,phase=ttfb}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=worker_page,phase=connect_acquire}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=worker_page,phase=response_parse}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=worker_page,phase=sdk_unmarshal}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=worker_page,phase=total}",
            "SUMMARY|swath.fetch.latency.phase|{call_class=worker_page,phase=ttfb}",
            "SUMMARY|swath.parquet.write.latency|{}",
            "SUMMARY|swath.queue.wait|{}");

    @Test
    void otlpMeterSetIdentityIsFrozen(@TempDir Path scratchDir) {
        Capture capture = new Capture();
        try {
            RunMetricsCharacterizationWorkload.drive(capture.registry(), scratchDir);

            List<String> expected = new ArrayList<>(EXPECTED_OTLP_METER_IDS);
            assertThat(expected).hasSize(EXPECTED_OTLP_METER_COUNT);
            if (ResourceMetrics.processCpuTimeNanos() < 0) {
                expected.remove(CPU_TIME_METER_ID);
            }
            assertThat(meterIds(capture.registry())).containsExactlyElementsOf(expected);
            assertThat(meterIds(capture.registry())).noneMatch(id -> id.contains(".percentile|"));
        } finally {
            capture.registry().close();
        }
    }

    /**
     * The swath-owned properties of the exported payload, under swath's own {@link
     * MeterRegistries.SwathOtlpConfig} — i.e. the pinned explicit-bucket flavour ({@code Capture}
     * drives that same config). This asserts what swath OWNS on the wire; it does not, and no longer
     * claims to, exercise both histogram flavours. See the class javadoc for why the full payload is
     * deliberately not frozen.
     */
    @Test
    void exportedWireCarriesTheSwathOwnedProperties(@TempDir Path scratchDir)
            throws Exception {
        Capture capture = new Capture();
        RunMetricsCharacterizationWorkload.drive(capture.registry(), scratchDir);
        capture.finalPublish();   // the ONLY captured publish — see Capture for the structural guarantee

        assertThat(capture.payloads())
                .as("exactly one deterministic export payload (only the explicit final publish is captured)")
                .hasSize(1);
        List<String> wire = capture.wireSeries();

        // 1. Percentile timers arrive as SUMMARY — this is how OTLP carries publishPercentiles, in
        //    place of the Simple registry's *.percentile gauges. Asserted PER ATTRIBUTE SET (the
        //    exact 10 series), not merely count + allowed names: that way one call_class/phase
        //    attribute set cannot be duplicated while another disappears with the guard still green.
        List<String> summaries = wire.stream().filter(id -> id.startsWith("SUMMARY|")).toList();
        assertThat(summaries).containsExactlyInAnyOrderElementsOf(EXPECTED_SUMMARY_SERIES);

        // 2. No *.percentile gauge ever reaches the wire.
        assertThat(wire).noneMatch(id -> nameOf(id).endsWith(".percentile"));

        // 3. Every exported distribution gets a synthesized `<name>.max` companion gauge carrying
        //    the SAME attribute set — asserted per attribute set, so one `.max` datapoint cannot
        //    stand in for a whole tagged family (e.g. the nine call_class/phase fetch-latency
        //    distributions). Companions are derived by transforming each distribution's own
        //    `TYPE|name|{attrs}` id into `GAUGE|name.max|{attrs}`, so the genuine
        //    `swath.s3.pool.max` gauge is never mistaken for a synthesized one.
        List<String> distributionSeries = wire.stream()
                .filter(id -> id.startsWith("HISTOGRAM|") || id.startsWith("SUMMARY|")
                        || id.startsWith("EXP_HISTOGRAM|"))
                .toList();
        assertThat(distributionSeries).isNotEmpty();
        List<String> expectedMaxCompanions = distributionSeries.stream()
                .map(id -> {
                    String[] parts = id.split("\\|", 3);
                    return "GAUGE|" + parts[1] + ".max|" + parts[2];
                })
                .toList();
        assertThat(wire).containsAll(expectedMaxCompanions);
        assertThat(wire).contains("GAUGE|swath.s3.pool.max|{}");   // a real gauge, not a companion

        // 4. The api-calls series is single and correctly tagged on the wire.
        assertThat(wire.stream().filter(id -> nameOf(id).equals("swath.api.calls")).toList())
                .containsExactly("SUM|swath.api.calls|{strategy=WORK_STEALING}");

        // 5. The always-registered memory gauges reach the wire unconditionally — Micrometer emits
        //    a literal NaN datapoint rather than dropping the series when a reading is unavailable
        //    (OtlpNaNOmissionTest), so their presence is NOT platform-conditional.
        assertThat(wire).contains(
                "GAUGE|swath.process.memory.rss.bytes|{kind=current}",
                "GAUGE|swath.process.memory.rss.bytes|{kind=peak}",
                "GAUGE|swath.process.memory.heap.bytes|{kind=current}",
                "GAUGE|swath.process.memory.heap.bytes|{kind=peak}");
    }

    /**
     * Captures ONLY the one explicit final OTLP publish, with NO scheduler thread able to exist.
     * {@link OtlpMeterRegistry} extends {@code PushMeterRegistry}, whose {@code start(ThreadFactory)}
     * — invoked from the registry constructor — creates and schedules the periodic export executor
     * ONLY when {@code config.enabled()} is true (verified against the Micrometer 1.17 bytecode: an
     * {@code enabled()} of false takes the early-return branch before any {@code
     * ScheduledExecutorService} is constructed). Building the capture registry with {@code enabled()
     * == false} therefore makes the push scheduler STRUCTURALLY impossible — no thread, no periodic
     * export, no race — rather than merely improbable via a large step. The single export is then
     * driven explicitly by invoking the (protected) {@code publish()} once from {@link
     * #finalPublish()}, so {@code payloads} holds exactly that one deterministic payload.
     */
    private static final class Capture {

        private final List<byte[]> payloads = new ArrayList<>();
        private final OtlpMeterRegistry registry;

        Capture() {
            OtlpConfig config = new MeterRegistries.SwathOtlpConfig(
                    "http://localhost:4318/v1/metrics", Duration.ofSeconds(5), Map.of()) {
                @Override
                public boolean enabled() {
                    return false;   // no push scheduler is ever created — structurally race-free
                }
            };
            this.registry = OtlpMeterRegistry.builder(config)
                    .metricsSender(request -> payloads.add(request.getMetricsData()))
                    .build();
        }

        OtlpMeterRegistry registry() {
            return registry;
        }

        /** Drives the ONE explicit export directly — there is no scheduler thread to race it. */
        void finalPublish() {
            try {
                Method publish = OtlpMeterRegistry.class.getDeclaredMethod("publish");
                publish.setAccessible(true);
                publish.invoke(registry);
            } catch (ReflectiveOperationException e) {
                throw new AssertionError("could not invoke OtlpMeterRegistry.publish()", e);
            }
        }

        List<byte[]> payloads() {
            return payloads;
        }

        List<String> wireSeries() throws Exception {
            List<String> series = new ArrayList<>();
            for (byte[] payload : payloads) {
                ExportMetricsServiceRequest request = ExportMetricsServiceRequest.parseFrom(payload);
                for (var resourceMetrics : request.getResourceMetricsList()) {
                    for (var scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                        for (Metric metric : scopeMetrics.getMetricsList()) {
                            for (List<KeyValue> attributes : attributeSets(metric)) {
                                series.add(otlpType(metric) + "|" + metric.getName() + "|{"
                                        + attributes.stream()
                                                .map(kv -> kv.getKey() + "=" + kv.getValue().getStringValue())
                                                .sorted()
                                                .collect(Collectors.joining(",")) + "}");
                            }
                        }
                    }
                }
            }
            return series.stream().sorted().toList();
        }
    }

    private static List<String> meterIds(OtlpMeterRegistry registry) {
        return registry.getMeters().stream()
                .map(meter -> meter.getId().getType() + "|" + meter.getId().getName() + "|{" + tags(meter) + "}")
                .sorted()
                .toList();
    }

    private static String tags(Meter meter) {
        return meter.getId().getTags().stream()
                .sorted(Comparator.comparing(Tag::getKey).thenComparing(Tag::getValue))
                .map(tag -> tag.getKey() + "=" + tag.getValue())
                .collect(Collectors.joining(","));
    }

    /** The metric name out of a {@code TYPE|name|{attrs}} id. */
    private static String nameOf(String seriesId) {
        return seriesId.split("\\|", 3)[1];
    }

    private static String otlpType(Metric metric) {
        if (metric.hasSum()) {
            return "SUM";
        } else if (metric.hasGauge()) {
            return "GAUGE";
        } else if (metric.hasHistogram()) {
            return "HISTOGRAM";
        } else if (metric.hasSummary()) {
            return "SUMMARY";
        } else if (metric.hasExponentialHistogram()) {
            return "EXP_HISTOGRAM";
        }
        return "UNKNOWN";
    }

    /**
     * Data-point attribute sets for every OTLP metric shape. The pinned explicit-bucket flavour
     * means {@code HISTOGRAM} rather than {@code EXPONENTIAL_HISTOGRAM} is what actually appears
     * here; the {@code EXPONENTIAL_HISTOGRAM} arm is defensive only (kept so a future flavour change
     * fails honestly rather than by silently returning no data points), not something this test
     * exercises today.
     */
    private static List<List<KeyValue>> attributeSets(Metric metric) {
        if (metric.hasSum()) {
            return metric.getSum().getDataPointsList().stream().map(p -> p.getAttributesList()).toList();
        } else if (metric.hasGauge()) {
            return metric.getGauge().getDataPointsList().stream().map(p -> p.getAttributesList()).toList();
        } else if (metric.hasHistogram()) {
            return metric.getHistogram().getDataPointsList().stream().map(p -> p.getAttributesList()).toList();
        } else if (metric.hasSummary()) {
            return metric.getSummary().getDataPointsList().stream().map(p -> p.getAttributesList()).toList();
        } else if (metric.hasExponentialHistogram()) {
            return metric.getExponentialHistogram().getDataPointsList().stream()
                    .map(p -> p.getAttributesList()).toList();
        }
        return List.of();
    }
}
