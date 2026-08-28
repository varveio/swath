/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.error.ThrottleType;
import io.varve.swath.sort.SortArm;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * The JSON run-summary artifact — serialize/parse round-trip, the {@code completed}/{@code
 * exit_code} lifecycle, and atomic-replace on flush.
 */
final class JsonRunSummaryWriterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static RunSummary summary() {
        // All three clocks deliberately differ -- session (31s) > run (25s) > listing (20s) -- so the
        // round-trip test below exercises duration_ms, session_duration_ms and listing_duration_ms
        // separately, rather than values where any two would coincide.
        return new RunSummary(
                42L, 1000L, 0L, Duration.ofSeconds(25), Duration.ofSeconds(31), Duration.ofSeconds(20),
                "WORK_STEALING", 118L, 0.00059,
                4L, 1234567L, 1000L, 112L, 61L, 16.5, 180L, 4200L, 232602L, 98L, 0L,
                4269.5, 1.07, 268435456L, 134217728L, 26.4, 1.03,
                2.0, 0.95, 0.1, 0.3, 0.6, 1.9,
                new RunSummary.SeedSummary("shallow", 5L, 12L, 4L, 13L),
                shape(), null, List.of(), List.of(), clientCost(), datasetWriter(), null);
    }

    /** One populated client-service-cost span, enough to pin the {@code client_cost[]} row shape. */
    private static List<RunSummary.ClientCostSpan> clientCost() {
        return List.of(new RunSummary.ClientCostSpan(RunMetrics.CLIENT_COST_SPAN_EMIT, 1000L,
                0.4, 1.2, 4.8, 9.1));
    }

    private static RunSummary.DatasetWriterSummary datasetWriter() {
        return RunSummary.DatasetWriterSummary.from("parquet", List.of(
                new RunSummary.DatasetWriterLane(
                        0, 8, 0, 8, false, 600L, 6000L, 6L, 12_000_000L,
                        2L, 4_000_000L, 1L, 3_000_000L, 2L, 2L, 5_000_000L),
                new RunSummary.DatasetWriterLane(
                        1, 8, 0, 4, true, 400L, 4000L, 4L, 8_000_000L,
                        0L, 0L, 0L, 0L, 1L, 1L, 2_000_000L)),
                30_000_000_000L, 2_000_000L,
                3L, 6_000_000L, 4L, 10_000_000L,
                64L * 1024 * 1024, 4, 768L * 1024 * 1024, false);
    }

    private static RunSummary.ShapeSummary shape() {
        return new RunSummary.ShapeSummary(
                new int[] {16, 10, 4, 0, 0, 0, 0, 0}, 3,
                new long[] {0, 2, 5, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0.42,
                37L, 210L, 12L, 41.2, 210.7);
    }

    private static JsonRunSummaryWriter.RunConfig runConfig() {
        return new JsonRunSummaryWriter.RunConfig(
                "s3://bucket/prefix", "us-east-2", "parquet", 64, true, null, 30_000L,
                List.of("include=\\.parquet$"), EngineToggles.DEFAULT, null, false, null, SortArm.NONE, false);
    }

    private static JsonRunSummaryWriter.Config config(Path path, Duration flushInterval) {
        return new JsonRunSummaryWriter.Config(path, flushInterval, "abc123hash", runConfig(),
                List.of("list", "s3://bucket/prefix", "--format", "parquet"));
    }

    private static SimpleMeterRegistry registryWithMeters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Counter.builder("swath.api.calls").tag("strategy", "work_stealing").register(registry).increment(118);
        Timer.builder("swath.api.latency").tag("op", "listObjectsV2").register(registry);
        DistributionSummary writebackBytes = DistributionSummary.builder("swath.data_sync.bytes")
                .baseUnit("bytes").tag("format", "tsv").register(registry);
        writebackBytes.record(32L * 1024 * 1024);
        writebackBytes.record(64L * 1024 * 1024);
        Gauge.builder("swath.process.memory.rss.bytes", () -> Double.NaN).tag("kind", "current").register(registry);
        return registry;
    }

    @Test
    void serializesAllBlocksAndParsesBackWithJackson(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = registryWithMeters();
        RunSummary summary = summary();
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.parse("2026-07-01T00:00:00Z"),
                () -> summary);
        try {
            writer.complete(summary);
        } finally {
            writer.close();
        }

        JsonNode root = MAPPER.readTree(path.toFile());
        assertThat(root.get("schema_version").asInt()).isEqualTo(2);
        assertThat(root.get("run_id").asLong()).isEqualTo(42L);
        assertThat(root.get("args_hash").asText()).isEqualTo("abc123hash");
        // Centrally sanitized argv alongside the narrow resume-safety args_hash.
        JsonNode argv = root.get("argv");
        assertThat(argv.isArray()).isTrue();
        List<String> argvValues = new ArrayList<>();
        argv.forEach(n -> argvValues.add(n.asText()));
        assertThat(argvValues).containsExactly("list", "s3://bucket/prefix", "--format", "parquet");
        assertThat(root.get("completed").asBoolean()).isTrue();
        assertThat(root.get("exit_code").asInt()).isZero();
        assertThat(root.get("stop_reason").asText())
                .as("completed:true ⇔ stop_reason=completed (schema v2)").isEqualTo("completed");
        assertThat(root.get("started_at").asText()).isEqualTo("2026-07-01T00:00:00Z");
        // Three clocks, three spans (RunSummary's javadoc): duration_ms is the post-seed whole-run
        // clock, session_duration_ms folds seeding back in, listing_duration_ms stops at the
        // listing->merge boundary.
        assertThat(root.get("duration_ms").asLong()).isEqualTo(25_000L);
        assertThat(root.get("session_duration_ms").asLong()).isEqualTo(31_000L);
        assertThat(root.get("listing_duration_ms").asLong()).isEqualTo(20_000L);
        assertThat(root.get("objects").asLong()).isEqualTo(1000L);

        JsonNode config = root.get("config");
        assertThat(config.get("target").asText()).isEqualTo("s3://bucket/prefix");
        assertThat(config.get("region").asText()).isEqualTo("us-east-2");
        assertThat(config.get("format").asText()).isEqualTo("parquet");
        assertThat(config.get("max_parallel_listings").asInt()).isEqualTo(64);
        assertThat(config.get("no_sign_request").asBoolean()).isTrue();
        assertThat(config.get("rate_limit_api").isNull()).isTrue();
        assertThat(config.get("progress_interval_ms").asLong()).isEqualTo(30_000L);
        assertThat(config.get("filters")).hasSize(1);
        assertThat(config.get("filters").get(0).asText()).isEqualTo("include=\\.parquet$");

        // The genuine engine on/off toggles, self-identifying the run.
        JsonNode engineFlags = root.get("engine_flags");
        assertThat(engineFlags.get("owner_split").asBoolean()).isTrue();
        assertThat(engineFlags.get("density_ewma").asBoolean()).isTrue();
        assertThat(engineFlags.get("radix_bands").asBoolean()).isTrue();
        assertThat(engineFlags.get("structure_probes").asBoolean()).isTrue();
        assertThat(engineFlags.get("far_ahead").asBoolean()).isTrue();
        assertThat(engineFlags.get("alphabet_pivots").asBoolean()).isTrue();
        assertThat(engineFlags.get("reflect").asBoolean()).isTrue();
        assertThat(engineFlags.get("max_duration_ms").isNull()).isTrue();

        JsonNode output = root.get("output");
        assertThat(output.get("format").asText()).isEqualTo("parquet");
        assertThat(output.get("files").asLong()).isEqualTo(4L);
        assertThat(output.get("compressed_size_bytes").asLong()).isEqualTo(1234567L);

        JsonNode cost = root.get("cost");
        assertThat(cost.get("api_calls").asLong()).isEqualTo(118L);
        assertThat(cost.get("cost_usd").asDouble()).isEqualTo(0.00059);

        JsonNode efficiency = root.get("efficiency");
        assertThat(efficiency.get("keys_per_sec").asDouble()).isEqualTo(4269.5);
        assertThat(efficiency.get("api_calls_per_1k_objects").asDouble()).isEqualTo(1.07);
        assertThat(efficiency.get("peak_rss_bytes").asLong()).isEqualTo(268435456L);
        assertThat(efficiency.get("peak_heap_bytes").asLong()).isEqualTo(134217728L);
        assertThat(efficiency.get("cpu_seconds").asDouble()).isEqualTo(26.4);
        assertThat(efficiency.get("cpu_efficiency").asDouble()).isEqualTo(1.03);
        // Derived efficiency ratios.
        assertThat(efficiency.get("overfetch_ratio").asDouble()).isEqualTo(2.0);
        assertThat(efficiency.get("page_fill_ratio").asDouble()).isEqualTo(0.95);
        assertThat(efficiency.get("empty_split_ratio").asDouble()).isEqualTo(0.1);
        assertThat(efficiency.get("wasted_probe_ratio").asDouble()).isEqualTo(0.3);
        assertThat(efficiency.get("steal_success_rate").asDouble()).isEqualTo(0.6);
        assertThat(efficiency.get("compression_ratio").asDouble()).isEqualTo(1.9);

        JsonNode engine = root.get("engine");
        assertThat(engine.get("pages").asLong()).isEqualTo(112L);
        assertThat(engine.get("peak_in_flight").asLong()).isEqualTo(61L);
        assertThat(engine.get("avg_in_flight").asDouble()).isEqualTo(16.5);
        assertThat(engine.get("steals").asLong()).isEqualTo(232602L);
        assertThat(engine.get("splits").asLong()).isEqualTo(98L);
        assertThat(engine.get("errors").asLong()).isZero();

        // Seed block: SeedStep's promoted mode/probes/cut_points/synthesized_cuts/ranges.
        JsonNode seed = root.get("seed");
        assertThat(seed.get("mode").asText()).isEqualTo("shallow");
        assertThat(seed.get("probes").asLong()).isEqualTo(5L);
        assertThat(seed.get("cut_points").asLong()).isEqualTo(12L);
        assertThat(seed.get("synthesized_cuts").asLong()).isEqualTo(4L);
        assertThat(seed.get("ranges").asLong()).isEqualTo(13L);

        // The flat shape feature-vector (post-hoc classification signals).
        JsonNode shape = root.get("shape");
        assertThat(shape).isNotNull();
        assertThat(shape.get("partial").asBoolean()).as("completed run's shape is not partial").isFalse();
        JsonNode cardinality = shape.get("alphabet_cardinality");
        assertThat(cardinality.isArray()).isTrue();
        assertThat(cardinality).hasSize(8);
        assertThat(cardinality.get(0).asInt()).isEqualTo(16);
        assertThat(shape.get("alphabet_positions_observed").asInt()).isEqualTo(3);
        assertThat(shape.get("divergence_depth_histogram")).hasSize(16);
        assertThat(shape.get("divergence_depth_histogram").get(2).asLong()).isEqualTo(5L);
        assertThat(shape.get("mass_skew_gini").asDouble()).isEqualTo(0.42);
        JsonNode fanout = shape.get("delimiter_fanout");
        assertThat(fanout.get("max").asLong()).isEqualTo(37L);
        assertThat(fanout.get("total").asLong()).isEqualTo(210L);
        assertThat(fanout.get("probes").asLong()).isEqualTo(12L);
        JsonNode regime = shape.get("regime");
        assertThat(regime.get("api_latency_p50_ms").asDouble()).isEqualTo(41.2);
        assertThat(regime.get("api_latency_p99_ms").asDouble()).isEqualTo(210.7);
        // This summary carries no worker_page latency row, so the data-page percentiles render as
        // JSON null rather than being omitted — a fleet parser never needs a key-presence check.
        assertThat(regime.get("worker_page_latency_p50_ms").isNull()).isTrue();
        assertThat(regime.get("worker_page_latency_p99_ms").isNull()).isTrue();
        assertThat(regime.get("worker_count").asInt()).isEqualTo(64);
        assertThat(regime.get("region").asText()).isEqualTo("us-east-2");
        JsonNode fingerprint = shape.get("fingerprint");
        assertThat(fingerprint.has("binary_sha256")).isTrue();
        assertThat(fingerprint.has("git_sha")).isTrue();
        assertThat(fingerprint.get("started_at").asText()).isEqualTo("2026-07-01T00:00:00Z");
        assertThat(fingerprint.get("finished_at").asText()).isEqualTo("2026-07-01T00:00:25Z");

        // The per-page client-service-cost decomposition -- a dedicated percentile readback,
        // because meters[] below carries a timer's count/total_ms/max_ms only.
        JsonNode clientCost = root.get("client_cost");
        assertThat(clientCost.isArray()).isTrue();
        assertThat(clientCost).hasSize(1);
        JsonNode emitSpan = clientCost.get(0);
        assertThat(emitSpan.get("span").asText()).isEqualTo(RunMetrics.CLIENT_COST_SPAN_EMIT);
        assertThat(emitSpan.get("count").asLong()).isEqualTo(1000L);
        assertThat(emitSpan.get("p50_ms").asDouble()).isEqualTo(0.4);
        assertThat(emitSpan.get("p90_ms").asDouble()).isEqualTo(1.2);
        assertThat(emitSpan.get("p99_ms").asDouble()).isEqualTo(4.8);
        assertThat(emitSpan.get("max_ms").asDouble()).isEqualTo(9.1);

        JsonNode datasetWriter = root.get("dataset_writer");
        assertThat(datasetWriter.get("format").asText()).isEqualTo("parquet");
        assertThat(datasetWriter.get("writer_count").asInt()).isEqualTo(2);
        assertThat(datasetWriter.get("total_queue_capacity").asLong()).isEqualTo(16L);
        assertThat(datasetWriter.get("part_rotation_interval_ms").asDouble()).isEqualTo(30_000.0);
        assertThat(datasetWriter.get("part_rotation_max_rows").asLong()).isEqualTo(2_000_000L);
        assertThat(datasetWriter.get("jvm_max_heap_bytes").asLong()).isPositive();
        assertThat(datasetWriter.get("row_group_target_bytes_per_writer").asLong())
                .isEqualTo(64L * 1024 * 1024);
        assertThat(datasetWriter.get("row_group_allowance_multiplier").asInt()).isEqualTo(4);
        assertThat(datasetWriter.get("planned_heap_bytes").asLong()).isEqualTo(768L * 1024 * 1024);
        assertThat(datasetWriter.get("heap_admission_applied").asBoolean()).isFalse();
        assertThat(datasetWriter.get("part_digest_count").asLong()).isEqualTo(3L);
        assertThat(datasetWriter.get("part_digest_ms").asDouble()).isEqualTo(6.0);
        assertThat(datasetWriter.get("manifest_write_count").asLong()).isEqualTo(4L);
        assertThat(datasetWriter.get("manifest_write_ms").asDouble()).isEqualTo(10.0);
        assertThat(datasetWriter.get("submit_blocked_count").asLong()).isEqualTo(2L);
        assertThat(datasetWriter.get("submit_blocked_ms").asDouble()).isEqualTo(4.0);
        assertThat(datasetWriter.get("head_of_line_blocked_count").asLong()).isEqualTo(1L);
        assertThat(datasetWriter.get("head_of_line_blocked_ms").asDouble()).isEqualTo(3.0);
        assertThat(datasetWriter.get("lanes")).hasSize(2);
        JsonNode lane0 = datasetWriter.get("lanes").get(0);
        assertThat(lane0.get("queue_depth_peak").asInt()).isEqualTo(8);
        assertThat(lane0.get("waiting_for_work").asBoolean()).isFalse();
        assertThat(lane0.get("rows_written").asLong()).isEqualTo(600L);
        assertThat(lane0.get("active_elapsed_ms").asDouble()).isEqualTo(12.0);
        assertThat(lane0.get("finalize_elapsed_ms").asDouble()).isEqualTo(5.0);

        JsonNode meters = root.get("meters");
        assertThat(meters.isArray()).isTrue();
        List<String> meterNames = new ArrayList<>();
        meters.forEach(m -> meterNames.add(m.get("name").asText()));
        assertThat(meterNames).contains("swath.api.calls", "swath.api.latency",
                "swath.data_sync.bytes", "swath.process.memory.rss.bytes");

        JsonNode apiCallsMeter = findMeter(meters, "swath.api.calls");
        assertThat(apiCallsMeter.get("type").asText()).isEqualTo("counter");
        assertThat(apiCallsMeter.get("value").asDouble()).isEqualTo(118.0);
        assertThat(apiCallsMeter.get("tags").get("strategy").asText()).isEqualTo("work_stealing");

        JsonNode latencyMeter = findMeter(meters, "swath.api.latency");
        assertThat(latencyMeter.get("type").asText()).isEqualTo("timer");
        assertThat(latencyMeter.has("count")).isTrue();
        assertThat(latencyMeter.has("total_ms")).isTrue();
        assertThat(latencyMeter.has("max_ms")).isTrue();

        JsonNode writebackMeter = findMeter(meters, "swath.data_sync.bytes");
        assertThat(writebackMeter.get("type").asText()).isEqualTo("distribution_summary");
        assertThat(writebackMeter.get("tags").get("format").asText()).isEqualTo("tsv");
        assertThat(writebackMeter.get("count").asLong()).isEqualTo(2L);
        assertThat(writebackMeter.get("total").asDouble()).isEqualTo(96L * 1024 * 1024);
        assertThat(writebackMeter.get("max").asDouble()).isEqualTo(64L * 1024 * 1024);
        assertThat(writebackMeter.get("mean").asDouble()).isEqualTo(48L * 1024 * 1024);

        JsonNode nanGaugeMeter = findMeter(meters, "swath.process.memory.rss.bytes");
        assertThat(nanGaugeMeter.get("type").asText()).isEqualTo("gauge");
        assertThat(nanGaugeMeter.get("value").isNull()).isTrue();
    }

    private static JsonNode findMeter(JsonNode meters, String name) {
        for (JsonNode m : meters) {
            if (m.get("name").asText().equals(name)) {
                return m;
            }
        }
        throw new AssertionError("no meter named " + name);
    }

    /**
     * A {@code --no-owner-split --max-duration} ablation run is self-identifying from {@code
     * engine_flags} alone — the opposite of the default toggles asserted in {@link
     * #serializesAllBlocksAndParsesBackWithJackson}. Also covers the other five {@code
     * --engine-toggle} names so the {@code engine_flags} echo is exercised for the whole
     * namespace, not just {@code owner_split}.
     */
    @Test
    void engineFlagsReflectNonDefaultToggles(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary summary = summary();
        EngineToggles toggles =
                EngineToggles.DEFAULT
                        .withOwnerSplit(false)
                        .withDensityEwma(false)
                        .withStructureProbes(false);
        JsonRunSummaryWriter.RunConfig runConfig = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket/prefix", "us-east-2", "parquet", 64, true, null, 30_000L,
                List.of(), toggles, 300_000L, false, null, SortArm.NONE, false);
        JsonRunSummaryWriter.Config config = new JsonRunSummaryWriter.Config(
                path, Duration.ofMinutes(10), "abc123hash", runConfig,
                List.of("list", "s3://bucket/prefix", "--no-owner-split", "--max-duration", "5m"));
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config, registry, Instant.now(), () -> summary);
        try {
            writer.complete(summary);
        } finally {
            writer.close();
        }

        JsonNode root = MAPPER.readTree(path.toFile());
        JsonNode engineFlags = root.get("engine_flags");
        assertThat(engineFlags.get("owner_split").asBoolean()).isFalse();
        assertThat(engineFlags.get("density_ewma").asBoolean()).isFalse();
        assertThat(engineFlags.get("radix_bands").asBoolean()).isTrue();
        assertThat(engineFlags.get("structure_probes").asBoolean()).isFalse();
        assertThat(engineFlags.get("far_ahead").asBoolean()).isTrue();
        assertThat(engineFlags.get("alphabet_pivots").asBoolean()).isTrue();
        assertThat(engineFlags.get("max_duration_ms").asLong()).isEqualTo(300_000L);

        JsonNode argv = root.get("argv");
        List<String> argvValues = new ArrayList<>();
        argv.forEach(n -> argvValues.add(n.asText()));
        assertThat(argvValues).containsExactly(
                "list", "s3://bucket/prefix", "--no-owner-split", "--max-duration", "5m");
    }

    /**
     * {@code engine_flags} is the ONLY post-hoc reproducibility signal for the {@code readahead}
     * (opt-in) / {@code mass_aware_seed} (opt-out) toggles (both excluded from {@code args_hash})
     * — the writer must render EVERY name {@link
     * EngineToggles} accepts, not just the original ablation set. Bound against
     * the canonical name set {@link EngineToggles#parse} itself validates
     * against ({@code NAMES} plus the two new-mechanism names, deliberately kept out of {@code NAMES}
     * per its javadoc) so a future toggle that is added to that set but never wired into the writer
     * fails RED here.
     */
    @Test
    void engineFlagsRenderEveryEngineTogglesName(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary summary = summary();
        JsonRunSummaryWriter.Config config = config(path, Duration.ofMinutes(10));
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(config, registry, Instant.now(), () -> summary);
        try {
            writer.complete(summary);
        } finally {
            writer.close();
        }

        JsonNode root = MAPPER.readTree(path.toFile());
        JsonNode engineFlags = root.get("engine_flags");
        Set<String> expectedNames = new LinkedHashSet<>(
                EngineToggles.NAMES);
        expectedNames.add(EngineToggles.READAHEAD_NAME);
        expectedNames.add(EngineToggles.MASS_AWARE_SEED_NAME);
        expectedNames.add(EngineToggles.RATE_ANCHORED_SENSING_NAME);
        expectedNames.add(EngineToggles.TAIL_FLOOR_NAME);
        assertThat(expectedNames).as("the 14 names EngineToggles.parse currently accepts").hasSize(14);
        for (String name : expectedNames) {
            assertThat(engineFlags.has(name)).as("engine_flags missing toggle '" + name + "'").isTrue();
        }
        assertThat(engineFlags.get(EngineToggles.TAIL_FLOOR_NAME).asText())
                .as("the one value-taking toggle renders as its mode code, not a boolean")
                .isEqualTo(EngineToggles.DEFAULT.tailFloor().code());
    }

    /**
     * Sort observability polish: {@code effective_fan_in} — the realized merge pass width
     * ({@code SortConfig#effectiveFanIn()}) — must be echoed in the {@code sort} block so a run is
     * self-describing about which knob (raw {@code fan-in} or the merge-budget bound) bound,
     * without the reader re-deriving it from {@code segments}/{@code passes}
     * (docs/metrics-and-observability.md §3).
     */
    @Test
    void sortBlockEchoesEffectiveFanIn(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary summary = summary();
        JsonRunSummaryWriter.RunConfig runConfig = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket/prefix", "us-east-2", "parquet", 64, true, null, 30_000L,
                List.of(), EngineToggles.DEFAULT, null, false, null, SortArm.NONE, false)
                        .withSortEnabled(true)
                        .withSortEffectiveFanIn(3);
        JsonRunSummaryWriter.Config config = new JsonRunSummaryWriter.Config(
                path, Duration.ofMinutes(10), "abc123hash", runConfig, List.of());
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(config, registry, Instant.now(), () -> summary);
        try {
            writer.complete(summary);
        } finally {
            writer.close();
        }

        JsonNode sort = MAPPER.readTree(path.toFile()).get("sort");
        assertThat(sort).isNotNull();
        assertThat(sort.get("enabled").asBoolean()).isTrue();
        assertThat(sort.get("arm").asText()).isEqualTo("LIVE_LIST_SORT");
        assertThat(sort.get("effective_fan_in").asInt()).isEqualTo(3);
    }

    @Test
    void sortBlockSeparatesParallelCloseServiceFromFinalizeWallTime(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        registry.get("swath.sort.merge.latency").timer().record(10, TimeUnit.SECONDS);
        metrics.recordSortMergeRange(TimeUnit.SECONDS.toNanos(2));
        metrics.recordSortMergeBoundaries(TimeUnit.SECONDS.toNanos(1));
        metrics.recordSortMergeRangeFramedBytes(12_345);
        metrics.recordSortMergeProofSpool(
                49_696, 2, 49_696, 777, 8_888, TimeUnit.MILLISECONDS.toNanos(12));
        metrics.recordSortFinalizeClose(TimeUnit.SECONDS.toNanos(3));
        metrics.recordSortFinalizeClose(TimeUnit.SECONDS.toNanos(3));
        metrics.recordSortManifestMd5(1234, TimeUnit.MILLISECONDS.toNanos(250));
        metrics.recordSortManifestBounds(0, 0, 0); // fresh writer: no post-close scan
        metrics.recordSortPublication(TimeUnit.SECONDS.toNanos(4));
        metrics.recordSortFinalizeTail(TimeUnit.SECONDS.toNanos(4));
        metrics.recordSortFinalizeParallelism(4);
        metrics.recordStealReason("SORT", "pack_on_fetch");
        metrics.recordStealReason("SORT", "backpressure_engaged");
        metrics.recordSortBackpressureWait(TimeUnit.MILLISECONDS.toNanos(7));
        metrics.recordSortStagingBytesLive(1234);
        metrics.recordSortHandoffQueueDepth(5);
        metrics.recordSortOffThreadBuffersLive(2);
        metrics.recordSortMergeOverlapCluster();
        metrics.recordSortMergeOverlapState(3, 12);

        RunSummary snapshot = summary();
        JsonRunSummaryWriter.RunConfig runConfig = runConfig().withSortEnabled(true);
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                new JsonRunSummaryWriter.Config(path, Duration.ofMinutes(10), "abc123hash",
                        runConfig, List.of()),
                registry, Instant.now(), () -> snapshot);
        try {
            writer.complete(snapshot);
        } finally {
            writer.close();
        }

        JsonNode sort = MAPPER.readTree(path.toFile()).get("sort");
        assertThat(sort.get("range_merge_ms").asLong()).isEqualTo(2_000);
        assertThat(sort.get("finalize_ms").asLong()).isEqualTo(4_000);
        assertThat(sort.get("finalize_close_ms").asLong()).isEqualTo(6_000);
        assertThat(sort.get("finalize_close_count").asLong()).isEqualTo(2L);
        assertThat(sort.get("finalize_close_max_ms").asLong()).isEqualTo(3_000L);
        assertThat(sort.get("finalize_close_p50_ms").asDouble()).isPositive();
        assertThat(sort.get("finalize_close_p90_ms").asDouble()).isPositive();
        assertThat(sort.get("finalize_close_p99_ms").asDouble()).isPositive();
        assertThat(sort.get("pack_on_fetch_pages").asLong()).isEqualTo(1L);
        assertThat(sort.get("backpressure_engaged").asLong()).isEqualTo(1L);
        assertThat(sort.get("backpressure_wait_ms").asLong()).isEqualTo(7L);
        assertThat(sort.get("backpressure_wait_max_ms").asLong()).isEqualTo(7L);
        assertThat(sort.get("staging_bytes_peak").asLong()).isEqualTo(1234L);
        assertThat(sort.get("handoff_queue_depth_peak").asLong()).isEqualTo(5L);
        assertThat(sort.get("off_thread_buffers_peak").asLong()).isEqualTo(2L);
        assertThat(sort.get("manifest_md5_bytes").asLong()).isEqualTo(1234);
        assertThat(sort.get("manifest_md5_ms").asLong()).isEqualTo(250);
        assertThat(sort.get("manifest_bounds_rows").asLong()).isZero();
        assertThat(sort.get("manifest_bounds_bytes").asLong()).isZero();
        assertThat(sort.get("manifest_bounds_ms").asLong()).isZero();
        assertThat(sort.get("local_publication_ms").asLong()).isEqualTo(4_000);
        assertThat(sort.get("finalize_parallelism").asLong()).isEqualTo(4);
        assertThat(sort.get("phase_rows_per_sec").asDouble()).isEqualTo(100.0);
        assertThat(sort.get("merge_overlap_clusters").asLong()).isEqualTo(1);
        assertThat(sort.get("merge_overlap_pages_peak").asLong()).isEqualTo(3);
        assertThat(sort.get("merge_overlap_rows_peak").asLong()).isEqualTo(12);
        assertThat(sort.get("merge_range_framed_bytes").asLong()).isEqualTo(12_345);
        assertThat(sort.get("merge_proof_spool_logical_extent_bytes").asLong()).isEqualTo(49_696);
        assertThat(sort.get("merge_proof_spool_preallocation_operations").asLong()).isEqualTo(2);
        assertThat(sort.get("merge_proof_spool_preallocation_attempted_bytes").asLong())
                .isEqualTo(49_696);
        assertThat(sort.get("merge_proof_spool_mapped_operations").asLong()).isEqualTo(777);
        assertThat(sort.get("merge_proof_spool_mapped_bytes").asLong()).isEqualTo(8_888);
        assertThat(sort.get("merge_proof_spool_ms").asLong()).isEqualTo(12);
    }

    /** The pre-{@code effective_fan_in} {@code sortEnabled} constructor renders the field as JSON null. */
    @Test
    void sortBlockEffectiveFanInIsNullWhenTheCallerOmitsIt(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary summary = summary();
        JsonRunSummaryWriter.RunConfig runConfig = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket/prefix", "us-east-2", "parquet", 64, true, null, 30_000L,
                List.of(), EngineToggles.DEFAULT, null, false, null, SortArm.NONE, false)
                        .withSortEnabled(true);
        JsonRunSummaryWriter.Config config = new JsonRunSummaryWriter.Config(
                path, Duration.ofMinutes(10), "abc123hash", runConfig, List.of());
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(config, registry, Instant.now(), () -> summary);
        try {
            writer.complete(summary);
        } finally {
            writer.close();
        }

        JsonNode sort = MAPPER.readTree(path.toFile()).get("sort");
        assertThat(sort.get("effective_fan_in").isNull()).isTrue();
    }

    /**
     * The external sort-merge phase is silent on every external-facing channel for the whole
     * multi-minute merge — {@code RunProgressReporter}'s
     * human log line is closed at the LISTING->MERGING transition (ListRunner#run try-with-
     * resources), and the {@code sort} block's {@code segments}/{@code passes} counters
     * ({@code swath.sort.segments.written}/{@code swath.sort.merge.passes}) are populated exactly
     * ONCE, after the whole merge finishes (ListRunner#sortMergeAndPublish calls
     * recordSortMergePasses AFTER transform.transform() returns) — so a mid-merge snapshot of
     * {@code summary.json} is flat and indistinguishable from a wedged run to any external
     * supervisor (log tail / summary.json poller), even though the merge IS actually advancing:
     * {@code KWayMerge.merge}'s per-pass batched progress callback already feeds
     * {@code RunMetrics#recordProgress} (wired at ListRunner#sortMergeAndPublish), which bumps the
     * {@code swath.progress.units} counter/tally the SAME counter the internal LivenessWatchdog
     * already trusts as its phase-agnostic forward-progress signal.
     *
     * <p>This asserts the {@code sort} block exposes a NAMED merge-progress field
     * ({@code merge_progress_units}) fed by that existing, already-advancing counter — NOT a new
     * mechanism, just surfacing the counter under an external-facing, self-describing name inside
     * the block a fleet parser already reads for sort observability. The field does not exist at
     * all today: this test is RED until the impl adds it.
     */
    @Test
    void sortBlockExposesMergeProgressField(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        // Simulate the merge's final (usually longest) streaming pass: the merge is deep into
        // real, observable forward progress (recordProgress ticks via KWayMerge's callback) while
        // segments/passes (both zero here — no segment written during merge, no pass completed
        // yet) stay exactly where they were when the merge started.
        Counter.builder("swath.progress.units").register(registry).increment(4_200_000L);

        RunSummary summary = summary();
        JsonRunSummaryWriter.RunConfig runConfig = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket/prefix", "us-east-2", "parquet", 64, true, null, 30_000L,
                List.of(), EngineToggles.DEFAULT, null, false, null, SortArm.NONE, false)
                        .withSortEnabled(true)
                        .withSortEffectiveFanIn(3);
        JsonRunSummaryWriter.Config config = new JsonRunSummaryWriter.Config(
                path, Duration.ofMinutes(10), "abc123hash", runConfig, List.of());
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(config, registry, Instant.now(), () -> summary);
        try {
            writer.complete(summary);
        } finally {
            writer.close();
        }

        JsonNode sort = MAPPER.readTree(path.toFile()).get("sort");
        assertThat(sort).isNotNull();
        // The ONCE-per-merge counters: flat/zero mid-merge by construction.
        assertThat(sort.get("segments").asLong()).isZero();
        assertThat(sort.get("passes").asLong()).isZero();
        assertThat(sort.has("merge_progress_units"))
                .as("sort block must expose a merge-progress field fed by swath.progress.units so "
                        + "an external watcher (log tail / summary.json) can distinguish "
                        + "alive-and-merging from wedged during a multi-pass merge)")
                .isTrue();
        assertThat(sort.get("merge_progress_units").asLong())
                .as("merge_progress_units must reflect the SAME progress tally recordProgress() "
                        + "already bumps during the merge, not a flat/zero placeholder")
                .isEqualTo(4_200_000L);
    }

    /**
     * Companion to {@link #sortBlockExposesMergeProgressField}: proves the new field genuinely
     * ADVANCES across successive mid-merge periodic flushes — distinguishing it from {@code
     * segments}/{@code passes}, which stay pinned at their merge-start values for the whole
     * merge. Mirrors the periodic-flush pattern of
     * {@code periodicFlushWritesIncompleteThenCompleteFinalizes} /
     * {@code closeReadsAFreshSnapshotNotTheLastPeriodicFlush} above.
     */
    @Test
    void sortBlockMergeProgressAdvancesAcrossPeriodicFlushesWhileSegmentsAndPassesStayFlat(@TempDir Path dir)
            throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        // Segments/passes are pinned at their merge-start snapshot for the whole merge (segments
        // accrue only during listing; passes only bump once, at merge end) — never touched again
        // by this test, so any change on-disk in those two fields would itself be a test bug.
        Counter.builder("swath.sort.segments.written").register(registry).increment(6L);
        Counter segmentsCounter = registry.find("swath.sort.segments.written").counter();
        assertThat(segmentsCounter).isNotNull();
        Counter progressCounter = Counter.builder("swath.progress.units").register(registry);

        RunSummary summary = summary();
        JsonRunSummaryWriter.RunConfig runConfig = new JsonRunSummaryWriter.RunConfig(
                "s3://bucket/prefix", "us-east-2", "parquet", 64, true, null, 30_000L,
                List.of(), EngineToggles.DEFAULT, null, false, null, SortArm.NONE, false)
                        .withSortEnabled(true)
                        .withSortEffectiveFanIn(3);
        JsonRunSummaryWriter.Config config = new JsonRunSummaryWriter.Config(
                path, Duration.ofMillis(20), "abc123hash", runConfig, List.of());
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(config, registry, Instant.now(), () -> summary);
        try {
            progressCounter.increment(1_000L);
            await().atMost(5, TimeUnit.SECONDS).until(() -> Files.exists(path)
                    && MAPPER.readTree(path.toFile()).get("completed").asBoolean() == false
                    && MAPPER.readTree(path.toFile()).get("sort").has("merge_progress_units")
                    && MAPPER.readTree(path.toFile()).get("sort").get("merge_progress_units").asLong() > 0);
            long firstSnapshot = MAPPER.readTree(path.toFile()).get("sort").get("merge_progress_units").asLong();

            progressCounter.increment(2_500L);
            await().atMost(5, TimeUnit.SECONDS).until(() ->
                    MAPPER.readTree(path.toFile()).get("sort").get("merge_progress_units").asLong() > firstSnapshot);

            JsonNode sort = MAPPER.readTree(path.toFile()).get("sort");
            assertThat(sort.get("merge_progress_units").asLong())
                    .as("merge_progress_units must have advanced between two mid-merge periodic "
                            + "flushes (distinguishing alive-and-merging from wedged)")
                    .isGreaterThan(firstSnapshot);
            assertThat(sort.get("segments").asLong())
                    .as("segments must stay pinned at its merge-start value for the whole merge — "
                            + "unlike merge_progress_units, it is populated once (the prior silence bug)")
                    .isEqualTo(6L);
            assertThat(sort.get("passes").asLong())
                    .as("passes must stay zero for the whole merge — only bumped once, after the "
                            + "entire k-way merge finishes (the prior silence bug)")
                    .isZero();
        } finally {
            writer.close();
        }
    }

    @Test
    void negativeOneResourceSentinelsSerializeAsJsonNull(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        // -1 is RunSummary's "unavailable" sentinel for peak_rss_bytes/peak_heap_bytes (long) and
        // cpu_seconds/cpu_efficiency (double); JSON renders these as null, not -1 (the log-line
        // sentinel).
        RunSummary summary = new RunSummary(
                42L, 1000L, 0L, Duration.ofSeconds(25), Duration.ofSeconds(25), Duration.ofSeconds(25),
                "WORK_STEALING", 118L, 0.00059,
                4L, 1234567L, 1000L, 112L, 61L, 16.5, 180L, 4200L, 232602L, 98L, 0L,
                4269.5, 1.07, -1L, -1L, -1.0, -1.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                null, null, null, List.of(), List.of(), List.of(), null, null);
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> summary);
        try {
            writer.complete(summary);
        } finally {
            writer.close();
        }

        JsonNode root = MAPPER.readTree(path.toFile());
        JsonNode efficiency = root.get("efficiency");
        assertThat(efficiency.get("peak_rss_bytes").isNull()).isTrue();
        assertThat(efficiency.get("peak_heap_bytes").isNull()).isTrue();
        assertThat(efficiency.get("cpu_seconds").isNull()).isTrue();
        assertThat(efficiency.get("cpu_efficiency").isNull()).isTrue();
        // A null SeedSummary (resumed run / never seeded) omits the "seed" block entirely
        // rather than writing a half-null object — null-safe.
        assertThat(root.has("seed")).isFalse();
        // A null ShapeSummary likewise omits the "shape" block entirely (null-safe).
        assertThat(root.has("shape")).isFalse();
        assertThat(root.has("dataset_writer")).isFalse();
    }

    @Test
    void periodicFlushWritesIncompleteThenCompleteFinalizes(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary midRun = summary();
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMillis(20)), registry, Instant.now(), () -> midRun);
        try {
            await().atMost(5, TimeUnit.SECONDS).until(() -> Files.exists(path)
                    && MAPPER.readTree(path.toFile()).get("completed").asBoolean() == false);

            JsonNode midNode = MAPPER.readTree(path.toFile());
            assertThat(midNode.get("completed").asBoolean()).isFalse();
            assertThat(midNode.get("exit_code").isNull()).isTrue();

            writer.complete(midRun);
            JsonNode finalNode = MAPPER.readTree(path.toFile());
            assertThat(finalNode.get("completed").asBoolean()).isTrue();
            assertThat(finalNode.get("exit_code").asInt()).isZero();
        } finally {
            writer.close();
        }
    }

    @Test
    void rangeFramedBytesAreCumulativeInPeriodicAndFinalSortSnapshots(@TempDir Path dir)
            throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Counter rangeFramedBytes = Counter.builder("swath.sort.merge.range.framed.bytes")
                .baseUnit("bytes").register(registry);
        JsonRunSummaryWriter.RunConfig runConfig = runConfig().withSortEnabled(true);
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                new JsonRunSummaryWriter.Config(path, Duration.ofMillis(20), "abc123hash",
                        runConfig, List.of()),
                registry, Instant.now(), JsonRunSummaryWriterTest::summary);
        try {
            rangeFramedBytes.increment(1_024);
            await().atMost(5, TimeUnit.SECONDS).until(() -> Files.exists(path)
                    && MAPPER.readTree(path.toFile()).get("sort").get("merge_range_framed_bytes")
                    .asLong() == 1_024L);

            rangeFramedBytes.increment(2_048);
            writer.complete(summary());
            JsonNode sort = MAPPER.readTree(path.toFile()).get("sort");
            assertThat(sort.get("merge_range_framed_bytes").asLong())
                    .as("final snapshot keeps the cumulative byte counter, not the last delta")
                    .isEqualTo(3_072L);
        } finally {
            writer.close();
        }
    }

    @Test
    void serialSortSummaryKeepsRangeFramedBytesAtZero(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        JsonRunSummaryWriter.RunConfig runConfig = runConfig().withSortEnabled(true);
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                new JsonRunSummaryWriter.Config(path, Duration.ofMinutes(10), "abc123hash",
                        runConfig, List.of()),
                new SimpleMeterRegistry(), Instant.now(), JsonRunSummaryWriterTest::summary);
        try {
            writer.complete(summary());
        } finally {
            writer.close();
        }

        assertThat(MAPPER.readTree(path.toFile()).get("sort").get("merge_range_framed_bytes")
                .asLong()).isZero();
    }

    @Test
    void closeWithoutCompleteLeavesAPartialFile(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary snapshot = summary();
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> snapshot);

        // Never call complete() — simulates an exception/SIGINT unwinding the run.
        writer.close();

        assertThat(path).exists();
        JsonNode node = MAPPER.readTree(path.toFile());
        assertThat(node.get("completed").asBoolean()).isFalse();
        assertThat(node.get("exit_code").isNull()).isTrue();
        // The default start() attributes an unexplained terminal partial as a crash.
        assertThat(node.get("stop_reason").asText()).isEqualTo("crash");
    }

    @Test
    void writesAreAtomicNoTmpFileSurvivesAndOutputParses(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("nested").resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary result = summary();
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> result);
        try {
            writer.complete(result);
        } finally {
            writer.close();
        }

        assertThat(path).exists();
        assertThat(path.resolveSibling("summary.json.tmp")).doesNotExist();
        assertThat(MAPPER.readTree(path.toFile()).get("schema_version").asInt()).isEqualTo(2);
    }

    @Test
    void midRunHeartbeatHasNoStopReasonThenCompleteNamesIt(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary midRun = summary();
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMillis(20)), registry, Instant.now(), () -> midRun);
        try {
            await().atMost(5, TimeUnit.SECONDS).until(() -> Files.exists(path)
                    && MAPPER.readTree(path.toFile()).get("completed").asBoolean() == false);

            // A running heartbeat has not stopped yet, so it names no reason.
            JsonNode midNode = MAPPER.readTree(path.toFile());
            assertThat(midNode.get("stop_reason").isNull()).isTrue();

            writer.complete(midRun);
            JsonNode finalNode = MAPPER.readTree(path.toFile());
            assertThat(finalNode.get("stop_reason").asText()).isEqualTo("completed");
        } finally {
            writer.close();
        }
    }

    @Test
    void terminalPartialUsesTheSuppliedStopReason(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary snapshot = summary();
        // A terminal partial (complete() never reached) is attributed by the terminalStatusSupplier.
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(),
                () -> snapshot, () -> new JsonRunSummaryWriter.TerminalStatus(StopReason.MAX_DURATION));
        writer.close();

        JsonNode node = MAPPER.readTree(path.toFile());
        assertThat(node.get("completed").asBoolean()).isFalse();
        assertThat(node.get("exit_code").isNull()).isTrue();
        assertThat(node.get("stop_reason").asText()).isEqualTo("max_duration");
        // A non-STUCK terminal carries no stop_source/error_class.
        assertThat(node.get("stop_source").isNull()).isTrue();
        assertThat(node.get("error_class").isNull()).isTrue();
    }

    @Test
    void terminalPartialWithNullReasonLeavesStopReasonNull(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary snapshot = summary();
        // A broken-pipe terminal partial is neither crash nor signal → supplier returns a null reason.
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(),
                () -> snapshot, () -> new JsonRunSummaryWriter.TerminalStatus(null));
        writer.close();

        JsonNode node = MAPPER.readTree(path.toFile());
        assertThat(node.get("completed").asBoolean()).isFalse();
        assertThat(node.get("stop_reason").isNull()).isTrue();
    }

    /**
     * A {@code STUCK} terminal's {@code TerminalStatus} carries {@code stop_source}/{@code
     * error_class}, and both land in the JSON exactly as supplied — the SAME pair {@code
     * ListCommand}'s {@code list_stuck_stop} marker
     * prints (derived via the shared {@code RunMetrics#stuckErrorClass} helper in production; this
     * test pins the writer's serialization independent of that derivation, which is covered
     * separately in {@code RunMetricsContractTest}/{@code JsonRunSummaryWiringTest}).
     */
    @Test
    void terminalPartialForStuckCarriesStopSourceAndErrorClass(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary snapshot = summary();
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> snapshot,
                () -> new JsonRunSummaryWriter.TerminalStatus(
                        StopReason.STUCK, "transient_retry_cap", "stuck_api_timeouts"));
        writer.close();

        JsonNode node = MAPPER.readTree(path.toFile());
        assertThat(node.get("completed").asBoolean()).isFalse();
        assertThat(node.get("stop_reason").asText()).isEqualTo("stuck");
        assertThat(node.get("stop_source").asText()).isEqualTo("transient_retry_cap");
        assertThat(node.get("error_class").asText()).isEqualTo("stuck_api_timeouts");
    }

    /**
     * Even a COMPLETED run's summary surfaces the recovered-error rollup — attempt-timeout
     * events, the timeout-shed / latency-freeze / connection-churn engagement counters, and the min
     * effective-T reached — so recovery never HIDES that errors happened.
     */
    @Test
    void completedSummaryCarriesTheRecoveredErrorRollup(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        // Drive the real RunMetrics so every meter is registered exactly as production wires it.
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        for (int i = 0; i < 12; i++) {
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
        }
        for (int i = 0; i < 3; i++) {
            metrics.recordTimeoutShed();
        }
        metrics.recordLatencyFreeze();
        metrics.recordLatencyFreeze();
        metrics.recordGrowthFreeze();
        metrics.recordConnectionAborted();
        metrics.recordConnectionHandshake();
        metrics.recordConnectionHandshake();
        metrics.recordConnectionHandshake();
        metrics.setConcurrencyTarget(64);
        metrics.setConcurrencyTarget(4);   // shed floor / ceiling hit

        RunSummary snapshot = summary();
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> snapshot);
        try {
            writer.complete(snapshot);
        } finally {
            writer.close();
        }

        JsonNode rollup = MAPPER.readTree(path.toFile()).get("recovered_errors");
        assertThat(rollup).as("the recovered-error rollup block must always appear").isNotNull();
        assertThat(rollup.get("attempt_timeout_events").asLong()).isEqualTo(12L);
        assertThat(rollup.get("timeout_sheds").asLong()).isEqualTo(3L);
        assertThat(rollup.get("latency_freezes").asLong()).isEqualTo(2L);
        assertThat(rollup.get("growth_freezes").asLong()).isEqualTo(1L);
        assertThat(rollup.get("connection_aborted").asLong()).isEqualTo(1L);
        assertThat(rollup.get("handshakes").asLong()).isEqualTo(3L);
        assertThat(rollup.get("min_effective_t").asLong())
                .as("the min effective-T the shed brake reached").isEqualTo(4L);
    }

    /**
     * {@link JsonRunSummaryWriter#close()} genuinely re-reads {@code snapshotSupplier} at close
     * time (not a cached/earlier snapshot) — so in the ordinary case the terminal partial IS
     * fresh, not stale. Distinguishes the periodic flush's duration from the close()-time
     * duration via a supplier that returns a strictly increasing duration on every call.
     */
    @Test
    void closeReadsAFreshSnapshotNotTheLastPeriodicFlush(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AtomicLong seq = new AtomicLong();
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMillis(20)), registry, Instant.now(),
                () -> summaryWithDuration(Duration.ofSeconds(seq.incrementAndGet())));
        long lastPeriodicDurationMs;
        try {
            await().atMost(5, TimeUnit.SECONDS).until(() -> Files.exists(path)
                    && MAPPER.readTree(path.toFile()).get("completed").asBoolean() == false);
            // Let at least 2 periodic flushes land so we have a definite "last periodic" value.
            await().atMost(5, TimeUnit.SECONDS).until(() -> seq.get() >= 2);
            Thread.sleep(50);   // let the in-flight periodic flush (if any) finish landing on disk
            lastPeriodicDurationMs = MAPPER.readTree(path.toFile()).get("duration_ms").asLong();
        } finally {
            writer.close();
        }

        long closeTimeDurationMs = MAPPER.readTree(path.toFile()).get("duration_ms").asLong();
        assertThat(closeTimeDurationMs)
                .as("close() must write a snapshot taken AT close time, not replay the last "
                        + "periodic flush's snapshot")
                .isGreaterThan(lastPeriodicDurationMs);
    }

    /**
     * {@link JsonRunSummaryWriter#close()} retries the terminal write and, if
     * every full attempt still fails, degrades to a {@code meters[]}-dropped fallback rather than
     * silently leaving the stale periodic-flush snapshot as final.
     *
     * <p>Failure is injected via the package-private {@link JsonRunSummaryWriter.WriteSink} seam
     * instead of OS permission bits, which flake under shared-tree gradle contention. A semantic
     * sink (fail while {@code meters[]} is present, i.e. every FULL attempt; succeed once it is
     * absent, i.e. the degraded attempt) is deterministic regardless of the exact retry count,
     * decoupling this test from {@code FINAL_WRITE_ATTEMPTS}'s private value.
     *
     * <p>Also pins that {@code stop_source}/{@code error_class} — terminal FACTS about why the
     * run stopped, not meters — survive the degraded write exactly like {@code completed}/{@code
     * stop_reason}/{@code as_of} do; only {@code meters[]} is dropped.
     */
    @Test
    void closeDegradesToAMetersDroppedFallbackWhenEveryFullWriteAttemptFails(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary snapshot = summaryWithDuration(Duration.ofSeconds(77));
        JsonRunSummaryWriter.WriteSink sink = root -> {
            if (root.has("meters")) {
                throw new IOException("simulated: full write always fails (fd exhaustion)");
            }
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), root);
        };
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> snapshot,
                () -> new JsonRunSummaryWriter.TerminalStatus(
                        StopReason.STUCK, "liveness_watchdog", "stuck_throttle"),
                sink);

        Logger logger =
                (Logger) LoggerFactory.getLogger(JsonRunSummaryWriter.class);
        ListAppender<ILoggingEvent> appender =
                new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            writer.close();   // must not throw — the never-crash-the-run invariant holds
        } finally {
            logger.detachAppender(appender);
        }

        JsonNode node = MAPPER.readTree(path.toFile());
        assertThat(node.has("meters")).as("the degraded fallback drops meters[]").isFalse();
        assertThat(node.get("completed").asBoolean()).isFalse();
        assertThat(node.get("duration_ms").asLong())
                .as("the degraded write is still the FRESH close()-time snapshot, not a stale replay")
                .isEqualTo(77_000L);
        assertThat(node.has("as_of")).as("the as_of marker survives the degraded write").isTrue();
        assertThat(node.get("stop_reason").asText()).isEqualTo("stuck");
        assertThat(node.get("stop_source").asText())
                .as("stop_source is a terminal fact, not a meter — it must survive the degraded write")
                .isEqualTo("liveness_watchdog");
        assertThat(node.get("error_class").asText())
                .as("error_class is a terminal fact, not a meter — it must survive the degraded write")
                .isEqualTo("stuck_throttle");

        String degraded = appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("summary_json_final_write_degraded"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no summary_json_final_write_degraded marker emitted"));
        assertThat(degraded).contains("path=").contains("attempts=");
    }

    /**
     * When EVERY attempt fails, including the degraded fallback, the never-crash-the-run
     * invariant still holds — but the failure is no longer silent: a grep-able
     * {@code ERROR} marker lands on the same stderr stream a fleet operator already scrapes
     * (logback routes {@code io.varve.swath} WARN/ERROR to {@code STDERR}). The pre-existing file
     * (simulating a stale periodic-flush snapshot from earlier in the run) is left untouched,
     * exactly as that invariant requires — the marker is what makes that fact visible now.
     */
    @Test
    void closeLogsAGrepableMarkerWhenEveryAttemptIncludingTheDegradedOneFails(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        Files.createDirectories(dir);
        Files.writeString(path, "{\"stale\":true}");   // simulates a surviving earlier flush
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary snapshot = summary();
        JsonRunSummaryWriter.WriteSink sink = root -> {
            throw new IOException("simulated: total I/O failure (fd/disk exhaustion)");
        };
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> snapshot, () -> null, sink);

        Logger logger =
                (Logger) LoggerFactory.getLogger(JsonRunSummaryWriter.class);
        ListAppender<ILoggingEvent> appender =
                new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            writer.close();   // must not throw
        } finally {
            logger.detachAppender(appender);
        }

        assertThat(Files.readString(path))
                .as("the never-crash-the-run invariant still holds: a totally-unwritable sink leaves the prior content untouched")
                .isEqualTo("{\"stale\":true}");

        String failed = appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("summary_json_final_write_failed"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "no summary_json_final_write_failed marker emitted — this is what a fleet "
                                + "operator greps to count 'no reliable terminal summary for this run'"));
        assertThat(failed).contains("path=").contains("attempts=");
    }

    /**
     * A retry that succeeds within the bounded attempt budget recovers the FULL
     * write (meters[] included) — the degraded fallback is a last resort, not the normal recovery
     * path — and says so at DEBUG (a transient blip already carried by the retry itself, not the
     * "no reliable summary" ERROR case).
     */
    @Test
    void closeRecoversTheFullWriteOnARetryAfterATransientFirstFailure(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary snapshot = summary();
        AtomicInteger calls = new AtomicInteger();
        JsonRunSummaryWriter.WriteSink sink = root -> {
            if (calls.incrementAndGet() == 1) {
                throw new IOException("simulated: one transient blip");
            }
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), root);
        };
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> snapshot, () -> null, sink);

        Logger logger =
                (Logger) LoggerFactory.getLogger(JsonRunSummaryWriter.class);
        ListAppender<ILoggingEvent> appender =
                new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        Level originalLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        try {
            writer.close();
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(originalLevel);
        }

        JsonNode node = MAPPER.readTree(path.toFile());
        assertThat(node.has("meters")).as("recovery within the retry budget keeps the FULL write").isTrue();
        assertThat(appender.list.stream().map(ILoggingEvent::getFormattedMessage)
                .anyMatch(m -> m.startsWith("summary_json_final_write_recovered"))).isTrue();
    }

    /**
     * Every write — periodic AND terminal — stamps a wall-clock {@code as_of} marker independent
     * of {@code duration_ms}/{@code started_at}, so a reader can always tell how fresh the file
     * is. Asserts it is present, ISO-8601 parseable, and strictly advances between a periodic
     * flush and the later terminal write.
     */
    @Test
    void asOfAdvancesBetweenAPeriodicFlushAndTheTerminalWrite(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary snapshot = summary();
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMillis(20)), registry, Instant.now(), () -> snapshot);
        Instant periodicAsOf;
        try {
            await().atMost(5, TimeUnit.SECONDS).until(() -> Files.exists(path)
                    && MAPPER.readTree(path.toFile()).has("as_of"));
            periodicAsOf = Instant.parse(MAPPER.readTree(path.toFile()).get("as_of").asText());
            Thread.sleep(50);   // ensure the terminal write's wall-clock stamp strictly advances
        } finally {
            writer.close();
        }

        Instant terminalAsOf = Instant.parse(MAPPER.readTree(path.toFile()).get("as_of").asText());
        assertThat(terminalAsOf)
                .as("as_of must advance on every write, not just once at process start")
                .isAfter(periodicAsOf);
    }

    /**
     * {@code close()} evaluates {@code snapshotSupplier.get()} INSIDE {@code
     * writeFinalPartialWithResilience}'s own try, not as an ARGUMENT in its caller's frame — so a
     * supplier that itself throws (e.g. a {@code RunSummary} built from partially-initialized
     * metrics state mid-abort) is swallowed like any other resilience-method failure, never
     * propagating out of {@code close()}. This is the regression test: {@code close()} must
     * swallow a throwing snapshot supplier and still emit a grep-able marker.
     */
    @Test
    void closeNeverThrowsWhenSnapshotSupplierThrows(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Supplier<RunSummary> throwingSupplier = () -> {
            throw new IllegalStateException("simulated: RunSummary built from partially-initialized state");
        };
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), throwingSupplier,
                () -> new JsonRunSummaryWriter.TerminalStatus(StopReason.CRASH));

        Logger logger =
                (Logger) LoggerFactory.getLogger(JsonRunSummaryWriter.class);
        ListAppender<ILoggingEvent> appender =
                new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            writer.close();   // must not throw — a JUnit test failure here IS the regression firing
        } finally {
            logger.detachAppender(appender);
        }

        assertThat(path).as("no summary was ever built, so nothing was ever written").doesNotExist();
        String marker = appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("summary_json_final_snapshot_failed"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no summary_json_final_snapshot_failed marker emitted"));
        assertThat(marker).contains("path=");
    }

    /** Sibling of {@link #closeNeverThrowsWhenSnapshotSupplierThrows}: the OTHER supplier throwing
     * (the terminal-status one, consulted in the same try) must be equally harmless. */
    @Test
    void closeNeverThrowsWhenTerminalReasonSupplierThrows(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunSummary snapshot = summary();
        Supplier<JsonRunSummaryWriter.TerminalStatus> throwingSupplier = () -> {
            throw new IllegalStateException("simulated: stop-reason classification threw");
        };
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> snapshot, throwingSupplier);

        Logger logger =
                (Logger) LoggerFactory.getLogger(JsonRunSummaryWriter.class);
        ListAppender<ILoggingEvent> appender =
                new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            writer.close();   // must not throw
        } finally {
            logger.detachAppender(appender);
        }

        assertThat(path).doesNotExist();
        assertThat(appender.list.stream().map(ILoggingEvent::getFormattedMessage)
                .anyMatch(m -> m.startsWith("summary_json_final_snapshot_failed"))).isTrue();
    }

    private static RunSummary summaryWithDuration(Duration duration) {
        return new RunSummary(
                42L, 1000L, 0L, duration, duration, duration, "WORK_STEALING", 118L, 0.00059,
                4L, 1234567L, 1000L, 112L, 61L, 16.5, 180L, 4200L, 232602L, 98L, 0L,
                4269.5, 1.07, 268435456L, 134217728L, 26.4, 1.03,
                2.0, 0.95, 0.1, 0.3, 0.6, 1.9,
                new RunSummary.SeedSummary("shallow", 5L, 12L, 4L, 13L),
                shape(), null, List.of(), List.of(), List.of(), null, null);
    }

    /** A clean run (target never driven by the engine) renders min_effective_t as null, not 0. */
    @Test
    void recoveredErrorRollupRendersMinEffectiveTNullWhenNeverSet(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new RunMetrics(registry);   // registers the meters; target never set
        RunSummary snapshot = summary();
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> snapshot);
        try {
            writer.complete(snapshot);
        } finally {
            writer.close();
        }

        JsonNode rollup = MAPPER.readTree(path.toFile()).get("recovered_errors");
        assertThat(rollup.get("attempt_timeout_events").asLong()).isZero();
        assertThat(rollup.get("min_effective_t").isNull()).isTrue();
        assertThat(rollup.get("freeze_gate_checks").asLong())
                .as("a run whose successes never reached the freeze gates reports zero TRIALS — "
                        + "which is what makes its zero freeze counts readable")
                .isZero();
    }

    /**
     * The freeze counters are only comparable against the trials they could have fired in, so the
     * denominator has to travel with them in the same block.
     */
    @Test
    void recoveredErrorRollupCarriesTheFreezeGateDenominator(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        metrics.recordFreezeGateCheck();
        metrics.recordFreezeGateCheck();
        metrics.recordFreezeGateCheck();
        metrics.recordLatencyFreeze();
        RunSummary snapshot = summary();
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> snapshot);
        try {
            writer.complete(snapshot);
        } finally {
            writer.close();
        }

        JsonNode rollup = MAPPER.readTree(path.toFile()).get("recovered_errors");
        assertThat(rollup.get("latency_freezes").asLong()).isEqualTo(1L);
        assertThat(rollup.get("freeze_gate_checks").asLong()).isEqualTo(3L);
    }

    /**
     * The regime's serial-baseline estimator is the {@code worker_page} DATA-page cost, not the
     * all-call-classes {@code api_latency_*} (which cheap structure/pivot probes pull low).
     */
    @Test
    void regimeReadsTheWorkerPagePercentilesOffTheCallClassRows(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        RunSummary snapshot = summaryWithCallClassLatency(List.of(
                // A cheap probe class and a non-total phase, both of which must be passed over.
                new RunSummary.CallClassLatencySummary(RunMetrics.CALL_CLASS_STRUCTURE_PROBE,
                        RunMetrics.LATENCY_PHASE_TOTAL, 12L, 8.0, 11.0, 14.0, 15.0),
                new RunSummary.CallClassLatencySummary(RunMetrics.CALL_CLASS_WORKER_PAGE,
                        RunMetrics.LATENCY_PHASE_TTFB, 900L, 33.0, 60.0, 140.0, 190.0),
                new RunSummary.CallClassLatencySummary(RunMetrics.CALL_CLASS_WORKER_PAGE,
                        RunMetrics.LATENCY_PHASE_TOTAL, 900L, 88.0, 150.0, 420.0, 610.0)));
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registryWithMeters(), Instant.now(), () -> snapshot);
        try {
            writer.complete(snapshot);
        } finally {
            writer.close();
        }

        JsonNode regime = MAPPER.readTree(path.toFile()).get("shape").get("regime");
        assertThat(regime.get("worker_page_latency_p50_ms").asDouble()).isEqualTo(88.0);
        assertThat(regime.get("worker_page_latency_p99_ms").asDouble()).isEqualTo(420.0);
        assertThat(regime.get("api_latency_p50_ms").asDouble())
                .as("the all-call-classes figure stays exactly as it was — the data-page pair is "
                        + "additive alongside it, not a redefinition")
                .isEqualTo(41.2);
    }

    /** {@link #summary()} with an explicit per-call-class latency row set. */
    private static RunSummary summaryWithCallClassLatency(
            List<RunSummary.CallClassLatencySummary> callClassLatency) {
        return new RunSummary(
                42L, 1000L, 0L, Duration.ofSeconds(25), Duration.ofSeconds(31), Duration.ofSeconds(20),
                "WORK_STEALING", 118L, 0.00059,
                4L, 1234567L, 1000L, 112L, 61L, 16.5, 180L, 4200L, 232602L, 98L, 0L,
                4269.5, 1.07, 268435456L, 134217728L, 26.4, 1.03,
                2.0, 0.95, 0.1, 0.3, 0.6, 1.9,
                new RunSummary.SeedSummary("shallow", 5L, 12L, 4L, 13L),
                shape(), null, List.of(), callClassLatency, clientCost(), null, null);
    }

    /**
     * End-to-end through {@link RunMetrics} over a fake clock: {@code listing_duration_ms} is the
     * listing-only clock, so it stops at the listing&rarr;merge boundary while {@code duration_ms}
     * runs on through the merge/publish tail.
     */
    @Test
    void listingDurationStopsAtTheMergeBoundaryWhileDurationRunsOn(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AtomicLong clock = new AtomicLong(1_000_000_000L);
        RunMetrics metrics = new RunMetrics(registry, clock::get);
        metrics.markRunStarted();
        clock.addAndGet(5_000_000_000L);    // 5s of listing
        metrics.markListingFinished();
        clock.addAndGet(10_000_000_000L);   // 10s of merge/publish

        RunSummary snapshot = metrics.summary(Duration.ofSeconds(15), "WORK_STEALING", 1L, 0L);
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> snapshot);
        try {
            writer.complete(snapshot);
        } finally {
            writer.close();
        }

        JsonNode root = MAPPER.readTree(path.toFile());
        assertThat(root.get("duration_ms").asLong()).isEqualTo(15_000L);
        assertThat(root.get("listing_duration_ms").asLong())
                .as("the merge lists nothing, so it is not part of the listing clock")
                .isEqualTo(5_000L);
    }

    /**
     * The rate numerator a consumer needs — {@code objects - recovered_objects} — has to be
     * computable from the artifact alone, not scraped off the {@code -v} progress line.
     */
    @Test
    void recoveredObjectsIsZeroOnAFreshRunAndCarriesTheBackfillOnAResume(@TempDir Path dir) throws Exception {
        Path fresh = dir.resolve("fresh.json");
        SimpleMeterRegistry freshRegistry = new SimpleMeterRegistry();
        RunMetrics freshMetrics = new RunMetrics(freshRegistry);
        freshMetrics.markRunStarted();
        freshMetrics.recordEntriesEmitted(900L);
        writeSummary(fresh, freshRegistry, freshMetrics.summary(Duration.ofSeconds(5), "WORK_STEALING", 1L, 0L));

        JsonNode freshRoot = MAPPER.readTree(fresh.toFile());
        assertThat(freshRoot.get("objects").asLong()).isEqualTo(900L);
        assertThat(freshRoot.get("recovered_objects").asLong())
                .as("a fresh run listed everything it reports — present as 0, never omitted")
                .isZero();

        Path resumed = dir.resolve("resumed.json");
        SimpleMeterRegistry resumedRegistry = new SimpleMeterRegistry();
        RunMetrics resumedMetrics = new RunMetrics(resumedRegistry);
        resumedMetrics.markRunStarted();
        resumedMetrics.recordRecoveredObjects(348L);   // the pre-crash durable rows, backfilled
        resumedMetrics.recordEntriesEmitted(900L);     // the tail this process actually relisted
        writeSummary(resumed, resumedRegistry,
                resumedMetrics.summary(Duration.ofSeconds(5), "WORK_STEALING", 1L, 0L));

        JsonNode resumedRoot = MAPPER.readTree(resumed.toFile());
        assertThat(resumedRoot.get("objects").asLong())
                .as("objects describes the DATASET, backfill included")
                .isEqualTo(1_248L);
        assertThat(resumedRoot.get("recovered_objects").asLong()).isEqualTo(348L);
        assertThat(resumedRoot.get("objects").asLong() - resumedRoot.get("recovered_objects").asLong())
                .as("the difference is the this-process-only numerator keys_per_sec divides")
                .isEqualTo(900L);
        assertThat(resumedRoot.get("efficiency").get("keys_per_sec").asDouble())
                .isCloseTo(900 / 5.0, within(1e-9));
    }

    /** Write one terminal summary and close, the shape every artifact assertion here starts from. */
    private static void writeSummary(Path path, SimpleMeterRegistry registry, RunSummary summary) {
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> summary);
        try {
            writer.complete(summary);
        } finally {
            writer.close();
        }
    }

    /** A run that never merges never stamps a boundary: the two clocks are one figure. */
    @Test
    void listingDurationEqualsDurationOnARunThatNeverMerges(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("summary.json");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AtomicLong clock = new AtomicLong(1_000_000_000L);
        RunMetrics metrics = new RunMetrics(registry, clock::get);
        metrics.markRunStarted();
        clock.addAndGet(15_000_000_000L);

        RunSummary snapshot = metrics.summary(Duration.ofSeconds(15), "WORK_STEALING", 1L, 0L);
        JsonRunSummaryWriter writer = JsonRunSummaryWriter.start(
                config(path, Duration.ofMinutes(10)), registry, Instant.now(), () -> snapshot);
        try {
            writer.complete(snapshot);
        } finally {
            writer.close();
        }

        JsonNode root = MAPPER.readTree(path.toFile());
        assertThat(root.get("listing_duration_ms").asLong())
                .isEqualTo(root.get("duration_ms").asLong())
                .isEqualTo(15_000L);
    }
}
