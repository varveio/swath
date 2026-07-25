/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Run-trace unit coverage (docs/internals/metrics-internals.md §7). Two things this driver-level
 * test owns:
 * <ol>
 *   <li>a deterministic {@link MockPageFetcher} run with {@code --trace} wired asserts the JSONL
 *       event SEQUENCE for a known small shape — seed, pages, at least one split with a mechanism
 *       tag, and completion — every line parsing as valid JSON with the {@code "v":1} schema
 *       envelope;</li>
 *   <li>the traces-OFF default: a {@link WorkStealingScan} built without an explicit
 *       {@link TraceSink} wires exactly the {@link TraceSink#NONE} singleton (not merely an
 *       inert-but-allocated no-op), proving the zero-cost-when-off path is the real default, not
 *       an accident of behavior.</li>
 * </ol>
 * NOT the PROP-1/CONC/RES interleavings (covered separately) — this is ordinary unit coverage for
 * the trace wiring itself.
 */
final class WorkStealingScanTraceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key(String hash) {
        return new RunKey("s3", null, "bucket", new byte[0], hash,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    @Timeout(30)
    void tracedRun_emitsValidSchemaEventSequence_seedThroughCompletion(@TempDir Path dir) throws Exception {
        // ~2000 keys, small pages (64) ⇒ several workers idle and steal (WorkStealingScanSmokeTest's
        // shape) ⇒ at least one split fires with an attributed pivot mechanism.
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            keyspace.add(b(String.format("data/%05d", i)));
        }
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        Path traceFile = dir.resolve("run.trace.jsonl");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key("trace-hash"), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            TraceSink trace = TraceSink.jsonl(traceFile);
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, new RunMetrics(
                            new SimpleMeterRegistry())).withTrace(trace),
                    fetcher, store, 4, 64, seeds, FilterChain.EMPTY);
            assertThat(engine.trace()).isSameAs(trace);

            try {
                // page contents are irrelevant here — WorkStealingScanSmokeTest covers I2/I3
                PipelineDrain.discard(1000, engine);
            } finally {
                trace.close();
            }
        }

        List<String> lines = Files.readAllLines(traceFile, StandardCharsets.UTF_8);
        assertThat(lines).as("trace file has events").isNotEmpty();

        Set<String> eventTypes = new HashSet<>();
        boolean sawSplitMechanism = false;
        for (String line : lines) {
            JsonNode node = MAPPER.readTree(line);   // every line must parse as standalone JSON
            assertThat(node.get("v").asInt()).as("schema version").isEqualTo(1);
            assertThat(node.hasNonNull("event")).as("event field present").isTrue();
            assertThat(node.hasNonNull("ts_ns")).as("monotonic ts present").isTrue();
            assertThat(node.hasNonNull("ts_ms")).as("wall ts present").isTrue();
            assertThat(node.hasNonNull("worker_id")).as("worker_id present").isTrue();
            String event = node.get("event").asText();
            eventTypes.add(event);
            if (event.equals("split") || event.equals("owner_split")) {
                assertThat(node.hasNonNull("mechanism")).as("split carries a pivot mechanism tag").isTrue();
                assertThat(node.hasNonNull("child_node_id")).isTrue();
                assertThat(node.hasNonNull("pivot")).isTrue();
                sawSplitMechanism = true;
            }
            if (event.equals("claimed")) {
                assertThat(node.hasNonNull("node_id")).isTrue();
            }
            if (event.equals("steal_attempt")) {
                assertThat(node.hasNonNull("outcome")).isTrue();
                assertThat(node.hasNonNull("reason")).isTrue();
            }
        }

        assertThat(eventTypes).as("full range lifecycle observed")
                .contains("seeded", "claimed", "page_committed", "steal_attempt", "completed");
        assertThat(sawSplitMechanism).as("at least one split fired with a mechanism tag").isTrue();
        assertThat(lines.getFirst()).as("the very first event is the initial seed").contains("\"event\":\"seeded\"");
    }

    @Test
    @Timeout(30)
    void untracedRun_defaultsToTheNoopSingleton(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = List.of(b("a"), b("b"), b("c"));
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key("no-trace-hash"), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            // A default context — no TraceSink supplied, so the trace seam coalesces to the no-op.
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
                    fetcher, store, 2, 64, seeds, FilterChain.EMPTY);

            assertThat(engine.trace()).as("default wiring is the exact no-op singleton")
                    .isSameAs(TraceSink.NONE);
            assertThat(engine.trace().enabled()).isFalse();
        }
    }
}
