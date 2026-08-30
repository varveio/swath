/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.ExcludeRegexFilter;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Pins the classified engagement reasons for zero-row sort completion markers. */
class WorkStealingScanCompletionMetricsTest {

    @Test
    void emptyTerminalPageRecordsItsCompletionMarkerCause(@TempDir Path dir) throws Exception {
        SimpleMeterRegistry registry = run(dir, "empty", List.of(), FilterChain.EMPTY);

        assertThat(reasonCount(registry, "completion_marker_empty_page")).isEqualTo(1);
        assertThat(reasonCount(registry, "completion_marker_filtered_page")).isZero();
    }

    @Test
    void fullyFilteredTerminalPageRecordsItsCompletionMarkerCause(@TempDir Path dir) throws Exception {
        SimpleMeterRegistry registry = run(dir, "filtered", List.of(bytes("a")),
                FilterChain.of(List.of(ExcludeRegexFilter.of(".*"))));

        assertThat(reasonCount(registry, "completion_marker_empty_page")).isZero();
        assertThat(reasonCount(registry, "completion_marker_filtered_page")).isEqualTo(1);
    }

    private static SimpleMeterRegistry run(Path dir, String label, List<byte[]> keys,
            FilterChain filters) throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(new RunKey("s3", null, "bucket", new byte[0], label,
                    "WORK_STEALING", ListingMode.OBJECTS, "", "parquet"), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    fetcher, store, 1, 1000, seeds, filters);
            engine.enableSortPacking(entries -> {
                throw new AssertionError("zero-row completion must not invoke the page packer");
            });

            PipelineDrain.discard(4, engine);
        }
        return registry;
    }

    private static double reasonCount(SimpleMeterRegistry registry, String reason) {
        return registry.counter("swath.steal_reason", "outcome", "SORT", "reason", reason).count();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
