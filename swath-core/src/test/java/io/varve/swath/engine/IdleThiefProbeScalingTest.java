/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * When workers greatly outnumber splittable work, idle thieves must not turn every park cycle into
 * a real 1-key store probe. The exact probe count is scheduling-sensitive, so this compares the same
 * keyspace at 8 vs. 64 workers and rejects near-linear worker scaling.
 */
final class IdleThiefProbeScalingTest {

    private static final int LOW_WORKERS = 8;
    private static final int HIGH_WORKERS = 64;
    private static final int OBJECTS = 16_000;
    private static final int MAX_KEYS = 1_000;
    private static final Duration BULK_PAGE_LATENCY = Duration.ofMillis(15);

    static Stream<Arguments> keyspaces() {
        return Stream.of(
                Arguments.of("deep-nested", Keyspaces.deepTree(42L, 16, OBJECTS / 16)),
                Arguments.of("flat", Keyspaces.singlePrefixFlat(OBJECTS)));
    }

    private static RunKey key(String shape, int workers) {
        return new RunKey("s3", null, "bucket", new byte[0], "probe-storm-" + shape + "-" + workers,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private record Run(long apiCalls, int probes, List<byte[]> emitted) {
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("keyspaces")
    @Timeout(60)
    void idleProbeVolumeIsBoundedAcrossWorkerCounts(String shape, List<byte[]> keyspace, @TempDir Path dir)
            throws Exception {
        Run low = scan(dir.resolve(shape + "-low.sqlite"), shape, keyspace, LOW_WORKERS);
        Run high = scan(dir.resolve(shape + "-high.sqlite"), shape, keyspace, HIGH_WORKERS);

        EngineHarness.assertExactlyOnce(low.emitted(), keyspace);
        EngineHarness.assertExactlyOnce(high.emitted(), keyspace);

        assertThat(high.apiCalls())
                .as("64-worker API calls should stay within a small factor of 8-worker calls; low=%d high=%d",
                        low.apiCalls(), high.apiCalls())
                .isLessThanOrEqualTo(4L * low.apiCalls());
        assertThat(high.probes())
                .as("64-worker 1-key probes should stay within a small factor of 8-worker probes; low=%d high=%d",
                        low.probes(), high.probes())
                .isLessThanOrEqualTo(Math.max(4 * low.probes(), 32));
    }

    private static Run scan(Path db, String shape, List<byte[]> keyspace, int workers) throws Exception {
        AtomicInteger probes = new AtomicInteger();
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                .latency(req -> req.maxKeys() > 1 ? BULK_PAGE_LATENCY : Duration.ZERO)
                .interceptor((PageRequest req, int idx, ListPage page) -> {
                    if (req.maxKeys() == 1) {
                        probes.incrementAndGet();
                    }
                    return page;
                })
                .build();

        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key(shape, workers), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);
            assertThat(seeds).hasSize(1);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
                    mock, store, workers, MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(1000, engine, emitted);
        }
        return new Run(mock.apiCalls(), probes.get(), emitted);
    }
}
