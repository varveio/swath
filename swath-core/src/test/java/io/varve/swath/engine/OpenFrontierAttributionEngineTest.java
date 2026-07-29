/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Engine-level fixture for issue #76: a run whose mass sits mostly past the last seed cut must show
 * the open-frontier signals actually MOVE, not merely exist. Seeded by hand with exactly two ranges —
 * a small bounded head {@code (⊥, cut]} and the final open tile {@code (cut, null]} (the same shape
 * {@code SeedStep} always closes a plan with) — and a single worker, so no thief ever narrows the open
 * tile's {@code hi} away from {@code null}: every one of its page commits is attributable to the
 * open frontier, for a clean, deterministic share.
 */
final class OpenFrontierAttributionEngineTest {

    private static final int MAX_KEYS = 100;
    private static final byte[] CUT = "aaaz".getBytes(StandardCharsets.UTF_8);

    /** 50 small head keys strictly below {@link #CUT}, 950 tail keys strictly above it. */
    private static List<byte[]> keyspace() {
        List<byte[]> keys = new ArrayList<>(1000);
        for (int i = 0; i < 50; i++) {
            keys.add(String.format("aaa%03d", i).getBytes(StandardCharsets.UTF_8));
        }
        for (int i = 0; i < 950; i++) {
            keys.add(String.format("zzz%04d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "open-frontier-attribution",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    @Timeout(60)
    void openFrontierTailShowsUpInBothNewSignals(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = keyspace();
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("open-frontier.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            // Hand-seeded exactly as SeedStep always closes a plan: one bounded head tile, then the
            // final open tile (cut, null] -- the range OwnerSplitGovernor can never self-split.
            List<NodeSpec> specs = List.of(
                    new NodeSpec(run.id(), null, NodeKind.RANGE, null, CUT, null, null),
                    new NodeSpec(run.id(), null, NodeKind.RANGE, CUT, null, CUT, null));
            store.insertNodes(specs);
            List<Node> seeds = store.loadResumable(run.id(), false);

            // A single worker: no thief ever exists to narrow the open tile's hi away from null, so
            // every one of its page commits is unambiguously attributable to the open frontier.
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    mock, store, 1, MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(2000, engine, emitted);
        }
        EngineHarness.assertExactlyOnce(emitted, keyspace);

        Counter openFrontierKeys = metrics.registry().find("swath.open_frontier.keys_emitted").counter();
        assertThat(openFrontierKeys).as("the new keys-emitted gauge is registered").isNotNull();
        assertThat(openFrontierKeys.count())
                .as("the tail's mass (950 of 1000 keys) sat past the last seed cut, and the signal moves")
                .isEqualTo(950.0);

        Counter openFrontierReason = metrics.registry().find("swath.steal_reason")
                .tags("outcome", "OWNER_SPLIT", "reason", "open_frontier").counter();
        assertThat(openFrontierReason).as("the skip is now counted like its siblings (issue #76)").isNotNull();
        assertThat(openFrontierReason.count())
                .as("the open-frontier skip engaged at least once while draining the tail")
                .isGreaterThan(0.0);
    }
}
