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
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A driver smoke test (NOT the PROP-1/CONC/RES critical case — covered separately):
 * a single seeded root node over a small deterministic {@link MockPageFetcher}
 * keyspace, {@code T=4}, small page size so workers idle and steal. Proves the
 * {@link WorkStealingScan} driver runs end-to-end: it <b>terminates</b> at quiescence
 * and emits <b>every key exactly once</b>.
 */
final class WorkStealingScanSmokeTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "smoke-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    @Timeout(30)
    void singleSeedTerminatesAndEmitsEveryKeyOnce(@TempDir Path dir) throws Exception {
        // ~2000 keys, small pages (64) ⇒ ~32 pages ⇒ several workers idle and steal.
        List<byte[]> keyspace = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            keyspace.add(b(String.format("data/%05d", i)));
        }
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);
            assertThat(seeds).hasSize(1);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
                    fetcher, store, 4, 64, seeds, FilterChain.EMPTY);

            List<byte[]> emitted = new ArrayList<>();
            PipelineDrain.collectKeys(1000, engine, emitted);

            // Exactly-once: no duplicates and the emitted set equals the whole keyspace.
            TreeSet<byte[]> distinct = new TreeSet<>(Arrays::compareUnsigned);
            distinct.addAll(emitted);
            assertThat(emitted).hasSize(keyspace.size());   // no duplicates
            assertThat(distinct).hasSize(keyspace.size());  // every key present
        }
    }
}
