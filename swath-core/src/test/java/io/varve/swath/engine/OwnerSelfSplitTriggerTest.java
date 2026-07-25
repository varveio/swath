/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

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
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Light trigger/rate-limit coverage for owner-side proactive self-split. Verifies
 * the <b>trigger conditions</b> of {@code OwnerSelfSplit.maybeOwnerSelfSplit} — that a large
 * <b>bounded</b> dense drain publishes owner splits ({@code OWNER_SPLIT.self_published} in the steal
 * reasons), while a small range and the open frontier do <b>not</b>. It does NOT assert the
 * no-gap/no-overlap guarantee (that acceptance test is covered separately); the byte-exact partition
 * guard lives in the PROP-1 suite.
 */
final class OwnerSelfSplitTriggerTest {

    private static final String OWNER_SPLIT_KEY = "OWNER_SPLIT.self_published";
    private static final int MAX_KEYS = 100;

    /** A dense flat directory {@code d/000000..} of {@code n} uniform keys, all below the bound {@code e}. */
    private static List<byte[]> denseFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("d/%06d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    private static RunKey key(String label) {
        return new RunKey("s3", null, "bucket", new byte[0], label,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static NodeSpec seed(long runId, byte[] lo, byte[] hi) {
        // A single seed (lo, hi]. A TIGHT window around the dense keys keeps the density / estRemaining
        // math meaningful (a ⊥ lower bound over high-clustered keys would spread the window over empty
        // low space and collapse the estimate).
        return new NodeSpec(runId, null, NodeKind.RANGE, lo, hi, null, null);
    }

    private long runScan(Path dir, String label, List<byte[]> keyspace, byte[] lo, byte[] hi, int workers)
            throws Exception {
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(key(label), false, false);
            store.insertNode(seed(run.id(), lo, hi));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    mock, store, workers, MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(2000, engine, emitted);
        }
        // Smoke: every input key surfaced exactly once (light sanity, not the adversarial suite).
        assertThat(emitted).hasSize(keyspace.size());
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        return reasons.getOrDefault(OWNER_SPLIT_KEY, 0L);
    }

    private static final byte[] LO = "d/00".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HI = "d/02".getBytes(StandardCharsets.UTF_8);

    @Test
    @Timeout(60)
    void largeBoundedDenseDrainPublishesOwnerSplits(@TempDir Path dir) throws Exception {
        // ~20k keys / 100 per page ≈ 200 pages of remaining work ≫ the 4-page trigger threshold, so the
        // draining owner self-publishes at least one far-ahead child (and, rate-limited to one per 32
        // pages, O(1) of them) — the progress-gated race-killer firing.
        long published = runScan(dir, "r1-dense", denseFlat(20_000), LO, HI, 4);
        assertThat(published).isGreaterThanOrEqualTo(1L);
    }

    @Test
    @Timeout(60)
    void smallBoundedRangeNeverSelfSplits(@TempDir Path dir) throws Exception {
        // 50 keys total < the 4×maxKeys (=400) estRemaining threshold ⇒ the owner never self-splits.
        long published = runScan(dir, "r1-small", denseFlat(50), LO, HI, 4);
        assertThat(published).isZero();
    }

    @Test
    @Timeout(60)
    void openFrontierNeverSelfSplits(@TempDir Path dir) throws Exception {
        // hi == null (open frontier), single worker so no thief ever bounds it: the owner keeps its
        // density-extrapolation path and must NOT self-split, no matter how dense — bounded-range only.
        long published = runScan(dir, "r1-frontier", denseFlat(20_000), null, null, 1);
        assertThat(published).isZero();
    }
}
