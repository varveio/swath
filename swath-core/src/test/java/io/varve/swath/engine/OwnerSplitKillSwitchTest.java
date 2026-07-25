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
 * Kill-switch coverage for the {@code --no-owner-split} experimental flag. On the same
 * large bounded dense drain that makes {@code maybeOwnerSelfSplit} fire, disabling owner-split
 * ({@code ownerSplitEnabled=false}) must suppress every {@code OWNER_SPLIT.self_published} event while
 * still emitting every key exactly once (pure thief-stealing fallback), and the default
 * ({@code true}) must still publish owner splits. Correctness (byte-exact partition) is the PROP-1
 * suite's job; this only guards that the switch actually gates the owner-split path.
 */
final class OwnerSplitKillSwitchTest {

    private static final String OWNER_SPLIT_KEY = "OWNER_SPLIT.self_published";
    private static final int MAX_KEYS = 100;

    private static final byte[] LO = "d/00".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HI = "d/02".getBytes(StandardCharsets.UTF_8);

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
        return new NodeSpec(runId, null, NodeKind.RANGE, lo, hi, null, null);
    }

    private record ScanResult(long ownerSplitsPublished, Map<String, Long> stealReasons) {
    }

    private ScanResult runScanFull(Path dir, String label, List<byte[]> keyspace, boolean ownerSplitEnabled)
            throws Exception {
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(key(label), false, false);
            store.insertNode(seed(run.id(), LO, HI));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(ownerSplitEnabled ? EngineToggles.DEFAULT : EngineToggles.DEFAULT.withOwnerSplit(false)),
                    mock, store, 4, MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(2000, engine, emitted);
        }
        // Both configs must surface every input key exactly once — the kill-switch changes the
        // parallelization strategy, never the emitted set.
        assertThat(emitted).hasSize(keyspace.size());
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        return new ScanResult(reasons.getOrDefault(OWNER_SPLIT_KEY, 0L), reasons);
    }

    private long runScan(Path dir, String label, List<byte[]> keyspace, boolean ownerSplitEnabled)
            throws Exception {
        return runScanFull(dir, label, keyspace, ownerSplitEnabled).ownerSplitsPublished();
    }

    @Test
    @Timeout(60)
    void ownerSplitEnabledPublishesSplits(@TempDir Path dir) throws Exception {
        long published = runScan(dir, "kill-on", denseFlat(20_000), true);
        assertThat(published).isGreaterThanOrEqualTo(1L);
    }

    @Test
    @Timeout(60)
    void killSwitchSuppressesAllOwnerSplits(@TempDir Path dir) throws Exception {
        long published = runScan(dir, "kill-off", denseFlat(20_000), false);
        assertThat(published).isZero();
    }

    /**
     * {@code owner_split} engagement smoke (§5 discipline): {@code --no-owner-split} /
     * {@code --engine-toggle owner_split=off} is the same {@link EngineToggles#ownerSplit()}
     * master switch, and the OFF state is provable post-hoc even with zero listing activity — the
     * {@code TOGGLE.owner_split_off} mark fires once at engine construction, independent of whether
     * {@code OWNER_SPLIT.self_published} would otherwise have fired on this shape.
     */
    @Test
    @Timeout(60)
    void toggleOffMarkFiresOnlyWhenOwnerSplitDisabled(@TempDir Path dir) throws Exception {
        ScanResult on = runScanFull(dir, "toggle-mark-on", denseFlat(200), true);
        assertThat(on.stealReasons().getOrDefault("TOGGLE.owner_split_off", 0L)).isZero();

        ScanResult off = runScanFull(dir, "toggle-mark-off", denseFlat(200), false);
        assertThat(off.stealReasons().getOrDefault("TOGGLE.owner_split_off", 0L)).isEqualTo(1L);
    }
}
