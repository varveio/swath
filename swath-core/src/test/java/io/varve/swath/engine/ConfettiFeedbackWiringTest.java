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
 * Basic engine-level wiring sanity for the confetti feedback gate — that every tagged owner-split
 * child gets classified exactly once through the REAL engine
 * ({@code OWNER_SPLIT_CHILD.confetti}/{@code .substantial} sum to {@code OWNER_SPLIT.self_published}),
 * and that {@code confetti_feedback=off} disables tagging/classification entirely (bit-for-bit
 * bypass) while leaving owner-split itself unaffected. This is an ordinary MockPageFetcher-driven
 * wiring smoke, like {@link OwnerSplitKillSwitchTest} — NOT the adversarial skewed-keyspace
 * regression that proves the gate actually suppresses confetti in the field (a separate suite).
 */
final class ConfettiFeedbackWiringTest {

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

    private Map<String, Long> runScan(Path dir, String label, List<byte[]> keyspace, EngineToggles toggles)
            throws Exception {
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(key(label), false, false);
            store.insertNode(seed(run.id(), LO, HI));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles),
                    mock, store, 4, MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(2000, engine, emitted);
        }
        assertThat(emitted).hasSize(keyspace.size());
        return metrics.diagnostics(Duration.ZERO).stealReasons();
    }

    @Test
    @Timeout(60)
    void confettiFeedbackOnClassifiesEveryTaggedChildExactlyOnce(@TempDir Path dir) throws Exception {
        Map<String, Long> reasons = runScan(dir, "confetti-on", denseFlat(20_000), EngineToggles.DEFAULT);

        long published = reasons.getOrDefault("OWNER_SPLIT.self_published", 0L);
        assertThat(published).as("this shape must actually owner-split").isGreaterThanOrEqualTo(1L);

        long confetti = reasons.getOrDefault("OWNER_SPLIT_CHILD.confetti", 0L);
        long substantial = reasons.getOrDefault("OWNER_SPLIT_CHILD.substantial", 0L);
        assertThat(confetti + substantial)
                .as("every self-published owner-split child must be classified exactly once")
                .isEqualTo(published);
    }

    @Test
    @Timeout(60)
    void confettiFeedbackOffNeverTagsOrClassifiesButOwnerSplitStillFires(@TempDir Path dir) throws Exception {
        EngineToggles confettiFeedbackOff = EngineToggles.DEFAULT.withConfettiFeedback(false);
        Map<String, Long> reasons = runScan(dir, "confetti-off", denseFlat(20_000), confettiFeedbackOff);

        long published = reasons.getOrDefault("OWNER_SPLIT.self_published", 0L);
        assertThat(published)
                .as("confetti_feedback=off must not disable owner-split itself")
                .isGreaterThanOrEqualTo(1L);

        assertThat(reasons.getOrDefault("OWNER_SPLIT_CHILD.confetti", 0L)).isZero();
        assertThat(reasons.getOrDefault("OWNER_SPLIT_CHILD.substantial", 0L)).isZero();
        assertThat(reasons.getOrDefault("OWNER_SPLIT.confetti_suppressed", 0L)).isZero();
        assertThat(reasons.getOrDefault("OWNER_SPLIT.confetti_probe", 0L)).isZero();
        assertThat(reasons.getOrDefault("TOGGLE.confetti_feedback_off", 0L))
                .as("the once-per-scan TOGGLE mark fired")
                .isEqualTo(1L);
    }

    /**
     * Classification rule: a tagged child is confetti only if it BOTH has a small own tally AND
     * never itself split. On a dense/uniform range most owner-split children ARE intermediate nodes
     * that themselves split further — {@code OWNER_SPLIT_CHILD.substantial} must dominate here, and
     * the gate must not engage at all (this shape has no skew, so its realized-mass evidence should
     * never trip the suppression threshold) — "zero-regression-by-construction" on the mechanism's
     * own target shape.
     */
    @Test
    @Timeout(60)
    void denseUniformShapeNeverEngagesTheGateAndSubstantialDominates(@TempDir Path dir) throws Exception {
        Map<String, Long> reasons = runScan(dir, "dense-uniform-classify", denseFlat(20_000), EngineToggles.DEFAULT);

        assertThat(reasons.getOrDefault("OWNER_SPLIT.confetti_suppressed", 0L))
                .as("a non-skewed dense/uniform shape must never trip the suppression threshold")
                .isZero();

        long confetti = reasons.getOrDefault("OWNER_SPLIT_CHILD.confetti", 0L);
        long substantial = reasons.getOrDefault("OWNER_SPLIT_CHILD.substantial", 0L);
        assertThat(substantial)
                .as("on a dense/uniform shape, intermediate nodes that split further must dominate "
                        + "the classification (confetti=%d, substantial=%d)", confetti, substantial)
                .isGreaterThan(confetti);
    }
}
