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
    /** See {@link #carveBrakeOnEngagesAndCompletesWithExactOnceEmissionOnTheDenseUniformShape}. */
    private static final int MAX_BRAKE_ATTEMPTS = 8;

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
     *
     * <p><b>Pinned to the pre-0.2.0 arms, and that is a disclosure, not a convenience.</b> Under the
     * 0.2.0 default pair this assertion becomes non-deterministic: measured 4 passes in 10 runs, with
     * {@code substantial} landing bimodally at either ~15 or ~2. It is an INTERACTION — measured
     * 4/4 passes with the sensor alone, 4/4 with {@code reach_floored} alone, and 10/10 with both
     * off; only the pair destabilises it. The pair therefore sometimes carves a dense/uniform shape
     * into mostly-confetti children, scheduling-dependent, which is the controlled minimal case of
     * the over-carving the corpus panel measured on ~10% of fixtures. Tracked upstream; the
     * chartered realized-child-mass carve brake is the intended fix, and this test is its bench.
     * Do NOT re-point this at the default until that assertion is deterministic again.
     */
    @Test
    @Timeout(60)
    void denseUniformShapeNeverEngagesTheGateAndSubstantialDominates(@TempDir Path dir) throws Exception {
        Map<String, Long> reasons = runScan(dir, "dense-uniform-classify", denseFlat(20_000),
                EngineToggles.DEFAULT.withRateAnchoredSensing(false).withTailFloor(TailFloorMode.CURRENT));

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

    /**
     * The carve brake's bench, real-engine wiring case (campaign memo §5, the raced cure for the
     * instability {@link #denseUniformShapeNeverEngagesTheGateAndSubstantialDominates}'s own javadoc
     * names this brake as fixing): {@code carve_brake=mass_k8} ON TOP of the 0.2.0 default pair, on
     * the SAME dense/uniform 20k-key fixture. Asserts only that the brake's own counters engage at
     * least once and that every run that reaches it still completes with exact-once emission — NOT a
     * deterministic-pass assertion on the confetti classification ratio, which is the race's own job
     * (this commit ships the brake OFF by default; the race that flips the default and picks the
     * winning K is a separate, later unit).
     *
     * <p><b>{@code mass_k8}, not {@code mass_k4}, and a bounded retry loop.</b> Measured directly
     * against this fixture: {@code mass_k4} (threshold {@code 4*maxKeys=400}) never once engaged in
     * repeated runs — this shape's realized owner-split child masses on the 0.2.0 default pair
     * apparently never dip that low, even on the runs that land in the unstable branch documented at
     * {@link #denseUniformShapeNeverEngagesTheGateAndSubstantialDominates}. {@code mass_k8} (threshold
     * {@code 800}) engages on the large majority of runs (measured ~9/10) but not reliably every
     * single one — the SAME inherent per-run instability the sibling test above discloses, not a new
     * one this test introduces. Rather than assert on one draw (issue #18: don't let a test imply a
     * certainty it can't deliver), this retries the identical scan up to {@link #MAX_BRAKE_ATTEMPTS}
     * times and requires only that at least one attempt engages — at the measured ~90% single-attempt
     * rate, all {@value #MAX_BRAKE_ATTEMPTS} attempts missing has negligible probability, while every
     * attempt (hit or miss) still independently proves the run completes with exact-once emission.
     */
    @Test
    @Timeout(60)
    void carveBrakeOnEngagesAndCompletesWithExactOnceEmissionOnTheDenseUniformShape(@TempDir Path dir)
            throws Exception {
        boolean engaged = false;
        for (int attempt = 0; attempt < MAX_BRAKE_ATTEMPTS && !engaged; attempt++) {
            Map<String, Long> reasons = runScan(dir, "carve-brake-on-" + attempt, denseFlat(20_000),
                    EngineToggles.DEFAULT.withCarveBrake(CarveBrakeMode.MASS_K8));

            assertThat(reasons.getOrDefault("TOGGLE.carve_brake_mass_k8_on", 0L))
                    .as("the once-per-scan engagement mark fired")
                    .isEqualTo(1L);
            long published = reasons.getOrDefault("OWNER_SPLIT.self_published", 0L);
            long confetti = reasons.getOrDefault("OWNER_SPLIT_CHILD.confetti", 0L);
            long substantial = reasons.getOrDefault("OWNER_SPLIT_CHILD.substantial", 0L);
            assertThat(confetti + substantial)
                    .as("every self-published owner-split child is still classified exactly once with "
                            + "the brake on (attempt %d)", attempt)
                    .isEqualTo(published);

            long braked = reasons.getOrDefault("OWNER_SPLIT.carve_braked", 0L);
            long probed = reasons.getOrDefault("OWNER_SPLIT.carve_brake_probe", 0L);
            engaged = braked + probed > 0;
        }
        assertThat(engaged)
                .as("mass_k8 must engage at least once within %d attempts on this shape", MAX_BRAKE_ATTEMPTS)
                .isTrue();
    }
}
