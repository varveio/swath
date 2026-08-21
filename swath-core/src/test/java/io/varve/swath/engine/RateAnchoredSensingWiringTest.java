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
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves, through the REAL {@link WorkStealingScan}, that {@code --engine-toggle
 * rate_anchored_sensing=on} actually installs {@link RateAnchoredEstimator} as the run's position
 * sensor, and that a run says so in its metrics alone (AGENTS.md's instrument-every-algo-path rule):
 * the once-per-run {@code TOGGLE.rate_anchored_sensing_on} mark for the route, and the sensor's own
 * per-site {@code SENSING_OWNER.*}/{@code SENSING_STEAL.*} classification counters for what its band
 * did to the estimates the gates consumed. The default arm is the control: the identical keyspace,
 * byte-identical coverage, and total silence on both counters.
 */
final class RateAnchoredSensingWiringTest {

    /** A bounded root {@code (⊥, hi]}: the owner-split gate and victim selection both score it. */
    private static final byte[] HI = "key0".getBytes(StandardCharsets.UTF_8);

    private static RunKey key(String hash) {
        return new RunKey("s3", null, "bucket", new byte[0], hash,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private record ScanResult(List<byte[]> emitted, Map<String, Long> stealReasons) {
    }

    private static ScanResult runBoundedRoot(Path dir, String label, List<byte[]> keyspace,
                                             EngineToggles toggles) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(key(label), false, false);
            store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, null, HI, null, null));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles),
                    fetcher, store, 4, 20, seeds, FilterChain.EMPTY);
            PipelineDrain.collectKeys(5000, engine, emitted);
        }
        return new ScanResult(emitted, metrics.diagnostics(Duration.ZERO).stealReasons());
    }

    private static void assertExactlyOnce(List<byte[]> emitted, List<byte[]> keyspace) {
        TreeSet<byte[]> distinctKeyspace = new TreeSet<>(Arrays::compareUnsigned);
        distinctKeyspace.addAll(keyspace);
        TreeSet<byte[]> distinctEmitted = new TreeSet<>(Arrays::compareUnsigned);
        distinctEmitted.addAll(emitted);
        assertThat(emitted).as("no duplicate emissions").hasSize(distinctEmitted.size());
        assertThat(distinctEmitted).as("full byte-exact coverage, no duplicates").isEqualTo(distinctKeyspace);
    }

    private static long sumCategory(Map<String, Long> reasons, String category) {
        return reasons.entrySet().stream()
                .filter(e -> e.getKey().startsWith(category + "."))
                .mapToLong(Map.Entry::getValue).sum();
    }

    @Test
    @Timeout(60)
    void theToggleInstallsThePortedSensorAndTheRunSaysSoInItsCountersAlone(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = Keyspaces.exactly(2000);
        EngineToggles toggles = EngineToggles.parse(List.of("rate_anchored_sensing=on"), false);
        assertThat(toggles.remainingWorkEstimator(20)).isInstanceOf(RateAnchoredEstimator.class);

        ScanResult result = runBoundedRoot(dir, "sensing-on", keyspace, toggles);
        assertExactlyOnce(result.emitted(), keyspace);

        assertThat(result.stealReasons().getOrDefault("TOGGLE.rate_anchored_sensing_on", 0L))
                .as("the once-per-run route mark fired").isEqualTo(1L);
        assertThat(sumCategory(result.stealReasons(), "SENSING_OWNER")
                + sumCategory(result.stealReasons(), "SENSING_STEAL"))
                .as("and the installed sensor classified the readings the gates consumed")
                .isGreaterThan(0L);
    }

    /**
     * Since the 0.2.0 default flip the promoted sensor IS the default, so the default run installs
     * it and marks itself — post-hoc analysis reads which sensor ran rather than assuming it.
     */
    @Test
    @Timeout(60)
    void theDefaultInstallsThePromotedSensorAndMarksItsArm(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = Keyspaces.exactly(2000);
        assertThat(EngineToggles.DEFAULT.rateAnchoredSensing()).isTrue();
        assertThat(EngineToggles.DEFAULT.remainingWorkEstimator(20))
                .as("the default resolves to the promoted rate-anchored estimator")
                .isInstanceOf(RateAnchoredEstimator.class);

        ScanResult result = runBoundedRoot(dir, "sensing-default", keyspace, EngineToggles.DEFAULT);
        assertExactlyOnce(result.emitted(), keyspace);

        assertThat(result.stealReasons().getOrDefault("TOGGLE.rate_anchored_sensing_on", 0L))
                .as("the default arm marks itself exactly once").isEqualTo(1L);
    }

    /**
     * The documented rollback, exercised end-to-end as the pair {@code docs/configuration.md} actually
     * tells a user to pass: {@code rate_anchored_sensing=off} AND {@code tail_floor=current}
     * together, parsed from those strings and driven through a real bounded scan.
     *
     * <p>Asserting the sensor half alone would leave the promise half-tested — the pair is what the
     * docs promise restores pre-0.2.0 behaviour. The dense/uniform carve instability tracked by
     * {@code ConfettiFeedbackWiringTest} (#78) belongs to the promoted <em>default</em> pair, not
     * this rollback configuration. So the rollback is verified as a pair or not at all.
     */
    @Test
    @Timeout(60)
    void theDocumentedRollbackPairRunsBothLegacyMechanismsAndIsSilentOnBothCounters(@TempDir Path dir)
            throws Exception {
        List<byte[]> keyspace = Keyspaces.exactly(2000);
        EngineToggles rollback = EngineToggles.parse(
                List.of("rate_anchored_sensing=off", "tail_floor=current"), false);
        assertThat(rollback.remainingWorkEstimator(20)).isSameAs(RemainingWorkEstimator.WINDOW);
        assertThat(rollback.tailFloor()).isEqualTo(TailFloorMode.CURRENT);

        ScanResult result = runBoundedRoot(dir, "rollback-pair", keyspace, rollback);
        assertExactlyOnce(result.emitted(), keyspace);

        assertThat(result.stealReasons().getOrDefault("TOGGLE.rate_anchored_sensing_on", 0L)).isZero();
        assertThat(sumCategory(result.stealReasons(), "SENSING_OWNER")
                + sumCategory(result.stealReasons(), "SENSING_STEAL"))
                .as("the legacy reading adds no counter to a run that has always existed").isZero();
        assertThat(result.stealReasons().keySet().stream().filter(k -> k.startsWith("TOGGLE.tail_floor")))
                .as("and the legacy floor is not an arm, so it marks nothing either").isEmpty();
        assertThat(sumCategory(result.stealReasons(), "TAIL_FLOOR"))
                .as("nor does it compute a second verdict to compare against").isZero();
    }
}
