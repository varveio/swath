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
 * Proves, through the REAL {@link WorkStealingScan}, that a {@code --engine-toggle tail_floor} arm
 * routes into the engine and says so in its metrics alone (AGENTS.md's instrument-every-algo-path
 * rule): the once-per-run {@code TOGGLE.tail_floor_<mode>_on} mark names the SELECTED mode, and the
 * arm changes only WHETHER an owner carves — coverage stays byte-exact, so the arm cannot be blamed
 * for a tiling defect (I2/I3). The default arm is the control: the identical keyspace and total
 * silence on both the mark and the {@code TAIL_FLOOR.*} divergence counters.
 *
 * <p>The divergence counters themselves are exercised at the decision level in {@code
 * OwnerSplitGovernorTest} — they need the wide-flat trailing density this synthetic keyspace does
 * not produce, and inventing one here would test the fixture, not the engine.
 */
final class TailFloorWiringTest {

    /** A bounded root {@code (⊥, hi]}: the owner-split gate chain scores it every page commit. */
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
    void eachArmMarksItselfByNameAndStillTilesTheKeyspaceExactly(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = Keyspaces.exactly(2000);
        for (TailFloorMode arm : new TailFloorMode[] {TailFloorMode.EST_DIRECT, TailFloorMode.REACH_FLOORED}) {
            EngineToggles toggles = EngineToggles.parse(List.of("tail_floor=" + arm.code()), false);
            assertThat(toggles.tailFloor()).isEqualTo(arm);

            ScanResult result = runBoundedRoot(dir, "tail-floor-" + arm.code(), keyspace, toggles);
            assertExactlyOnce(result.emitted(), keyspace);

            assertThat(result.stealReasons().getOrDefault("TOGGLE.tail_floor_" + arm.code() + "_on", 0L))
                    .as("%s: the once-per-run arm mark fired, naming the mode", arm).isEqualTo(1L);
        }
    }

    @Test
    @Timeout(60)
    void theDefaultRunsTheShippedFloorAndIsSilentOnEveryTailFloorCounter(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = Keyspaces.exactly(2000);
        assertThat(EngineToggles.DEFAULT.tailFloor()).isEqualTo(TailFloorMode.CURRENT);

        ScanResult result = runBoundedRoot(dir, "tail-floor-current", keyspace, EngineToggles.DEFAULT);
        assertExactlyOnce(result.emitted(), keyspace);

        assertThat(result.stealReasons().keySet().stream().filter(k -> k.startsWith("TOGGLE.tail_floor")))
                .as("the shipped mode is not an arm and marks nothing").isEmpty();
        assertThat(sumCategory(result.stealReasons(), "TAIL_FLOOR"))
                .as("and it never computes a second verdict to compare against").isZero();
    }
}
