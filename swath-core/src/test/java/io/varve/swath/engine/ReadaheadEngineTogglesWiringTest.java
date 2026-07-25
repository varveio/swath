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
 * Proves, via the REAL {@link WorkStealingScan} (not a bare {@link RangeScanner} unit construction),
 * that (a) {@code TOGGLE.readahead_on} fires exactly once when enabled and never otherwise, and
 * (b) {@code WorkStealingScan} actually wires {@link ReadaheadConfig#on()}/{@link ReadaheadConfig#off()}
 * per the toggle — proven behaviorally: {@code READAHEAD.*} counters engage end to end when the
 * toggle is on (over a dense bounded tail) and are entirely silent when it is off.
 *
 * <p>{@code owner_split=off} composes with both arms so the engage heuristic (consecutive full,
 * un-split serial pages) is exercised deterministically, without a competing splitting mechanism —
 * the engage heuristic itself is already covered by {@code RangeScannerReadaheadTest}/the adversarial suite;
 * this file is purely the wiring seam.
 */
final class ReadaheadEngineTogglesWiringTest {

    private static RunKey key(String hash) {
        return new RunKey("s3", null, "bucket", new byte[0], hash,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private record ScanResult(List<byte[]> emitted, Map<String, Long> stealReasons) {
    }

    /**
     * Seeds a single BOUNDED root range {@code (⊥, hi]} (unlike {@code NodeSpec.rootRange}, which is
     * the OPEN frontier — readahead never engages there) and drives the full engine to quiescence.
     */
    private static ScanResult runBoundedRoot(Path dir, String label, List<byte[]> keyspace, byte[] hi,
                                             int workers, int maxKeys, EngineToggles toggles) throws Exception {
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(key(label), false, false);
            store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, null, hi, null, null));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles),
                    fetcher, store, workers, maxKeys, seeds, FilterChain.EMPTY);
            drain(engine, emitted);
        }
        return new ScanResult(emitted, metrics.diagnostics(Duration.ZERO).stealReasons());
    }

    private static void drain(WorkStealingScan engine, List<byte[]> emitted) throws Exception {
        PipelineDrain.collectKeys(5000, engine, emitted);
    }

    private static void assertExactlyOnce(List<byte[]> emitted, List<byte[]> keyspace) {
        TreeSet<byte[]> distinctKeyspace = new TreeSet<>(Arrays::compareUnsigned);
        distinctKeyspace.addAll(keyspace);
        TreeSet<byte[]> distinctEmitted = new TreeSet<>(Arrays::compareUnsigned);
        distinctEmitted.addAll(emitted);
        assertThat(distinctEmitted).as("full byte-exact coverage, no duplicates").isEqualTo(distinctKeyspace);
    }

    private static long sumCategory(Map<String, Long> reasons, String category) {
        return reasons.entrySet().stream()
                .filter(e -> e.getKey().startsWith(category + "."))
                .mapToLong(Map.Entry::getValue).sum();
    }

    @Test
    @Timeout(60)
    void readaheadOnFiresToggleMarkOnceAndEngagesEndToEnd(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = Keyspaces.exactly(2000);
        byte[] hi = "key0".getBytes(StandardCharsets.UTF_8);   // > every "key/NNNNNNNNN" key
        EngineToggles toggles = EngineToggles.parse(List.of("readahead=on", "owner_split=off"), false);

        ScanResult result = runBoundedRoot(dir, "readahead-on", keyspace, hi, 1, 20, toggles);
        assertExactlyOnce(result.emitted(), keyspace);

        assertThat(result.stealReasons().getOrDefault("TOGGLE.readahead_on", 0L))
                .as("the once-per-scan opt-in engagement mark fired").isEqualTo(1L);
        assertThat(result.stealReasons().getOrDefault("READAHEAD.engaged", 0L))
                .as("WorkStealingScan actually wired ReadaheadConfig.on() into the worker's RangeScanner "
                        + "-- readahead engaged on the dense bounded tail")
                .isGreaterThan(0L);
        assertThat(sumCategory(result.stealReasons(), "READAHEAD")).isGreaterThan(0L);
    }

    @Test
    @Timeout(60)
    void readaheadOffAtDefaultFiresNoToggleMarkAndNeverEngages(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = Keyspaces.exactly(2000);
        byte[] hi = "key0".getBytes(StandardCharsets.UTF_8);
        EngineToggles toggles = EngineToggles.parse(List.of("owner_split=off"), false);
        assertThat(toggles.readahead()).isFalse();

        ScanResult result = runBoundedRoot(dir, "readahead-off", keyspace, hi, 1, 20, toggles);
        assertExactlyOnce(result.emitted(), keyspace);

        assertThat(result.stealReasons().getOrDefault("TOGGLE.readahead_on", 0L)).isZero();
        // WorkStealingScan wired ReadaheadConfig.off() into the worker's RangeScanner -- zero
        // behavior change, zero READAHEAD.* engagement, even on the identical dense bounded tail
        // that engages readahead in the sibling ON test above.
        assertThat(sumCategory(result.stealReasons(), "READAHEAD")).isZero();
    }

    @Test
    void readaheadOffIsTheEngineDefaultAtTheConstructorLevel() {
        // EngineToggles.DEFAULT (the implicit toggle set every shorter WorkStealingScan constructor
        // falls back to) has readahead OFF -- readahead is opt-in.
        assertThat(EngineToggles.DEFAULT.readahead()).isFalse();
    }
}
