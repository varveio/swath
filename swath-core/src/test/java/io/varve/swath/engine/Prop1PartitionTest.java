/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.Keyspaces;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.GenerationMode;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.lifecycle.AfterContainer;

/**
 * PROP-1 — the headline correctness proof of {@link WorkStealingScan} (algorithms.md §6,
 * I2/I3/I10). Over the adversarial bucket-shape matrix, drive the full
 * engine with stealing <b>actually exercised</b> (many workers + tiny pages force splits),
 * then assert:
 *
 * <ul>
 *   <li>the union of all emitted keys == the full keyspace, each key <b>exactly once</b>,
 *       byte-exact (unsigned, never {@code String.compareTo}) — no gap AND no overlap on the
 *       realized keyspace;</li>
 *   <li>the durable range set reconstructed from every committed split tiles {@code (⊥, ⊤]}
 *       (each pivot strictly between its victim's {@code lo} and {@code hi}; the boundary key
 *       {@code m} belongs LEFT) — the structural no-gap/no-overlap invariant.</li>
 * </ul>
 *
 * <p>The mandated adversarial shapes are each covered exhaustively
 * ({@link #everyMandatedShapeTiles}); a randomized worker-count sweep adds breadth.
 *
 * <p>Keyspaces are valid UTF-8 by construction: every real S3 key is valid UTF-8
 * (algorithms.md §3.1), and a non-UTF-8 keyspace would violate {@code byteMidpoint}'s
 * precondition — so "0xFF-run keys" are realized as max-scalar (U+10FFFF) runs, the
 * valid-UTF-8 high-byte extreme a split must tolerate.
 */
class Prop1PartitionTest {

    /** Total committed splits across every case this run — the aggregate stealing-coverage signal. */
    private static final AtomicInteger STEALS_OBSERVED = new AtomicInteger();

    /**
     * Coverage guard: the property suite must exercise stealing <i>somewhere</i> (else it would
     * silently degrade to single-worker scans, which tile trivially). Asserted once in aggregate
     * — robust, unlike a per-case "this run split" check which races the engine's timing.
     */
    @AfterContainer
    static void stealingWasExercised() {
        assertThat(STEALS_OBSERVED.get())
                .as("PROP-1 suite must commit at least one split across its cases")
                .isGreaterThan(0);
    }

    /** A named adversarial keyspace plus the page size + whether stealing is expected (informational). */
    record NamedKeyspace(String name, List<byte[]> keys, int maxKeys, boolean expectSplits) {
        @Override
        public String toString() {
            return name + "(" + keys.size() + " keys)";
        }
    }

    /** The mandated bucket-shape matrix (§0). */
    private static List<NamedKeyspace> mandated() {
        return List.of(
                new NamedKeyspace("empty", Keyspaces.empty(), 7, false),
                new NamedKeyspace("oneKey", Keyspaces.oneKey(), 7, false),
                new NamedKeyspace("badlySkewed", Keyspaces.badlySkewed(7, 600, 0.9), 7, true),
                new NamedKeyspace("singlePrefixFlat", Keyspaces.singlePrefixFlat(500), 7, true),
                new NamedKeyspace("lastByteDiffer", Keyspaces.lastByteDiffer(94), 5, true),
                new NamedKeyspace("maxScalarRun", Keyspaces.maxScalarRun(120), 7, true),
                new NamedKeyspace("longKeys1024", Keyspaces.longKeys1024(300), 7, true),
                new NamedKeyspace("supplementaryPlane", Keyspaces.supplementaryPlane(400), 7, true),
                new NamedKeyspace("exactly1000", Keyspaces.exactly(1000), 7, true),
                new NamedKeyspace("exactly1001", Keyspaces.exactly(1001), 7, true),
                new NamedKeyspace("flatRandom", Keyspaces.flatRandom(7, 800), 7, true),
                new NamedKeyspace("deepTree", Keyspaces.deepTree(7, 8, 25), 7, true));
    }

    @Provide
    Arbitrary<NamedKeyspace> keyspaces() {
        return Arbitraries.of(mandated());
    }

    /** Every mandated adversarial shape, exhaustively, with a fixed worker count. */
    @Property(generation = GenerationMode.EXHAUSTIVE)
    void everyMandatedShapeTiles(@ForAll("keyspaces") NamedKeyspace ks) throws Exception {
        assertTilesWithStealing(ks, 6);
    }

    /** Breadth: the same shapes across a range of worker counts (steal pressure varies). */
    @Property(tries = 60)
    void tilesUnderForcedStealing(@ForAll("keyspaces") NamedKeyspace ks,
                                  @ForAll @IntRange(min = 2, max = 8) int workers) throws Exception {
        assertTilesWithStealing(ks, workers);
    }

    private static void assertTilesWithStealing(NamedKeyspace ks, int workers) throws Exception {
        Path dir = Files.createTempDirectory("prop1-");
        try {
            EngineHarness.Result r = EngineHarness.run(ks.keys(), workers, ks.maxKeys(), dir);
            EngineHarness.assertExactlyOnceAndTiles(r, ks.keys());
            // Whether a given concurrent run lands a steal is timing-dependent (a fast run may
            // finish before any worker idles), so we do NOT assert splits per-case — that was
            // flaky. Tiling above is the invariant and holds with or without a split. Coverage
            // that stealing is genuinely exercised is checked in aggregate below; the
            // deterministic split paths are proven in Prop1InterleavingTest.
            STEALS_OBSERVED.addAndGet(r.splits().size());
        } finally {
            deleteRecursively(dir);
        }
    }

    private static void deleteRecursively(Path dir) throws Exception {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception ignored) {
                    // best-effort temp cleanup
                }
            });
        }
    }
}
