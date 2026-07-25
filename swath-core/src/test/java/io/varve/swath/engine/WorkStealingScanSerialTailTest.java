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
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * <b>The seed-side flat-wide-tail subdivision guard</b> (algorithms.md §3/§6/§8).
 *
 * <p>This guards <b>one</b> of the causes of a slow serial tail — {@code SeedStep} subdividing
 * flat-wide seed tails; it is <b>not</b> the whole story. The dominant cause is a
 * <b>degenerate parent-empty split pivot</b> (baton-passing) in {@code Thief}, not the
 * seed-partition shape this test targets.
 *
 * <p><b>The symptom.</b> On a WIDE keyspace that is finely structured near its head but a
 * single large flat region at its tail, the engine bursts to high {@code in_flight} at the
 * start (seed parallelism), then <b>collapses to {@code in_flight} 1–2</b> and drains the
 * tail near-serially — byte-exact output, just slow.
 *
 * <p><b>The deterministic root this test guards.</b> The collapse has a <i>structural,
 * timing-free</i> precondition in the {@code delimiter=/} seed (algorithms.md §8): the seed's
 * adaptive descent refines the small, directory-structured head into <b>one tile per
 * sub-directory</b> (~200 fine ranges) but, because the flat tail has <b>no {@code /}
 * structure to cut on</b>, it leaves the <b>entire tail as a single seed range</b> — even
 * though the tail holds the large majority of the keys. The fleet therefore has <b>nothing to
 * grab in the tail</b>: one worker owns ~85% of the keyspace as a single range, and the tail
 * can only be re-parallelized by <i>demand-driven stealing</i>, which (under real page
 * latency) re-ramps far slower than the tail drains. The over-partitioned head + the single
 * giant tail range <b>is</b> the serial tail, expressed as a deterministic property of the
 * seed partition alone (no wall clock, no latency, no scheduling dependence).
 *
 * <p><b>What this asserts (currently VIOLATED — this is the target the fix must satisfy).</b>
 * The shallow seed must not leave one giant unpartitioned range covering most of the
 * keyspace: <b>no single seed range may cover more than {@value #MAX_SEED_RANGE_SHARE} of the
 * objects.</b> On the current engine the flat tail is one range covering ~85% — this test
 * FAILS. A fix that partitions a large flat region at seed time (e.g. synthesizing
 * byte-midpoint cut-points for an over-large tile, or allocating the cut-point budget by
 * keyspace fraction rather than per-prefix directory count — algorithms.md §8) drops the
 * busiest seed range below the bar, so the fleet can work the tail in parallel from the first
 * moment. This is the same shape validated on a real skewed bucket; here it is a deterministic
 * in-process reproduction.
 *
 * <p>The companion {@link #serialTailKeyspaceIsListedExactlyOnce} runs the full engine over
 * the same keyspace and asserts <b>exactly-once</b> (no gap/overlap/dup, I1/I2/I3), so any
 * seed-partitioning fix that satisfies the guard above is held to byte-exact correctness.
 *
 * <p><b>Scope note.</b> This guards only the deterministic, seed-structural cause this test
 * constructs: a front-loaded/uneven shape where the seed leaves one giant unpartitioned flat
 * tail. The <i>evenly</i>-seeded variant (uniform top-level prefixes, e.g. a
 * uniformly-partitioned real bucket) has no such seed asymmetry — for that shape the dominant
 * residual is the degenerate parent-empty pivot / owner-drainer race (not a "re-ramp" effect —
 * that hypothesis was instrumented and disproven), and the open fix (far-ahead/owner-side pivot
 * placement) has not landed; it is not assertable deterministically on a zero-latency mock the
 * way this seed-shape guard is.
 */
final class WorkStealingScanSerialTailTest {

    private static final int WORKERS = 64;
    private static final int MAX_KEYS = 1000;
    /**
     * No single seed range may own more than this fraction of the objects. The flat tail on the
     * current engine is one range at ~0.85; a seed that partitions large flat regions keeps every
     * range well below this. Generous (a balanced seed puts each range near {@code 1/seedCount});
     * chosen so the current one-giant-tail-range engine fails by a wide margin.
     */
    private static final double MAX_SEED_RANGE_SHARE = 0.40;

    private static final byte[] HEAD_PREFIX = "aa/".getBytes(StandardCharsets.UTF_8);

    /**
     * The front-loaded / uneven shape: a small head finely structured into {@code headDirs}
     * sub-directories ({@code aa/<dir>/<key>}) so the {@code delimiter=/} seed cuts it into one
     * range per directory, plus a large flat uniform tail ({@code zz/<uuid>}) with no {@code /}
     * structure, so the seed leaves it as a single range. The tail holds the majority of keys.
     */
    private static List<byte[]> frontLoaded(long seed, int headKeys, int tailKeys, int headDirs) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(headKeys + tailKeys);
        for (int i = 0; i < headKeys; i++) {
            keys.add(String.format("aa/%04d/%012x", i % headDirs, r.nextLong())
                    .getBytes(StandardCharsets.UTF_8));
        }
        for (int i = 0; i < tailKeys; i++) {
            keys.add(String.format("zz/%016x%016x", r.nextLong(), r.nextLong())
                    .getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    private static List<byte[]> keyspace() {
        // 15k head keys across 100 sub-directories, 90k flat tail keys: the tail is ~86% of the
        // keyspace but the seed leaves it as ONE range.
        return frontLoaded(42L, 15_000, 90_000, 100);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "serial-tail-4c",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    void shallowSeedDoesNotLeaveAGiantSerialTailRange() throws Exception {
        List<byte[]> keyspace = keyspace();
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();

        // The real seed path (ListCommand builds exactly this) on the front-loaded keyspace.
        SeedStep seedStep = SeedSteps.of(mock, new byte[0], WORKERS);
        List<NodeSpec> specs = seedStep.seedSpecs(1L, SeedMode.SHALLOW);

        // Attribute every object to its covering seed tile (the specs already tile (⊥, null] in
        // ascending order), then find the busiest range's share of the keyspace.
        long[] perTile = keysPerTile(specs, keyspace);
        int busiest = argmax(perTile);
        long busiestKeys = perTile[busiest];
        double busiestShare = (double) busiestKeys / keyspace.size();

        // The seed lavishes a range per head sub-directory but leaves the large flat tail as one
        // range: currently the busiest range owns ~0.85 of the keyspace, so the fleet cannot work
        // the tail in parallel from the start and it drains near-serially. A seed that partitions
        // a large flat region keeps every range below the bar.
        assertThat(busiestShare)
                .as("no single seed range may own >= %.0f%% of the keyspace — the flat tail must be "
                        + "partitioned so the fleet can parallelize it. busiest range owns "
                        + "%d of %d objects", MAX_SEED_RANGE_SHARE * 100, busiestKeys, keyspace.size())
                .isLessThan(MAX_SEED_RANGE_SHARE);
    }

    @Test
    @Timeout(120)
    void serialTailKeyspaceIsListedExactlyOnce(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = keyspace();
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();

        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("serial-tail.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            SeedStep seedStep = SeedSteps.of(mock, new byte[0], WORKERS);
            store.insertNodes(seedStep.seedSpecs(run.id(), SeedMode.SHALLOW));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
                    mock, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(2000, engine, emitted);
        }
        EngineHarness.assertExactlyOnce(emitted, keyspace);
    }

    // ---- helpers ---------------------------------------------------------------

    /** Keys covered by each seed tile; {@code specs} already tile {@code (⊥, null]} in order. */
    private static long[] keysPerTile(List<NodeSpec> specs, List<byte[]> keyspace) {
        List<byte[]> sorted = new ArrayList<>(keyspace);
        sorted.sort(Arrays::compareUnsigned);
        long[] counts = new long[specs.size()];
        int ti = 0;
        for (byte[] k : sorted) {
            while (ti < specs.size() - 1
                    && specs.get(ti).rangeEnd() != null
                    && KeyBytes.compareUnsigned(k, specs.get(ti).rangeEnd()) > 0) {
                ti++;
            }
            counts[ti]++;
        }
        return counts;
    }

    private static int argmax(long[] a) {
        int best = 0;
        for (int i = 1; i < a.length; i++) {
            if (a[i] > a[best]) {
                best = i;
            }
        }
        return best;
    }

}
