/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortStamp;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.parquet.example.data.Group;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * SORT-1 — tiny segment gate + a small fan-in force {@code > fan-in} staging segments, so the
 * cascaded multi-pass k-way merge genuinely engages, and the published output is still globally
 * sorted under the §0.3 total order. The keyspace mixes flat keys with 1&nbsp;KB keys so the merge's
 * byte-exact ordering is exercised at worst-case key size, not just short ASCII.
 *
 * <p>Drives the production sort pipeline ({@link ListRunner#runToSortedParquetWorkStealing}) over a
 * {@link MockPageFetcher} — scale is synthesized, never a real store. Asserts, from the
 * metrics alone (§4 discipline), that the cascade actually fired:
 * {@code swath.sort.merge.passes > 1} AND the {@code SORT.merge_pass_cascaded} engagement counter.
 *
 * <p>Stable-tiebreak-with-duplicates is owned by {@code PropSortContractTest} (PROP-SORT-1 iii): a
 * {@link MockPageFetcher} keyspace is unique-by-key by construction, so no duplicate-key rows can
 * reach this end-to-end path — here the tiebreak reduces to strict key-byte ascending, asserted
 * byte-exact.
 */
final class SortCascadeContractTest {

    private static final String ARGS_HASH = "sort1-cascade-hash";
    private static final int MAX_KEYS = 16;

    private static RunKey sortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static ListRunner.ParquetSpec spec() {
        return new ListRunner.ParquetSpec(new byte[0], 256, MAX_KEYS, FilterChain.EMPTY,
                2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    /** segment-entries=8, fan-in=4 ⇒ many small segments and a multi-pass cascade on a tiny keyspace. */
    private static SortConfig cascadingSegments() {
        return SortConfigs.base()
                .withSegmentEntries(8)
                .withFanIn(4);
    }

    @Test
    @Timeout(120)
    void cascadedMultiPassMergeProducesGloballySortedOutputIncluding1KbKeys(@TempDir Path tmp)
            throws Exception {
        // Flat keys + 1 KB keys — the merge must order both byte-exact under the §0.3 comparator.
        List<byte[]> keyspace = new ArrayList<>(Keyspaces.singlePrefixFlat(1200));
        keyspace.addAll(Keyspaces.longKeys1024(40));

        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        Path db = tmp.resolve("c.sqlite");

        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, ctx.metrics())) {
            RunMeta run = store.openRun(sortKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), true);

            MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            new ListRunner().runToSortedParquetWorkStealing(ctx, fetcher, outputDir, stagingDir, spec(),
                    store, run.id(), 4, seeds, cascadingSegments(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, false);

            Path finalFile = DatasetLayout.of(outputDir).dataFile("part-00000.parquet");
            assertThat(Files.exists(finalFile)).isTrue();

            // Byte-exact global order + completeness (no gap, no overlap) against the ground truth.
            List<byte[]> outKeys = keyBytes(finalFile);
            assertThat(outKeys).as("every input key present exactly once").hasSize(keyspace.size());
            assertThat(isStrictlyAscending(outKeys)).as("globally, byte-exactly sorted (§0.3)").isTrue();
            assertThat(outKeys).usingElementComparator(Arrays::compareUnsigned)
                    .containsExactlyInAnyOrderElementsOf(keyspace);

            assertThat(SortStamp.read(finalFile)).hasValueSatisfying(s ->
                    assertThat(s.mode()).isEqualTo(SortMode.OBJECTS));
            assertThat(store.sortPhase(run.id())).isEqualTo(SortPhase.PUBLISHED);
            assertThat(store.loadResumable(run.id(), true)).as("output-complete").isEmpty();

            // The cascade genuinely engaged — provable from the metrics alone (§4).
            assertThat(counter(ctx, "swath.sort.merge.passes"))
                    .as("multi-pass cascade: merge.passes > 1").isGreaterThan(1.0);
            assertThat(counter(ctx, "swath.steal_reason", "merge_pass_cascaded"))
                    .as("SORT.merge_pass_cascaded engagement counter fired").isGreaterThan(0.0);
            assertThat(counter(ctx, "swath.sort.segments.written"))
                    .as("> fan-in segments were flushed").isGreaterThan(4.0);
        }
    }

    private static List<byte[]> keyBytes(Path file) throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (Group g : ParquetReads.readAll(file)) {
            keys.add(g.getBinary("key", 0).getBytes());
        }
        return keys;
    }

    private static boolean isStrictlyAscending(List<byte[]> keys) {
        for (int i = 1; i < keys.size(); i++) {
            if (Arrays.compareUnsigned(keys.get(i - 1), keys.get(i)) >= 0) {
                return false;
            }
        }
        return true;
    }

    private static double counter(RunContext ctx, String name) {
        Counter c = ctx.metrics().registry().find(name).counter();
        return c == null ? 0.0 : c.count();
    }

    private static double counter(RunContext ctx, String name, String reason) {
        Counter c = ctx.metrics().registry().find(name).tag("outcome", "SORT").tag("reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }
}
