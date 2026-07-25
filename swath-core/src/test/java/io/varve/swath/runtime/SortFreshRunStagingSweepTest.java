/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A FRESH (or {@code --restart}) sorted run
 * must sweep the whole owned staging dir content ({@code seg-*.parquet} and {@code merge-*.parquet}
 * alike) before listing begins — {@code --restart}/a fresh start only discards CHECKPOINT rows
 * ({@code SqliteCheckpointStore#deleteRun}), never staging files, so an abandoned prior run's
 * leftovers would otherwise linger forever (a disk leak, never actually merged in — see the class
 * javadoc below for why that part was already safe).
 *
 * <p><b>Independently true regardless of the sweep:</b> the merge only ever reads segment
 * paths from THIS run's own {@code finalizedParts} checkpoint rows ({@code sortedSegmentPaths}), never
 * a directory listing — so a stale {@code seg-*.parquet} could never be
 * silently picked up by the merge and folded into the published output. This test's assertion that the
 * final output contains ONLY the real listing's keys (not any stale content) demonstrates that
 * directly, on top of asserting the stale files are actually swept.
 */
final class SortFreshRunStagingSweepTest {

    private static final int MAX_KEYS = 32;

    private static SortConfig sortConfig() {
        return SortConfigs.base();
    }

    @Test
    @Timeout(60)
    void freshSortRun_sweepsAbandonedStagingContentBeforeListingBegins(@TempDir Path tmp) throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        Path db = tmp.resolve("c.sqlite");

        // Leftovers from a DIFFERENT, abandoned prior --sort run sharing this same output directory
        // (a crash before merge, or a --restart that discarded checkpoint rows but never touched
        // staging). Deliberately garbage/non-parquet content: if the merge ever tried to read these,
        // it would throw — so a run that succeeds AND produces only the real keys below proves both
        // that the sweep ran and that the stale content was never merged in.
        Path staleSeg = stagingDir.resolve("seg-999-deadbeef-0.parquet");
        Path staleMerge = stagingDir.resolve("merge-0.parquet");
        Files.writeString(staleSeg, "not a real parquet segment — abandoned prior run leftover");
        Files.writeString(staleMerge, "not a real cascade intermediate — abandoned prior run leftover");

        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(200);
        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, ctx.metrics())) {
            RunMeta run = store.openRun(freshSortKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), true);

            MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            ListRunner.ParquetSpec spec = new ListRunner.ParquetSpec(
                    new byte[0], 256, MAX_KEYS, FilterChain.EMPTY, 2, 1024, 16, "fresh-sweep-hash",
                            null, null, 0L, 0L, "");

            // reattach=false: exactly the fresh/--restart path (ListCommand passes run.resumed(), which
            // is false for a brand-new run).
            new ListRunner().runToSortedParquetWorkStealing(ctx, fetcher, outputDir, stagingDir, spec,
                    store, run.id(), 4, seeds, sortConfig(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, false);

            assertThat(Files.exists(staleSeg)).as("abandoned stale segment swept before listing began").isFalse();
            assertThat(Files.exists(staleMerge)).as("abandoned stale cascade intermediate swept too").isFalse();

            Path finalFile = DatasetLayout.of(outputDir).dataFile("part-00001.parquet");
            List<String> keys = ParquetReads.keys(finalFile);
            assertThat(keys).as("only this run's real keys — the stale content was never merged in")
                    .containsExactlyElementsOf(expectedSorted(keyspace));
        }
    }

    private static RunKey freshSortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "fresh-sweep-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static List<String> expectedSorted(List<byte[]> keyspace) {
        return keyspace.stream().map(k -> new String(k, StandardCharsets.UTF_8)).sorted().toList();
    }
}
