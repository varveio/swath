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
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SegmentSink;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortLane;
import io.varve.swath.sort.SortLaneMeters;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortStamp;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration coverage for the {@code --sort} pipeline: the listing → ordered sort lane →
 * merge → publish path through {@link ListRunner#runToSortedParquetWorkStealing}, and the
 * merge-pending resume re-entry through {@link ListRunner#runSortMergeOnly}. This pins the ordinary
 * end-to-end behavior of the wiring: sorted + stamped output, manifest-last publish, staging
 * namespace, durable_cursor advance, and the {@code sort_phase} state machine.
 */
final class SortPipelineTest {

    private static final String ARGS_HASH = "sort-pipeline-hash";
    private static final int MAX_KEYS = 32;

    private static RunKey sortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static ListRunner.ParquetSpec spec() {
        return new ListRunner.ParquetSpec(new byte[0], 256, MAX_KEYS, FilterChain.EMPTY,
                2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    /** Small segment gate ⇒ many segments ⇒ the cascade + merge actually engage on a tiny keyspace. */
    private static SortConfig smallSegments() {
        return SortConfigs.base()
                .withSegmentEntries(4);
    }

    @Test
    @Timeout(60)
    void endToEnd_producesSortedStampedSingleFileWithManifestLastAndStagingGone(@TempDir Path tmp)
            throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(300);
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = outputDir.resolve("_staging");
        Files.createDirectories(stagingDir);
        Path db = tmp.resolve("c.sqlite");

        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, ctx.metrics())) {
            RunMeta run = store.openRun(sortKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), true);

            MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            new ListRunner().runToSortedParquetWorkStealing(ctx, fetcher, outputDir, stagingDir, spec(),
                    store, run.id(), 4, seeds, smallSegments(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, false);

            // One published, globally-sorted, stamped file (under <root>/data/).
            Path finalFile = DatasetLayout.of(outputDir).dataFile("part-00001.parquet");
            assertThat(Files.exists(finalFile)).isTrue();
            List<String> keys = ParquetReads.keys(finalFile);
            assertThat(keys).containsExactlyElementsOf(expectedSorted(keyspace));
            assertThat(keys).isSorted();
            assertThat(SortStamp.read(finalFile)).hasValueSatisfying(s ->
                    assertThat(s.mode()).isEqualTo(SortMode.OBJECTS));

            // Manifest published (LAST) referencing only the final file — never a staging segment.
            String manifest = Files.readString(DatasetLayout.of(outputDir).manifest());
            assertThat(manifest).contains("part-00001.parquet");
            assertThat(manifest).doesNotContain("seg-");
            assertThat(manifest).doesNotContain("_staging");

            // Staging fully cleaned after a successful publish: the DIRECTORY itself is
            // removed, not just its contents — the sorter owns it exclusively; state
            // machine + run terminal.
            assertThat(Files.exists(stagingDir)).as("staging dir removed after publish").isFalse();
            assertThat(store.sortPhase(run.id())).isEqualTo(SortPhase.PUBLISHED);
            assertThat(store.loadResumable(run.id(), true)).as("output-complete: nothing to resume").isEmpty();

            // durable_cursor advanced via the reused part-finalize machinery (segments carried it).
            assertThat(nodesWithDurableCursor(db, run.id())).isPositive();

            // First-class meters engaged.
            assertThat(counter(ctx, "swath.sort.entries")).isEqualTo(keyspace.size());
            assertThat(counter(ctx, "swath.sort.segments.written")).isPositive();

            // §3.2: swath.progress.units advances in BOTH phases — once for the listed entries
            // (recordEntriesEmitted, the LISTING phase) and again for the merged rows (the k-way
            // merge's progress-callback feed, the MERGING phase) — so the total is double the
            // keyspace size, never a single-count of one phase only.
            assertThat(counter(ctx, "swath.progress.units")).isEqualTo(2.0 * keyspace.size());
        }
    }

    @Test
    @Timeout(60)
    void mergePendingResume_republishesFromStagingWithZeroListFetches(@TempDir Path tmp) throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = outputDir.resolve("_staging");
        Files.createDirectories(stagingDir);
        Path db = tmp.resolve("c.sqlite");

        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, ctx.metrics())) {
            RunMeta run = store.openRun(sortKey(), false, false);
            long nodeId = store.insertNode(NodeSpec.rootRange(run.id()));

            // Simulate a completed listing that crashed before publish: durable staging segments
            // recorded in the checkpoint, no manifest. Build them with the real sort lane (no fetcher).
            List<byte[]> keyspace = Keyspaces.singlePrefixFlat(120);
            SegmentSink sink = result -> {
                List<PartFinalize.DurableAdvance> advances = result.perNodeMaxKeys().entrySet().stream()
                        .map(e -> new PartFinalize.DurableAdvance(e.getKey(), e.getValue())).toList();
                store.partFinalized(new PartFinalize(run.id(), 0, result.path().getFileName().toString(),
                        ListRunner.SORT_SEGMENT_FORMAT, result.rows(), result.bytes(), advances));
            };
            SortLane lane = new SortLane(smallSegments(), new ListEntryComparator(), DuplicateHook.NO_OP,
                    SortMetrics.NO_OP, SortLaneMeters.NO_OP, stagingDir, "seg-" + run.id() + "-x", sink);
            for (List<ListEntry> page : pages(keyspace, MAX_KEYS)) {
                lane.admit(nodeId, page);
            }
            lane.close();

            List<PartRef> segmentRows = store.finalizedParts(run.id());
            assertThat(segmentRows).allMatch(p -> ListRunner.SORT_SEGMENT_FORMAT.equals(p.format()));
            assertThat(Files.exists(DatasetLayout.of(outputDir).manifest())).isFalse();

            // Merge-pending re-entry: re-run ONLY the merge (no fetcher ⇒ zero LIST fetches).
            new ListRunner().runSortMergeOnly(ctx, outputDir, stagingDir, store, run.id(),
                    smallSegments(), SortMode.OBJECTS, spec());

            Path finalFile = DatasetLayout.of(outputDir).dataFile("part-00001.parquet");
            assertThat(ParquetReads.keys(finalFile)).containsExactlyElementsOf(expectedSorted(keyspace));
            assertThat(SortStamp.read(finalFile)).isPresent();
            assertThat(Files.readString(DatasetLayout.of(outputDir).manifest())).contains("part-00001.parquet");
            assertThat(store.sortPhase(run.id())).isEqualTo(SortPhase.PUBLISHED);
            assertThat(counter(ctx, "swath.steal_reason", "SORT", "merge_redone")).isEqualTo(1.0);
            assertThat(Files.exists(stagingDir)).as("staging dir removed after republish").isFalse();
        }
    }

    // --- helpers ---

    private static List<String> expectedSorted(List<byte[]> keyspace) {
        return keyspace.stream().map(k -> new String(k, StandardCharsets.UTF_8)).sorted().toList();
    }

    private static List<List<ListEntry>> pages(List<byte[]> keyspace, int pageSize) {
        List<byte[]> sorted = new ArrayList<>(keyspace);
        sorted.sort(Arrays::compareUnsigned);   // S3 pages arrive ascending
        List<List<ListEntry>> pages = new ArrayList<>();
        for (int i = 0; i < sorted.size(); i += pageSize) {
            List<ListEntry> page = new ArrayList<>();
            for (int j = i; j < Math.min(i + pageSize, sorted.size()); j++) {
                page.add(new ObjectEntry(KeyBytes.of(sorted.get(j)), 1L, 0L, null, null, null,
                        false, null, null, null, null));
            }
            pages.add(page);
        }
        return pages;
    }

    private static double counter(RunContext ctx, String name) {
        Counter c = ctx.metrics().registry().find(name).counter();
        return c == null ? 0.0 : c.count();
    }

    private static double counter(RunContext ctx, String name, String outcome, String reason) {
        Counter c = ctx.metrics().registry().find(name).tag("outcome", outcome).tag("reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }

    private static int nodesWithDurableCursor(Path db, long runId) throws Exception {
        try (var c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             var ps = c.prepareStatement(
                     "SELECT COUNT(*) FROM listing_node WHERE run_id=? AND durable_cursor IS NOT NULL")) {
            ps.setLong(1, runId);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }
}
