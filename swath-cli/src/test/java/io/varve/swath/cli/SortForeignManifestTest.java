/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SegmentSink;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortLane;
import io.varve.swath.sort.SortLaneMeters;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.testkit.ParquetReads;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code ListCommand.runSortedParquet}'s PUBLISHED
 * dispatch must not treat bare {@code Files.exists(manifest.json)} as "this run's final output is
 * present" — a fresh/{@code --restart} run only discards checkpoint rows (never output files), and
 * {@code openParquetDir} just creates/reuses the {@code -o} directory, so a STALE {@code
 * manifest.json} (+ stale {@code part-*.parquet}) left by a completely DIFFERENT prior run sharing
 * the same output directory would otherwise make a resumed post-listing/pre-merge run silently SKIP
 * its own merge and report success over someone else's output.
 *
 * <p>This test plants exactly that foreign manifest + a stale final file, drives a {@code --sort} run
 * to the nodes-empty/merge-pending state with real durable staging segments recorded in the
 * checkpoint, and asserts the merge actually RUNS: the final file is overwritten with THIS run's
 * correctly sorted content and the manifest is republished with THIS run's {@code args_hash}/{@code
 * run_id}.
 */
final class SortForeignManifestTest {

    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX = "data/";
    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    /** Small segment gate ⇒ a real merge (not just the "empty listing" single-file shortcut). */
    private static SortConfig sortConfig() {
        return SortConfigs.base().withSegmentEntries(4);
    }

    @Test
    void foreignManifestIsNeverTrustedAsPublished_mergeRunsAndRepublishesUnderThisRunsIdentity(
            @TempDir Path dir) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        Path outputDir = Files.createDirectories(dir.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve(ListCommand.SORT_STAGING_DIR));
        Path db = dir.resolve("c.sqlite");

        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false, outputDir.toString(), false, null, null), true);
        long runId;
        List<String> expectedKeys = List.of("a", "b", "c", "d");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            runId = run.id();
            long node = store.insertNode(NodeSpec.rootRange(run.id()));

            // Real durable staging segments (this run's actual, correct content) tracked in the
            // checkpoint — a genuinely completed listing waiting on the merge.
            SegmentSink sink = result -> {
                List<PartFinalize.DurableAdvance> advances = result.perNodeMaxKeys().entrySet().stream()
                        .map(e -> new PartFinalize.DurableAdvance(e.getKey(), e.getValue())).toList();
                // Record the typed format the real SortLane encoder emitted, so the checkpoint's
                // staging identity cannot diverge from these page-run bytes.
                store.partFinalized(new PartFinalize(run.id(), 0, result.path().getFileName().toString(),
                        result.pageRunFormat(), result.rows(), result.bytes(), advances));
            };
            SortLane lane = new SortLane(sortConfig(), new ListEntryComparator(),
                    DuplicateHook.NO_OP, SortMetrics.NO_OP, SortLaneMeters.NO_OP, stagingDir,
                    "seg-" + run.id(), sink);
            lane.admit(node, objects(expectedKeys));
            lane.close();
            List<PartRef> segRows = store.finalizedParts(run.id());
            assertThat(segRows).as("real staging segments are durable before the foreign-manifest plant")
                    .isNotEmpty();

            // Mark the node COMPLETED (the listing itself is done) before latching output-complete —
            // otherwise loadResumable would still return it and this test would fire a real S3 call.
            store.commitPage(new PageCommit(node,
                    "d".getBytes(StandardCharsets.UTF_8), true));
            store.markOutputComplete(run.id());   // durable_cursor caught up ⇒ loadResumable empty
            // Do NOT call markRunFinished/setSortPhase(PUBLISHED) — this run itself never published.
        }

        // Plant a FOREIGN resume identity (.swath-state.json with a different args_hash AND run_id) +
        // a stale final file under data/, as a completely different prior run sharing this same -o
        // directory would leave behind (identity lives in .swath-state.json, not manifest.json;
        // finals live under <root>/data/).
        long foreignRunId = runId + 999;
        String foreignArgsHash = "not-" + argsHash;
        DatasetLayout layout = DatasetLayout.of(outputDir);
        Files.writeString(layout.state(),
                "{\"args_hash\":\"" + foreignArgsHash + "\",\"run_id\":" + foreignRunId + "}");
        Files.writeString(layout.manifest(),
                "{\"sourceBucket\":\"other\",\"version\":\"1\",\"fileFormat\":\"Parquet\",\"files\":[]}");
        // Even a genuinely-COMPLETE foreign dataset (its own _SUCCESS present) must be overwritten,
        // never silently trusted — the identity mismatch is decisive before _SUCCESS is even consulted.
        Files.writeString(layout.success(), "");
        Path dataDir = Files.createDirectories(layout.dataDir());
        Path staleFinal = dataDir.resolve("part-00000.parquet");
        Files.writeString(staleFinal, "not a real parquet file — a foreign prior run's leftover");
        assertThat(Manifest.readIdentity(outputDir)).hasValueSatisfying(id -> {
            assertThat(id.argsHash()).isEqualTo(foreignArgsHash);
            assertThat(id.runId()).isEqualTo(foreignRunId);
        });

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = outputDir.toString();
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;
        cmd.sorting.sort = true;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);

        // The merge RAN (a bare Files.exists check on the manifest would have short-circuited it
        // and left the stale content untouched): the final file now holds THIS run's correctly
        // sorted keys.
        assertThat(ParquetReads.keys(staleFinal))
                .as("the stale foreign final file must be overwritten by this run's own merge output")
                .containsExactlyElementsOf(expectedKeys);

        // The manifest is republished under THIS run's identity, not the foreign one.
        Optional<Manifest.Identity> republished = Manifest.readIdentity(outputDir);
        assertThat(republished).isPresent();
        assertThat(republished.get().argsHash()).isEqualTo(argsHash);
        assertThat(republished.get().runId()).isEqualTo(runId);

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            assertThat(store.sortPhase(runId)).isEqualTo(SortPhase.PUBLISHED);
        }
    }

    private static List<ListEntry> objects(List<String> keys) {
        List<ListEntry> out = new ArrayList<>();
        for (String k : keys) {
            out.add(new ObjectEntry(KeyBytes.ofUtf8(k), 1L, 0L, null, null, null,
                    false, null, null, null, null));
        }
        return out;
    }
}
