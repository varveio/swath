/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.runtime.ArgsHashFields;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The PUBLISHED re-entry branch of the {@code --sort} state machine
 * ({@code ListCommand.runSortedParquet}) must mark the run terminal (COMPLETED), not merely clean
 * staging and return. Without this fix a re-invocation that finds a published run (e.g. a crash
 * strictly between the manifest write and the original {@code markRunFinished} call) leaves
 * {@code run_meta.status} as {@code RUNNING} forever.
 *
 * <p>PUBLISHED is gated on the LAST-written marker, {@code _SUCCESS} — not on the
 * {@code .swath-state.json} identity alone. The positive test writes {@code _SUCCESS} (a genuine full
 * publish) and asserts the short-circuit fires WITHOUT rewriting the manifest. The negative test
 * plants matching identity but NO {@code _SUCCESS} (a publish that crashed mid-commit) and asserts
 * reentry does NOT short-circuit — it re-runs the merge, which rewrites the manifest and finally
 * writes {@code _SUCCESS}.
 */
final class SortPublishedReentryTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX = "data/";

    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    @Test
    void publishedReentry_marksRunCompletedInsteadOfLeavingItRunningForever(@TempDir Path dir) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        Path outputDir = Files.createDirectories(dir.resolve("out"));
        Path db = dir.resolve("c.sqlite");

        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false, outputDir.toString(), false, null, null), true);
        long runId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            runId = run.id();
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            // The node is output-complete (durable_cursor caught up to cursor) so loadResumable
            // returns nothing — but markRunFinished is deliberately NOT called here, reproducing the
            // crash-strictly-between-publish-and-terminal-status scenario the fix must close.
            store.markOutputComplete(run.id());
        }
        // The resume identity must correctly identify as THIS run's (args_hash + run_id) for
        // the PUBLISHED short-circuit to fire at all — an unidentified/foreign identity is
        // SortForeignManifestTest's own regression test, not this one (which targets markRunFinished).
        // Identity lives in .swath-state.json (not manifest.json), which is what
        // ListCommand.isPublishedByThisRun / Manifest.readIdentity consult. The consumer manifest.json
        // exists alongside it at the same publish commit point.
        DatasetLayout layout =
                DatasetLayout.of(outputDir);
        Files.writeString(layout.state(),
                "{\"args_hash\":\"" + argsHash + "\",\"run_id\":" + runId + "}");
        // A recognizable planted manifest: a real merge would REWRITE it under this run's bucket/schema;
        // the PUBLISHED short-circuit must leave it untouched (the sentinel survives).
        Files.writeString(layout.manifest(),
                "{\"sourceBucket\":\"PLANTED-SENTINEL\",\"version\":\"1\",\"fileFormat\":\"Parquet\",\"files\":[]}");
        // A GENUINE full publish wrote _SUCCESS last — this is what makes it PUBLISHED.
        Files.writeString(layout.success(), "");
        // Resume guard: a resumed run must NOT clear a finalized part — plant one and require it
        // to survive the PUBLISHED reentry (prepareDatasetForFreshRun runs ONLY on a non-resumed run).
        Path finalizedPart = Files.createDirectories(layout.dataDir()).resolve("part-00000.parquet");
        Files.writeString(finalizedPart, "this run's already-published finalized part");
        assertThat(CheckpointDbProbe.runStatus(db, runId)).isEqualTo("RUNNING");

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = outputDir.toString();
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;
        cmd.sorting.sort = true;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);

        assertThat(CheckpointDbProbe.runStatus(db, runId))
                .as("m1: PUBLISHED re-entry must mark the run terminal, not leave it RUNNING forever")
                .isEqualTo("COMPLETED");
        assertThat(Files.readString(layout.manifest()))
                .as("with _SUCCESS present the PUBLISHED short-circuit fires — the manifest is NOT rewritten")
                .contains("PLANTED-SENTINEL");
        assertThat(finalizedPart)
                .as("a resumed run must NOT clear finalized parts")
                .exists();
        JsonNode summary = MAPPER.readTree(outputDir.resolve("_swath_summary.json").toFile());
        assertThat(summary.get("completed").asBoolean()).isTrue();
        assertThat(summary.get("sort").get("arm").asText())
                .as("published re-entry is neither a live listing nor a merge-only resume")
                .isEqualTo("PUBLISHED_REENTRY");
        assertThat(summary.get("sort").get("merge_only_resume").asBoolean()).isFalse();
        assertThat(summary.get("objects").asLong()).isZero();
        assertThat(summary.get("sort").get("passes").asLong()).isZero();
        assertThat(summary.get("sort").get("merge_ms").asLong()).isZero();
    }

    @Test
    void publishIncompleteWithoutSuccessMarker_reRunsMergeInsteadOfShortCircuiting(@TempDir Path dir)
            throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        Path outputDir = Files.createDirectories(dir.resolve("out"));
        Path db = dir.resolve("c.sqlite");

        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false, outputDir.toString(), false, null, null), true);
        long runId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            runId = run.id();
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            store.markOutputComplete(run.id());
        }
        // Adversarial fixture: a publish that wrote manifest.json + .swath-state.json with THIS run's
        // identity but crashed BEFORE writing the LAST marker (_SUCCESS is deliberately absent).
        DatasetLayout layout =
                DatasetLayout.of(outputDir);
        Files.writeString(layout.state(),
                "{\"args_hash\":\"" + argsHash + "\",\"run_id\":" + runId + "}");
        Files.writeString(layout.manifest(),
                "{\"sourceBucket\":\"PLANTED-SENTINEL\",\"version\":\"1\",\"fileFormat\":\"Parquet\",\"files\":[]}");
        assertThat(Files.exists(layout.success())).as("fixture: _SUCCESS is absent (crash pre-marker)").isFalse();

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = outputDir.toString();
        cmd.checkpoint.location = db.toString();
        cmd.checkpoint.resume = true;
        cmd.sorting.sort = true;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);

        // Reentry did NOT short-circuit: the merge RAN, rewriting the manifest (sentinel gone) and
        // finally writing _SUCCESS — the dataset ends consistent and complete.
        assertThat(Files.readString(layout.manifest()))
                .as("without _SUCCESS the merge re-runs and REWRITES the manifest under this run")
                .doesNotContain("PLANTED-SENTINEL");
        assertThat(Files.exists(layout.success()))
                .as("the re-run's publish now writes _SUCCESS, leaving the dataset complete")
                .isTrue();
        assertThat(CheckpointDbProbe.runStatus(db, runId)).isEqualTo("COMPLETED");
    }
}
