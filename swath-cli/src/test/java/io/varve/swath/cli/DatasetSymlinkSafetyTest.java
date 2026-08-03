/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Adversarial pre-existing-link checks for swath-managed dataset paths. */
final class DatasetSymlinkSafetyTest {

    private static final String BUCKET = "bucket";
    private static final String PREFIX = "data/";
    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    private static ListCommand restartCommand(Path outputDir, Path checkpoint) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/data/";
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = outputDir.toString();
        cmd.checkpoint.restart = true;
        if (checkpoint != null) {
            cmd.checkpoint.location = checkpoint.toString();
        }
        cmd.fetcherOverride = MockPageFetcher.builder().keys(List.<byte[]>of()).build();
        return cmd;
    }

    @Test
    void symlinkedDatasetRootIsRefusedWithoutTouchingItsExternalTarget(@TempDir Path root)
            throws Exception {
        Path externalDataset = Files.createDirectories(root.resolve("external-dataset"));
        Path externalData = Files.createDirectories(externalDataset.resolve("data"));
        Path externalPart = externalData.resolve("part-w0-00000.parquet");
        Files.writeString(externalPart, "must survive root-link refusal");
        Path externalSwath = Files.createDirectories(externalDataset.resolve(".swath"));
        Path externalCheckpoint = externalSwath.resolve("checkpoint.sqlite");
        Files.writeString(externalCheckpoint, "not a SQLite checkpoint; must never be parsed");
        Path outputLink = root.resolve("output");
        Files.createSymbolicLink(outputLink, externalDataset);
        Path checkpoint = root.resolve("checkpoint.sqlite");

        assertSymlinkRefusal(restartCommand(outputLink, checkpoint), outputLink);
        assertResumeSymlinkRefusal(outputLink, outputLink);

        assertThat(externalPart).hasContent("must survive root-link refusal");
        assertThat(externalCheckpoint)
                .hasContent("not a SQLite checkpoint; must never be parsed");
        assertThat(checkpoint).doesNotExist();
    }

    @Test
    void symlinkedDataDirIsRefusedBeforeRestartCanDeleteAnExternalPart(@TempDir Path root)
            throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("output"));
        Path externalData = Files.createDirectories(root.resolve("external-data"));
        Path externalPart = externalData.resolve("part-w0-00000.parquet");
        Files.writeString(externalPart, "must survive data-link refusal");
        Path dataLink = outputDir.resolve("data");
        Files.createSymbolicLink(dataLink, externalData);
        Path checkpoint = root.resolve("checkpoint.sqlite");

        assertSymlinkRefusal(restartCommand(outputDir, checkpoint), dataLink);

        assertThat(externalPart).hasContent("must survive data-link refusal");
        assertThat(checkpoint).doesNotExist();
    }

    @Test
    void symlinkedStagingDirIsRefusedBeforeSortedRestartCanDeleteExternalSegments(
            @TempDir Path root) throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("output"));
        Path externalStaging = Files.createDirectories(root.resolve("external-staging"));
        Path externalSegment = externalStaging.resolve("seg-1-0.pageseg");
        Files.writeString(externalSegment, "must survive staging-link refusal");
        Path stagingLink = outputDir.resolve(ListCommand.SORT_STAGING_DIR);
        Files.createSymbolicLink(stagingLink, externalStaging);
        Path checkpoint = root.resolve("checkpoint.sqlite");
        ListCommand cmd = restartCommand(outputDir, checkpoint);
        cmd.sorting.sort = true;
        cmd.sorting.forceSort = true;

        assertSymlinkRefusal(cmd, stagingLink);

        assertThat(externalSegment).hasContent("must survive staging-link refusal");
        assertThat(checkpoint).doesNotExist();
    }

    @Test
    void freshSortedRunUnlinksPlantedSegmentSymlinkWithoutTouchingExternalTarget(
            @TempDir Path root) throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("output"));
        Path stagingDir = Files.createDirectories(outputDir.resolve(ListCommand.SORT_STAGING_DIR));
        Path externalTarget = root.resolve("external-segment-target");
        Files.writeString(externalTarget, "must survive fresh sorted staging cleanup");
        Path plantedSegment = stagingDir.resolve("seg-0-0.pageseg");
        Files.createSymbolicLink(plantedSegment, externalTarget);
        Path checkpoint = root.resolve("checkpoint.sqlite");
        ListCommand cmd = restartCommand(outputDir, checkpoint);
        cmd.sorting.sort = true;
        cmd.sorting.forceSort = true;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);

        assertThat(externalTarget).hasContent("must survive fresh sorted staging cleanup");
        assertThat(plantedSegment).doesNotExist();
    }

    @Test
    void symlinkedSwathDirIsRefusedBeforeAutoCheckpointCreation(@TempDir Path root)
            throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("output"));
        Path externalSwath = Files.createDirectories(root.resolve("external-swath"));
        Path sentinel = externalSwath.resolve("keep.txt");
        Files.writeString(sentinel, "must remain in external storage");
        Path externalCheckpoint = externalSwath.resolve("checkpoint.sqlite");
        Files.writeString(externalCheckpoint, "not a SQLite checkpoint; must never be parsed");
        Path swathLink = outputDir.resolve(".swath");
        Files.createSymbolicLink(swathLink, externalSwath);

        assertSymlinkRefusal(restartCommand(outputDir, null), swathLink);
        assertResumeSymlinkRefusal(outputDir, swathLink);

        assertThat(sentinel).hasContent("must remain in external storage");
        assertThat(externalCheckpoint)
                .hasContent("not a SQLite checkpoint; must never be parsed");
    }

    @Test
    void managedDirectoryEntriesThatArePlainFilesAreRefusedBeforeCheckpointCreation(
            @TempDir Path root) throws Exception {
        for (String name : List.of(
                Manifest.DATA_DIR, ListCommand.SORT_STAGING_DIR, CheckpointOptions.COLOCATED_DIR)) {
            Path caseRoot = Files.createDirectories(root.resolve(name.replace('.', '_')));
            Path outputDir = Files.createDirectories(caseRoot.resolve("output"));
            Path managedPath = outputDir.resolve(name);
            Files.writeString(managedPath, "not a managed directory");
            Path checkpoint = caseRoot.resolve("checkpoint.sqlite");

            assertThatThrownBy(restartCommand(outputDir, checkpoint)::call)
                    .isInstanceOf(InvalidArgsException.class)
                    .hasMessageContaining(managedPath.toString())
                    .hasMessageContaining("not a directory")
                    .hasMessageContaining("remove the file");
            assertThat(checkpoint).doesNotExist();
        }
    }

    @Test
    void markerlessDataPartSymlinkIsForeignAndItsExternalTargetRemainsUntouched(
            @TempDir Path root) throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("output"));
        Path dataDir = Files.createDirectories(outputDir.resolve(Manifest.DATA_DIR));
        Path externalTarget = root.resolve("external-part-target");
        Files.writeString(externalTarget, "must survive markerless ownership validation");
        Path plantedPart = dataDir.resolve("part-w0-00000.parquet");
        Files.createSymbolicLink(plantedPart, externalTarget);
        Path checkpoint = root.resolve("checkpoint.sqlite");

        assertThatThrownBy(restartCommand(outputDir, checkpoint)::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("not empty")
                .hasMessageContaining("refusing to write into it");

        assertThat(externalTarget).hasContent("must survive markerless ownership validation");
        assertThat(plantedPart).isSymbolicLink();
        assertThat(checkpoint).doesNotExist();
    }

    @Test
    void symlinkedCheckpointAndSqliteSidecarsAreRefusedBeforeExternalTargetsAreOpened(
            @TempDir Path root) throws Exception {
        for (String name : List.of(
                CheckpointOptions.COLOCATED_FILE,
                CheckpointOptions.COLOCATED_FILE + "-wal",
                CheckpointOptions.COLOCATED_FILE + "-shm",
                CheckpointOptions.COLOCATED_FILE + "-journal")) {
            Path caseRoot = Files.createDirectories(root.resolve(name.replace('-', '_')));
            Path outputDir = Files.createDirectories(caseRoot.resolve("output"));
            Path swathDir = Files.createDirectories(
                    outputDir.resolve(CheckpointOptions.COLOCATED_DIR));
            Path externalTarget = caseRoot.resolve("external-target");
            Files.writeString(externalTarget, "must survive " + name + " refusal");
            Path plantedLink = swathDir.resolve(name);
            Files.createSymbolicLink(plantedLink, externalTarget);

            assertSymlinkRefusal(restartCommand(outputDir, null), plantedLink);

            assertThat(externalTarget).hasContent("must survive " + name + " refusal");
            assertThat(plantedLink).isSymbolicLink();
        }
    }

    @Test
    void manifestAtomicTempSymlinkIsRefusedBeforeExternalTargetIsTruncated(
            @TempDir Path root) throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("output"));
        Files.writeString(outputDir.resolve(".swath-state.json"),
                "{\"args_hash\":\"prior\",\"run_id\":1}");
        Path externalTarget = root.resolve("external-manifest-target");
        Files.writeString(externalTarget, "must survive manifest temp refusal");
        Path manifestTemp = outputDir.resolve("manifest.json.tmp");
        Files.createSymbolicLink(manifestTemp, externalTarget);
        Path checkpoint = root.resolve("checkpoint.sqlite");

        assertSymlinkRefusal(restartCommand(outputDir, checkpoint), manifestTemp);

        assertThat(externalTarget).hasContent("must survive manifest temp refusal");
        assertThat(checkpoint).doesNotExist();
    }

    @Test
    void explicitCheckpointResumeRefusesSymlinkRestoredWithDatasetBeforeOutputAccess(
            @TempDir Path root) throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("restored-output"));
        Path externalTarget = root.resolve("external-restored-manifest-target");
        Files.writeString(externalTarget, "must survive restored-output refusal");
        Path manifestTemp = outputDir.resolve("manifest.json.tmp");
        Files.createSymbolicLink(manifestTemp, externalTarget);
        Path checkpoint = root.resolve("explicit-checkpoint.sqlite");
        seedCompletedRun(checkpoint, outputDir);

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.checkpoint.location = checkpoint.toString();
        cmd.checkpoint.resume = true;

        assertSymlinkRefusal(cmd, manifestTemp);

        assertThat(externalTarget).hasContent("must survive restored-output refusal");
        assertThat(checkpoint).exists();
    }

    private static void seedCompletedRun(Path checkpoint, Path outputDir) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", "", BUCKET, PREFIX).hash();
        RunKey key = new RunKey(
                "s3", null, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8), argsHash,
                "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(
                        true, null, "us-east-1", false, false, outputDir.toString(), false,
                        null, null),
                false);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(checkpoint)) {
            RunMeta run = store.openRun(key, false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k9".getBytes(StandardCharsets.UTF_8), true));
            store.markOutputComplete(run.id());
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }
    }

    private static void assertSymlinkRefusal(ListCommand cmd, Path refusedPath) {
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining(refusedPath.toString())
                .hasMessageContaining("symbolic link")
                .hasMessageContaining("remove the link");
    }

    private static void assertResumeSymlinkRefusal(Path runHandle, Path refusedPath) {
        ResumeCommand cmd = new ResumeCommand();
        cmd.directory = runHandle;
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining(refusedPath.toString())
                .hasMessageContaining("symbolic link")
                .hasMessageContaining("remove the link");
    }
}
