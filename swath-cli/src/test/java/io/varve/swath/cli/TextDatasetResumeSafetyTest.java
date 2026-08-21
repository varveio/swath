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
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.runtime.ArgsHashFields;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class TextDatasetResumeSafetyTest {

    private static final String BUCKET = "bucket";
    private static final String PREFIX = "data/";

    @Test
    void persistentCheckpointResumeIsRefusedWithoutMutatingTheTextDataset(@TempDir Path root)
            throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("dataset"));
        Path checkpoint = root.resolve("checkpoint.sqlite");
        String argsHash = ArgsHashFields.forListing("s3", "", BUCKET, PREFIX).hash();
        RunKey key = new RunKey(
                "s3", null, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8), argsHash,
                "auto", ListingMode.OBJECTS,
                FilterSpecCodec.encode(null, null, null, null, null, null, null),
                OutputFormat.JSONL.name(),
                new SoftRestoreContext(
                        true, null, "us-east-1", false, false, outputDir.toString(), false,
                        OutputOptions.DestinationKind.DIRECTORY.name(), null),
                false);
        long runId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(checkpoint)) {
            RunMeta run = store.openRun(key, false, false);
            runId = run.id();
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(
                    node, "data/key-0001".getBytes(StandardCharsets.UTF_8), false));
        }

        DatasetLayout layout = DatasetLayout.of(outputDir);
        Manifest.writeState(outputDir, argsHash, runId);
        Path personal = Files.writeString(
                Files.createDirectories(layout.dataDir()).resolve("part-personal.jsonl"),
                "must survive\n");
        Path sentinel = Files.writeString(outputDir.resolve("operator-note.txt"), "do not touch\n");
        String stateBefore = Files.readString(layout.state());

        ListCommand command = new ListCommand();
        command.uri = "s3://" + BUCKET + "/" + PREFIX;
        command.connection.region = "us-east-1";
        command.connection.noSignRequest = true;
        command.checkpoint.location = checkpoint.toString();
        command.checkpoint.resume = true;
        command.output.format = OutputFormat.JSONL;
        command.output.destination = outputDir.toString();

        assertThatThrownBy(command::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("partitioned text datasets")
                .hasMessageContaining("non-resumable")
                .hasMessageContaining("--checkpoint none");

        assertThat(personal).hasContent("must survive\n");
        assertThat(sentinel).hasContent("do not touch\n");
        assertThat(layout.state()).hasContent(stateBefore);
        assertThat(layout.manifest()).doesNotExist();
        assertThat(layout.success()).doesNotExist();
        assertThat(layout.symlink()).doesNotExist();
        assertThat(outputDir.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME)).doesNotExist();
    }
}
