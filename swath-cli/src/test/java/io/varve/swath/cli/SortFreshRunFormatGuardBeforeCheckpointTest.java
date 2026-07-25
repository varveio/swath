/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

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
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.runtime.ArgsHashFields;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A fresh (non-{@code swath resume}) {@code --sort} invocation with a non-{@code parquet}
 * effective format must be rejected at exit 2. The effective (TTY-defaulted) format in a test JVM
 * (no console) is {@code jsonl}, never {@code parquet}.
 *
 * <p>The first two cases below use stdout with {@code --checkpoint auto}, which is structurally
 * ephemeral (no checkpoint file exists to open, whether or not this guard fires), rather than an
 * explicit durable checkpoint path: a fresh, non-directory destination paired with an EXPLICIT
 * (non-{@code auto}, non-{@code none}) checkpoint path is itself refused even earlier, by the
 * stdout/single-file checkpoint guard -- so there is no longer a reachable vehicle that pairs a
 * non-{@code parquet} format with a real, would-be-mutated checkpoint file to observe ordering
 * against. That earlier guard makes the "no durable mutation for a doomed --sort request"
 * invariant structural rather than something this test can still pin by file-existence; what
 * remains directly provable here is the exit code and message. {@link
 * #freshSortWithParquetFormat_isAcceptedAndDoesOpenTheCheckpoint} is the contrasting valid path,
 * where a real checkpoint file IS opened.
 */
final class SortFreshRunFormatGuardBeforeCheckpointTest {

    @Test
    void freshSortWithoutExplicitFormat_isRejectedAtExitTwo() {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/data/";
        cmd.checkpoint.location = "auto";
        cmd.sorting.sort = true;
        // No cmd.output.format set — the effective format defaults (no TTY in a test JVM) to jsonl, not
        // parquet, and no swath resume is set either.

        InvalidArgsException ex = catchThrowableOfType(InvalidArgsException.class, cmd::call);
        assertThat(ex).as("--sort with an implicit non-parquet format must be rejected").isNotNull();
        assertThat(ex).hasMessageContaining("--sort requires --format parquet");
        assertThat(ex.exitCode()).isEqualTo(2);
    }

    @Test
    void freshSortWithExplicitBadFormat_isAlsoRejectedAtExitTwo() {
        // The pre-existing explicit-format case must keep working identically (same early guard).
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/data/";
        cmd.checkpoint.location = "auto";
        cmd.sorting.sort = true;
        cmd.output.format = OutputFormat.JSONL;

        InvalidArgsException ex = catchThrowableOfType(InvalidArgsException.class, cmd::call);
        assertThat(ex).isNotNull();
        assertThat(ex.exitCode()).isEqualTo(2);
    }

    @Test
    void freshSortWithParquetFormat_isAcceptedAndDoesOpenTheCheckpoint(@TempDir Path dir) throws Exception {
        // Sanity/contrast: a VALID --sort invocation (parquet, no resume) still reaches (and creates)
        // the checkpoint — this fix only rejects the invalid case earlier, it doesn't change the
        // valid path's plumbing. Seeded output-complete so the run finishes without any real S3 call.
        Path outputDir = Files.createDirectories(dir.resolve("out"));
        Path db = dir.resolve("c.sqlite");
        String argsHash = ArgsHashFields.forListing(
                "s3", "http://localhost:4566", "bucket", "data/").hash();
        String noFilterSpec = FilterSpecCodec.encode(null, null, null, null, null, null, null);
        RunKey key = new RunKey("s3", "http://localhost:4566",
                "bucket", "data/".getBytes(StandardCharsets.UTF_8), argsHash, "auto",
                ListingMode.OBJECTS, noFilterSpec, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false, outputDir.toString(), false, null, null), true);
        long runId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            runId = run.id();
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, "k".getBytes(
                    StandardCharsets.UTF_8), true));
            store.markOutputComplete(run.id());
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
            // Identity lives in .swath-state.json (what isPublishedByThisRun consults).
            DatasetLayout layout =
                    DatasetLayout.of(outputDir);
            Files.writeString(layout.state(),
                    "{\"args_hash\":\"" + argsHash + "\",\"run_id\":" + run.id() + "}");
            Files.writeString(layout.manifest(),
                    "{\"sourceBucket\":\"bucket\",\"version\":\"1\",\"fileFormat\":\"Parquet\",\"files\":[]}");
        }

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/data/";
        cmd.connection.endpointUrl = "http://localhost:4566";
        cmd.checkpoint.location = db.toString();
        cmd.output.destination = outputDir.toString();
        cmd.output.format = OutputFormat.PARQUET;
        cmd.checkpoint.resume = true;
        cmd.sorting.sort = true;

        assertThat(cmd.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(Files.exists(db)).as("the valid path still opens the checkpoint").isTrue();
        assertThat(CheckpointDbProbe.runStatus(db, runId)).isEqualTo("COMPLETED");
    }
}
