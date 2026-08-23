/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.ParquetWriterMemoryPlan;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Writer heap admission must fail before external work at both effective-output resolution points. */
final class ParquetWriterAdmissionOrderingTest {

    private static final String BUCKET = "bucket";
    private static final String PREFIX = "prefix/";

    @Test
    void freshRunRejectsBeforeCreatingCheckpointOrFetching(@TempDir Path dir) {
        AtomicInteger fetches = new AtomicInteger();
        ListCommand command = command(dir.resolve("dataset"), fetches);
        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(
                Path.of(command.output.destination));

        assertThatThrownBy(command::call)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("needs a conservative heap plan");
        assertThat(fetches).hasValue(0);
        assertThat(checkpoint).doesNotExist();
    }

    @Test
    void resumedRunRejectsAfterOutputRestoreButBeforeFetching(@TempDir Path dir) throws Exception {
        AtomicInteger fetches = new AtomicInteger();
        Path output = dir.resolve("dataset");
        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(output);
        Files.createDirectories(checkpoint.getParent());

        String argsHash = ArgsHashFields.forListing(
                "s3", "", BUCKET, PREFIX).hash();
        RunKey key = new RunKey(
                "s3", null, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8), argsHash,
                "auto", ListingMode.OBJECTS, "", OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, "us-east-1", false, false,
                        output.toString(), false, "DIRECTORY", null), false);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(checkpoint)) {
            RunMeta run = store.openRun(key, false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
        }

        ListCommand command = command(output, fetches);
        command.checkpoint.resume = true;
        command.checkpoint.location = checkpoint.toString();

        assertThatThrownBy(command::call)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("needs a conservative heap plan");
        assertThat(fetches).hasValue(0);
    }

    @Test
    void sortedRunIgnoresDirectDatasetWriterAdmission(@TempDir Path dir) {
        AtomicInteger fetches = new AtomicInteger();
        ListCommand command = command(dir.resolve("dataset"), fetches);
        command.sorting.sort = true;

        assertThatThrownBy(command::call)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("writer admission must happen before a fetch");
        assertThat(fetches).hasValue(1);
    }

    private static ListCommand command(Path output, AtomicInteger fetches) {
        ListCommand command = new ListCommand();
        command.uri = "s3://" + BUCKET + "/" + PREFIX;
        command.output.format = OutputFormat.PARQUET;
        command.output.destination = output.toString();
        command.output.parquetWriters = 5;
        command.connection.region = "us-east-1";
        command.maxHeapBytesOverride = ParquetWriterMemoryPlan.plannedHeapBytes(5) - 1;
        command.fetcherOverride = countingFetcher(fetches);
        return command;
    }

    private static PageFetcher countingFetcher(AtomicInteger fetches) {
        return new PageFetcher() {
            @Override public ListPage fetchPage(PageRequest request) {
                fetches.incrementAndGet();
                throw new AssertionError("writer admission must happen before a fetch");
            }

            @Override public StoreCapabilities capabilities() {
                return StoreCapabilities.s3();
            }
        };
    }
}
