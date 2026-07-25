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
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.UnsupportedBucketException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
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

final class SafetyBoundaryTest {

    @Test
    void directoryBucketIsTypedActionableAndRefusedBeforeAnyFetchOrCheckpoint(@TempDir Path dir) {
        AtomicInteger fetches = new AtomicInteger();
        ListCommand command = new ListCommand();
        command.uri = "s3://orders--use1-az4--x-s3/prefix";
        command.connection.region = "us-east-1";
        command.output.format = OutputFormat.PARQUET;
        command.output.destination = dir.resolve("dataset").toString();
        command.fetcherOverride = countingFetcher(fetches);

        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(
                Path.of(command.output.destination));
        assertThatThrownBy(command::call)
                .isInstanceOf(UnsupportedBucketException.class)
                .hasMessageContaining("directory bucket")
                .hasMessageContaining("general-purpose S3 bucket")
                .hasMessageContaining("opaque-token sequential listing support")
                .satisfies(error -> assertThat(ExitCodes.forThrowable(error)).isEqualTo(2));
        assertThat(fetches).hasValue(0);
        assertThat(checkpoint).doesNotExist();
    }

    @Test
    void credentialBearingS3EndpointFailsBeforeCheckpointAndNeverEchoesSecret(@TempDir Path dir) {
        ListCommand command = new ListCommand();
        command.uri = "s3://bucket/prefix";
        command.connection.endpointUrl = "https://user:top-secret@example.test";
        command.connection.region = "us-east-1";
        command.output.format = OutputFormat.PARQUET;
        command.output.destination = dir.resolve("dataset").toString();

        Path checkpoint = CheckpointOptions.CheckpointMode.colocatedCheckpoint(
                Path.of(command.output.destination));
        assertThatThrownBy(command::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("invalid --endpoint-url")
                .hasMessageNotContaining("top-secret")
                .satisfies(error -> assertThat(ExitCodes.forThrowable(error)).isEqualTo(2));
        assertThat(checkpoint).doesNotExist();
    }

    @Test
    void resumeRejectsCredentialBearingEndpointFromCheckpointBeforeFetchAndWithoutEcho(@TempDir Path dir)
            throws Exception {
        String endpoint = "https://user:checkpoint-secret@example.test";
        String bucket = "bucket";
        String prefix = "data/";
        Path output = dir.resolve("dataset");
        Path db = CheckpointOptions.CheckpointMode.colocatedCheckpoint(output);
        Files.createDirectories(db.getParent());
        String argsHash = ArgsHashFields.forListing("s3", endpoint, bucket, prefix).hash();
        RunKey key = new RunKey("s3", endpoint, bucket, prefix.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, "", OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, "us-east-1", false, false,
                        output.toString(), false, "DIRECTORY", null), false);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
        }

        AtomicInteger fetches = new AtomicInteger();
        ResumeCommand command = new ResumeCommand();
        command.directory = output;
        command.fetcherOverride = countingFetcher(fetches);

        assertThatThrownBy(command::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("invalid --endpoint-url")
                .hasMessageNotContaining("checkpoint-secret")
                .satisfies(error -> assertThat(ExitCodes.forThrowable(error)).isEqualTo(2));
        assertThat(fetches).hasValue(0);
    }

    private static PageFetcher countingFetcher(AtomicInteger fetches) {
        return new PageFetcher() {
            @Override
            public ListPage fetchPage(PageRequest req) throws ListingException {
                fetches.incrementAndGet();
                throw new AssertionError("fetch must not occur");
            }

            @Override
            public StoreCapabilities capabilities() {
                return StoreCapabilities.s3();
            }
        };
    }
}
