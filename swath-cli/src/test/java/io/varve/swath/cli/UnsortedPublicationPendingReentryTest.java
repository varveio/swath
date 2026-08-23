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
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.ListingException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.PartWriter;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unsorted Parquet re-entry must distinguish "nothing left to list" from "nothing left to
 * publish." A process can fail after every part and durable cursor are checkpointed but before the
 * one completion manifest is written. That state has an empty resumable node set and no consumer
 * snapshot; resume must publish from {@code part_file} without issuing another LIST request.
 */
final class UnsortedPublicationPendingReentryTest {

    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String PREFIX = "data/";
    private static final String NO_FILTER_SPEC =
            FilterSpecCodec.encode(null, null, null, null, null, null, null);

    @Test
    void outputCompleteCheckpointWithoutManifestIsPublishedOnResumeWithoutRelisting(
            @TempDir Path dir) throws Exception {
        String argsHash = ArgsHashFields.forListing("s3", ENDPOINT, BUCKET, PREFIX).hash();
        Path outputDir = Files.createDirectories(dir.resolve("out"));
        Path db = dir.resolve("checkpoint.sqlite");
        DatasetLayout layout = DatasetLayout.of(outputDir);
        Path part = Files.createDirectories(layout.dataDir()).resolve("part-w00-00000.parquet");
        byte[] keyBytes = "data/key-00000".getBytes(StandardCharsets.UTF_8);

        try (PartWriter writer = new PartWriter(part, ParquetSchema.canonical())) {
            writer.write(new ObjectEntry(KeyBytes.of(keyBytes), 7L, 1L, "etag", "STANDARD",
                    null, false, null, null, null, null));
        }

        RunKey key = new RunKey("s3", ENDPOINT, BUCKET, PREFIX.getBytes(StandardCharsets.UTF_8),
                argsHash, "auto", ListingMode.OBJECTS, NO_FILTER_SPEC, OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, "us-east-1", false, false,
                        outputDir.toString(), false, "DIRECTORY", null), false);
        long runId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key, false, false);
            runId = run.id();
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, keyBytes, true));
            store.partFinalized(new PartFinalize(run.id(), 0, "data/" + part.getFileName(),
                    "parquet", 1L, Files.size(part),
                    List.of(new PartFinalize.DurableAdvance(node, keyBytes))));
            store.markOutputComplete(run.id());
            assertThat(store.loadResumable(run.id(), true))
                    .as("fixture: listing and output durability are complete")
                    .isEmpty();
            // Deliberately do not publish or markRunFinished: this is the terminal-publication
            // failure/crash window. The checkpoint must remain RUNNING and authoritative.
        }

        assertThat(layout.manifest()).doesNotExist();
        assertThat(layout.success()).doesNotExist();
        assertThat(CheckpointDbProbe.runStatus(db, runId)).isEqualTo("RUNNING");

        AtomicInteger fetches = new AtomicInteger();
        ListCommand resume = resumeCommand(outputDir, db, fetches);

        assertThat(resume.call()).isEqualTo(ExitCodes.SUCCESS);

        assertThat(fetches).hasValue(0);
        assertThat(layout.manifest()).exists();
        assertThat(layout.success()).exists();
        assertThat(CheckpointDbProbe.runStatus(db, runId)).isEqualTo("COMPLETED");
        JsonNode files = new ObjectMapper().readTree(layout.manifest().toFile()).get("files");
        assertThat(files).hasSize(1);
        assertThat(files.get(0).get("key").asText()).isEqualTo("data/" + part.getFileName());
        assertThat(files.get(0).get("rowCount").asLong()).isEqualTo(1L);

        // An explicit checkpoint can outlive a completed dataset. Re-entering it is a true no-op:
        // trust the last-written marker + matching direct identity, without rereading every part or
        // replacing the consumer snapshot merely to re-mark an already-COMPLETED row.
        String manifestBeforeNoOp = Files.readString(layout.manifest());
        AtomicInteger noOpFetches = new AtomicInteger();
        assertThat(resumeCommand(outputDir, db, noOpFetches).call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(noOpFetches).hasValue(0);
        assertThat(Files.readString(layout.manifest())).isEqualTo(manifestBeforeNoOp);
    }

    private static ListCommand resumeCommand(Path outputDir, Path db, AtomicInteger fetches) {
        ListCommand resume = new ListCommand();
        resume.uri = "s3://" + BUCKET + "/" + PREFIX;
        resume.connection.endpointUrl = ENDPOINT;
        resume.connection.region = "us-east-1";
        resume.connection.noSignRequest = true;
        resume.output.format = OutputFormat.PARQUET;
        resume.output.destination = outputDir.toString();
        resume.checkpoint.location = db.toString();
        resume.checkpoint.resume = true;
        resume.fetcherOverride = failOnFetch(fetches);
        return resume;
    }

    private static PageFetcher failOnFetch(AtomicInteger fetches) {
        return new PageFetcher() {
            @Override
            public ListPage fetchPage(PageRequest request) throws ListingException {
                fetches.incrementAndGet();
                throw new AssertionError("publication-only resume must not issue a LIST request");
            }

            @Override
            public StoreCapabilities capabilities() {
                return StoreCapabilities.s3();
            }
        };
    }
}
