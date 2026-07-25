/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.error.ListingException;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A resumed managed-Parquet run publishes a dataset made of BOTH the previous attempt's carried-over
 * finalized parts and the tail it relists, and derives {@code output_files}/{@code
 * compressed_size_bytes} from all of them — so its summary's {@code objects} must describe that same
 * whole dataset. This drives the real CLI end to end (crash, then {@code swath resume}) and pins
 * {@code objects} to the row total of the manifest the very same run wrote.
 *
 * <h2>Why this cannot go vacuous</h2>
 * The interesting state only exists if the crashed attempt left at least one DURABLY finalized part
 * for the resume to carry: with none, {@code objects} and the manifest agree trivially and the
 * assertion proves nothing. So the crash fires only once a writer lane has demonstrably rotated, and
 * the carried-over part count and row total read back out of the checkpoint are asserted non-zero —
 * and short of the whole dataset — before the resume is allowed to run.
 *
 * <h2>Why only the COMPLETED run asserts equality</h2>
 * {@code objects} counts rows the engine emitted; the manifest counts rows that reached a finalized
 * part. An interrupted run therefore legitimately shows {@code objects} ≥ manifest rows: whatever sat
 * in a still-open part at the crash was emitted but never finalized, is not in the manifest, and is
 * correctly discarded and relisted on resume ({@code I6}). That gap is bounded by the in-flight
 * writers and is asserted in that direction below; equality is a property of a run that closed its
 * writers, which is where it is asserted.
 */
final class ResumedParquetDatasetTotalsTest {

    private static final String BUCKET = "bucket";
    private static final String PREFIX = "data/";
    private static final int OBJECTS = 8_000;
    private static final int PAGE_SIZE = 100;
    /** Small enough that parts finalize early in the first attempt, leaving a durable prefix to carry. */
    private static final long PART_ROTATION_MAX_ROWS = 250;
    /**
     * Per-page latency, with the listing held to one in-flight fetch: without it a zero-latency mock
     * outruns the writers so far that every rotation lands in the drain tail AFTER the last fetch,
     * where no injected fetch fault can catch it. Real store latency is what gives the writer lanes
     * their head start; this reproduces it, cheaply.
     */
    private static final Duration PAGE_LATENCY = Duration.ofMillis(15);

    @Test
    void resumedRunReportsTheWholeDatasetItPublishesNotOnlyTheRelistedTail(@TempDir Path tmp)
            throws Exception {
        Path dataset = tmp.resolve("dataset");
        Path ckpt = CheckpointOptions.CheckpointMode.colocatedCheckpoint(dataset);
        List<byte[]> keys = keyspace();

        // Attempt 1: crash on the first fetch after a writer lane has ROTATED -- the durable prefix
        // the resume will carry. An InterruptedException passes through ListCommand#runEngineGuarded
        // un-marked, the same resumable disposition a SIGKILL mid-listing leaves.
        MockPageFetcher faulty = MockPageFetcher.builder()
                .keys(keys)
                .maxKeysCap(PAGE_SIZE)
                .pageDelay(PAGE_LATENCY)
                .interceptor((req, idx, page) -> {
                    if (anyLaneRotated(dataset)) {
                        throw new InterruptedException("injected crash once a lane had rotated a part");
                    }
                    return page;
                })
                .build();
        ListCommand crashRun = parquetRun(dataset);
        crashRun.connection.maxParallelListings = 1;
        crashRun.fetcherOverride = faulty;

        assertThatThrownBy(crashRun::call).isInstanceOf(InterruptedException.class);

        SigkillResumeHarnessSupport.FinalizedParts carried =
                SigkillResumeHarnessSupport.finalizedParts(ckpt);
        assertThat(carried.count())
                .as("the resume must have carried-over parts to describe, or this test proves nothing")
                .isPositive();
        assertThat(carried.rows())
                .as("carried-over parts must hold rows, or the two totals agree trivially")
                .isPositive();
        assertThat(carried.rows())
                .as("the crash left a genuine tail to relist -- the durable prefix is not the dataset")
                .isLessThan(OBJECTS);

        // Attempt 2: `swath resume <dir>` -- the output directory is the whole run handle.
        ResumeCommand resume = new ResumeCommand();
        resume.directory = dataset;
        resume.fetcherOverride = MockPageFetcher.builder().keys(keys).maxKeysCap(PAGE_SIZE).build();

        assertThat(resume.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(DatasetLayout.of(dataset).success()).exists();

        long manifestRows = manifestRowCount(dataset);
        assertThat(manifestRows)
                .as("the published dataset holds every object exactly once")
                .isEqualTo(OBJECTS);
        assertThat(reportObjects(dataset))
                .as("objects describes the dataset this run published -- the carried-over parts plus "
                        + "the relisted tail -- not the tail alone, so it agrees with the manifest the "
                        + "same run wrote")
                .isEqualTo(manifestRows);
    }

    private static ListCommand parquetRun(Path dataset) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = dataset.toString();
        cmd.output.partRotationMaxRows = PART_ROTATION_MAX_ROWS;
        return cmd;
    }

    private static List<byte[]> keyspace() {
        List<byte[]> keys = new ArrayList<>(OBJECTS);
        for (int i = 0; i < OBJECTS; i++) {
            keys.add(String.format(PREFIX + "key-%05d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * Whether some lane has opened its SECOND part — the signal that a rotation is already durable.
     * A lane opens {@code part-w<lane>-00001} only after {@code finalizeCurrent} has footered
     * {@code -00000} and its {@code partFinalized} commit has returned, so a lane sequence above zero
     * means at least one part is recorded in the checkpoint, with no race against the commit.
     */
    private static boolean anyLaneRotated(Path dataset) throws ListingException {
        try {
            for (Path part : DatasetLayout.of(dataset).dataParts()) {
                String name = part.getFileName().toString();
                String seq = name.substring(name.lastIndexOf('-') + 1, name.length() - ".parquet".length());
                if (Integer.parseInt(seq) > 0) {
                    return true;
                }
            }
            return false;
        } catch (IOException e) {
            throw new ListingException("harness could not read the dataset parts under " + dataset, e);
        }
    }

    private static long manifestRowCount(Path dataset) throws IOException {
        JsonNode manifest = new ObjectMapper().readTree(DatasetLayout.of(dataset).manifest().toFile());
        long rows = 0;
        for (JsonNode file : manifest.get("files")) {
            rows += file.get("rowCount").asLong();
        }
        return rows;
    }

    private static long reportObjects(Path dataset) throws IOException {
        Path report = dataset.resolve(OutputOptions.DEFAULT_SUMMARY_JSON_NAME);
        return new ObjectMapper().readTree(report.toFile()).get("objects").asLong();
    }
}
