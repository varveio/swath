/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A seed that hits a client-side abort resolves to the resumable exit-75 STUCK disposition — the run
 * stays RUNNING (resumable), never a fatal FAILED / exit-1 crash.
 *
 * <p><b>Why this must hold.</b> {@code S3PageFetcher} classifies a client-side abort with no thread
 * interrupt as a retryable {@code ThrottleException} ({@code Kind.ATTEMPT_TIMEOUT}), never a bare
 * {@link InterruptedException} — so a real client-side abort is retried under
 * {@code TransientRetryFetcher}'s bounded cap, same as an attempt timeout. {@code
 * ListCommand.runWithCheckpoint}'s seed catch treats ANY {@code InterruptedException} it receives as
 * resumable STUCK, never fatal: a fatal seed catch writes a SEED_FAILURE summary, marks the run
 * FAILED (poisoning {@code swath resume}), and rethrows — which {@code ExitCodes.forThrowable} maps
 * to exit 1 ({@code UNEXPECTED}) instead of the resumable exit 75. A mutant that lets either
 * classification slip turns this test RED.
 *
 * <p><b>Injection layer (why this seam).</b> {@code ListCommand}'s only test seam is
 * {@code fetcherOverride}, a {@link PageFetcher} that sits ABOVE {@code S3PageFetcher} — there is no
 * {@code S3Client} seam to drive a real abort through the client end-to-end. The injected fetcher
 * instead throws a bare {@link InterruptedException} directly, WITHOUT setting the interrupt flag,
 * matching {@code SeedStuckReseedResumeContractTest}'s seam — making the ListCommand / seed-handling
 * disposition of that exception the object under test, and driving a REAL end-to-end exit code
 * (75 vs 1). Unlike a {@code ThrottleException} burst (which burns the 8-retry cap over ~8s of
 * jittered backoff), a bare {@code InterruptedException} is not retried by
 * {@code TransientRetryFetcher}, so the seed aborts on the first probe — the test is fast.
 */
final class SeedAbortResumableContractTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<byte[]> KEYS = List.of(
            b("data/a/1"), b("data/a/2"), b("data/b/1"));

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A fetcher whose every fetch throws a bare {@link InterruptedException}, thrown WITHOUT setting
     * the interrupt flag, so the run's cancellation is never tripped and the seed's very first probe
     * aborts NOT-cancelled.
     */
    private static MockPageFetcher abortStormFetcher() {
        return MockPageFetcher.builder().keys(KEYS)
                .interceptor((req, idx, page) -> {
                    throw new InterruptedException(
                            "S3 listObjectsV2 aborted: simulated SDK abort storm");
                })
                .build();
    }

    /** The REAL process exit code: {@code call()}'s return, or the throwable's mapped code if it throws. */
    private static int effectiveExit(ListCommand cmd) {
        try {
            return cmd.call();
        } catch (Exception e) {
            return ExitCodes.forThrowable(e);
        }
    }

    @Test
    @Timeout(60)
    void seedAbortStorm_isResumableStuckExit75_notFatalExit1(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path out = dir.resolve("out");
        Path summary = dir.resolve("summary.json");
        ListCommand stuck = SeedListCommands.baseCommand(db, abortStormFetcher());
        stuck.output.format = OutputFormat.PARQUET;
        stuck.output.destination = out.toString();
        stuck.output.summaryJson = summary.toString();

        int exit = effectiveExit(stuck);

        assertThat(exit)
                .as("a seed-time client abort storm is a RESUMABLE tempfail (exit 75 STUCK), not a "
                        + "fatal exit 1")
                .isEqualTo(ExitCodes.STUCK);
        assertThat(CheckpointDbProbe.nodeCount(db)).as("I2: an aborted seed commits ZERO nodes").isZero();
        assertThat(CheckpointDbProbe.runStatus(db))
                .as("an aborted seed leaves the run RUNNING (resumable), never fatal-FAILED")
                .isEqualTo("RUNNING");
        assertThat(CheckpointDbProbe.fatalError(db))
                .as("an abort storm is never fatal (a fatal flag would poison swath resume)")
                .isFalse();
        assertThat(DatasetLayout.of(out).dataParts())
                .as("no listing happened, so the dataset has no output parts")
                .isEmpty();
        assertThat(Files.exists(DatasetLayout.of(out).success()))
                .as("a STUCK seed never publishes the dataset")
                .isFalse();

        JsonNode root = MAPPER.readTree(summary.toFile());
        assertThat(root.get("completed").asBoolean()).as("a STUCK seed is never completed").isFalse();
        assertThat(root.get("stop_reason").asText())
                .as("the terminal disposition is STUCK, not the fatal seed_failure")
                .isEqualTo("stuck");
    }
}
