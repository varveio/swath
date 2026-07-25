/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.error.ThrottleException;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A fresh run whose SEED aborts STUCK (a seed-time transient storm exhausts
 * {@code TransientRetryFetcher}'s retry cap) commits ZERO worklist nodes (I2, all-or-nothing) and
 * leaves the run RUNNING — NOT fatal-FAILED. A later {@code swath resume} therefore passes the
 * fatal-refusal gate with an EMPTY worklist.
 *
 * <p>Do not seed only {@code if (!run.resumed())}: a never-seeded (zero-node) resumed run must be
 * re-seeded, because skipping the re-seed trades the loud STUCK for a silent false-completeness — a
 * seed failure must never resume as complete:
 * <ul>
 *   <li><b>plain path</b>: {@code nodes.isEmpty()} → wrote {@code completed:true} / exit 0 with NO
 *       output — a false COMPLETE;</li>
 *   <li><b>--sort path</b>: {@code nodes.isEmpty()} → merged over empty staging and PUBLISHED an
 *       empty {@code _SUCCESS}/{@code COMPLETED} dataset.</li>
 * </ul>
 * These tests drive the SEED end-to-end through the full {@code call()} flow with a
 * {@link MockPageFetcher} that throttles on the fresh-run seed (so it cancels STUCK) then succeeds
 * on resume, and assert the resumed run's output equals a clean (never-STUCK) run's — with no false
 * completion and no empty publish: the resumed plain output must equal the clean run's key set, and
 * the resumed sorted dataset must equal the clean sorted keys.
 */
@Tag("deep")   // each seed-STUCK exhausts the fixed 8-retry cap via real jittered backoffs (~8s).
final class SeedStuckReseedResumeContractTest {

    private static final List<byte[]> KEYS = List.of(
            b("data/a/1"), b("data/a/2"), b("data/b/1"), b("data/b/2"), b("data/c/1"));
    private static final List<String> EXPECTED_SORTED =
            List.of("data/a/1", "data/a/2", "data/b/1", "data/b/2", "data/c/1");

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A clean fetcher: seeds + lists normally. */
    private static MockPageFetcher cleanFetcher() {
        return MockPageFetcher.builder().keys(KEYS).build();
    }

    /**
     * A fetcher whose every fetch throws a non-voting client attempt-timeout — the seed's very first
     * probe exhausts {@code TransientRetryFetcher.MAX_TRANSIENT_RETRIES} and trips the run's
     * cancellation with {@code StopReason.STUCK}, so the fresh-run seed aborts before committing any
     * node.
     */
    private static MockPageFetcher throttlingFetcher() {
        return MockPageFetcher.builder().keys(KEYS)
                .interceptor((req, idx, page) -> {
                    throw ThrottleException.attemptTimeout("seed-time transient storm");
                })
                .build();
    }

    @Test
    @Timeout(120)
    void plainPath_seedStuckThenResume_reSeedsAndMatchesACleanRun(@TempDir Path dir) throws Exception {
        // (0) A clean reference run establishes the expected output union.
        Path cleanDb = dir.resolve("clean.sqlite");
        Path cleanOut = dir.resolve("clean");
        ListCommand clean = SeedListCommands.baseCommand(cleanDb, cleanFetcher());
        clean.output.format = OutputFormat.PARQUET;
        clean.output.destination = cleanOut.toString();
        assertThat(clean.call()).isEqualTo(ExitCodes.SUCCESS);
        Set<String> expected = parquetKeys(cleanOut);
        assertThat(expected).as("the clean run lists every object").hasSize(KEYS.size());

        // (1) Fresh run whose SEED aborts STUCK: a resumable exit 75, ZERO nodes, RUNNING (not fatal).
        Path db = dir.resolve("c.sqlite");
        Path out = dir.resolve("out");
        ListCommand stuck = SeedListCommands.baseCommand(db, throttlingFetcher());
        stuck.output.format = OutputFormat.PARQUET;
        stuck.output.destination = out.toString();
        // A ThrottleException seed storm exhausts the cap and aborts STUCK only under
        // RetryPolicy.BOUNDED — i.e. when NO watchdog is armed. Disarm both watchdog windows so this
        // test exercises exactly that bounded seed-STUCK path (with a watchdog armed the seed would
        // RIDE OUT the storm indefinitely, never reaching the re-seed-on-resume subject here).
        stuck.liveness.stallTimeout = "0";
        stuck.liveness.noProgressTimeout = "0";
        assertThat(stuck.call())
                .as("a seed-time transient storm aborts resumably STUCK (exit 75), not fatally")
                .isEqualTo(ExitCodes.STUCK);
        assertThat(CheckpointDbProbe.nodeCount(db)).as("I2: a STUCK seed commits ZERO nodes").isZero();
        assertThat(CheckpointDbProbe.runStatus(db))
                .as("a STUCK seed leaves the run RUNNING, resumable").isEqualTo("RUNNING");
        assertThat(CheckpointDbProbe.fatalError(db))
                .as("a STUCK seed is never fatal (would poison swath resume)").isFalse();
        assertThat(DatasetLayout.of(out).dataParts())
                .as("no listing happened, so the dataset has no output parts yet")
                .isEmpty();
        assertThat(Files.exists(DatasetLayout.of(out).success()))
                .as("a STUCK seed publishes no dataset")
                .isFalse();

        // (2) Resume with a clean fetcher: the never-seeded run must RE-SEED, list, and complete.
        ListCommand resume = SeedListCommands.baseCommand(db, cleanFetcher());
        resume.output.format = OutputFormat.PARQUET;
        resume.output.destination = out.toString();
        resume.checkpoint.resume = true;
        assertThat(resume.call())
                .as("the resumed run re-seeds and lists to genuine completion")
                .isEqualTo(ExitCodes.SUCCESS);

        assertThat(parquetKeys(out))
                .as("re-seeded resume output union == the clean run (no false-complete, no missing/dup keys)")
                .isEqualTo(expected);
        assertThat(CheckpointDbProbe.runStatus(db))
                .as("the run reaches genuine COMPLETED only AFTER real listing")
                .isEqualTo("COMPLETED");
    }

    @Test
    @Timeout(120)
    void sortPath_seedStuckThenResume_reSeedsAndPublishesTheCorrectSortedDataset(@TempDir Path dir)
            throws Exception {
        // (0) A clean reference sorted run establishes the expected published dataset.
        Path cleanDb = dir.resolve("clean.sqlite");
        Path cleanOut = Files.createDirectories(dir.resolve("clean-out"));
        ListCommand clean = SeedListCommands.baseCommand(cleanDb, cleanFetcher());
        clean.output.format = OutputFormat.PARQUET;
        clean.output.destination = cleanOut.toString();
        clean.sorting.sort = true;
        assertThat(clean.call()).isEqualTo(ExitCodes.SUCCESS);
        assertThat(sortedKeys(cleanOut))
                .as("the clean sorted run publishes every object in key order")
                .isEqualTo(EXPECTED_SORTED);

        // (1) Fresh sorted run whose SEED aborts STUCK: resumable exit 75, ZERO nodes, nothing published.
        Path db = dir.resolve("c.sqlite");
        Path out = Files.createDirectories(dir.resolve("out"));
        ListCommand stuck = SeedListCommands.baseCommand(db, throttlingFetcher());
        stuck.output.format = OutputFormat.PARQUET;
        stuck.output.destination = out.toString();
        stuck.sorting.sort = true;
        // A ThrottleException seed storm aborts STUCK only under RetryPolicy.BOUNDED (no watchdog
        // armed) — disarm both windows (see the plain-path note above).
        stuck.liveness.stallTimeout = "0";
        stuck.liveness.noProgressTimeout = "0";
        assertThat(stuck.call())
                .as("a seed-time transient storm aborts resumably STUCK (exit 75), not fatally")
                .isEqualTo(ExitCodes.STUCK);
        assertThat(CheckpointDbProbe.nodeCount(db)).as("I2: a STUCK seed commits ZERO nodes").isZero();
        assertThat(CheckpointDbProbe.runStatus(db)).isEqualTo("RUNNING");
        assertThat(Files.exists(DatasetLayout.of(out).success()))
                .as("a STUCK seed publishes nothing — no _SUCCESS")
                .isFalse();

        // (2) Resume with a clean fetcher: the never-seeded run must RE-SEED, list, merge, and publish
        //     the CORRECT dataset — NOT an empty _SUCCESS/COMPLETED (the pre-fix false-completeness).
        ListCommand resume = SeedListCommands.baseCommand(db, cleanFetcher());
        resume.output.format = OutputFormat.PARQUET;
        resume.output.destination = out.toString();
        resume.sorting.sort = true;
        resume.checkpoint.resume = true;
        assertThat(resume.call())
                .as("the resumed sorted run re-seeds, lists, merges, and publishes")
                .isEqualTo(ExitCodes.SUCCESS);

        assertThat(Files.exists(DatasetLayout.of(out).success()))
                .as("the resumed sorted run genuinely publishes _SUCCESS")
                .isTrue();
        assertThat(sortedKeys(out))
                .as("re-seeded sorted resume publishes the CORRECT dataset, NOT an empty publish")
                .isNotEmpty()
                .isEqualTo(EXPECTED_SORTED);
        assertThat(CheckpointDbProbe.runStatus(db))
                .as("the run reaches genuine COMPLETED only AFTER real listing + merge")
                .isEqualTo("COMPLETED");
    }

    // ---- reads over the produced output (checkpoint reads: CheckpointDbProbe) --------------------------

    private static Set<String> parquetKeys(Path outputDir) throws IOException {
        Set<String> keys = new LinkedHashSet<>();
        for (Path part : DatasetLayout.of(outputDir).dataParts()) {
            keys.addAll(ParquetReads.keys(part));
        }
        return keys;
    }

    private static List<String> sortedKeys(Path outputDir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Path part : DatasetLayout.of(outputDir).dataParts()) {
            keys.addAll(ParquetReads.keys(part));
        }
        return keys;
    }
}
