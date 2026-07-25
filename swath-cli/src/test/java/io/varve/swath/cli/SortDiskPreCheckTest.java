/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.varve.swath.error.OutputException;
import io.varve.swath.output.OutputFormat;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * {@link ListCommand#checkSortDiskHeadroom} — the one-shot startup half of {@code
 * SortDiskGuard}, exercised directly (package-private method, plain object construction — no
 * picocli parsing needed) with a FAKED usable-free-bytes value ({@link
 * ListCommand#usableFreeBytesOverride}) so no real disk is ever filled.
 */
final class SortDiskPreCheckTest {

    @Test
    void refusesWhenUsableFreeSpaceIsBelowTheMinimumFloor(@TempDir Path root) throws Exception {
        Path outDir = Files.createDirectories(root.resolve("out"));
        ListCommand cmd = new ListCommand();
        cmd.output.destination = outDir.toString();
        cmd.usableFreeBytesOverride = dir -> 100L * 1024 * 1024;   // 100 MiB, well below the 1 GiB floor

        assertThatThrownBy(cmd::checkSortDiskHeadroom)
                .isInstanceOf(OutputException.class)
                .hasMessageContaining("--sort disk pre-check refused")
                .hasMessageContaining("sort.ignore-disk-check");
    }

    /**
     * The pre-check's refusal must be greppable via the SAME distinct classification
     * as the in-run halt path ({@code SortDiskGuard#logExhaustionMarker}) so an external supervisor
     * can tell "ran out of --sort staging disk" apart from an unclassified crash regardless of
     * WHEN it fires (startup vs. mid-run).
     */
    @Test
    void refusalLogsADistinctErrorClassAndAResumableStopReasonMarker(@TempDir Path root) throws Exception {
        Path outDir = Files.createDirectories(root.resolve("out"));
        ListCommand cmd = new ListCommand();
        cmd.output.destination = outDir.toString();
        cmd.usableFreeBytesOverride = dir -> 100L * 1024 * 1024;   // below the 1 GiB floor

        Logger logger =
                (Logger) LoggerFactory.getLogger(ListCommand.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.ERROR);
        logger.addAppender(appender);
        try {
            assertThatThrownBy(cmd::checkSortDiskHeadroom).isInstanceOf(OutputException.class);
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        String marker = appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("sort_disk_precheck_refused"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no sort_disk_precheck_refused marker emitted"));
        assertThat(marker)
                .contains("error_class=sort_disk_exhausted")
                .contains("stop_reason=sort_disk_exhausted")
                .contains("resumable=true");
    }

    @Test
    void refusesWhenStagingAlreadyOnDiskProjectsPastFreeSpace(@TempDir Path root) throws Exception {
        Path outDir = Files.createDirectories(root.resolve("out"));
        Path stagingDir = Files.createDirectories(outDir.resolve(ListCommand.SORT_STAGING_DIR));
        // A SMALL real file on disk (400 MiB, never gigabytes-scale -- no real disk is meaningfully
        // filled) so the pre-check's own staging-bytes-on-disk WALK (not the override, which only
        // fakes usable free space) is genuinely exercised end to end. Large enough that 3x it
        // clears the 1 GiB absolute floor, so this trips the PROJECTION arm specifically, not the
        // floor arm (SortDiskGuardTest's pure-function tests already pin the floor arm precisely).
        long stagedBytes = 400L * 1024 * 1024;
        writeFileOfSize(stagingDir.resolve("seg-0.parquet"), stagedBytes);

        ListCommand cmd = new ListCommand();
        cmd.output.destination = outDir.toString();
        // 1100 MiB free: above the 1 GiB floor, but below the default 3x-staged (~1200 MiB) need.
        cmd.usableFreeBytesOverride = dir -> 1100L * 1024 * 1024;

        assertThatThrownBy(cmd::checkSortDiskHeadroom)
                .isInstanceOf(OutputException.class)
                .hasMessageContaining("already staged/observed");
    }

    @Test
    void allowsWhenUsableFreeSpaceIsGenerous(@TempDir Path root) throws Exception {
        Path outDir = Files.createDirectories(root.resolve("out"));
        ListCommand cmd = new ListCommand();
        cmd.output.destination = outDir.toString();
        cmd.usableFreeBytesOverride = dir -> 500L * 1024 * 1024 * 1024;   // 500 GiB, ample

        assertThatNoException().isThrownBy(cmd::checkSortDiskHeadroom);
    }

    @Test
    void forceSortSkipsTheCheckEntirelyEvenWithNoUsableSpace(@TempDir Path root) throws Exception {
        Path outDir = Files.createDirectories(root.resolve("out"));
        ListCommand cmd = new ListCommand();
        cmd.output.destination = outDir.toString();
        cmd.sorting.forceSort = true;
        cmd.usableFreeBytesOverride = dir -> 0L;   // would refuse if the check ran at all

        assertThatNoException().isThrownBy(cmd::checkSortDiskHeadroom);
    }

    @Test
    void aFreshRunWithNothingStagedYetIsNeverRefusedByTheProjectionArm(@TempDir Path root) throws Exception {
        Path outDir = Files.createDirectories(root.resolve("out"));
        ListCommand cmd = new ListCommand();
        cmd.output.destination = outDir.toString();
        // Below the default 3x-staged need, but still above the absolute floor and nothing is
        // staged yet -- a brand-new run's pre-check cannot know the eventual bucket size (the
        // periodic in-run SortDiskGuard is what catches that case once real data starts flowing).
        cmd.usableFreeBytesOverride = dir -> 2L * 1024 * 1024 * 1024;

        assertThatNoException().isThrownBy(cmd::checkSortDiskHeadroom);
    }

    /**
     * If {@code checkSortDiskHeadroom()} ran AFTER {@code seedFreshRun}, the pre-check's "refused
     * before any listing began" marker would be FALSE on a fresh run (a seed probe + a checkpoint
     * node would already have landed). {@code runWithCheckpoint} must call it strictly BEFORE the
     * {@code S3Client}/{@code seedFreshRun}. This end-to-end test (real {@code cmd.call()}, real
     * sqlite file) pins that ordering: a fresh {@code --sort} run with insufficient disk must
     * refuse with ZERO checkpoint nodes ever inserted — the run row itself (bookkeeping, not
     * "listing") is allowed to exist.
     */
    @Test
    void freshSortRunRefusalFiresBeforeAnySeedNodeIsInserted(@TempDir Path root) throws Exception {
        Path outDir = Files.createDirectories(root.resolve("out"));
        Path db = root.resolve("c.sqlite");

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/data/";
        cmd.connection.region = "us-east-1";
        cmd.checkpoint.location = db.toString();
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = outDir.toString();
        cmd.sorting.sort = true;
        cmd.usableFreeBytesOverride = dir -> 100L * 1024 * 1024;   // well below the 1 GiB floor

        OutputException ex =
                catchThrowableOfType(OutputException.class, cmd::call);
        assertThat(ex).as("a fresh --sort run with insufficient disk must refuse via the pre-check")
                .isNotNull();
        assertThat(ex.exitCode()).isEqualTo(1);

        assertThat(Files.exists(db)).as("openRun's run-row bookkeeping still happens first").isTrue();
        assertThat(CheckpointDbProbe.nodeCount(db))
                .as("no node was ever seeded -- the disk refusal preceded seedFreshRun's first S3 probe")
                .isZero();
    }

    private static void writeFileOfSize(Path file, long bytes) throws Exception {
        try (var out = Files.newOutputStream(file)) {
            byte[] buf = new byte[1024 * 1024];
            long remaining = bytes;
            while (remaining > 0) {
                int n = (int) Math.min(buf.length, remaining);
                out.write(buf, 0, n);
                remaining -= n;
            }
        }
    }
}
