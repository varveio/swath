/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.fixture;

import io.micrometer.core.instrument.Timer;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.DuplicateKeyException;
import io.varve.swath.sort.SortArm;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortTransformResult;
import io.varve.swath.sort.VersionedCaptureException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * {@code sort-fixture}: turns a legacy/unsorted swath Parquet capture into one stamped, sorted
 * Parquet file a user could keep — nothing replay-specific about the output itself. All
 * the actual sorting lives in the root {@link CaptureSorter}; this class only resolves CLI args,
 * times the run, and reports a summary — so no parquet/hadoop type needs to appear on this
 * module's compile classpath.
 *
 * <p>Duplicate-key and versioned-capture fail-fasts (§0.6) surface as {@link
 * DuplicateKeyException}/{@link VersionedCaptureException}; this
 * command catches both and prints a one-line error instead of a stack trace, exiting {@code 1}.
 */
@Command(name = "sort-fixture",
        mixinStandardHelpOptions = true,
        description = "Sort a legacy/unsorted swath Parquet capture into one stamped, sorted Parquet file.")
public final class SortFixtureCommand implements Callable<Integer> {

    @Option(names = "--capture", required = true, paramLabel = "PATH",
            description = "Unsorted swath Parquet capture: a single part file, or a directory of *.parquet parts.")
    Path capture;

    @Option(names = "--output", required = true, paramLabel = "DIR",
            description = "Output directory the sorted Parquet file(s) are published into.")
    Path output;

    private final FixtureMetrics metrics;

    public SortFixtureCommand() {
        this(new FixtureMetrics());
    }

    SortFixtureCommand(FixtureMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public Integer call() {
        long wallStartNanos = System.nanoTime();
        Timer.Sample sample = metrics.startTimer();
        SortTransformResult result;
        try {
            CaptureSorter sorter = new CaptureSorter(SortConfig.fromSystemProperties(), metrics);
            result = sorter.sort(capture, output);
        } catch (DuplicateKeyException | VersionedCaptureException e) {
            System.err.println("sort-fixture: " + e.getMessage());
            return 1;
        } catch (IOException e) {
            System.err.println("sort-fixture: " + e);
            return 1;
        }

        long outputBytes;
        try {
            outputBytes = totalBytes(result.finalFiles());
        } catch (IOException e) {
            outputBytes = 0;
        }
        metrics.recordSortFixtureBuild(sample, outputBytes);
        long wallMs = (System.nanoTime() - wallStartNanos) / 1_000_000;
        // segments has no field on SortTransformResult, so it's read back from the registry-backed
        // metrics adapter above (SORT.segment_flushed) — see FixtureMetrics#segmentsFlushed.
        System.out.printf("sort_fixture arm=%s rows=%d files=%d bytes=%d wall_ms=%d segments=%d "
                        + "merge_passes=%d cascaded_passes=%d "
                        + "proof_spool_logical_extent_bytes=%d "
                        + "proof_spool_preallocation_operations=%d "
                        + "proof_spool_preallocation_attempted_bytes=%d "
                        + "proof_spool_mapped_operations=%d proof_spool_mapped_bytes=%d "
                        + "proof_spool_ms=%d output=%s%n",
                SortArm.SORT_FIXTURE, result.totalRows(), result.finalFiles().size(), outputBytes, wallMs,
                metrics.segmentsFlushed(), result.mergePasses(), result.cascadedPasses(),
                metrics.proofSpoolLogicalExtentBytes(), metrics.proofSpoolPreallocationOperations(),
                metrics.proofSpoolPreallocationAttemptedBytes(), metrics.proofSpoolMappedOperations(),
                metrics.proofSpoolMappedBytes(), metrics.proofSpoolMillis(),
                output.toAbsolutePath());
        return 0;
    }

    private static long totalBytes(List<Path> files) throws IOException {
        long total = 0;
        for (Path f : files) {
            total += Files.size(f);
        }
        return total;
    }
}
