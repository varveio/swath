/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import io.varve.swath.output.parquet.sorted.SortedParquetWriter;
import io.varve.swath.output.sorted.SortedDatasetResult;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortedFileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Coverage beyond {@code ReplayServingFactoryTest}'s own regression tests (which prove the
 * FIRST-file-vouches bug and a 2-file trailing-unstamped/-truncated case): genuinely {@code
 * MIDDLE}-positioned failures in a {@code >= 3}-file rolled fixture, and the {@code mode=versions}
 * eligibility branch, which no existing multi-file test exercised. These guard that {@code auto}
 * checks EVERY resolved file in order and never short-circuits on "first is good" or "last is bad".
 */
class ReplayServingFactoryEligibilityScanTest {

    /**
     * (a) A genuinely MIDDLE file — not the last of the set — is unstamped, sandwiched between two
     * good stamped files (index 1 before it, a good stamped index-3-final file after it). Every file
     * must be checked in order.
     */
    @Test
    void aMiddleUnstampedFileInAThreeFileDirIsCaughtBeforeReachingTheGoodFinalFile(@TempDir Path dir)
            throws IOException {
        Path fixtureDir = rollToNFiles(dir, "a", "m", "z");   // part-00001=a, 00002=m, 00003=z(final)
        List<Path> files = SortedFixtures.resolveFiles(fixtureDir);
        assertThat(files).hasSize(3);
        Path middle = files.get(1);
        // Replace the middle file's bytes with a plain, unstamped part (arbitrary different keys —
        // content doesn't matter, only that the stamp is absent).
        writeUnsortedPartInPlace(middle, "unstamped-x", "unstamped-y");

        assertThatThrownBy(() -> ReplayServingFactory.open(fixtureDir, ServingMode.SORTED, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no_stamp");

        ReplayServingFactory.Result result = ReplayServingFactory.open(fixtureDir, ServingMode.DUCKDB, 1);
        try {
            // DuckDB role-1 still serves every key from every file in the directory, correctly.
            List<String> served = keys(result.fixture().list(
                    new S3ListRequest("bucket", null, null, null, null, 1000, true, false)));
            assertThat(served).containsExactlyInAnyOrder("a", "z", "unstamped-x", "unstamped-y");
        } finally {
            result.fixture().close();
        }
    }

    /**
     * (b) A MIDDLE file is stamped {@code mode=versions} — a shape no existing multi-file test covers
     * (the mixed-row-type / unsupported-mode regressions in {@code ReplayServingFactoryTest} are all
     * single-file). {@code --serving-mode sorted} must hard-fail with {@code unsupported_mode} before
     * ever reaching the completeness check or the good final file.
     */
    @Test
    void aMiddleFileStampedVersionsModeIsRefusedAsUnsupportedMode(@TempDir Path dir) throws IOException {
        Path fixtureDir = rollToNFiles(dir, "a", "m", "z");
        List<Path> files = SortedFixtures.resolveFiles(fixtureDir);
        assertThat(files).hasSize(3);
        Path middle = files.get(1);
        // Overwrite the middle file with a genuinely versions-stamped file at the SAME position
        // (file_index=2, not final — matching what a real 3-file roll would have produced there).
        try (SortedFileWriter writer = new SortedParquetWriter(
                middle, SortConfig.fromSystemProperties(), SortMode.VERSIONS, 2)) {
            writer.write(objectEntry("m"));
        }
        assertThat(SortedParquetStamp.read(middle)).hasValueSatisfying(s -> assertThat(s.mode()).isEqualTo(SortMode.VERSIONS));

        assertThatThrownBy(() -> ReplayServingFactory.open(fixtureDir, ServingMode.SORTED, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsupported_mode");
    }

    /**
     * (c) The missing-MIDDLE-file variant of a crash-partial rolled publish, at the real {@code
     * ReplayServingFactory.open()} integration level (the existing {@code
     * ReplayServingFactoryCompletenessTest} only proves the pure {@code multiFileCompletenessViolation}
     * math in isolation with fabricated stamps; the existing {@code ReplayServingFactoryTest} crash
     * test only deletes the TRUE LAST file). A real 4-file roll with a real middle file (index 2 of 4)
     * deleted — as if the process crashed after renaming files 1 and 2, skipped 3 somehow, then died
     * before 4 could ever be reached — leaves indices {1, 3, 4} against {@code n=3}: index 4 exceeds
     * the observed count, so the contiguity check itself (not just "no final present") catches it.
     * {@code sorted} must refuse it as {@code incomplete_multifile}; DuckDB must still serve whatever
     * genuinely remains on disk.
     */
    @Test
    void aRealMiddleFileDeletedFromAFourFileRollIsRefusedAsIncompleteMultifile(@TempDir Path dir)
            throws IOException {
        Path fixtureDir = rollToNFiles(dir, "a", "m", "p", "z");
        List<Path> files = SortedFixtures.resolveFiles(fixtureDir);
        assertThat(files).hasSize(4);
        Path middle = files.get(1);   // file_index=2 of 4 — a genuine middle position, not first/last
        assertThat(SortedParquetStamp.read(middle)).hasValueSatisfying(s -> {
            assertThat(s.fileIndex()).isEqualTo(2);
            assertThat(s.fileFinal()).isFalse();
        });
        Files.delete(middle);

        assertThatThrownBy(() -> ReplayServingFactory.open(fixtureDir, ServingMode.SORTED, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incomplete_multifile");

        ReplayServingFactory.Result result = ReplayServingFactory.open(fixtureDir, ServingMode.DUCKDB, 1);
        try {
            List<String> served = keys(result.fixture().list(
                    new S3ListRequest("bucket", null, null, null, null, 1000, true, false)));
            assertThat(served).containsExactlyInAnyOrder("a", "p", "z");   // "m" genuinely gone from disk
        } finally {
            result.fixture().close();
        }
    }

    // --- helpers ---

    /** Rolls {@code keys} into one file per key (tiny {@code final-file-bytes}) under a fresh directory. */
    private static Path rollToNFiles(Path dir, String... keys) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("cap-" + System.nanoTime()));
        writeUnsortedPart(capture.resolve("part-0.parquet"), keys);
        Path outputDir = Files.createDirectories(dir.resolve("out-" + System.nanoTime()));
        SortConfig rolling = SortConfigs.rolledPerEntry();
        SortedDatasetResult result = new CaptureSorter(rolling).sort(capture, outputDir);
        assertThat(result.finalFiles()).hasSize(keys.length);
        return outputDir;
    }

    private static void writeUnsortedPartInPlace(Path target, String... keys) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".replacement");
        writeUnsortedPart(tmp, keys);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
    }

    private static void writeUnsortedPart(Path path, String... keys) throws IOException {
        try (var writer = ParquetFixtures.open(path)) {
            for (String k : keys) {
                writer.write(objectEntry(k));
            }
        }
    }

    private static ObjectEntry objectEntry(String key) {
        return ObjectEntries.bare(key);
    }

    private static List<String> keys(S3ListResult result) {
        return result.entries().stream()
                .filter(e -> e instanceof S3ResultEntry.ObjectResult)
                .map(e -> ByteKeys.utf8(e.key()))
                .toList();
    }
}
