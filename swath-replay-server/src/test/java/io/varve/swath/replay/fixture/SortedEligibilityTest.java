/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.fixture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfigs;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SortedEligibility#decide} over real sorted, stamped fixtures — the callers this decision
 * feeds ({@code ReplayServingFactory}, {@code io.varve.swath.sim.store.SimStoreFactory}) both treat
 * an {@link UncheckedIOException} out of {@code decide} as a genuine failure to guard against
 * (never swallowed the way a declined {@link SortedEligibility.Result.Ineligible} is), so that path
 * must actually be reachable, not merely theoretical.
 */
class SortedEligibilityTest {

    @Test
    void decideThrowsUncheckedIOExceptionWhenAStampedFilesRowGroupDataIsUnreadable(@TempDir Path dir)
            throws IOException {
        List<Path> files = stampedFixtureWithCorruptedRowGroupData(dir);

        // The stamp/mode/format-version/completeness checks all still pass here (the footer,
        // trailing the file, is untouched) -- only the index-derive step, which must actually
        // decode each row group's first row, reaches the corrupted data and fails.
        assertThatThrownBy(() -> SortedEligibility.decide(files, new FixtureMetrics(), false))
                .isInstanceOf(UncheckedIOException.class);
    }

    /**
     * Builds a real sorted, multi-row-group, stamped fixture via the production sorter, then
     * zeroes out the first half of the file's bytes (the row-group data section) in place, keeping
     * the file's total length unchanged so the footer — which trails the data section and records
     * absolute byte offsets into it — is itself untouched and still parses as a valid stamp.
     */
    private static List<Path> stampedFixtureWithCorruptedRowGroupData(Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            keys.add(String.format("key-%05d", i));
        }
        Path capture = Files.createDirectory(dir.resolve("cap"));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (String key : keys) {
                writer.write(ObjectEntries.bare(key));
            }
        }
        Path out = Files.createDirectory(dir.resolve("out"));
        new CaptureSorter(SortConfigs.manySmallRowGroups()).sort(capture, out);
        List<Path> files = SortedFixtures.resolveFiles(out);
        Path file = files.get(0);
        long length = Files.size(file);
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(0);
            raf.write(new byte[(int) (length / 2)]);
        }
        return files;
    }
}
