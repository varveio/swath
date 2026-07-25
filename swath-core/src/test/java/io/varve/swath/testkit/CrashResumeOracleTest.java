/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.sort.SortedFileWriter;
import io.varve.swath.sort.SortedFileWriterFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The teeth for the {@link CrashResumeOracle}: proves the oracle is NOT vacuous by feeding
 * it tiny synthetic baseline + candidate output dirs and checking it PASSES on a faithful copy and
 * THROWS an {@link AssertionError} on each corruption a crash-resume could produce — a duplicated
 * key, a missing key, an extra/foreign key, and (the critical case) a mis-ordered pair that a set-only oracle
 * would miss.
 */
class CrashResumeOracleTest {

    // Baseline: two parts, globally ascending. Global ordered sequence = a,b,c,d,e,f.
    private static final List<String> PART1 = List.of("a", "b", "c");
    private static final List<String> PART2 = List.of("d", "e", "f");

    @Test
    void acceptsAFaithfulUnsortedCopy(@TempDir Path tmp) throws IOException {
        Path baseline = baseline(tmp.resolve("base"));
        Path resumed = tmp.resolve("res");
        writePart(resumed, "part-00001.parquet", PART1);
        writePart(resumed, "part-00002.parquet", PART2);

        assertThatCode(() -> CrashResumeOracle.assertResumeMatchesBaseline(baseline, resumed, false))
                .doesNotThrowAnyException();
    }

    @Test
    void acceptsAFaithfulSortedCopy(@TempDir Path tmp) throws IOException {
        Path baseline = baseline(tmp.resolve("base"));
        Path resumed = tmp.resolve("res");
        writePart(resumed, "part-00001.parquet", PART1);
        writePart(resumed, "part-00002.parquet", PART2);

        assertThatCode(() -> CrashResumeOracle.assertResumeMatchesBaseline(baseline, resumed, true))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsADuplicatedKey(@TempDir Path tmp) throws IOException {
        Path baseline = baseline(tmp.resolve("base"));
        Path resumed = tmp.resolve("res");
        writePart(resumed, "part-00001.parquet", PART1);
        // 'c' duplicated across the two parts: union=7, distinct=6.
        writePart(resumed, "part-00002.parquet", List.of("c", "d", "e", "f"));

        assertThatThrownBy(() -> CrashResumeOracle.assertResumeMatchesBaseline(baseline, resumed, false))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void rejectsAMissingKey(@TempDir Path tmp) throws IOException {
        Path baseline = baseline(tmp.resolve("base"));
        Path resumed = tmp.resolve("res");
        writePart(resumed, "part-00001.parquet", PART1);
        // 'e' dropped — the data-loss failure a crash-resume could cause.
        writePart(resumed, "part-00002.parquet", List.of("d", "f"));

        assertThatThrownBy(() -> CrashResumeOracle.assertResumeMatchesBaseline(baseline, resumed, false))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void rejectsAnExtraForeignKey(@TempDir Path tmp) throws IOException {
        Path baseline = baseline(tmp.resolve("base"));
        Path resumed = tmp.resolve("res");
        writePart(resumed, "part-00001.parquet", PART1);
        // 'z' is not in the baseline set.
        writePart(resumed, "part-00002.parquet", List.of("d", "e", "f", "z"));

        assertThatThrownBy(() -> CrashResumeOracle.assertResumeMatchesBaseline(baseline, resumed, false))
                .isInstanceOf(AssertionError.class);
    }

    /**
     * The key assertion: the resumed set is IDENTICAL to the baseline (same keys, exactly-once) but two
     * adjacent rows are swapped so the global order breaks. A set-only oracle passes this; the
     * sorted oracle MUST catch it.
     */
    @Test
    void rejectsAMisorderedPairThatASetOnlyOracleWouldMiss(@TempDir Path tmp) throws IOException {
        Path baseline = baseline(tmp.resolve("base"));
        Path resumed = tmp.resolve("res");
        // Swap b and c within part 1: set/count unchanged, only the order breaks.
        writePart(resumed, "part-00001.parquet", List.of("a", "c", "b"));
        writePart(resumed, "part-00002.parquet", PART2);

        // Set-only oracle is fooled — proving the mis-order case is exactly the one it would miss.
        assertThatCode(() -> CrashResumeOracle.assertSameObjectSet(baseline, resumed))
                .doesNotThrowAnyException();

        // Sorted oracle catches it.
        assertThatThrownBy(() -> CrashResumeOracle.assertResumeMatchesBaseline(baseline, resumed, true))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> CrashResumeOracle.assertSortedGlobalOrderMatches(baseline, resumed))
                .isInstanceOf(AssertionError.class);
    }

    // ------------------------------------------------------------------------

    /** Writes the two-part globally-ascending baseline and returns its root. */
    private static Path baseline(Path root) throws IOException {
        writePart(root, "part-00001.parquet", PART1);
        writePart(root, "part-00002.parquet", PART2);
        return root;
    }

    /** Writes {@code keys} as a single Parquet part under {@code <root>/data/<partName>}. */
    private static void writePart(Path root, String partName, List<String> keys) throws IOException {
        Path data = root.resolve("data");
        Files.createDirectories(data);
        Path path = data.resolve(partName);
        try (SortedFileWriter writer = SortedFileWriterFactory.DEFAULT.create(path, 1)) {
            for (String key : keys) {
                writer.write(object(key));
            }
        }
    }

    private static ListEntry object(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null, false, null, null, null, null);
    }
}
