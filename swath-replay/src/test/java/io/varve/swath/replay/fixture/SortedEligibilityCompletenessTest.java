/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.fixture;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.output.parquet.sorted.SortStamp;
import io.varve.swath.sort.SortMode;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * {@link SortedEligibility#multiFileCompletenessViolation} — the pure completeness math,
 * exercised directly over fabricated {@link SortStamp}s so every edge case (complete set, truncated
 * tail from a crashed publish, missing middle file, duplicate/out-of-range index) is a fast in-memory
 * check, no parquet I/O.
 */
class SortedEligibilityCompletenessTest {

    private static final Path F1 = Path.of("part-00001.parquet");
    private static final Path F2 = Path.of("part-00002.parquet");
    private static final Path F3 = Path.of("part-00003.parquet");

    @Test
    void aCompleteSingleFileSetIsFine() {
        List<Path> files = List.of(F1);
        List<SortStamp> stamps = List.of(stamp(1, true));

        assertThat(SortedEligibility.multiFileCompletenessViolation(files, stamps)).isNull();
    }

    @Test
    void aCompleteThreeFileSetIsFine() {
        List<Path> files = List.of(F1, F2, F3);
        List<SortStamp> stamps = List.of(stamp(1, false), stamp(2, false), stamp(3, true));

        assertThat(SortedEligibility.multiFileCompletenessViolation(files, stamps)).isNull();
    }

    /**
     * The exact crash scenario: a 3-file publish crashes after renaming files 1 and 2 but
     * before renaming (the true) file 3 — the resolved set is {1, 2}, contiguous, but neither carries
     * {@code file_final=true} (only the never-renamed file 3 would have). Must be flagged.
     */
    @Test
    void aTruncatedTailFromACrashedPublishIsFlagged() {
        List<Path> files = List.of(F1, F2);
        List<SortStamp> stamps = List.of(stamp(1, false), stamp(2, false));   // neither is final

        assertThat(SortedEligibility.multiFileCompletenessViolation(files, stamps)).isNotNull();
    }

    @Test
    void aMissingMiddleFileIsFlaggedByTheContiguityCheck() {
        // Files 1 and 3 present (2 deleted); each file's OWN embedded index is unaffected by the
        // deletion, so the observed set is {1, 3} against n=2 — index 3 is out of range for n=2.
        List<Path> files = List.of(F1, F3);
        List<SortStamp> stamps = List.of(stamp(1, false), stamp(3, true));

        assertThat(SortedEligibility.multiFileCompletenessViolation(files, stamps)).isNotNull();
    }

    @Test
    void aDuplicateIndexIsFlagged() {
        List<Path> files = List.of(F1, F2);
        List<SortStamp> stamps = List.of(stamp(1, false), stamp(1, true));   // both claim index 1

        assertThat(SortedEligibility.multiFileCompletenessViolation(files, stamps)).isNotNull();
    }

    @Test
    void moreThanOneFinalFlagIsFlaggedEvenIfIndicesAreContiguous() {
        List<Path> files = List.of(F1, F2);
        List<SortStamp> stamps = List.of(stamp(1, true), stamp(2, true));   // two finals

        assertThat(SortedEligibility.multiFileCompletenessViolation(files, stamps)).isNotNull();
    }

    @Test
    void aFinalFlagOnTheWrongIndexIsFlagged() {
        List<Path> files = List.of(F1, F2);
        List<SortStamp> stamps = List.of(stamp(1, true), stamp(2, false));   // final on 1, not max index 2

        assertThat(SortedEligibility.multiFileCompletenessViolation(files, stamps)).isNotNull();
    }

    private static SortStamp stamp(int fileIndex, boolean fileFinal) {
        return new SortStamp("order", SortMode.OBJECTS, 1, fileIndex, fileFinal);
    }
}
