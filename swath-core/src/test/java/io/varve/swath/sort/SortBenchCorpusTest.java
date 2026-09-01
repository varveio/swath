/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import org.junit.jupiter.api.Test;

class SortBenchCorpusTest {

    @Test
    void generatedCursorRejectsZeroBlockRows() {
        assertThatThrownBy(() -> SortBenchCorpus.generatedCursor(0, 1, 0, 1, 1,
                LocalDate.of(2026, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blockRows")
                .hasMessageContaining("positive");
    }

    @Test
    void generatedCursorRejectsNegativeBlockRows() {
        assertThatThrownBy(() -> SortBenchCorpus.generatedCursor(0, 1, -1, 1, 1,
                LocalDate.of(2026, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blockRows")
                .hasMessageContaining("positive");
    }

    @Test
    void generatedCursorRejectsNonPositiveNumSegments() {
        assertThatThrownBy(() -> SortBenchCorpus.generatedCursor(0, 0, 1, 1, 1,
                LocalDate.of(2026, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("numSegments")
                .hasMessageContaining("positive");
        assertThatThrownBy(() -> SortBenchCorpus.generatedCursor(0, -1, 1, 1, 1,
                LocalDate.of(2026, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("numSegments")
                .hasMessageContaining("positive");
    }

    @Test
    void generatedCursorRejectsSegmentOutsideLowerBound() {
        assertThatThrownBy(() -> SortBenchCorpus.generatedCursor(-1, 2, 1, 1, 1,
                LocalDate.of(2026, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("segment")
                .hasMessageContaining("[0, 2)");
    }

    @Test
    void generatedCursorRejectsSegmentOutsideUpperBound() {
        assertThatThrownBy(() -> SortBenchCorpus.generatedCursor(2, 2, 1, 1, 1,
                LocalDate.of(2026, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("segment")
                .hasMessageContaining("[0, 2)");
    }

    @Test
    void generatedCursorAcceptsPositiveBlockRows() {
        for (int segment : new int[]{0, 1}) {
            try (SortedEntryCursor cursor = SortBenchCorpus.generatedCursor(segment, 2, 1, 2, 1,
                    LocalDate.of(2026, 1, 1))) {
                assertThat(cursor.hasNext()).isTrue();
                assertThat(cursor.next()).isNotNull();
                assertThat(cursor.hasNext()).isFalse();
            }
        }
    }
}
