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
    void generatedCursorAcceptsPositiveBlockRows() {
        try (SortedCursor cursor = SortBenchCorpus.generatedCursor(0, 1, 1, 1, 1,
                LocalDate.of(2026, 1, 1))) {
            assertThat(cursor.hasNext()).isTrue();
            assertThat(cursor.next()).isNotNull();
            assertThat(cursor.hasNext()).isFalse();
        }
    }
}
