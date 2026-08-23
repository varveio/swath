/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.output.parquet.ParquetWriterMemoryPlan;
import org.junit.jupiter.api.Test;

/**
 * Pins the Parquet writer admission policy: the established 2-4 envelope is unconditional, while
 * higher expert counts require enough configured heap for the conservative planning allowance.
 */
class ParquetWritersValidationTest {

    @Test
    void acceptsTheReleaseEnvelopeRegardlessOfHeap() throws Exception {
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.DIRECTORY, 2, 1)).isEqualTo(2);
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.DIRECTORY, 3, 1)).isEqualTo(3);
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.DIRECTORY, 4, 1)).isEqualTo(4);
    }

    @Test
    void admitsExpertCountWhenHeapCoversItsPlan() throws Exception {
        int writers = 12;
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.DIRECTORY,
                writers, ParquetWriterMemoryPlan.plannedHeapBytes(writers))).isEqualTo(writers);
    }

    @Test
    void rejectsExpertCountThatExceedsHeapPlan() {
        int writers = 5;
        assertThatThrownBy(() -> OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.DIRECTORY,
                writers, ParquetWriterMemoryPlan.plannedHeapBytes(writers) - 1))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("needs a conservative heap plan")
                .hasMessageContaining("maximum heap")
                .hasMessageContaining("JVM/container memory limit");
    }

    @Test
    void rejectsBelowRangeAndAbsoluteResourceCeiling() {
        for (int bad : new int[]{0, 1, ParquetWriterMemoryPlan.ABSOLUTE_MAX_WRITERS + 1, 1000}) {
            assertThatThrownBy(() -> OutputOptions.resolveParquetWriters(
                    OutputOptions.DestinationKind.DIRECTORY, bad, Long.MAX_VALUE))
                    .as("parquet.writers=%d must be rejected", bad)
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining("parquet.writers");
        }
    }

    @Test
    void singleFileDestinationCollapsesToOneLaneRegardlessOfTheCount() throws Exception {
        // A single-file -o (kind=FILE) is the explicit "one output part" request; it overrides
        // the count (and a value that would otherwise be out of range is irrelevant under it).
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.FILE, 3, 1)).isEqualTo(1);
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.FILE, 1, 1)).isEqualTo(1);
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.FILE, 1000, 1)).isEqualTo(1);
    }
}
