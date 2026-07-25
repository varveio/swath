/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidConfigException;
import org.junit.jupiter.api.Test;

/**
 * Pins the Parquet writer-count bound ("2–4 writers", contract §7 memory
 * model / I11): {@code --tune parquet.writers=N} must be in {@code [2,4]} unless a single-file
 * destination (a {@code .parquet} extension on {@code -o}, replacing the old
 * {@code --single-file} flag) collapses it to one lane. An arbitrary count (e.g. {@code 1000})
 * would create that many row-group buffers and break the bounded-heap invariant.
 */
class ParquetWritersValidationTest {

    @Test
    void acceptsTheContractRangeTwoToFour() throws Exception {
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.DIRECTORY, 2)).isEqualTo(2);
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.DIRECTORY, 3)).isEqualTo(3);   // default
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.DIRECTORY, 4)).isEqualTo(4);
    }

    @Test
    void rejectsBelowRangeAndAboveRange() {
        for (int bad : new int[]{0, 1, 5, 64, 1000}) {
            assertThatThrownBy(() -> OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.DIRECTORY, bad))
                    .as("parquet.writers=%d must be rejected", bad)
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining("parquet.writers");
        }
    }

    @Test
    void singleFileDestinationCollapsesToOneLaneRegardlessOfTheCount() throws Exception {
        // A single-file -o (kind=FILE) is the explicit "one output part" request; it overrides
        // the count (and a value that would otherwise be out of range is irrelevant under it).
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.FILE, 3)).isEqualTo(1);
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.FILE, 1)).isEqualTo(1);
        assertThat(OutputOptions.resolveParquetWriters(OutputOptions.DestinationKind.FILE, 1000)).isEqualTo(1);
    }
}
