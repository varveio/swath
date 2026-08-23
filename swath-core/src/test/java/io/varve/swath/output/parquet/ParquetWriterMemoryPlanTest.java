/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ParquetWriterMemoryPlanTest {

    @Test
    void planningBytesScaleLinearlyFromOneFixedReserve() {
        long perWriter = PartWriter.ROW_GROUP_BYTES
                * ParquetWriterMemoryPlan.ROW_GROUP_ALLOWANCE_MULTIPLIER;

        assertThat(ParquetWriterMemoryPlan.plannedHeapBytes(8))
                .isEqualTo(ParquetWriterMemoryPlan.BASE_HEAP_RESERVE_BYTES + 8 * perWriter);
    }

    @Test
    void heapAdmissionPreservesFourAndThenScalesToTheProcessCeiling() {
        assertThat(ParquetWriterMemoryPlan.maxWritersForHeap(1)).isEqualTo(4);
        assertThat(ParquetWriterMemoryPlan.maxWritersForHeap(
                ParquetWriterMemoryPlan.plannedHeapBytes(8))).isEqualTo(8);
        assertThat(ParquetWriterMemoryPlan.maxWritersForHeap(Long.MAX_VALUE))
                .isEqualTo(ParquetWriterMemoryPlan.ABSOLUTE_MAX_WRITERS);
    }

    @Test
    void planningRejectsCountsOutsideTheProcessResourceBound() {
        assertThatThrownBy(() -> ParquetWriterMemoryPlan.plannedHeapBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ParquetWriterMemoryPlan.plannedHeapBytes(
                ParquetWriterMemoryPlan.ABSOLUTE_MAX_WRITERS + 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
