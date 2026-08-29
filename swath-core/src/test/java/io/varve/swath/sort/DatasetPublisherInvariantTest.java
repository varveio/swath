/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class DatasetPublisherInvariantTest {

    @Test
    void cardinalityMismatchIsTypedAndInstrumentedOnce() {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        assertThatThrownBy(() -> DatasetPublisher.requireExactCardinality(3, 2, 2, metrics))
                .isInstanceOfSatisfying(SortCardinalityException.class, failure ->
                        assertThat(failure.errorClass())
                                .isEqualTo(SortCardinalityException.ERROR_CLASS))
                .hasMessageContaining("source_rows=3")
                .hasMessageContaining("drained_rows=2")
                .hasMessageContaining("final_part_rows=2");
        assertThat(metrics.count("SORT.sort_output_cardinality_mismatch")).isEqualTo(1);
    }

    @Test
    void exactCardinalityEmitsNoFailureSignal() throws Exception {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        DatasetPublisher.requireExactCardinality(3, 3, 3, metrics);

        assertThat(metrics.count("SORT.sort_output_cardinality_mismatch")).isZero();
    }
}
