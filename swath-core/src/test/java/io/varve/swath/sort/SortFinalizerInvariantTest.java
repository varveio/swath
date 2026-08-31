/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SortFinalizerInvariantTest {

    @Test
    void completePreparedSetCanOnlyBeMintedInsideTheSortPackage() {
        assertThat(PreparedSortedParts.class.getDeclaredConstructors())
                .allSatisfy(constructor ->
                        assertThat(Modifier.isPublic(constructor.getModifiers())).isFalse());
    }

    @Test
    void cardinalityMismatchIsTypedAndInstrumentedOnce() {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();

        assertThatThrownBy(() -> SortFinalizer.requireExactCardinality(3, 2, 2, metrics))
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

        SortFinalizer.requireExactCardinality(3, 3, 3, metrics);

        assertThat(metrics.count("SORT.sort_output_cardinality_mismatch")).isZero();
    }

    @Test
    void nonUtf8RawBoundsRejectCrossPartOverlap() {
        SortTestSupport.CountingMetrics metrics = new SortTestSupport.CountingMetrics();
        PreparedSortedParts.Part high = part(new byte[] {(byte) 0x80}, new byte[] {(byte) 0x80});
        PreparedSortedParts.Part low = part(new byte[] {0x7f}, new byte[] {0x7f});

        assertThatThrownBy(() -> SortFinalizer.requireDisjointParts(List.of(high, low), metrics))
                .isInstanceOfSatisfying(SortOrderException.class, failure ->
                        assertThat(failure.errorClass()).isEqualTo(SortOrderException.ERROR_CLASS))
                .hasMessageContaining("raw unsigned key order");
        assertThat(metrics.count("SORT.cross_part_overlap_rejected")).isEqualTo(1);
    }

    @Test
    void nonUtf8RawBoundsPermitStrictUnsignedAdjacency() {
        SortFinalizer.requireDisjointParts(List.of(
                part(new byte[] {0x7f}, new byte[] {0x7f}),
                part(new byte[] {(byte) 0x80}, new byte[] {(byte) 0x80})),
                SortMetrics.NO_OP);
    }

    @Test
    void adjacencyFailureRetainsPrecedenceWhenCardinalityAlsoDisagrees() {
        PreparedSortedParts.Part high = part(
                new byte[] {(byte) 0x80}, new byte[] {(byte) 0x80});
        PreparedSortedParts.Part low = part(new byte[] {0x7f}, new byte[] {0x7f});

        assertThatThrownBy(() -> SortFinalizer.requirePreparedSet(
                List.of(high, low), 3, 2, 2, SortMetrics.NO_OP))
                .isInstanceOf(SortOrderException.class)
                .isNotInstanceOf(SortCardinalityException.class);
    }

    private static PreparedSortedParts.Part part(byte[] min, byte[] max) {
        // Both invalid-UTF-8 bounds would become the same replacement-character String. The
        // finalizer must compare these raw bytes instead of lossy display fields.
        return new PreparedSortedParts.Part(
                Path.of("part.tmp"), 1, 1, min, max, Optional.empty());
    }
}
