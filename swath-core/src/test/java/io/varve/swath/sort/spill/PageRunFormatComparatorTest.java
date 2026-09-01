/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.sorted.StaleFinalSweep;
import java.util.Comparator;
import org.junit.jupiter.api.Test;

class PageRunFormatComparatorTest {

    private static final Comparator<ListEntry> ALTERNATE = (left, right) -> 0;

    @Test
    void persistedWriterRejectsAnAlternateComparator() {
        assertThatThrownBy(() -> new PageRunSegmentWriter(
                ALTERNATE, DuplicateHook.NO_OP, SortMetrics.NO_OP, PageCodec.NONE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("page-run format v4 requires ListEntryComparator");
    }

    @Test
    void sortRunRejectsAnAlternateComparator() {
        assertThatThrownBy(() -> new SortRun(
                SortConfigs.base(), ALTERNATE, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("page-run format v4 requires ListEntryComparator");
    }
}
