/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import static io.varve.swath.sort.finalize.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.ListEntry;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortOrderException;
import io.varve.swath.sort.SortedEntryCursor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DuplicateReportingCursorTest {

    private final ListEntryComparator comparator = new ListEntryComparator();

    @Test
    void reportsEveryAdjacentEqualPairWithoutDroppingOrReorderingRows() {
        List<ListEntry> input = List.of(
                object("a"), object("a"), object("a"),
                object("b"), object("b"), object("c"));
        List<String> reports = new ArrayList<>();

        List<ListEntry> output = drain(new DuplicateReportingCursor(
                SortTestSupport.cursor(input), comparator,
                (previous, current) -> reports.add(current.key().asString())));

        assertThat(output).containsExactlyElementsOf(input);
        assertThat(reports).containsExactly("a", "a", "b");
    }

    @Test
    void hasNextNeverReportsAndCloseDelegatesExactlyOnce() {
        AtomicInteger closes = new AtomicInteger();
        SortedEntryCursor inner = new SortedEntryCursor() {
            private final List<ListEntry> rows = List.of(object("a"), object("a"));
            private int index;

            @Override
            public boolean hasNext() {
                return index < rows.size();
            }

            @Override
            public ListEntry next() {
                return rows.get(index++);
            }

            @Override
            public void close() {
                closes.incrementAndGet();
            }
        };
        AtomicInteger reports = new AtomicInteger();
        DuplicateReportingCursor reporting = new DuplicateReportingCursor(
                inner, comparator, (previous, current) -> reports.incrementAndGet());

        assertThat(reporting.hasNext()).isTrue();
        assertThat(reports).hasValue(0);
        reporting.next();
        assertThat(reporting.hasNext()).isTrue();
        assertThat(reports).hasValue(0);
        reporting.next();
        assertThat(reports).hasValue(1);
        reporting.close();
        reporting.close();
        assertThat(closes).hasValue(1);
    }

    @Test
    void rejectsAComparatorRegressionAfterPayingTheAdjacentComparison() {
        DuplicateReportingCursor reporting = new DuplicateReportingCursor(
                SortTestSupport.cursor(List.of(object("a"), object("z"), object("m"))),
                comparator, DuplicateHook.NO_OP);

        assertThat(reporting.next()).isEqualTo(object("a"));
        assertThat(reporting.next()).isEqualTo(object("z"));
        assertThatThrownBy(reporting::next)
                .isInstanceOfSatisfying(SortOrderException.class, failure ->
                        assertThat(failure.errorClass()).isEqualTo(SortOrderException.ERROR_CLASS))
                .hasMessageContaining("merged output order regressed")
                .hasMessageContaining("z")
                .hasMessageContaining("m");
    }

    private static List<ListEntry> drain(SortedEntryCursor cursor) {
        List<ListEntry> rows = new ArrayList<>();
        try (cursor) {
            cursor.forEachRemaining(rows::add);
        }
        return rows;
    }
}
