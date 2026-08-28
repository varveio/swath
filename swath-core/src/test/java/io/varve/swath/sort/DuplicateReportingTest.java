/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortTestSupport.object;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.ListEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DuplicateReportingTest {

    private final ListEntryComparator comparator = new ListEntryComparator();

    @Test
    void reportsEveryAdjacentEqualPairWithoutDroppingOrReorderingRows() {
        List<ListEntry> input = List.of(
                object("a"), object("a"), object("a"),
                object("b"), object("b"), object("c"));
        List<String> reports = new ArrayList<>();

        List<ListEntry> output = drain(new DuplicateReporting(
                new InMemoryCursor(input, comparator, DuplicateHook.NO_OP), comparator,
                (previous, current) -> reports.add(current.key().asString())));

        assertThat(output).containsExactlyElementsOf(input);
        assertThat(reports).containsExactly("a", "a", "b");
    }

    @Test
    void hasNextNeverReportsAndCloseDelegatesExactlyOnce() {
        AtomicInteger closes = new AtomicInteger();
        SortedCursor inner = new SortedCursor() {
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
        DuplicateReporting reporting = new DuplicateReporting(
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
        DuplicateReporting reporting = new DuplicateReporting(
                new InMemoryCursor(List.of(object("a"), object("z"), object("m")),
                        comparator, DuplicateHook.NO_OP),
                comparator, DuplicateHook.NO_OP);

        assertThat(reporting.next()).isEqualTo(object("a"));
        assertThat(reporting.next()).isEqualTo(object("z"));
        assertThatThrownBy(reporting::next)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("merged output order regressed")
                .hasMessageContaining("z")
                .hasMessageContaining("m");
    }

    private static List<ListEntry> drain(SortedCursor cursor) {
        List<ListEntry> rows = new ArrayList<>();
        try (cursor) {
            cursor.forEachRemaining(rows::add);
        }
        return rows;
    }
}
