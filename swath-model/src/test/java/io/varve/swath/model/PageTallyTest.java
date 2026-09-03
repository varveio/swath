/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class PageTallyTest {

    private static ObjectEntry obj(String key, long size) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), size, 0L, null, null, null, false, null, null, null, null);
    }

    @Test
    void countsEachSubtypeOnceAndSumsObjectSizes() {
        PageTally tally = PageTally.of(List.of(
                obj("a", 3L),
                new CommonPrefixEntry(KeyBytes.ofUtf8("p/")),
                obj("b", 4L),
                new DeleteMarkerEntry(KeyBytes.ofUtf8("d"), "v1", true, 0L, null)));

        assertThat(tally).isEqualTo(new PageTally(2L, 1L, 1L, 7L));
        assertThat(tally.rows()).isEqualTo(4L);
    }

    @Test
    void emptyPageIsTheEmptyTally() {
        assertThat(PageTally.of(List.of())).isEqualTo(PageTally.EMPTY);
        assertThat(PageTally.EMPTY.rows()).isZero();
    }
}
