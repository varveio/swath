/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ListEntry;
import java.util.List;
import org.junit.jupiter.api.Test;

final class SortBufferTest {

    private final ListEntryComparator comparator = new ListEntryComparator();
    private final SortConfig config = SortConfigs.base();

    @Test
    void sealMovesAdmissionOrderedPagesAndDistinctNodeCountWithoutLiveAliases() {
        SortBuffer buffer = new SortBuffer(config, comparator);
        PageBlock first = page("a");
        PageBlock second = page("b");
        PageBlock third = page("c");
        buffer.admit(7L, first);
        buffer.admit(9L, second);
        buffer.admit(7L, third);

        SealedBuffer sealed = buffer.seal(SealTrigger.DRAIN);

        assertThat(sealed.pages()).containsExactly(first, second, third);
        assertThat(sealed.pages()).isSameAs(sealed.pages());
        assertThat(sealed.runCount()).isEqualTo(2);
        assertThat(buffer.isEmpty()).isTrue();

        PageBlock next = page("d");
        buffer.admit(11L, next);
        SealedBuffer nextSeal = buffer.seal(SealTrigger.DRAIN);

        assertThat(sealed.pages()).containsExactly(first, second, third);
        assertThat(sealed.perNodeMaxKeys()).containsOnlyKeys(7L, 9L);
        assertThat(nextSeal.pages()).containsExactly(next);
        assertThat(nextSeal.runCount()).isEqualTo(1);
        assertThat(nextSeal.perNodeMaxKeys()).containsOnlyKeys(11L);
    }

    private PageBlock page(String key) {
        List<ListEntry> entries = List.of(SortTestSupport.object(key));
        return PageBlock.pack(entries, comparator, config.segmentCodec());
    }
}
