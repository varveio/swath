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

final class SegmentGateTest {

    @Test
    void thresholdsAreInclusiveAndByteGateWinsWhenBothFire() {
        SortConfig config = SortConfigs.base().withSegmentBytes(130).withSegmentEntries(3);
        SegmentGate gate = new SegmentGate(config);

        assertThat(gate.trigger(129, 2)).isEqualTo(SealTrigger.DRAIN);
        assertThat(gate.full(129, 2)).isFalse();
        assertThat(gate.trigger(130, 2)).isEqualTo(SealTrigger.BYTE_GATE);
        assertThat(gate.trigger(129, 3)).isEqualTo(SealTrigger.ENTRY_CAP);
        assertThat(gate.trigger(130, 3)).isEqualTo(SealTrigger.BYTE_GATE);
        assertThat(gate.full(130, 3)).isTrue();
    }

    @Test
    void captureEntriesAndPackedPageUseTheSameLogicalByteEstimate() {
        List<ListEntry> entries = List.of(
                SortTestSupport.object("a"),
                SortTestSupport.object("longer-key"),
                SortTestSupport.object("\u0000\u00ff"));
        long captureEntryEstimate = entries.stream().mapToLong(PageBlock::estimatedBytes).sum();

        PageBlock block = PageBlock.pack(entries, new ListEntryComparator(), PageCodec.NONE);

        assertThat(block.stagingEstimatedBytes()).isEqualTo(captureEntryEstimate);
    }
}
