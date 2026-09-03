/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link PageBatch} dual-form (raw entries vs. a pre-{@link PackedPage} page). The
 * non-sort pipelines keep carrying a raw entry list ({@link PageBatch#isPacked()} false); the sort
 * pipeline carries a packed page with {@code entries() == null} — and exactly one form is ever present.
 */
final class PageBatchTest {

    private static ObjectEntry obj(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null, false, null, null, null, null);
    }

    @Test
    void entriesFormIsNotPackedAndWeighsByListSize() {
        PageBatch batch = new PageBatch(7L, 3L, List.of(obj("a"), obj("b")));
        assertThat(batch.isPacked()).isFalse();
        assertThat(batch.packed()).isNull();
        assertThat(batch.entries()).hasSize(2);
        assertThat(batch.entryCount()).isEqualTo(2);
        assertThat(batch.nodeId()).isEqualTo(7L);
        assertThat(batch.pageSeq()).isEqualTo(3L);
        assertThat(batch.nodeCompleted()).isFalse();
        assertThat(batch.channelWeight()).isEqualTo(2);
        assertThat(batch.tally()).as("tallied on build, on the producing thread")
                .isEqualTo(new PageTally(2L, 0L, 0L, 2L));
    }

    @Test
    void packedFormCarriesPackedAndNullEntries() {
        PackedPage packed = new StubPacked(5);
        PageBatch batch = PageBatch.ofPacked(2L, 9L, packed);
        assertThat(batch.isPacked()).isTrue();
        assertThat(batch.entries()).isNull();
        assertThat(batch.packed()).isSameAs(packed);
        assertThat(batch.entryCount()).isEqualTo(5);
        assertThat(batch.nodeId()).isEqualTo(2L);
        assertThat(batch.pageSeq()).isEqualTo(9L);
        assertThat(batch.tally()).as("read off the packer's own counts")
                .isEqualTo(new PageTally(5L, 0L, 0L, 0L));
    }

    @Test
    void completionMarkerHasNoRowsButConsumesBoundedChannelWeight() {
        PageBatch completion = PageBatch.completion(4L, 11L);

        assertThat(completion.nodeCompleted()).isTrue();
        assertThat(completion.completionOnly()).isTrue();
        assertThat(completion.entryCount()).isZero();
        assertThat(completion.channelWeight()).isEqualTo(1);
        assertThat(completion.tally()).isEqualTo(PageTally.EMPTY);
    }

    @Test
    void rejectsAMissingTally() {
        assertThatThrownBy(() -> new PageBatch(0L, 0L, List.of(obj("a")), null, false, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tally");
    }

    @Test
    void rejectsATallyWhoseRowsDisagreeWithThePayload() {
        assertThatThrownBy(() -> new PageBatch(0L, 0L, List.of(obj("a")), null, false, PageTally.EMPTY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tally rows (0)")
                .hasMessageContaining("entry count (1)");
        assertThatThrownBy(() -> new PageBatch(0L, 0L, null, new StubPacked(3), false, PageTally.EMPTY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("entry count (3)");
        assertThatThrownBy(() -> new PageBatch(0L, 0L, List.of(), null, true, new PageTally(1L, 0L, 0L, 0L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tally rows (1)");
    }

    @Test
    void rejectsBothOrNeitherForm() {
        assertThatThrownBy(() -> new PageBatch(0L, 0L, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PageBatch(0L, 0L, List.of(obj("a")), new StubPacked(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private record StubPacked(long entryCount) implements PackedPage {
        @Override public long objectCount() { return entryCount; }
        @Override public long commonPrefixCount() { return 0; }
        @Override public long deleteMarkerCount() { return 0; }
        @Override public long totalObjectSize() { return 0; }
        @Override public byte[] firstKey() { return new byte[0]; }
        @Override public byte[] lastKey() { return new byte[0]; }
    }
}
