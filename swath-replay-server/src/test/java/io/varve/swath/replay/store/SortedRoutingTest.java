/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.protocol.ByteKey;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * {@link SortedRouting#startRowGroup} in isolation (no files, no reader): the "which row group
 * contains this key" search, at every boundary case a real range read can land on.
 *
 * <p>This class used to also pin an upper-bound invariant — the window a bounded SQL query had to be
 * given so it would not scan to the end of the file. The store reads through Parquet's page index
 * now and stops as soon as it holds the rows it was asked for, so there is no window to plan and
 * nothing left here but the search.
 */
class SortedRoutingTest {

    private static final Path F1 = Path.of("part-00001.parquet");
    private static final Path F2 = Path.of("part-00002.parquet");

    @Test
    void emptyIndexStartsAtZero() {
        assertThat(SortedRouting.startRowGroup(List.of(), key("a"))).isZero();
    }

    @Test
    void anAbsentLowerBoundStartsAtTheFirstRowGroup() {
        assertThat(SortedRouting.startRowGroup(index(entry(F1, 0, "a", 10), entry(F1, 1, "m", 10)), null))
                .isZero();
    }

    @Test
    void fromBeforeTheFirstKeyStartsAtRowGroupZero() {
        List<IndexEntry> index = index(entry(F1, 0, "m", 10), entry(F1, 1, "s", 10));
        assertThat(SortedRouting.startRowGroup(index, key("a"))).isZero();
    }

    @Test
    void fromInsideAGroupStartsAtThatGroup() {
        List<IndexEntry> index = index(entry(F1, 0, "a", 10), entry(F1, 1, "m", 10), entry(F1, 2, "s", 10));
        assertThat(SortedRouting.startRowGroup(index, key("p"))).isEqualTo(1);
    }

    @Test
    void aFromEqualToAGroupsFirstKeyStartsAtThatGroupNotThePreviousOne() {
        // The boundary the whole search turns on: "contains" is inclusive of the group's own first key.
        List<IndexEntry> index = index(entry(F1, 0, "a", 10), entry(F1, 1, "m", 10));
        assertThat(SortedRouting.startRowGroup(index, key("m"))).isEqualTo(1);
    }

    @Test
    void fromPastTheLastGroupsFirstKeyStartsAtTheLastGroup() {
        List<IndexEntry> index = index(entry(F1, 0, "a", 10), entry(F1, 1, "m", 10));
        assertThat(SortedRouting.startRowGroup(index, key("zzz"))).isEqualTo(1);
    }

    @Test
    void theSearchSpansFilesBecauseTheIndexIsGlobalNotPerFile() {
        List<IndexEntry> index = index(
                entry(F1, 0, "a", 10), entry(F1, 1, "m", 10), entry(F2, 0, "s", 10), entry(F2, 1, "w", 10));
        assertThat(SortedRouting.startRowGroup(index, key("t"))).isEqualTo(2);
        assertThat(SortedRouting.startRowGroup(index, key("s"))).isEqualTo(2);
        assertThat(SortedRouting.startRowGroup(index, key("w"))).isEqualTo(3);
    }

    private static List<IndexEntry> index(IndexEntry... entries) {
        return List.of(entries);
    }

    private static IndexEntry entry(Path file, int rowGroup, String firstKey, long rowCount) {
        return new IndexEntry(file, rowGroup, key(firstKey), rowCount);
    }

    private static ByteKey key(String s) {
        return ByteKey.copyOf(s.getBytes(StandardCharsets.UTF_8));
    }
}
