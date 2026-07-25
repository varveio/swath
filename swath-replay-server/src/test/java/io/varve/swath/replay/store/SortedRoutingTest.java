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
 * The upper-bound invariant and its edge cases in isolation (no DuckDB, no files) — the routing math
 * that guarantees a bounded query's window holds at least {@code limit} rows past the range start
 * while enough rows remain, clamps to open at the tail, intersects with the prefix upper bound, and touches
 * only the file(s) the range spans.
 */
class SortedRoutingTest {

    private static final Path F1 = Path.of("part-00001.parquet");
    private static final Path F2 = Path.of("part-00002.parquet");

    @Test
    void upperBoundIsTheSmallestBoundaryHoldingAtLeastLimitRowsAcrossShortGroups() {
        // Four two-row groups in one file: a,c,e,g. from="a" (in rg0), limit=3.
        List<IndexEntry> index = List.of(
                rg(F1, 0, "a", 2), rg(F1, 1, "c", 2), rg(F1, 2, "e", 2), rg(F1, 3, "g", 2));

        SortedRouting.QueryPlan plan = SortedRouting.plan(index, key("a"), null, 3);

        // rg1(2) < 3, rg1+rg2(4) >= 3 -> bound at firstKey(rg3) = "g" (conservatively skips rg0).
        assertThat(bytes(plan.upperBound())).isEqualTo("g");
        assertThat(plan.files()).containsExactly(F1);
    }

    @Test
    void clampsToAnOpenUpperBoundWhenTheTailCannotSupplyLimitRows() {
        List<IndexEntry> index = List.of(
                rg(F1, 0, "a", 2), rg(F1, 1, "c", 2), rg(F1, 2, "e", 2));

        SortedRouting.QueryPlan plan = SortedRouting.plan(index, key("e"), null, 100);

        assertThat(plan.upperBound()).isNull();                 // open: nothing after "e" reaches 100
        assertThat(plan.files()).containsExactly(F1);
    }

    @Test
    void intersectsTheInvariantBoundWithTheCallerPrefixUpperBound() {
        List<IndexEntry> index = List.of(
                rg(F1, 0, "a", 2), rg(F1, 1, "c", 2), rg(F1, 2, "e", 2), rg(F1, 3, "g", 2));

        // Invariant bound would be "g", but the prefix upper bound "d" is tighter.
        SortedRouting.QueryPlan plan = SortedRouting.plan(index, key("a"), key("d"), 3);

        assertThat(bytes(plan.upperBound())).isEqualTo("d");
    }

    @Test
    void emptyIndexTouchesNoFiles() {
        assertThat(SortedRouting.plan(List.of(), key("a"), null, 1000).isEmpty()).isTrue();
    }

    @Test
    void fromBeforeTheFirstKeyStartsAtRowGroupZero() {
        List<IndexEntry> index = List.of(rg(F1, 0, "m", 2), rg(F1, 1, "q", 2));

        assertThat(SortedRouting.startRowGroup(index, key("a"))).isZero();
    }

    @Test
    void fromInOrPastTheLastRowGroupYieldsAnOpenBoundOverThatGroupOnly() {
        List<IndexEntry> index = List.of(rg(F1, 0, "a", 2), rg(F2, 0, "e", 2));

        SortedRouting.QueryPlan plan = SortedRouting.plan(index, key("z"), null, 1000);

        assertThat(SortedRouting.startRowGroup(index, key("z"))).isEqualTo(1);
        assertThat(plan.upperBound()).isNull();
        assertThat(plan.files()).containsExactly(F2);
    }

    @Test
    void aRangeEntirelyBelowTheFirstKeyTouchesNoFiles() {
        List<IndexEntry> index = List.of(rg(F1, 0, "m", 2), rg(F1, 1, "q", 2));

        // from open, but the prefix upper bound "m" == first key -> window [.., "m") is empty.
        SortedRouting.QueryPlan plan = SortedRouting.plan(index, null, key("m"), 1000);

        assertThat(plan.isEmpty()).isTrue();
    }

    @Test
    void aWindowSpanningTwoFilesTouchesBothInKeyOrder() {
        // f1: a,c ; f2: e,g. from="a", limit large enough to reach into f2.
        List<IndexEntry> index = List.of(
                rg(F1, 0, "a", 2), rg(F1, 1, "c", 2), rg(F2, 0, "e", 2), rg(F2, 1, "g", 2));

        SortedRouting.QueryPlan plan = SortedRouting.plan(index, key("a"), null, 5);

        assertThat(plan.upperBound()).isNull();                 // 2+2+2 < 5 -> open tail
        assertThat(plan.files()).containsExactly(F1, F2);
    }

    private static IndexEntry rg(Path file, int rowGroup, String firstKey, long rowCount) {
        return new IndexEntry(file, rowGroup, key(firstKey), rowCount);
    }

    private static ByteKey key(String value) {
        return ByteKey.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String bytes(ByteKey key) {
        return key == null ? null : new String(key.toByteArray(), StandardCharsets.UTF_8);
    }
}
