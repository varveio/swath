/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.protocol.ByteKey;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import org.junit.jupiter.api.Test;

/**
 * Property-based guard (jqwik) for the {@link SortedRouting} upper-bound invariant over arbitrary
 * synthetic indexes — random row-group sizes (incl. 1-row groups), short/final groups,
 * and multi-file layouts. It reconstructs the ground-truth key set behind an index and asserts the routing
 * bound is never too tight — {@code [from, bound)} holds at least {@code limit} rows whenever that
 * many exist strictly past {@code from}, and clamps to an open bound otherwise. Because {@code from}
 * may sit anywhere inside its row group, the store counts zero rows from the start group; the
 * guarantee therefore holds conservatively and this test proves it never under-provisions across a
 * group boundary — the exact failure the mixed-row-type red case demonstrates when {@code rowCount}
 * is wrong.
 */
class SortedRoutingPropertyTest {

    @Provide
    Arbitrary<List<byte[]>> keyBags() {
        return Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(5)
                .list().ofMinSize(1).ofMaxSize(80);
    }

    @Property(tries = 500)
    void boundGuaranteesAtLeastLimitRowsWheneverThatManyExistPastFrom(
            @ForAll("keyBags") List<byte[]> rawKeys,
            @ForAll long structureSeed,
            @ForAll @IntRange(min = -2, max = 200) int fromSelector,
            @ForAll @IntRange(min = 1, max = 90) int limit) {

        List<byte[]> keys = sortedDistinct(rawKeys);
        Assume.that(!keys.isEmpty());

        List<IndexEntry> index = buildIndex(keys, new Random(structureSeed));
        ByteKey from = chooseFrom(keys, fromSelector);

        SortedRouting.QueryPlan plan = SortedRouting.plan(index, from, null, limit);
        ByteKey bound = plan.upperBound();

        long tailCount = keys.stream().filter(k -> strictlyAfter(from, k)).count();
        long windowCount = keys.stream()
                .filter(k -> strictlyAfter(from, k))
                .filter(k -> bound == null || ByteKey.copyOf(k).compareTo(bound) < 0)
                .count();

        assertThat(windowCount)
                .as("bound must not be too tight: window >= min(limit, tail); "
                        + "tail=%d limit=%d bound=%s", tailCount, limit, bound)
                .isGreaterThanOrEqualTo(Math.min(limit, tailCount));

        // Every file that actually holds a windowed key must be among the touched files (superset OK).
        Set<Path> filesWithWindowedKeys = new LinkedHashSet<>();
        for (IndexEntry entry : index) {
            int rg = index.indexOf(entry);
            ByteKey groupUpper = rg + 1 < index.size() ? index.get(rg + 1).firstKey() : null;
            for (byte[] k : keys) {
                ByteKey key = ByteKey.copyOf(k);
                boolean inGroup = key.compareTo(entry.firstKey()) >= 0
                        && (groupUpper == null || key.compareTo(groupUpper) < 0);
                boolean windowed = strictlyAfter(from, k) && (bound == null || key.compareTo(bound) < 0);
                if (inGroup && windowed) {
                    filesWithWindowedKeys.add(entry.file());
                }
            }
        }
        assertThat(plan.files()).containsAll(filesWithWindowedKeys);
    }

    @Test
    void emptyIndexAlwaysTouchesNoFiles() {
        assertThat(SortedRouting.plan(List.of(), key(new byte[]{'a'}), null, 1000).isEmpty()).isTrue();
        assertThat(SortedRouting.plan(List.of(), null, null, 1000).isEmpty()).isTrue();
    }

    @Test
    void limitLargerThanTheWholeTailClampsOpen() {
        List<byte[]> keys = List.of(new byte[]{'a'}, new byte[]{'b'}, new byte[]{'c'});
        List<IndexEntry> index = buildIndex(keys, new Random(1));   // any partition
        SortedRouting.QueryPlan plan = SortedRouting.plan(index, null, null, 10_000);
        assertThat(plan.upperBound()).isNull();
    }

    // --- fixture construction -------------------------------------------------

    /** Partition the sorted distinct keys into contiguous row groups (random sizes >=1) across files. */
    private static List<IndexEntry> buildIndex(List<byte[]> sortedKeys, Random random) {
        List<IndexEntry> index = new ArrayList<>();
        int fileIndex = 0;
        int i = 0;
        while (i < sortedKeys.size()) {
            int size = 1 + random.nextInt(4);   // 1..4 rows per group, incl. 1-row groups
            int end = Math.min(sortedKeys.size(), i + size);
            Path file = Path.of(String.format("part-%05d.parquet", fileIndex));
            index.add(new IndexEntry(file, index.size(), key(sortedKeys.get(i)), end - i));
            // Occasionally roll to a new file at a group boundary (multi-file, range-disjoint).
            if (random.nextInt(3) == 0) {
                fileIndex++;
            }
            i = end;
        }
        return index;
    }

    private static ByteKey chooseFrom(List<byte[]> keys, int selector) {
        if (selector == -1) {
            return null;                                   // open lower bound
        }
        if (selector == -2) {
            return key(new byte[]{0x00});                  // before/at the very first possible key
        }
        if (selector >= keys.size()) {
            return key(new byte[]{(byte) 0xFF, (byte) 0xFF});   // in/past the last group
        }
        return key(keys.get(selector));                    // exactly on a real key (exclusive boundary)
    }

    private static boolean strictlyAfter(ByteKey from, byte[] k) {
        return from == null || ByteKey.copyOf(k).compareTo(from) > 0;
    }

    private static List<byte[]> sortedDistinct(List<byte[]> raw) {
        TreeSet<ByteKey> sorted = new TreeSet<>();
        for (byte[] k : raw) {
            sorted.add(ByteKey.copyOf(k));
        }
        List<byte[]> out = new ArrayList<>(sorted.size());
        for (ByteKey k : sorted) {
            out.add(k.toByteArray());
        }
        return out;
    }

    private static ByteKey key(byte[] value) {
        return ByteKey.copyOf(value);
    }
}
