/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class KeyArenaTest {

    /** The smallest legal segment: every key of a multi-hundred-byte set then straddles boundaries. */
    private static final int TIGHT_SEGMENT_BYTES = KeyArena.MAX_KEY_BYTES;

    private static final long GENEROUS_BUDGET = 1L << 20;

    @Test
    void keysStraddlingSegmentBoundariesRoundTripByteExact() {
        List<byte[]> keys = new ArrayList<>();
        Random random = new Random(20260727L);
        // 300-byte keys against 1024-byte segments: keys 3, 6, 10, ... start mid-segment and run
        // past its end, which is exactly the case a single-byte[] arena could never hit.
        for (int i = 0; i < 40; i++) {
            byte[] key = new byte[300];
            random.nextBytes(key);
            key[0] = (byte) i;
            keys.add(key);
        }
        keys.sort(Arrays::compareUnsigned);

        KeyArena arena = arenaOf(keys, TIGHT_SEGMENT_BYTES);

        assertThat(arena.size()).isEqualTo(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            assertThat(arena.keyAt(i)).as("key %d", i).isEqualTo(keys.get(i));
        }
    }

    @Test
    void compareKeyAtMatchesUnsignedByteOrderAcrossSegments() {
        List<byte[]> keys = List.of(
                bytes(0x41),                                       // "A"
                bytes(0x41, 0x00),                                 // prefix-of case
                bytes(0x7E),                                       // '~' — sorts BEFORE the high bytes
                "é".getBytes(StandardCharsets.UTF_8),              // 0xC3 0xA9
                bytes(0xFF),
                bytes(0xFF, 0xFF));
        KeyArena arena = arenaOf(keys, TIGHT_SEGMENT_BYTES);

        for (int i = 0; i < keys.size(); i++) {
            assertThat(arena.compareKeyAt(i, keys.get(i))).as("self at %d", i).isZero();
            for (int j = 0; j < keys.size(); j++) {
                assertThat(Integer.signum(arena.compareKeyAt(i, keys.get(j))))
                        .as("arena[%d] vs key[%d]", i, j)
                        .isEqualTo(Integer.signum(Arrays.compareUnsigned(keys.get(i), keys.get(j))));
            }
        }
    }

    @Test
    void lowerAndUpperBoundStraddleAnExactMatch() {
        KeyArena arena = arenaOf(List.of(utf8("a"), utf8("c"), utf8("e")), TIGHT_SEGMENT_BYTES);

        assertThat(arena.lowerBound(utf8("c"))).isEqualTo(1);
        assertThat(arena.upperBound(utf8("c"))).isEqualTo(2);
        // A miss lands on the same index either way.
        assertThat(arena.lowerBound(utf8("b"))).isEqualTo(1);
        assertThat(arena.upperBound(utf8("b"))).isEqualTo(1);
        // Below the first key, and past the last.
        assertThat(arena.lowerBound(utf8("A"))).isZero();
        assertThat(arena.lowerBound(utf8("z"))).isEqualTo(arena.size());
        assertThat(arena.upperBound(utf8("e"))).isEqualTo(arena.size());
    }

    @Test
    void emptyArenaSearchesToZeroAndReportsOffsetTableOnly() {
        KeyArena arena = new KeyArena.Builder(GENEROUS_BUDGET, TIGHT_SEGMENT_BYTES).build();

        assertThat(arena.size()).isZero();
        assertThat(arena.lowerBound(utf8("anything"))).isZero();
        assertThat(arena.encodedBytes()).isEqualTo(Long.BYTES);
    }

    @Test
    void encodedBytesCountsKeyBytesPlusTheOffsetTable() {
        KeyArena arena = arenaOf(List.of(utf8("aa"), utf8("bbb")), TIGHT_SEGMENT_BYTES);

        assertThat(arena.encodedBytes()).isEqualTo(5 + 3L * Long.BYTES);
    }

    @Test
    void theBudgetIsBytesNotKeys() {
        // Same key COUNT, three orders of magnitude apart in bytes: a count-based threshold would
        // admit both, a byte-based one admits only the short set.
        long budget = KeyArena.encodedBytes(64, 8);
        KeyArena.Builder shortKeys = new KeyArena.Builder(budget, TIGHT_SEGMENT_BYTES);
        KeyArena.Builder longKeys = new KeyArena.Builder(budget, TIGHT_SEGMENT_BYTES);

        for (int i = 0; i < 8; i++) {
            assertThat(shortKeys.append(new byte[8])).as("short key %d", i).isTrue();
        }
        assertThat(longKeys.append(new byte[KeyArena.MAX_KEY_BYTES])).isFalse();
    }

    @Test
    void appendDeclinesTheKeyThatWouldCrossTheBudget() {
        // Room for exactly two 4-byte keys and their three offsets.
        KeyArena.Builder builder = new KeyArena.Builder(KeyArena.encodedBytes(8, 2), TIGHT_SEGMENT_BYTES);

        assertThat(builder.append(utf8("aaaa"))).isTrue();
        assertThat(builder.append(utf8("bbbb"))).isTrue();
        assertThat(builder.append(utf8("cccc"))).isFalse();
        // Declining does not corrupt what was already appended.
        assertThat(builder.build().size()).isEqualTo(2);
    }

    @Test
    void aMaximumLengthKeyIsStoredAndAnOversizedOneIsRejected() {
        KeyArena.Builder builder = new KeyArena.Builder(GENEROUS_BUDGET, TIGHT_SEGMENT_BYTES);
        byte[] maximum = new byte[KeyArena.MAX_KEY_BYTES];
        Arrays.fill(maximum, (byte) 0xFF);

        assertThat(builder.append(maximum)).isTrue();
        assertThat(builder.build().keyAt(0)).isEqualTo(maximum);

        assertThatThrownBy(() -> new KeyArena.Builder(GENEROUS_BUDGET, TIGHT_SEGMENT_BYTES)
                .append(new byte[KeyArena.MAX_KEY_BYTES + 1]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(String.valueOf(KeyArena.MAX_KEY_BYTES));
    }

    private static KeyArena arenaOf(List<byte[]> keys, int segmentBytes) {
        KeyArena.Builder builder = new KeyArena.Builder(GENEROUS_BUDGET, segmentBytes);
        for (byte[] key : keys) {
            assertThat(builder.append(key)).as("append").isTrue();
        }
        return builder.build();
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
