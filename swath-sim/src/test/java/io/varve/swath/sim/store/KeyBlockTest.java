/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * {@link KeyBlock}'s own fail-fast guards: {@link KeyBlock.Builder} is the streaming tier's hottest
 * loop, and every one of these checks exists because a row group that violates it would not merely
 * fail loudly elsewhere — it would corrupt every later binary search over the block and the tier
 * would report confident wrong answers instead. Round-trip correctness of a well-formed block is
 * covered end to end by {@code StreamingListingStoreTest} and {@code SimStoreDifferentialTest}; what
 * is pinned here is the two guards those tests never exercise on a bad row group.
 */
class KeyBlockTest {

    private static final long GENEROUS_BYTES = 1L << 20;

    @Test
    void aBuiltBlockIsSearchableAndRoundTripsKeysByteExact() {
        KeyBlock.Builder builder = KeyBlock.builder(3, GENEROUS_BYTES);
        assertThat(builder.append(utf8("a"))).isTrue();
        assertThat(builder.append(utf8("c"))).isTrue();
        assertThat(builder.append(utf8("e"))).isTrue();

        try (KeyBlock block = builder.build()) {
            assertThat(block.size()).isEqualTo(3);
            assertThat(block.keyAt(0)).isEqualTo(utf8("a"));
            assertThat(block.keyAt(1)).isEqualTo(utf8("c"));
            assertThat(block.keyAt(2)).isEqualTo(utf8("e"));
            assertThat(block.lowerBound(utf8("c"))).isEqualTo(1);
            assertThat(block.upperBound(utf8("c"))).isEqualTo(2);
            assertThat(block.lowerBound(utf8("b"))).isEqualTo(1);
            assertThat(block.lowerBound(utf8("z"))).isEqualTo(block.size());
        }
    }

    /**
     * The plain case: a key that sorts below its predecessor, mirroring
     * {@code KeyArenaTest#appendRejectsANonAscendingKeyRatherThanCorruptingBinarySearch}.
     */
    @Test
    void appendRejectsANonAscendingKeyRatherThanCorruptingBinarySearch() {
        KeyBlock.Builder builder = KeyBlock.builder(2, GENEROUS_BYTES);
        assertThat(builder.append(utf8("b"))).isTrue();

        try {
            assertThatThrownBy(() -> builder.append(utf8("a")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("strictly ascending");
        } finally {
            builder.discard();
        }
    }

    /**
     * The boundary case a plain byte compare must not get wrong: a key that is an exact prefix of
     * its predecessor (shorter, but not "less" by any per-byte mismatch) sorts below it, mirroring
     * {@code KeyArenaTest#theAscendingCheckComparesAcrossASegmentBoundary} — there the boundary is a
     * segment split, here it is the shared-prefix boundary between two keys of different lengths.
     */
    @Test
    void appendRejectsAKeyThatIsAPrefixOfItsPredecessor() {
        KeyBlock.Builder builder = KeyBlock.builder(2, GENEROUS_BYTES);
        assertThat(builder.append(utf8("ab"))).isTrue();

        try {
            assertThatThrownBy(() -> builder.append(utf8("a")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("strictly ascending");
        } finally {
            builder.discard();
        }
    }

    /**
     * A row group that hands over more keys than the routing index declared for it must fail loudly,
     * not silently keep the first {@code rowCount} and drop the rest — a decoder bug here would
     * otherwise serve a truncated row group with no signal that anything was lost.
     */
    @Test
    void appendRejectsAKeyOnceTheDeclaredRowCountIsReached() {
        KeyBlock.Builder builder = KeyBlock.builder(1, GENEROUS_BYTES);
        assertThat(builder.append(utf8("a"))).isTrue();

        try {
            assertThatThrownBy(() -> builder.append(utf8("b")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("declared 1 rows")
                    .hasMessageContaining("further key arrived");
        } finally {
            builder.discard();
        }
    }

    /**
     * A row group that hands over fewer keys than declared must fail {@link KeyBlock.Builder#build()}
     * rather than build a block whose offset table's tail was never written — a search that landed
     * there would read a zero offset as a real one instead of the missing-rows bug it actually is.
     */
    @Test
    void buildRejectsFewerKeysThanDeclared() {
        KeyBlock.Builder builder = KeyBlock.builder(2, GENEROUS_BYTES);
        assertThat(builder.append(utf8("a"))).isTrue();

        try {
            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("declared 2 rows")
                    .hasMessageContaining("only 1 keys");
        } finally {
            builder.discard();
        }
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
