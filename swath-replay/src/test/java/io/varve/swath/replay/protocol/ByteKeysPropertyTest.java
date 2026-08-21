/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import org.junit.jupiter.api.Test;

/**
 * REPLAY-PROP-1 (jqwik): the 0xFF-carry laws for {@link ByteKeys#successor(byte[])} and
 * {@link ByteKeys#prefixUpper(byte[])} — the two <b>role-distinct</b> readings of the same carry
 * bytes. These guard the serving contract adversarially over arbitrary prefixes including
 * 0xFF-laden and fully-saturated ones, asserting <em>both</em> role directions so the "open upper
 * bound" vs "end of listing" meanings can never be transposed.
 *
 * <p>The load-bearing law is stated as a biconditional that pins {@code successor(P)} as the
 * <b>least</b> key strictly greater than every {@code P}-prefixed key: the set of keys in
 * {@code [P, successor(P))} is <em>exactly</em> the set of keys with prefix {@code P}. One direction
 * ({@code prefixed ⇒ in range}) makes {@code prefixUpper} a sound exclusive upper bound; the other
 * ({@code in range ⇒ prefixed}) is leastness — nothing between {@code P} and {@code successor(P)}
 * escapes the prefix, so no smaller successor exists.
 */
class ByteKeysPropertyTest {

    // Arbitrary keys/prefixes across the FULL unsigned byte range (0x00..0xFF), incl. empty.
    @Provide
    Arbitrary<byte[]> keys() {
        return Arbitraries.bytes().array(byte[].class).ofMinSize(0).ofMaxSize(6);
    }

    // Prefixes that HAVE a finite successor (not empty, not all-0xFF) — the Key/Bounded role.
    @Provide
    Arbitrary<byte[]> nonSaturatedPrefixes() {
        return Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(6)
                .filter(p -> ByteKeys.successor(p) instanceof Successor.Key);
    }

    /**
     * The core law: for a finite-successor prefix {@code P}, an arbitrary key {@code t} has prefix
     * {@code P} <b>iff</b> {@code P <= t < successor(P)} — pinning both the exclusive-upper-bound and
     * the leastness halves at once. Also asserts the dual-role byte identity: {@code prefixUpper(P)}'s
     * bound bytes equal {@code successor(P)}'s key bytes (same carry, different types).
     */
    @Property(tries = 600)
    void prefixRangeIsExactlyKeysBelowTheSuccessor(@ForAll("nonSaturatedPrefixes") byte[] p,
                                                   @ForAll("keys") byte[] t) {
        byte[] successorBytes = successorBytes(p);

        boolean prefixed = ByteKeys.startsWith(t, p);
        boolean inRange = ByteKeys.compareUnsigned(p, t) <= 0
                && ByteKeys.compareUnsigned(t, successorBytes) < 0;
        assertThat(prefixed).isEqualTo(inRange);

        assertThat(ByteKeys.prefixUpper(p)).isInstanceOf(UpperBound.Bounded.class);
        byte[] upperBytes = ((UpperBound.Bounded) ByteKeys.prefixUpper(p)).exclusiveUpper().toByteArray();
        assertThat(upperBytes).isEqualTo(successorBytes);
    }

    /**
     * Adversarial coverage of the {@code prefixed} branch specifically (random {@code t} rarely lands
     * inside a random prefix): every key formed as {@code P ++ suffix} — including a suffix of all
     * {@code 0xFF}s — is strictly below {@code successor(P)} and does start with {@code P}.
     */
    @Property(tries = 600)
    void everyPrefixedKeyIsStrictlyBelowTheSuccessor(@ForAll("nonSaturatedPrefixes") byte[] p,
                                                     @ForAll("keys") byte[] suffix) {
        byte[] t = concat(p, suffix);
        byte[] successorBytes = successorBytes(p);

        assertThat(ByteKeys.startsWith(t, p)).isTrue();
        assertThat(ByteKeys.compareUnsigned(t, successorBytes)).isLessThan(0);
        // successor(P) itself never carries the prefix (it escaped the range).
        assertThat(ByteKeys.startsWith(successorBytes, p)).isFalse();
    }

    /**
     * The dual-role disambiguation at the saturation boundary, BOTH directions: an all-{@code 0xFF}
     * prefix is {@link UpperBound.Open} in the bound role AND {@link Successor.EndOfKeyspace} in the
     * resume role — never one masquerading as the other.
     */
    @Property(tries = 100)
    void allFfPrefixIsOpenUpperBoundAndEndOfKeyspace(@ForAll @IntRange(min = 1, max = 6) int length) {
        byte[] saturated = new byte[length];
        Arrays.fill(saturated, (byte) 0xFF);

        assertThat(ByteKeys.prefixUpper(saturated)).isInstanceOf(UpperBound.Open.class);
        assertThat(ByteKeys.successor(saturated)).isInstanceOf(Successor.EndOfKeyspace.class);
    }

    @Test
    void emptyPrefixIsOpenUpperBoundAndEndOfKeyspace() {
        byte[] empty = new byte[0];
        assertThat(ByteKeys.prefixUpper(empty)).isInstanceOf(UpperBound.Open.class);
        assertThat(ByteKeys.successor(empty)).isInstanceOf(Successor.EndOfKeyspace.class);
    }

    @Test
    void nullPrefixIsOpenUpperBoundAndEndOfKeyspace() {
        assertThat(ByteKeys.prefixUpper(null)).isInstanceOf(UpperBound.Open.class);
        assertThat(ByteKeys.successor(null)).isInstanceOf(Successor.EndOfKeyspace.class);
    }

    @Test
    void trailingFfBytesAreStrippedThenTheLastNonFfByteIsCarried() {
        // "ab\xFF\xFF" -> strip the two 0xFF, carry 'b' -> "ac".
        byte[] prefix = {'a', 'b', (byte) 0xFF, (byte) 0xFF};
        byte[] successor = successorBytes(prefix);
        assertThat(successor).isEqualTo(new byte[]{'a', 'c'});
    }

    private static byte[] successorBytes(byte[] prefix) {
        Successor successor = ByteKeys.successor(prefix);
        assertThat(successor).isInstanceOf(Successor.Key.class);
        return ((Successor.Key) successor).key().toByteArray();
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
