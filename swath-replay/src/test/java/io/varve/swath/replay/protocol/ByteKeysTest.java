/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class ByteKeysTest {

    @Test
    void percentEncodeUsesObservedS3ListObjectsUrlEncoding() {
        assertThat(ByteKeys.percentEncode(bytes("a/has space+tilde~.txt")))
                .isEqualTo("a/has%20space%2Btilde%7E.txt");
        assertThat(ByteKeys.percentEncode(bytes("test_file(3).png")))
                .isEqualTo("test_file%283%29.png");
    }

    @Test
    void prefixUpperCarriesTheLastByteAndTruncates() {
        UpperBound upper = ByteKeys.prefixUpper(bytes("photos/"));
        assertThat(upper).isInstanceOf(UpperBound.Bounded.class);
        assertThat(((UpperBound.Bounded) upper).exclusiveUpper())
                .isEqualTo(ByteKey.copyOf(bytes("photos0")));
    }

    @Test
    void prefixUpperOfEmptyPrefixIsOpen() {
        assertThat(ByteKeys.prefixUpper(new byte[0])).isInstanceOf(UpperBound.Open.class);
        assertThat(ByteKeys.prefixUpper(null)).isInstanceOf(UpperBound.Open.class);
    }

    @Test
    void prefixUpperOfAllHighBytesIsOpenBoundNotEndOfListing() {
        assertThat(ByteKeys.prefixUpper(new byte[]{(byte) 0xFF, (byte) 0xFF}))
                .isInstanceOf(UpperBound.Open.class);
    }

    @Test
    void successorIsTheLeastKeyPastTheWholePrefixRange() {
        Successor successor = ByteKeys.successor(bytes("photos/"));
        assertThat(successor).isInstanceOf(Successor.Key.class);
        assertThat(((Successor.Key) successor).key()).isEqualTo(ByteKey.copyOf(bytes("photos0")));
    }

    @Test
    void successorOfAllHighBytesIsEndOfKeyspaceNotOpenBound() {
        assertThat(ByteKeys.successor(new byte[]{(byte) 0xFF, (byte) 0xFF}))
                .isInstanceOf(Successor.EndOfKeyspace.class);
    }

    @Test
    void successorCarryPropagatesThroughTrailingHighBytes() {
        // "a\xFF" -> increment 'a' to 'b', trailing 0xFF dropped -> "b"
        Successor successor = ByteKeys.successor(new byte[]{'a', (byte) 0xFF});
        assertThat(((Successor.Key) successor).key()).isEqualTo(ByteKey.copyOf(bytes("b")));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
