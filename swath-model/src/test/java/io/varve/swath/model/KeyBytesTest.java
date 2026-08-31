/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * UNIT-1 (I10): {@code KeyBytes.compareUnsigned} (S3's UTF-8 byte
 * order) must <b>differ</b> from {@code String.compareTo} (UTF-16 order) on keys
 * with code points ≥ U+0800 and across supplementary planes — using
 * {@code String} comparison for boundaries is a silent correctness bug.
 */
class KeyBytesTest {

    @ParameterizedTest(name = "accepts well-formed UTF-8 {0}")
    @MethodSource("wellFormedUtf8")
    void recognizesWellFormedUtf8(String description, byte[] bytes) {
        assertThat(KeyBytes.isValidUtf8(bytes)).as(description).isTrue();
    }

    @ParameterizedTest(name = "rejects malformed UTF-8 {0}")
    @MethodSource("malformedUtf8")
    void rejectsMalformedUtf8(String description, byte[] bytes) {
        assertThat(KeyBytes.isValidUtf8(bytes)).as(description).isFalse();
    }

    private static Object[][] malformedUtf8() {
        return new Object[][]{
                {"bare continuation", new byte[]{(byte) 0x80}},
                {"invalid lead", new byte[]{(byte) 0xFF}},
                {"two-byte overlong", new byte[]{(byte) 0xC0, (byte) 0x80}},
                {"three-byte overlong", new byte[]{(byte) 0xE0, (byte) 0x80, (byte) 0x80}},
                {"four-byte overlong", new byte[]{(byte) 0xF0, (byte) 0x80, (byte) 0x80, (byte) 0x80}},
                {"surrogate", new byte[]{(byte) 0xED, (byte) 0xA0, (byte) 0x80}},
                {"above Unicode maximum", new byte[]{(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80}},
                {"truncated multibyte tail", new byte[]{(byte) 0xE2, (byte) 0x82}}
        };
    }

    private static Object[][] wellFormedUtf8() {
        return new Object[][]{
                {"empty", new byte[0]},
                {"ASCII", "plain/ascii".getBytes(StandardCharsets.UTF_8)},
                {"two-byte minimum", new byte[]{(byte) 0xC2, (byte) 0x80}},
                {"BMP below surrogate", new byte[]{(byte) 0xED, (byte) 0x9F, (byte) 0xBF}},
                {"BMP above surrogate", new byte[]{(byte) 0xEE, (byte) 0x80, (byte) 0x80}},
                {"Unicode maximum", new byte[]{(byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF}}
        };
    }

    @Test
    void supplementaryVsBmpKeysOrderOppositelyUnderByteVsUtf16() {
        // U+E000 (BMP) → UTF-8 EE 80 80 ; U+10000 (supplementary) → UTF-8 F0 90 80 80.
        KeyBytes bmp = KeyBytes.ofUtf8("");
        KeyBytes supplementary = KeyBytes.ofUtf8("𐀀");   // U+10000

        // Sanity: the UTF-8 encodings are what we claim.
        assertThat(bmp.raw()).containsExactly(0xEE, 0x80, 0x80);
        assertThat(supplementary.raw()).containsExactly(0xF0, 0x90, 0x80, 0x80);

        int byteOrder = bmp.compareTo(supplementary);                       // EE < F0 ⇒ negative
        int utf16Order = bmp.asString().compareTo(supplementary.asString()); // 0xE000 > 0xD800 ⇒ positive

        assertThat(byteOrder).isNegative();
        assertThat(utf16Order).isPositive();
        assertThat(Integer.signum(byteOrder)).isNotEqualTo(Integer.signum(utf16Order));
    }

    @Test
    void compareUnsignedTreatsHighBytesAsUnsigned() {
        // 0x80 is negative as a signed byte but must sort AFTER 0x7f.
        byte[] low = {0x7f};
        byte[] high = {(byte) 0x80};
        assertThat(KeyBytes.compareUnsigned(low, high)).isNegative();
        // Signed comparison would (wrongly) say 0x80 < 0x7f:
        assertThat(Byte.compare(low[0], high[0])).isPositive();
    }

    @Test
    void multibyteBmpKeysAtOrAboveU0800() {
        // U+0800 → E0 A0 80 ; ensure ordering is by raw bytes.
        KeyBytes k0800 = KeyBytes.ofUtf8("ࠀ");
        KeyBytes k07ff = KeyBytes.ofUtf8("߿");   // 2-byte DF BF
        assertThat(k0800.raw()).containsExactly(0xE0, 0xA0, 0x80);
        assertThat(k07ff.raw()).containsExactly(0xDF, 0xBF);
        assertThat(k07ff.compareTo(k0800)).isNegative();   // DF < E0
    }

    @Test
    void equalityAndAsStringRoundTrip() {
        KeyBytes a = KeyBytes.ofUtf8("crawl=01/pid=ab/part.parquet");
        KeyBytes b = KeyBytes.of("crawl=01/pid=ab/part.parquet".getBytes(StandardCharsets.UTF_8));
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a.asString()).isEqualTo("crawl=01/pid=ab/part.parquet");
        assertThat(a.length()).isEqualTo(a.raw().length);
    }

    @Test
    void publicRawReturnsDefensiveCopy() {
        KeyBytes key = KeyBytes.ofUtf8("abc");
        byte[] exposed = key.raw();

        exposed[0] = 'z';

        assertThat(key.asString()).isEqualTo("abc");
        assertThat(key).isEqualTo(KeyBytes.ofUtf8("abc"));
        assertThat(key.raw()).containsExactly('a', 'b', 'c');
    }
}
