/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * {@link CountingWriter} derives UTF-8 byte counts arithmetically (no re-encoding), across
 * all three {@link java.io.FilterWriter} overloads it forwards verbatim. The load-bearing
 * case is an astral-plane (surrogate-pair) code point written in a single {@code
 * write(String)} call, which must count 4 bytes, not 6 (3+3).
 */
class CountingWriterTest {

    @Test
    void asciiStringCountsOneByteMerChar() throws IOException {
        CountingWriter writer = new CountingWriter(new StringWriter());
        writer.write("hello");
        assertThat(writer.bytesWritten()).isEqualTo(5L);
    }

    @Test
    void twoByteCodePointCountsTwoBytes() throws IOException {
        // U+00E9 (é) is 2 bytes in UTF-8.
        CountingWriter writer = new CountingWriter(new StringWriter());
        writer.write("é");
        assertThat(writer.bytesWritten()).isEqualTo(2L);
    }

    @Test
    void threeByteBmpCodePointCountsThreeBytes() throws IOException {
        // U+20AC (€) is 3 bytes in UTF-8.
        CountingWriter writer = new CountingWriter(new StringWriter());
        writer.write("€");
        assertThat(writer.bytesWritten()).isEqualTo(3L);
    }

    @Test
    void astralSurrogatePairWrittenInOneStringCallCountsFourBytesNotSix() throws IOException {
        // U+1F600 (😀) is 2 Java chars (a high+low surrogate pair) but a single 4-byte UTF-8
        // code point. This is the load-bearing case: a naive per-char tally would count
        // 3 (unpaired-surrogate fallback) + 3 = 6 bytes instead of the correct 4.
        CountingWriter writer = new CountingWriter(new StringWriter());
        String astral = "😀";
        assertThat(astral).hasSize(2);   // sanity: confirms this really is a surrogate pair in Java

        writer.write(astral);

        assertThat(writer.bytesWritten()).isEqualTo(4L);
    }

    @Test
    void astralSurrogatePairViaCharArrayWriteAlsoCountsFourBytes() throws IOException {
        // The char[] overload has its own pair-detection loop; exercise it directly (not just via
        // the String overload) so a divergence between the two implementations is caught.
        CountingWriter writer = new CountingWriter(new StringWriter());
        char[] astral = "😀".toCharArray();

        writer.write(astral, 0, astral.length);

        assertThat(writer.bytesWritten()).isEqualTo(4L);
    }

    @Test
    void perCharWriteIntOfASurrogatePairCountsThreePlusThreeByDesign() throws IOException {
        // Documented, deliberate divergence from the single-call 4-byte count: write(int) sees only
        // one char at a time, so it can never detect a pair spanning two calls and falls back to
        // treating each lone surrogate half as its own 3-byte code point (see the class javadoc's
        // "keep pairs within a single write" caveat). Asserted explicitly so a future change to this
        // behavior is a deliberate, visible decision rather than a silent regression.
        CountingWriter writer = new CountingWriter(new StringWriter());
        String astral = "😀";

        writer.write(astral.charAt(0));
        writer.write(astral.charAt(1));

        assertThat(writer.bytesWritten()).isEqualTo(6L);
    }

    @Test
    void theThreeOverloadsAgreeOnPureBmpContent() throws IOException {
        String s = "abcé€xyz";
        char[] chars = s.toCharArray();

        CountingWriter viaString = new CountingWriter(new StringWriter());
        viaString.write(s);

        CountingWriter viaCharArray = new CountingWriter(new StringWriter());
        viaCharArray.write(chars, 0, chars.length);

        CountingWriter viaWriteInt = new CountingWriter(new StringWriter());
        for (char c : chars) {
            viaWriteInt.write(c);
        }

        long expected = s.getBytes(StandardCharsets.UTF_8).length;
        assertThat(viaString.bytesWritten()).isEqualTo(expected);
        assertThat(viaCharArray.bytesWritten()).isEqualTo(expected);
        assertThat(viaWriteInt.bytesWritten()).isEqualTo(expected);
    }

    @Test
    void emptyAndZeroLengthWritesCountZeroBytes() throws IOException {
        CountingWriter writer = new CountingWriter(new StringWriter());

        writer.write("");
        writer.write(new char[0], 0, 0);
        writer.write("nonempty", 0, 0);

        assertThat(writer.bytesWritten()).isZero();
    }

    @Test
    void runningTotalAccumulatesAcrossMultipleWrites() throws IOException {
        CountingWriter writer = new CountingWriter(new StringWriter());

        writer.write("ab");     // 2 bytes
        writer.write("é");      // 2 bytes
        writer.write("😀");     // 4 bytes

        assertThat(writer.bytesWritten()).isEqualTo(8L);
    }

    @Test
    void delegateReceivesTheActualCharsUnmodified() throws IOException {
        StringWriter delegate = new StringWriter();
        CountingWriter writer = new CountingWriter(delegate);

        writer.write("abc");

        assertThat(delegate.toString()).isEqualTo("abc");
        assertThat(writer.bytesWritten()).isEqualTo(3L);
    }
}
