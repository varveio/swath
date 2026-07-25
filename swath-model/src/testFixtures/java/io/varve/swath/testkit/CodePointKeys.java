/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import java.nio.charset.StandardCharsets;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;

/**
 * The single shared home for {@code ByteMidpoint}'s property-test key generators and byte-array
 * builders (mirrors {@link ScalarSafety}, its sibling excluded-set predicate) — kept in ONE place
 * so every {@code ByteMidpoint*Test} class samples the SAME scalar space with the SAME bounds and
 * distributions instead of re-declaring (and silently drifting from) its own copy.
 */
public final class CodePointKeys {

    private CodePointKeys() {
    }

    /** Unicode scalar values: U+0000..U+10FFFF excluding the surrogate block U+D800..U+DFFF. */
    public static Arbitrary<Integer> scalars() {
        return Arbitraries.integers().between(0x0000, 0x10FFFF)
                .filter(cp -> cp < 0xD800 || cp > 0xDFFF);
    }

    /** SAFE scalar values (E-free, no surrogate): the synthesizable / XML-legal code points. */
    public static Arbitrary<Integer> safeScalars() {
        return scalars().filter(cp -> !ScalarSafety.isExcludedScalar(cp));
    }

    /**
     * Keys drawn from a NARROW scalar band (U+0020..U+0040) bracketing '%' (U+0025) — may itself
     * carry the excluded '%' (an arbitrary bound), forcing pivot synthesis to repeatedly weigh
     * {@code 0x25} as a candidate.
     */
    public static Arbitrary<byte[]> narrowBandKeys() {
        return Arbitraries.integers().between(0x20, 0x40)
                .list().ofMinSize(0).ofMaxSize(6)
                .map(CodePointKeys::encode);
    }

    /** Narrow-band keys whose scalars are all SAFE (percent-free and otherwise unexcluded). */
    public static Arbitrary<byte[]> narrowBandSafeKeys() {
        return Arbitraries.integers().between(0x20, 0x40)
                .filter(cp -> !ScalarSafety.isExcludedScalar(cp))
                .list().ofMinSize(0).ofMaxSize(6)
                .map(CodePointKeys::encode);
    }

    /** UTF-8 bytes of a list of Unicode code points, in order. */
    public static byte[] encode(List<Integer> codePoints) {
        StringBuilder sb = new StringBuilder();
        for (int cp : codePoints) {
            sb.appendCodePoint(cp);
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** A byte array from int literals (each truncated to a byte) — compact key literals in tests. */
    public static byte[] b(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }
}
