/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.testkit.CodePointKeys;
import io.varve.swath.testkit.ScalarSafety;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * PROP-2 (algorithms.md §3.1): over valid-UTF-8 bounds (which may
 * themselves carry unsafe code points — the asymmetry), {@code byteMidpoint(a,b)}
 * returns {@code m} with {@code a < m < b} (unsigned) <b>and {@code m} is valid
 * UTF-8</b>, whose SYNTHESIZED code point is drawn from the safe set {@code E^c}
 * ({@link ScalarSafety#isExcludedScalar} is {@code E}: contract I12 —
 * C0/DEL/C1 controls, the BMP and supplementary noncharacters, surrogates); or
 * {@code null} iff no such safe {@code m} exists strictly between (the successor
 * region up to {@code b} is a control/noncharacter sliver). Inputs are valid UTF-8
 * because every real S3 key is, and every pivot is chosen valid UTF-8. The
 * synthesized scalar is safe by construction; copied bound prefixes are tested separately
 * with safe-prefix inputs.
 */
class ByteMidpointPropertyTest {

    @Property(tries = 5000)
    void strictlyBetweenAndValidUtf8(@ForAll("utf8Keys") byte[] x, @ForAll("utf8Keys") byte[] y) {
        int cmp = Arrays.compareUnsigned(x, y);
        Assume.that(cmp != 0);
        byte[] a = cmp < 0 ? x : y;
        byte[] b = cmp < 0 ? y : x;

        byte[] m = ByteMidpoint.between(a, b);

        if (m == null) {
            // Keys are short (≤128 bytes here), so the 1024-byte cap never fires. The broadened
            // null contract: null iff a is a proper code-point prefix of b AND the first code point
            // of b's tail (t) admits no SAFE key strictly between — i.e. t ≤ U+0020, except when
            // t == U+0020 and b is strictly longer than a·U+0020 (then a·U+0020 splits).
            assertNullIsControlSliver(a, b);
        } else {
            assertThat(Arrays.compareUnsigned(a, m)).as("a < m").isNegative();
            assertThat(Arrays.compareUnsigned(m, b)).as("m < b").isNegative();
            assertThat(KeyBytes.isValidUtf8(m)).as("m is valid UTF-8: %s", Arrays.toString(m)).isTrue();
        }
    }

    /**
     * The single safety guard this class exists to keep from recurring (the asymmetry, on the
     * {@code between} path): {@code a} is a SAFE key (a real cursor is XML-legal, and a pivot copies
     * {@code a}'s prefix — or all of {@code a} in the append fallback — verbatim), while {@code b}
     * is ARBITRARY and may carry unsafe code points at the divergence / tail ({@code b}'s bytes are
     * never copied into {@code m} except a tail code point that the kernel guards as safe). Every
     * non-null {@code between(a,b)} carries NO code point in {@code E} and stays strictly between.
     */
    @Property(tries = 5000)
    void betweenPivotsAreAlwaysSafe(@ForAll("safeKeys") byte[] a, @ForAll("utf8Keys") byte[] b) {
        Assume.that(Arrays.compareUnsigned(a, b) < 0);

        byte[] m = ByteMidpoint.between(a, b);
        if (m != null) {
            assertThat(Arrays.compareUnsigned(a, m)).as("between: a < m").isNegative();
            assertThat(Arrays.compareUnsigned(m, b)).as("between: m < b").isNegative();
            assertNoExcluded(m, "between");
        }
    }

    /**
     * Companion guard for the open-frontier path: {@link ByteMidpoint#forwardReflect} reflects the
     * consumed span {@code (lo, c]} forward, where {@code lo} is an ARBITRARY bound (it only feeds
     * the reflection index math — it is never copied into {@code m}) and {@code c} is a SAFE cursor
     * (for this whole-output safety property, {@code m} copies {@code c}'s prefix verbatim). Every non-null
     * reflected pivot is {@code > c} and carries no code point in {@code E} — the bug class (a low
     * cursor char reflected to U+000F, a U+10FFFF run appended with a raw mid-scalar) cannot recur.
     */
    @Property(tries = 5000)
    void forwardReflectPivotsAreAlwaysSafe(@ForAll("utf8Keys") byte[] lo, @ForAll("safeKeys") byte[] c) {
        Assume.that(c.length > 0);   // forwardReflect requires a non-empty cursor
        byte[] m = ByteMidpoint.forwardReflect(lo, c);
        if (m != null) {
            assertThat(Arrays.compareUnsigned(c, m)).as("forwardReflect: c < m").isNegative();
            assertNoExcluded(m, "forwardReflect");
        }
    }

    /**
     * Bias toward the structural cases a flat random generator rarely lands: {@code a} a proper
     * code-point prefix of {@code b} ({@code b == a ++ [tail]}). The single-code-point successor is
     * splittable iff a SAFE scalar lies strictly below {@code tail}, i.e. iff {@code tail > U+0020};
     * for {@code tail ≤ U+0020} there is no safe key strictly between, so the pivot is {@code null}.
     */
    @Property(tries = 5000)
    void prefixAndSuccessorStructures(@ForAll("safeKeys") byte[] a, @ForAll("scalars") int tail) {
        byte[] tailBytes = new String(Character.toChars(tail)).getBytes(StandardCharsets.UTF_8);
        byte[] b = new byte[a.length + tailBytes.length];
        System.arraycopy(a, 0, b, 0, a.length);
        System.arraycopy(tailBytes, 0, b, a.length, tailBytes.length);
        Assume.that(Arrays.compareUnsigned(a, b) < 0);   // always true: b extends a

        byte[] m = ByteMidpoint.between(a, b);
        if (tail <= 0x20) {
            assertThat(m).as("b == a ++ [tail ≤ U+0020] ⇒ no safe key strictly between ⇒ null").isNull();
        } else {
            assertThat(m).isNotNull();
            assertThat(Arrays.compareUnsigned(a, m)).as("a < m").isNegative();
            assertThat(Arrays.compareUnsigned(m, b)).as("m < b").isNegative();
            assertThat(KeyBytes.isValidUtf8(m)).as("m is valid UTF-8").isTrue();
            assertNoExcluded(m, "between");   // a is safe and the synthesized scalar is safe
        }
    }

    /**
     * Max-length keys with scalar-adjacent leading code points (the case short generators miss):
     * {@code a = lead + 1023×U+0000} (exactly 1024 bytes) and {@code b = lead+1} (the adjacent
     * ASCII successor). The naïve append is 1025 bytes (> cap), but a valid 1024-byte pivot always
     * exists by bumping {@code a}'s last scalar to the next SAFE scalar — the cap fallback must find
     * it and never falsely return {@code null}.
     */
    @Property(tries = 200)
    void maxLengthAdjacentLeadingScalarsAlwaysSplit(@ForAll("asciiLead") int lead) {
        byte[] a = new byte[1024];
        a[0] = (byte) lead;                         // lead, then 1023 × U+0000 ⇒ exactly 1024 bytes
        byte[] b = {(byte) (lead + 1)};             // scalar-adjacent successor (stays 1-byte ASCII)

        byte[] m = ByteMidpoint.between(a, b);
        assertThat(m).as("append overflows but a bumpable-tail pivot exists").isNotNull();
        assertThat(m.length).isLessThanOrEqualTo(1024);
        assertThat(Arrays.compareUnsigned(a, m)).as("a < m").isNegative();
        assertThat(Arrays.compareUnsigned(m, b)).as("m < b").isNegative();
        assertThat(KeyBytes.isValidUtf8(m)).as("m is valid UTF-8").isTrue();
    }

    /** ASCII leads whose +1 successor is still a single-byte scalar (0x41..0x7D ⇒ ≤0x7E). */
    @Provide
    Arbitrary<Integer> asciiLead() {
        return Arbitraries.integers().between(0x41, 0x7D);
    }

    /**
     * Valid-UTF-8 keys over the FULL scalar space (surrogate block excluded), UTF-8-encoded — so a
     * bound MAY carry unsafe code points (C0/DEL/U+FFFE/U+FFFF/U+0000): the asymmetric input.
     */
    @Provide
    Arbitrary<byte[]> utf8Keys() {
        return CodePointKeys.scalars().list().ofMinSize(0).ofMaxSize(32).map(CodePointKeys::encode);
    }

    /** Valid-UTF-8 keys whose code points are all SAFE (E-free): a realistic key/cursor. */
    @Provide
    Arbitrary<byte[]> safeKeys() {
        return CodePointKeys.safeScalars().list().ofMinSize(0).ofMaxSize(16).map(CodePointKeys::encode);
    }

    /** Unicode scalar values: U+0000..U+10FFFF excluding the surrogate block U+D800..U+DFFF. */
    @Provide
    Arbitrary<Integer> scalars() {
        return CodePointKeys.scalars();
    }

    /** SAFE scalar values (E-free, no surrogate): the synthesizable / XML-legal code points. */
    @Provide
    Arbitrary<Integer> safeScalars() {
        return CodePointKeys.safeScalars();
    }

    private static void assertNoExcluded(byte[] m, String site) {
        new String(m, StandardCharsets.UTF_8).codePoints().forEach(c ->
                assertThat(ScalarSafety.isExcludedScalar(c))
                        .as("%s: no excluded (unsafe) code point U+%04X in %s", site, c, Arrays.toString(m))
                        .isFalse());
    }

    /**
     * {@code between(a,b) == null} iff a is a proper code-point prefix of b and the first tail code
     * point {@code t} admits no SAFE key strictly between: {@code t < U+0020}, or {@code t == U+0020}
     * with b NOT strictly longer than a·U+0020.
     */
    private static void assertNullIsControlSliver(byte[] a, byte[] b) {
        int[] ca = a.length == 0 ? new int[0] : new String(a, StandardCharsets.UTF_8).codePoints().toArray();
        int[] cb = new String(b, StandardCharsets.UTF_8).codePoints().toArray();
        assertThat(cb.length).as("a is a proper prefix of b").isGreaterThan(ca.length);
        for (int i = 0; i < ca.length; i++) {
            assertThat(cb[i]).as("a is a code-point prefix of b at %d", i).isEqualTo(ca[i]);
        }
        int t = cb[ca.length];
        assertThat(t).as("tail code point admits no safe key strictly between").isLessThanOrEqualTo(0x20);
        if (t == 0x20) {
            assertThat(cb.length).as("t == U+0020 ⇒ b is NOT strictly longer than a·U+0020")
                    .isEqualTo(ca.length + 1);
        }
    }
}
