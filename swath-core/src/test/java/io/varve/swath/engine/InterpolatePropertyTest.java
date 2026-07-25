/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.testkit.ScalarSafety;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * The far-ahead interpolated pivot — the independent, adversarial guard of the
 * {@link StealMath#interpolate(byte[], byte[], double)} /
 * {@link ByteMidpoint#between(byte[], byte[], double)} contract. Pure math — no engine, no I/O.
 *
 * <p>These properties are designed to FAIL if the far-ahead generalization ever regresses the
 * byte-exact split contract that PROP-1's no-gap/no-overlap tiling rests on.
 *
 * <ol>
 *   <li><b>Equivalence (regression guard):</b> {@code between(a,b,0.5)} is byte-for-byte identical
 *       to {@code between(a,b)} — the far-ahead knob at its midpoint MUST reproduce the prior
 *       split exactly, or every existing tiling proof silently shifts.</li>
 *   <li><b>Monotone far-ahead placement:</b> a valid-UTF-8 pivot strictly between the bounds that
 *       is monotonically non-decreasing in {@code f}, with the SAME {@code null} (unsplittable)
 *       contract as {@code byteMidpoint} for every {@code f ∈ (0,1)}.</li>
 * </ol>
 */
class InterpolatePropertyTest {

    /** The far-ahead fraction grid used across the monotonicity / null-contract sweeps. */
    private static final double[] F_GRID = {0.05, 0.1, 0.25, 0.5, 0.6, 0.75, 0.9, 0.95};

    // ------------------------------------------------------------------------
    // (1) Equivalence: between(a,b,0.5) is byte-for-byte between(a,b).
    // ------------------------------------------------------------------------

    /**
     * The midpoint fraction must coincide with the plain midpoint <b>byte-for-byte</b> over a broad
     * sweep (random keys incl. multibyte, boundary, and adjacent bounds). If the two ever diverge,
     * the far-ahead generalization has silently shifted the default split and this FAILS.
     */
    @Property(tries = 5000)
    void halfFractionIsByteForByteByteMidpoint(@ForAll("utf8Keys") byte[] x, @ForAll("utf8Keys") byte[] y) {
        int cmp = Arrays.compareUnsigned(x, y);
        Assume.that(cmp != 0);
        byte[] a = cmp < 0 ? x : y;
        byte[] b = cmp < 0 ? y : x;

        byte[] plain = ByteMidpoint.between(a, b);
        byte[] half = ByteMidpoint.between(a, b, 0.5);
        byte[] viaInterpolate = StealMath.interpolate(a, b, 0.5);

        assertThat(half).as("between(a,b,0.5) == between(a,b) byte-for-byte").isEqualTo(plain);
        assertThat(viaInterpolate).as("interpolate(a,b,0.5) == between(a,b) byte-for-byte").isEqualTo(plain);
    }

    /** Same equivalence, biased toward the adjacent-bound / prefix-successor slivers a flat generator misses. */
    @Property(tries = 5000)
    void halfFractionEquivalenceOnSuccessorShapes(@ForAll("safeKeys") byte[] a, @ForAll("scalars") int tail) {
        byte[] tailBytes = new String(Character.toChars(tail)).getBytes(StandardCharsets.UTF_8);
        byte[] b = new byte[a.length + tailBytes.length];
        System.arraycopy(a, 0, b, 0, a.length);
        System.arraycopy(tailBytes, 0, b, a.length, tailBytes.length);
        Assume.that(Arrays.compareUnsigned(a, b) < 0);

        assertThat(ByteMidpoint.between(a, b, 0.5))
                .as("between(a,b,0.5) == between(a,b) on b == a ++ [tail]")
                .isEqualTo(ByteMidpoint.between(a, b));
    }

    // ------------------------------------------------------------------------
    // (2) Monotone far-ahead placement + shared null contract.
    // ------------------------------------------------------------------------

    /**
     * For every splittable bounded range: each {@code interpolate(lo,hi,f)} pivot is valid UTF-8 and
     * strictly {@code lo <_u m <_u hi}, and the pivots are monotonically non-decreasing across an
     * ascending {@code f} grid — a larger fraction places the split at/further from {@code lo}
     * (never behind it). An overshoot to {@code >= hi} or a landing {@code <= lo} for any
     * {@code f ∈ (0,1)}, or a non-monotone regression, FAILS here.
     */
    @Property(tries = 5000)
    void interpolateIsStrictlyBetweenAndMonotoneInF(@ForAll("utf8Keys") byte[] x,
                                                     @ForAll("utf8Keys") byte[] y) {
        int cmp = Arrays.compareUnsigned(x, y);
        Assume.that(cmp != 0);
        byte[] lo = cmp < 0 ? x : y;
        byte[] hi = cmp < 0 ? y : x;

        // The null (unsplittable) verdict is fraction-independent, so gate the placement sweep on it.
        Assume.that(ByteMidpoint.between(lo, hi) != null);

        byte[] prev = null;
        for (double f : F_GRID) {
            byte[] m = StealMath.interpolate(lo, hi, f);
            assertThat(m).as("splittable range yields a non-null pivot at f=%s", f).isNotNull();
            assertThat(isValidUtf8(m)).as("pivot is valid UTF-8 at f=%s: %s", f, Arrays.toString(m)).isTrue();
            assertThat(Arrays.compareUnsigned(lo, m)).as("lo < m at f=%s (never lands at/behind lo)", f).isNegative();
            assertThat(Arrays.compareUnsigned(m, hi)).as("m < hi at f=%s (never overshoots the bound)", f).isNegative();
            if (prev != null) {
                assertThat(Arrays.compareUnsigned(prev, m))
                        .as("pivot is monotonically non-decreasing in f (prev f -> %s)", f)
                        .isLessThanOrEqualTo(0);
            }
            prev = m;
        }
    }

    /**
     * The {@code null} contract is <b>identical</b> to {@code byteMidpoint}: for every {@code f ∈ (0,1)},
     * {@code interpolate(lo,hi,f) == null} exactly when {@code byteMidpoint(lo,hi) == null} (the bounds
     * are UTF-8-adjacent / unsplittable). The fraction shifts only the synthesized scalar, never whether
     * one exists — so far-ahead placement can never turn a splittable range unsplittable, or vice versa.
     */
    @Property(tries = 5000)
    void interpolateNullContractMatchesByteMidpoint(@ForAll("utf8Keys") byte[] x,
                                                     @ForAll("utf8Keys") byte[] y) {
        int cmp = Arrays.compareUnsigned(x, y);
        Assume.that(cmp != 0);
        byte[] lo = cmp < 0 ? x : y;
        byte[] hi = cmp < 0 ? y : x;

        boolean midpointNull = ByteMidpoint.between(lo, hi) == null;
        for (double f : F_GRID) {
            boolean interpNull = StealMath.interpolate(lo, hi, f) == null;
            assertThat(interpNull)
                    .as("interpolate null iff byteMidpoint null at f=%s (unsplittable is fraction-independent)", f)
                    .isEqualTo(midpointNull);
        }
    }

    // ------------------------------------------------------------------------
    // Generators (mirror ByteMidpointPropertyTest / PROP-2).
    // ------------------------------------------------------------------------

    /** Valid-UTF-8 keys over the FULL scalar space (surrogate block excluded) — bounds may carry unsafe cps. */
    @Provide
    Arbitrary<byte[]> utf8Keys() {
        return scalars().list().ofMinSize(0).ofMaxSize(32).map(InterpolatePropertyTest::encode);
    }

    /** Valid-UTF-8 keys whose code points are all SAFE (E-free): a realistic key/cursor. */
    @Provide
    Arbitrary<byte[]> safeKeys() {
        return scalars().filter(cp -> !ScalarSafety.isExcludedScalar(cp)).list().ofMinSize(0).ofMaxSize(16)
                .map(InterpolatePropertyTest::encode);
    }

    /** Unicode scalar values: U+0000..U+10FFFF excluding the surrogate block U+D800..U+DFFF. */
    @Provide
    Arbitrary<Integer> scalars() {
        return Arbitraries.integers().between(0x0000, 0x10FFFF)
                .filter(cp -> cp < 0xD800 || cp > 0xDFFF);
    }

    private static byte[] encode(List<Integer> cps) {
        StringBuilder sb = new StringBuilder();
        for (int cp : cps) {
            sb.appendCodePoint(cp);
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static boolean isValidUtf8(byte[] bytes) {
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }
}
