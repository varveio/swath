/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ByteMidpoint.ScalarChooser;
import io.varve.swath.testkit.CodePointKeys;
import io.varve.swath.testkit.ScalarSafety;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.DoubleRange;
import org.junit.jupiter.api.Test;

/**
 * I12-safety guard — the independent guard that
 * {@link ByteMidpoint#between(byte[], byte[], double, ScalarChooser)} <b>re-validates</b> whatever a
 * {@link ScalarChooser} hands it. The alphabet chooser is read lock-free from a torn-writable
 * digest, so a hostile / stale return must NEVER breach the tiling or I12 safety contract: the pivot
 * is always valid UTF-8, carries no excluded (XML-unsafe) synthesized scalar, and is strictly
 * {@code a <_u m <_u b} — or a correct {@code null}. This guards {@code pickScalar}'s
 * {@code isSafe + strict-betweenness} re-check.
 *
 * <p>Because a <b>structurally invalid</b> chooser value (out of range, surrogate, unsafe, at/outside
 * a bound) can only be rejected, the output must be BYTE-FOR-BYTE the plain {@code null}-chooser
 * fallback — a hostile chooser is inert. Deterministic / pure — no engine, no I/O.
 */
final class ByteMidpointHostileChooserTest {

    private static final double[] F_GRID = {0.05, 0.25, 0.5, 0.75, 0.95};

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // =========================================================================
    // Structurally-invalid choosers are inert: output == the null-chooser split.
    // =========================================================================

    /**
     * Each hostile chooser returns a value that can NEVER pass {@code isSafe + strict-betweenness}
     * re-validation (out of scalar range, a surrogate, an XML-unsafe control/noncharacter, at a bound,
     * or safe-but-outside the open interval). For every such chooser and every {@code f}, the pivot
     * must equal the plain {@code between(a,b,f,null)} byte-for-byte — the chooser changed nothing.
     */
    @Property(tries = 4000)
    void hostileChooserIsByteForByteThePlainFallback(@ForAll("safeKeys") byte[] x,
                                                     @ForAll("utf8Keys") byte[] y,
                                                     @ForAll @DoubleRange(min = 0.01, max = 0.99) double f) {
        int cmp = Arrays.compareUnsigned(x, y);
        Assume.that(cmp != 0);
        byte[] a = cmp < 0 ? x : y;
        byte[] b = cmp < 0 ? y : x;

        byte[] plain = ByteMidpoint.between(a, b, f, null);
        for (ScalarChooser hostile : structurallyInvalidChoosers()) {
            byte[] m = ByteMidpoint.between(a, b, f, hostile);
            assertThat(m)
                    .as("a structurally-invalid chooser must be inert (== the null-chooser split)")
                    .isEqualTo(plain);
        }
    }

    /**
     * The whole-output safety contract holds regardless of what ANY chooser returns (including ones
     * whose return depends on the live bounds): the result is either a correct {@code null} (only when
     * the plain fallback is {@code null}) or a valid-UTF-8, excluded-scalar-free pivot strictly between
     * {@code a} and {@code b}. This is PROP-2's guarantee, re-asserted through the chooser seam.
     */
    @Property(tries = 4000)
    void anyChooserYieldsAValidPivotOrCorrectNull(@ForAll("safeKeys") byte[] a,
                                                  @ForAll("utf8Keys") byte[] b,
                                                  @ForAll @DoubleRange(min = 0.01, max = 0.99) double f) {
        // The whole-output safety claim (no excluded scalar) is the PROP-2 asymmetry: the LOWER bound
        // (cursor) is SAFE — a pivot may copy its prefix verbatim — while the upper bound is arbitrary
        // and only ever contributes a re-validated safe tail scalar. So a is safeKeys, b is utf8Keys
        // (no unsigned-swap, which would let an unsafe key become the copied-prefix side).
        Assume.that(Arrays.compareUnsigned(a, b) < 0);

        boolean plainNull = ByteMidpoint.between(a, b, f, null) == null;
        for (ScalarChooser chooser : allAdversarialChoosers()) {
            byte[] m = ByteMidpoint.between(a, b, f, chooser);
            if (m == null) {
                assertThat(plainNull)
                        .as("a chooser may only yield null when the plain split is null (unsplittable is chooser-independent)")
                        .isTrue();
            } else {
                assertThat(Arrays.compareUnsigned(a, m)).as("a < m (%s)", Arrays.toString(m)).isNegative();
                assertThat(Arrays.compareUnsigned(m, b)).as("m < b (%s)", Arrays.toString(m)).isNegative();
                assertThat(ByteMidpoint.isValidUtf8(m)).as("m is valid UTF-8 (%s)", Arrays.toString(m)).isTrue();
                assertNoExcludedScalar(m);
            }
        }
    }

    /**
     * Targeted: at the exact hex-boundary divergence position, a chooser that hands back a plausible
     * but ILLEGAL value ({@code loCp} itself, {@code hiCp} itself, a surrogate, and an unsafe control)
     * is rejected and the split degrades to the plain code-point midpoint — never an out-of-order or
     * unsafe pivot.
     */
    @Test
    void plausibleButIllegalChoicesDegradeToThePlainSplit() {
        byte[] a = b("u/9");
        byte[] b = b("u/c");
        byte[] plain = ByteMidpoint.between(a, b, 0.5, null);
        ScalarChooser[] illegal = {
                (i, lo, hi, frac) -> lo,                 // == the lower bound (loCp) — not strictly above
                (i, lo, hi, frac) -> hi,                 // == the upper bound (hiCp) — not strictly below
                (i, lo, hi, frac) -> 0xD800,             // a surrogate — not a scalar
                (i, lo, hi, frac) -> 0x00,               // C0 control — unsafe (excluded set E)
                (i, lo, hi, frac) -> hi + 1,             // safe but outside the open interval (> hiCp)
        };
        for (ScalarChooser c : illegal) {
            assertThat(ByteMidpoint.between(a, b, 0.5, c))
                    .as("an illegal chooser value is rejected → plain split")
                    .isEqualTo(plain);
        }
    }

    /**
     * A chooser that returns a value at fixed {@code f} but is otherwise inert must not perturb the
     * far-ahead grid: sweeping {@code f} with each hostile chooser still tracks the plain split exactly.
     */
    @Test
    void hostileChooserTracksThePlainSplitAcrossTheFractionGrid() {
        byte[] a = b("2022/03/05/1");
        byte[] b = b("2022/03/05/9");
        for (double f : F_GRID) {
            byte[] plain = ByteMidpoint.between(a, b, f, null);
            for (ScalarChooser hostile : structurallyInvalidChoosers()) {
                assertThat(ByteMidpoint.between(a, b, f, hostile))
                        .as("hostile chooser inert at f=%s", f).isEqualTo(plain);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Hostile choosers.
    // -------------------------------------------------------------------------

    /** Choosers whose return can NEVER pass {@code isSafe + strict-betweenness} — always rejected. */
    private static List<ScalarChooser> structurallyInvalidChoosers() {
        return List.of(
                (i, lo, hi, f) -> Integer.MIN_VALUE,                   // far-negative garbage
                (i, lo, hi, f) -> -5,                                  // negative (but != NO_SCALAR)
                (i, lo, hi, f) -> 0x110000,                            // beyond U+10FFFF
                (i, lo, hi, f) -> Integer.MAX_VALUE,                   // absurdly high
                (i, lo, hi, f) -> 0xD800,                             // low surrogate boundary
                (i, lo, hi, f) -> 0xDFFF,                             // high surrogate boundary
                (i, lo, hi, f) -> 0x00,                               // NUL (excluded set E)
                (i, lo, hi, f) -> 0x1F,                               // C0 control (E)
                (i, lo, hi, f) -> 0x7F,                               // DEL (E)
                (i, lo, hi, f) -> 0xFFFE,                             // BMP noncharacter (E)
                (i, lo, hi, f) -> Math.max(lo, 0),                    // == loCp (or 0 when loCp is ⊥) — not strictly above
                (i, lo, hi, f) -> hi,                                 // == hiCp — not strictly below
                (i, lo, hi, f) -> hi + 1000);                        // safe-ish but far above hiCp — outside interval
    }

    /** The structurally-invalid set plus {@link ScalarChooser#NO_SCALAR} (the explicit decline). */
    private static List<ScalarChooser> allAdversarialChoosers() {
        var all = new ArrayList<>(structurallyInvalidChoosers());
        all.add((i, lo, hi, f) -> ScalarChooser.NO_SCALAR);
        return all;
    }

    // -------------------------------------------------------------------------
    // Generators (mirror ByteMidpointPropertyTest / PROP-2).
    // -------------------------------------------------------------------------

    /** Valid-UTF-8 keys over the FULL scalar space (surrogate block excluded) — may carry unsafe cps. */
    @Provide
    Arbitrary<byte[]> utf8Keys() {
        return CodePointKeys.scalars().list().ofMinSize(0).ofMaxSize(24).map(CodePointKeys::encode);
    }

    /** Valid-UTF-8 keys whose code points are all SAFE (E-free): a realistic cursor {@code a}. */
    @Provide
    Arbitrary<byte[]> safeKeys() {
        return CodePointKeys.safeScalars().list().ofMinSize(0).ofMaxSize(16).map(CodePointKeys::encode);
    }

    private static void assertNoExcludedScalar(byte[] m) {
        new String(m, StandardCharsets.UTF_8).codePoints().forEach(c ->
                assertThat(ScalarSafety.isExcludedScalar(c))
                        .as("no excluded (XML-unsafe) code point U+%04X in %s", c, Arrays.toString(m))
                        .isFalse());
    }
}
