/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The independent, adversarial guard of the <b>rank-space / alphabet-aware pivot</b>
 * contract. Pure math over a constructed {@link AlphabetDigest} — no engine, no I/O,
 * deterministic. Complements the light {@code AlphabetDigestTest}.
 *
 * <p>These properties are designed to FAIL if the alphabet-aware pivot ever regresses so a
 * synthesized pivot lands back in a sparse-alphabet <b>dead zone</b> (between
 * {@code 9}=0x39 and {@code a}=0x61 for hex/UUID keys) instead of on a populated value, or if a
 * cold/dirty digest ever stops falling back byte-for-byte to the plain code-point split PROP-1's
 * tiling rests on.
 *
 * <ol>
 *   <li><b>Dead-zone avoidance (the core property):</b> over hex, sparse-decimal, and base64-ish
 *       alphabets, every {@code interpolate(lo,hi,f,digest)} pivot's divergence scalar is a POPULATED
 *       observed value, and there is an {@code f} where the plain (no-digest) pivot lands OFF the
 *       alphabet while the aware pivot stays on it — proving the digest moved the placement onto a
 *       real value.</li>
 *   <li><b>Cold fallback + equivalence:</b> a {@code null} digest, a digest with no clean signal, and
 *       an unpopulated gap each reproduce the plain code-point {@code interpolate(lo,hi,f)}
 *       byte-for-byte across a spread of {@code f} (extends the far-ahead pivot's {@code f=0.5}
 *       equivalence). A digest never yields a pivot that is invalid, out of order, or outside
 *       {@code (lo,hi)}.</li>
 * </ol>
 */
final class F1DeadZonePivotTest {

    /** A spread of far-ahead fractions across {@code (0,1)} (matches the far-ahead pivot's grid shape). */
    private static final double[] F_GRID = {0.1, 0.25, 0.5, 0.75, 0.9};

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // =========================================================================
    // (1) Dead-zone avoidance — pivots land on POPULATED values.
    // =========================================================================

    /**
     * Hex / UUID shape: the position alphabet is {@code 0-9a-f}, so the
     * plain code-point midpoint between {@code 9} and {@code c} falls in the {@code 0x3A..0x60}
     * dead zone (no hex value there). The alphabet-aware pivot must instead land on {@code a}/{@code b}.
     */
    @Test
    void hexPivotsAvoidTheDeadZone() {
        AlphabetDigest d = new AlphabetDigest(b("u/0"), b("u/f"));
        Set<Integer> alphabet = observeLeadingChars(d, "0123456789abcdef");
        // (9, c) straddles the classic hex dead zone: every grid f puts the plain midpoint off-alphabet.
        assertDeadZoneAvoided(d.snapshot(), alphabet, b("u/9"), b("u/c"));
    }

    /**
     * Sparse-decimal shape: a counter whose leading position only ever takes {@code {1,4,7}}. The plain
     * midpoint can land on an UNPOPULATED digit ({@code 2,3,5,8}); the aware pivot must stay on an
     * observed one ({@code 4}/{@code 7}).
     */
    @Test
    void sparseDecimalPivotsAvoidUnpopulatedDigits() {
        AlphabetDigest d = new AlphabetDigest(b("u/0"), b("u/9"));
        Set<Integer> alphabet = observeLeadingChars(d, "147");
        assertDeadZoneAvoided(d.snapshot(), alphabet, b("u/1"), b("u/9"));
    }

    /**
     * Base64-ish shape: the alphabet {@code A-Za-z0-9+/} has a dead gap between {@code Z}=0x5A and
     * {@code a}=0x61 ({@code [ \\ ] ^ _ `}). Bounds {@code (Y, c)} straddle it; the plain midpoint lands
     * in that gap for the middle fractions, while the aware pivot stays on {@code Z}/{@code a}/{@code b}.
     */
    @Test
    void base64PivotsAvoidTheDeadZone() {
        AlphabetDigest d = new AlphabetDigest(b("u/0"), b("u/z"));
        Set<Integer> alphabet = observeLeadingChars(d, "ABCXYZabcxyz0189+/");
        assertDeadZoneAvoided(d.snapshot(), alphabet, b("u/Y"), b("u/c"));
    }

    /**
     * Core + contrast in one sweep: for EVERY grid {@code f} the aware pivot's divergence scalar is a
     * populated (observed) value and strictly between the bounds; and there EXISTS an {@code f} at which
     * the plain (no-digest) pivot lands OFF the alphabet while the aware pivot stays on it (and the two
     * pivots differ) — the direct proof the digest changed the placement onto a real value.
     */
    private static void assertDeadZoneAvoided(AlphabetDigest.Snapshot d, Set<Integer> alphabet, byte[] lo,
            byte[] hi) {
        boolean sawDeadZoneContrast = false;
        for (double f : F_GRID) {
            byte[] aware = StealMath.interpolate(lo, hi, f, d);
            byte[] plain = StealMath.interpolate(lo, hi, f);

            assertThat(aware).as("aware pivot is non-null at f=%s", f).isNotNull();
            assertThat(isValidUtf8(aware)).as("aware pivot is valid UTF-8 at f=%s", f).isTrue();
            assertThat(Arrays.compareUnsigned(lo, aware)).as("lo < aware at f=%s", f).isNegative();
            assertThat(Arrays.compareUnsigned(aware, hi)).as("aware < hi at f=%s", f).isNegative();

            int awareScalar = divergenceScalar(lo, hi, aware);
            assertThat(alphabet)
                    .as("aware pivot divergence scalar U+%04X must be a POPULATED value at f=%s", awareScalar, f)
                    .contains(awareScalar);

            int plainScalar = divergenceScalar(lo, hi, plain);
            if (!alphabet.contains(plainScalar)) {
                sawDeadZoneContrast = true;
                assertThat(aware)
                        .as("digest moved the pivot off the dead-zone plain value at f=%s", f)
                        .isNotEqualTo(plain);
            }
        }
        assertThat(sawDeadZoneContrast)
                .as("the plain (no-digest) pivot must land in a dead zone for some f — else there is nothing to fix")
                .isTrue();
    }

    // =========================================================================
    // (3) Cold fallback + equivalence — a digest never breaks the plain split.
    // =========================================================================

    /**
     * Extends the far-ahead pivot's {@code f=0.5} equivalence to a spread of {@code f}: a {@code null} digest reproduces
     * the plain code-point {@code interpolate} byte-for-byte over hex, decimal, and deep-prefix bounds.
     */
    @Test
    void nullDigestReproducesPlainInterpolateAcrossFractions() {
        byte[][][] bounds = {
                {b("u/9"), b("u/c")},
                {b("2022/03/05/1"), b("2022/03/05/9")},
                {b("obj/0000000000000000"), b("obj/ffffffffffffffff")},
        };
        for (byte[][] pair : bounds) {
            for (double f : F_GRID) {
                assertThat(StealMath.interpolate(pair[0], pair[1], f, null))
                        .as("null digest == plain interpolate at f=%s", f)
                        .isEqualTo(StealMath.interpolate(pair[0], pair[1], f));
            }
        }
    }

    /**
     * A digest whose divergence position saw a non-ASCII (multi-byte) scalar is marked not-clean, so it
     * emits no alphabet signal there: {@code interpolate(...,f,dirty)} must fall back byte-for-byte to
     * the plain form across the whole {@code f} grid (a dirty read can only decline, never distort).
     */
    @Test
    void cleanlessDigestFallsBackByteForByteAcrossFractions() {
        AlphabetDigest dirty = new AlphabetDigest(b("u/0"), b("u/f"));
        dirty.observe(b("u/7"));                                     // a clean hex observation...
        dirty.observe("u/é".getBytes(StandardCharsets.UTF_8));      // ...then a multi-byte scalar at index 2 → not-clean
        for (double f : F_GRID) {
            assertThat(StealMath.interpolate(b("u/9"), b("u/c"), f, dirty.snapshot()))
                    .as("cleanless digest == plain interpolate at f=%s", f)
                    .isEqualTo(StealMath.interpolate(b("u/9"), b("u/c"), f));
        }
    }

    /**
     * An unpopulated gap (nothing observed strictly between the bounds) declines and falls back
     * byte-for-byte — the digest never invents a value it did not see.
     */
    @Test
    void unpopulatedGapFallsBackByteForByteAcrossFractions() {
        AlphabetDigest d = new AlphabetDigest(b("u/0"), b("u/f"));
        d.observe(b("u/9"));
        d.observe(b("u/a"));   // nothing observed strictly between '9' and 'a' (the hex dead zone)
        for (double f : F_GRID) {
            assertThat(StealMath.interpolate(b("u/9"), b("u/a"), f, d.snapshot()))
                    .as("unpopulated gap == plain interpolate at f=%s", f)
                    .isEqualTo(StealMath.interpolate(b("u/9"), b("u/a"), f));
        }
    }

    /**
     * Broad deterministic sweep: across every ordered hex sub-range of a populated {@code 0-9a-f}
     * position and the whole {@code f} grid, an aware pivot is ALWAYS valid UTF-8 and strictly
     * {@code lo <_u m <_u hi}; when the open interval contains an observed value the divergence scalar
     * is one of them, otherwise the pivot equals the plain fallback. A digest can never make a pivot
     * invalid, out of order, or outside {@code (lo,hi)}.
     */
    @Test
    void digestNeverProducesAnInvalidOrOutOfOrderPivot() {
        AlphabetDigest d = new AlphabetDigest(b("u/0"), b("u/f"));
        String hex = "0123456789abcdef";
        Set<Integer> alphabet = observeLeadingChars(d, hex);
        AlphabetDigest.Snapshot frozen = d.snapshot();   // what a policy actually consults (issue #30)

        for (int i = 0; i < hex.length(); i++) {
            for (int j = i + 1; j < hex.length(); j++) {
                byte[] lo = b("u/" + hex.charAt(i));
                byte[] hi = b("u/" + hex.charAt(j));
                Set<Integer> populatedBetween = new LinkedHashSet<>();
                for (int c : alphabet) {
                    if (c > hex.charAt(i) && c < hex.charAt(j)) {
                        populatedBetween.add(c);
                    }
                }
                for (double f : F_GRID) {
                    byte[] aware = StealMath.interpolate(lo, hi, f, frozen);
                    byte[] plain = StealMath.interpolate(lo, hi, f);
                    assertThat(aware).as("aware non-null (%s,%s) f=%s", hex.charAt(i), hex.charAt(j), f).isNotNull();
                    assertThat(isValidUtf8(aware)).as("aware valid UTF-8 (%s,%s) f=%s", hex.charAt(i), hex.charAt(j), f).isTrue();
                    assertThat(Arrays.compareUnsigned(lo, aware)).as("lo < aware (%s,%s) f=%s", hex.charAt(i), hex.charAt(j), f).isNegative();
                    assertThat(Arrays.compareUnsigned(aware, hi)).as("aware < hi (%s,%s) f=%s", hex.charAt(i), hex.charAt(j), f).isNegative();
                    if (populatedBetween.isEmpty()) {
                        assertThat(aware).as("no populated value between → plain fallback (%s,%s) f=%s",
                                hex.charAt(i), hex.charAt(j), f).isEqualTo(plain);
                    } else {
                        assertThat(populatedBetween)
                                .as("aware divergence scalar is a populated value (%s,%s) f=%s",
                                        hex.charAt(i), hex.charAt(j), f)
                                .contains(divergenceScalar(lo, hi, aware));
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers.
    // -------------------------------------------------------------------------

    /** Fold one key per leading character (at code-point index 2, past the {@code "u/"} prefix). */
    private static Set<Integer> observeLeadingChars(AlphabetDigest d, String chars) {
        Set<Integer> alphabet = new LinkedHashSet<>();
        for (char c : chars.toCharArray()) {
            d.observe(b("u/" + c));
            alphabet.add((int) c);
        }
        return alphabet;
    }

    /**
     * The single code point a shortest {@code between}-pivot invents at the bounds' first divergence
     * position — the value the alphabet-aware pivot steers onto the observed alphabet.
     */
    private static int divergenceScalar(byte[] lo, byte[] hi, byte[] m) {
        int i = commonPrefixCodePoints(decode(lo), decode(hi));
        int[] cm = decode(m);
        assertThat(i).as("pivot must reach the divergence position").isLessThan(cm.length);
        return cm[i];
    }

    private static int commonPrefixCodePoints(int[] a, int[] b) {
        int n = Math.min(a.length, b.length);
        int i = 0;
        while (i < n && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    private static int[] decode(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8).codePoints().toArray();
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
