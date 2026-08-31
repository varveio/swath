/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import static io.varve.swath.testkit.CodePointKeys.b;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.testkit.ScalarSafety;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * PROP-2 enumerated edge cases (algorithms.md §3.1): {@code a} empty, {@code a}
 * a proper prefix of {@code b}, {@code b == a++[U+0000]} (→ null), scalar-adjacent
 * code points, code points straddling the surrogate gap (U+D7FF ↔ U+E000),
 * supplementary code points (≥ U+10000), 1024-byte keys — and that every non-null
 * output is valid UTF-8, never a surrogate, and carries no excluded (unsafe) code
 * point {@code E = U+0000..U+001F ∪ U+007F ∪ U+0080..U+009F ∪ U+FDD0..U+FDEF ∪
 * {every plane's trailing xFFFE/xFFFF}} (the C1 block and the
 * supplementary noncharacters were added to {@code E}). Bounds themselves may carry unsafe
 * code points (the asymmetry); only the synthesized pivot is safe.
 */
class ByteMidpointEdgeCasesTest {

    /** UTF-8 bytes of a single Unicode scalar value (avoids fragile source-file literals). */
    private static byte[] cp(int codePoint) {
        return new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[] x, byte[] y) {
        byte[] out = Arrays.copyOf(x, x.length + y.length);
        System.arraycopy(y, 0, out, x.length, y.length);
        return out;
    }

    private static void assertStrictlyBetween(byte[] a, byte[] bb, byte[] m) {
        assertThat(Arrays.compareUnsigned(a, m)).as("a < m").isNegative();
        assertThat(Arrays.compareUnsigned(m, bb)).as("m < b").isNegative();
        assertValidUtf8(m);
    }

    /** @see ScalarSafety#isExcludedScalar the shared excluded-set {@code E} predicate. */
    private static boolean isExcluded(int c) {
        return ScalarSafety.isExcludedScalar(c);
    }

    /**
     * No code point of {@code m} is excluded. Use only when the BOUNDS are themselves all-safe — a
     * pivot copies the bounds' common prefix verbatim (the asymmetry), so an unsafe bound prefix
     * would legitimately appear in {@code m}; the safety guarantee is on the SYNTHESIZED scalar.
     */
    private static void assertNoExcluded(byte[] m) {
        new String(m, StandardCharsets.UTF_8).codePoints().forEach(c ->
                assertThat(isExcluded(c)).as("no excluded (unsafe) code point U+%04X", c).isFalse());
    }

    private static void assertValidUtf8(byte[] bytes) {
        assertThat(KeyBytes.isValidUtf8(bytes)).as("not valid UTF-8: %s", Arrays.toString(bytes)).isTrue();
    }

    private static void assertNoSurrogate(byte[] m) {
        new String(m, StandardCharsets.UTF_8).codePoints().forEach(c ->
                assertThat(c < 0xD800 || c > 0xDFFF).as("no surrogate code point U+%04X", c).isTrue());
    }

    @Test
    void emptyLowerBound() {
        assertThat(ByteMidpoint.between(b(), b(0x00))).isNull();             // b == [] ++ [U+0000]
        // [] vs [U+0001]: the only key strictly between is [U+0000], which is unsafe ⇒ null.
        assertThat(ByteMidpoint.between(b(), b(0x01))).isNull();
        // [] vs [U+0041] ('A'): a safe scalar (U+0020) lies below 'A' ⇒ pivot [U+0020].
        byte[] m = ByteMidpoint.between(b(), b(0x41));
        assertThat(m).containsExactly(0x20);
        assertStrictlyBetween(b(), b(0x41), m);
        assertNoExcluded(m);
    }

    @Test
    void immediateSuccessorReturnsNull() {
        assertThat(ByteMidpoint.between(b(0x41), b(0x41, 0x00))).isNull();   // "A", "A\0"
        // A multibyte key (U+00E9, bytes C3 A9) and its code-point successor (append U+0000).
        assertThat(ByteMidpoint.between(cp(0x00E9), b(0xC3, 0xA9, 0x00))).isNull();
    }

    @Test
    void controlSliverSuccessorsReturnNull() {
        // b == a ++ [unsafe control] has no SAFE key strictly between ⇒ null (broadened contract).
        assertThat(ByteMidpoint.between(b(0x41), b(0x41, 0x01))).isNull();   // "A" vs "A"+U+0001
        assertThat(ByteMidpoint.between(b(0x41), b(0x41, 0x1F))).isNull();   // "A" vs "A"+U+001F
        // b == a ++ [U+0020] exactly (b NOT longer): a·U+0020 == b, nothing safe strictly between.
        assertThat(ByteMidpoint.between(b(0x41), b(0x41, 0x20))).isNull();
    }

    @Test
    void minSafeBoundaryRecoveredWhenBLonger() {
        // b == a ++ [U+0020] ++ [more]: no safe scalar < U+0020, but U+0020 is itself safe and b is
        // strictly longer than a·U+0020, so the pivot recovers a·U+0020 (< b).
        byte[] m = ByteMidpoint.between(b(0x41), b(0x41, 0x20, 0x00));
        assertThat(m).containsExactly(0x41, 0x20);
        assertStrictlyBetween(b(0x41), b(0x41, 0x20, 0x00), m);
        assertNoExcluded(m);
    }

    @Test
    void properPrefixWithLongerSuffix() {
        byte[] m = ByteMidpoint.between(b(0x41), b(0x41, 0x41));             // "A" vs "AA"
        assertThat(m).containsExactly(0x41, 0x20);                           // "A" + U+0020 (safe)
        assertStrictlyBetween(b(0x41), b(0x41, 0x41), m);
        assertNoExcluded(m);
    }

    @Test
    void adjacentScalarsAppendMinSafe() {
        // 'A'(U+0041) and 'B'(U+0042) are scalar-adjacent: no safe scalar strictly between,
        // so the pivot extends 'A' with U+0020 (MIN_SAFE) — NOT the old unsafe U+0000.
        byte[] m = ByteMidpoint.between(b(0x41), b(0x42));
        assertThat(m).containsExactly(0x41, 0x20);
        assertStrictlyBetween(b(0x41), b(0x42), m);
        assertNoExcluded(m);
    }

    @Test
    void boundsStraddlingC0BlockAppendMinSafe() {
        // a ends just inside the C0 block (U+0007) and b just above it (U+0009): no safe scalar
        // strictly between two controls ⇒ extend a with MIN_SAFE (U+0020), a real split > a, < b.
        byte[] m = ByteMidpoint.between(cp(0x0007), cp(0x0009));
        assertStrictlyBetween(cp(0x0007), cp(0x0009), m);
        assertThat(m[m.length - 1]).isEqualTo((byte) 0x20);                 // synthesized MIN_SAFE tail
    }

    @Test
    void gappedScalarsUseMidpoint() {
        byte[] m = ByteMidpoint.between(b(0x41), b(0x45));                    // 'A'..'E', gap ≥ 2
        assertThat(m).containsExactly(0x43);                                 // 'C' = midpoint scalar
        assertStrictlyBetween(b(0x41), b(0x45), m);
        assertNoExcluded(m);
    }

    @Test
    void multibyteGapUsesValidScalarMidpoint() {
        byte[] a = cp(0x4E2D);      // CJK U+4E2D
        byte[] bb = cp(0x6587);     // CJK U+6587
        byte[] m = ByteMidpoint.between(a, bb);
        assertStrictlyBetween(a, bb, m);                                     // valid UTF-8 enforced inside
        assertNoExcluded(m);
    }

    @Test
    void straddlingSurrogateGapNeverEmitsSurrogate() {
        // U+D7FF (last scalar before the gap) and U+E000 (first after) are scalar-adjacent
        // in the contiguous index space ⇒ append U+0000; never a lone/embedded surrogate.
        byte[] a = cp(0xD7FF);
        byte[] bb = cp(0xE000);
        byte[] m = ByteMidpoint.between(a, bb);
        assertThat(m).isEqualTo(concat(cp(0xD7FF), b(0x20)));   // append MIN_SAFE, not the old U+0000
        assertStrictlyBetween(a, bb, m);
        assertNoSurrogate(m);
        assertNoExcluded(m);
    }

    @Test
    void surrogateIndexBoundaryMapsToScalar() {
        // Force the scalar index to land at 0xD800 (cp(0xD800) == U+E000, a real scalar):
        // lo idx 0xD700 (U+D700), hi idx 0xD900 (U+E100) ⇒ midpoint index 0xD800.
        byte[] a = cp(0xD700);
        byte[] bb = cp(0xE100);
        byte[] m = ByteMidpoint.between(a, bb);
        assertThat(m).isEqualTo(cp(0xE000));     // the real scalar at index 0xD800, not a surrogate
        assertStrictlyBetween(a, bb, m);
        assertNoSurrogate(m);
        assertNoExcluded(m);
    }

    @Test
    void supplementaryPlaneMidpoint() {
        byte[] a = cp(0x1F680);     // 🚀
        byte[] bb = cp(0x1F6F0);    // 🛰
        byte[] m = ByteMidpoint.between(a, bb);
        assertStrictlyBetween(a, bb, m);
        assertNoExcluded(m);
    }

    @Test
    void boundsStraddlingBmpNoncharactersNeverEmitThem() {
        // Bounds straddle U+FFFD ↔ U+10001: the only scalars strictly between are U+FFFE, U+FFFF
        // (noncharacters) and U+10000. The natural midpoint lands on a noncharacter, so the safe
        // outward scan must skip U+FFFE/U+FFFF and emit U+10000 instead.
        byte[] a = cp(0xFFFD);
        byte[] bb = cp(0x10001);
        byte[] m = ByteMidpoint.between(a, bb);
        assertThat(m).isEqualTo(cp(0x10000));
        assertStrictlyBetween(a, bb, m);
        assertNoSurrogate(m);
        assertNoExcluded(m);   // must never be U+FFFE or U+FFFF
    }

    /**
     * Regression guard: before the fix, {@code isSafe} kept the C1 control block
     * (U+0080..U+009F) IN the safe set, so a natural midpoint landing there was returned as-is —
     * a synthesized pivot that could 400 as a real S3 {@code start-after} cursor. {@code a =
     * U+0060, b = U+00C0} puts the natural midpoint at U+0090 (deep inside C1); the tightened
     * outward scan must skip the WHOLE C1 block and land on the first safe scalar past it
     * (U+00A0), not fall back into the earlier DEL/C0 territory.
     */
    @Test
    void midpointLandingInC1BlockSkipsToNextSafeScalar() {
        byte[] a = cp(0x0060);
        byte[] bb = cp(0x00C0);
        byte[] m = ByteMidpoint.between(a, bb);
        assertThat(m).isEqualTo(cp(0x00A0));
        assertStrictlyBetween(a, bb, m);
        assertNoExcluded(m);
        assertNeverC1OrNoncharacter(m);
    }

    /**
     * Regression guard, the exhausted-interval case: {@code a = U+007E, b = U+00A0} —
     * every scalar strictly between (U+007F..U+009F: DEL + the whole C1 block) is now excluded,
     * so {@code safeBetween} must report {@link}-documented exhaustion and {@code between} must
     * fall back to {@code a ++ MIN_SAFE} (progress is preserved: it is never left {@code null}
     * just because the natural gap is now fully unsafe).
     */
    @Test
    void midpointFullyInsideC1BlockFallsBackToMinSafeAppend() {
        byte[] a = cp(0x007E);
        byte[] bb = cp(0x00A0);
        byte[] m = ByteMidpoint.between(a, bb);
        assertThat(m).isEqualTo(concat(a, b(0x20)));   // a ++ MIN_SAFE — every direct scalar is excluded
        assertStrictlyBetween(a, bb, m);
        assertNoExcluded(m);
        assertNeverC1OrNoncharacter(m);
    }

    /**
     * Regression guard: before the fix, {@code isSafe} only excluded the BMP
     * noncharacters (U+FFFE/U+FFFF), so a supplementary-plane pair like U+1FFFE/U+1FFFF was
     * returned as a synthesized pivot. {@code a = U+1FFFD, b = U+20001}: the only scalars
     * strictly between are U+1FFFE, U+1FFFF (plane-1 noncharacters) and U+20000. The tightened
     * scan must skip both noncharacters and land on U+20000.
     */
    @Test
    void midpointLandingOnSupplementaryNoncharacterSkipsToNextSafeScalar() {
        byte[] a = cp(0x1FFFD);
        byte[] bb = cp(0x20001);
        byte[] m = ByteMidpoint.between(a, bb);
        assertThat(m).isEqualTo(cp(0x20000));
        assertStrictlyBetween(a, bb, m);
        assertNoExcluded(m);
        assertNeverC1OrNoncharacter(m);
    }

    /**
     * {@link ByteMidpoint#forwardReflect} companion to the two {@code between} guards above: a
     * reflection target that lands on a supplementary noncharacter must snap past it. {@code lo =
     * U+1FFFC, c = U+1FFFD} reflects to {@code 2·idx(c) − idx(lo) = U+1FFFE} (a noncharacter,
     * excluded); the snap must land on the next safe scalar (U+20000, skipping U+1FFFF too).
     */
    @Test
    void forwardReflectLandingOnSupplementaryNoncharacterSnapsPastIt() {
        byte[] m = ByteMidpoint.forwardReflect(cp(0x1FFFC), cp(0x1FFFD));
        assertThat(m).isEqualTo(cp(0x20000));
        assertThat(Arrays.compareUnsigned(cp(0x1FFFD), m)).as("c < m").isNegative();
        assertNoExcluded(m);
        assertNeverC1OrNoncharacter(m);
    }

    /**
     * Determinism: repeated calls with the same inputs, including ones whose
     * natural midpoint now falls in a newly-excluded region, must return byte-identical pivots.
     */
    @Test
    void pivotSynthesisIsDeterministicAcrossNewlyExcludedRegions() {
        byte[] a = cp(0x0060);
        byte[] bb = cp(0x00C0);
        byte[] first = ByteMidpoint.between(a, bb);
        byte[] second = ByteMidpoint.between(a, bb);
        assertThat(second).isEqualTo(first);
    }

    /**
     * No synthesized pivot may ever carry a C1 control (U+0080..U+009F) or a noncharacter
     * (standalone BMP block U+FDD0..U+FDEF, or any plane's trailing xFFFE/xFFFF pair) — the two
     * ranges closed. Independent of {@code isExcluded}/{@code isSafe} so it actually catches
     * a regression rather than mirroring the implementation.
     */
    private static void assertNeverC1OrNoncharacter(byte[] m) {
        new String(m, StandardCharsets.UTF_8).codePoints().forEach(c -> {
            assertThat(c >= 0x80 && c <= 0x9F).as("no C1 control U+%04X", c).isFalse();
            assertThat(c >= 0xFDD0 && c <= 0xFDEF).as("no standalone BMP noncharacter U+%04X", c).isFalse();
            assertThat((c & 0xFFFE) == 0xFFFE).as("no trailing-pair noncharacter U+%04X", c).isFalse();
        });
    }

    @Test
    void thousandTwentyFourByteKeys() {
        // a = 1024 × U+0000; b differs only at the last code point, with a gap wide enough that a
        // SAFE midpoint scalar exists ([U+0000, U+0040) ⇒ U+0020). The unsafe U+0000 prefix bytes
        // are copied from the bounds verbatim (the asymmetry); only the synthesized tail is safe.
        byte[] a = new byte[1024];                 // 1024 × U+0000
        byte[] bb = new byte[1024];
        bb[1023] = 0x40;                            // differ only at the last code point
        byte[] m = ByteMidpoint.between(a, bb);
        assertThat(m).hasSize(1024);
        assertThat(m[1023]).isEqualTo((byte) 0x20); // safe midpoint scalar of [U+0000, U+0040)
        assertStrictlyBetween(a, bb, m);
    }

    @Test
    void thousandTwentyFourByteKeysWithControlSliverAreUnsplittable() {
        // a = 1024 × U+0000, b differs at the last by 2 (U+0002): the only key strictly between is
        // 1023 × U+0000 + U+0001 — an unsafe control — so there is no SAFE pivot ⇒ null.
        byte[] a = new byte[1024];
        byte[] bb = new byte[1024];
        bb[1023] = 0x02;
        assertThat(ByteMidpoint.between(a, bb)).isNull();
    }

    @Test
    void adjacentKeysAtMaxLengthAreUnsplittable() {
        // a, b are 1024 × U+0000 differing only at the last by 1; the only key strictly
        // between is 1025 bytes (> S3 max) ⇒ null (unsplittable, balance-only).
        byte[] a = new byte[1024];
        byte[] bb = new byte[1024];
        bb[1023] = 0x01;
        assertThat(ByteMidpoint.between(a, bb)).isNull();
    }

    @Test
    void adjacentKeysBelowMaxLengthStillSplitToMaxLength() {
        // a is 1023 × U+0000; appending U+0000 yields a valid 1024-byte pivot.
        byte[] a = new byte[1023];
        byte[] bb = new byte[1023];
        bb[1022] = 0x01;
        byte[] m = ByteMidpoint.between(a, bb);
        assertThat(m).isNotNull();
        assertThat(m).hasSize(1024);
        assertStrictlyBetween(a, bb, m);
    }

    @Test
    void maxLengthAdjacentLeadingScalarsStillSplit() {
        // Regression fixture: a = "A" + 1023×U+0000 (exactly 1024 bytes), b = "B".
        // 'A','B' are scalar-adjacent at index 0, so the naïve a·U+0000 would be 1025 bytes
        // (> max) — yet a valid 1024-byte pivot exists by bumping a's last scalar and dropping
        // nothing: "A" + 1022×U+0000 + U+0001. Returning null here is a PROP-2 false negative.
        byte[] a = new byte[1024];
        a[0] = 0x41;                          // 'A', then 1023 × U+0000
        byte[] bb = b(0x42);                  // "B"
        byte[] m = ByteMidpoint.between(a, bb);
        assertThat(m).as("a valid 1024-byte pivot exists; must not be null").isNotNull();
        assertThat(m).hasSize(1024);
        byte[] expected = new byte[1024];
        expected[0] = 0x41;
        expected[1023] = 0x20;               // "A" + 1022×U+0000 + U+0020 (next SAFE scalar bump)
        assertThat(m).isEqualTo(expected);
        assertStrictlyBetween(a, bb, m);
    }

    @Test
    void capFallbackSkipsUnbumpableMaxScalarTail() {
        // a = 'A' + 1019×U+0000 + U+10FFFF (1 + 1019 + 4 = 1024 bytes); b = "B".
        // Adjacent leading scalars ⇒ a·U+0000 overflows. The right-most scalar (U+10FFFF) is
        // unbumpable, so the fallback must skip it and bump the previous U+0000 instead.
        byte[] a = concat(concat(b(0x41), new byte[1019]), cp(0x10FFFF));
        assertThat(a).hasSize(1024);
        byte[] bb = b(0x42);
        byte[] m = ByteMidpoint.between(a, bb);
        assertThat(m).as("bumpable tail before the max scalar yields a valid pivot").isNotNull();
        assertStrictlyBetween(a, bb, m);
        assertThat(m.length).isLessThanOrEqualTo(1024);
        assertNoSurrogate(m);
    }

    @Test
    void preconditionRejectsNonIncreasingBounds() {
        assertThatThrownBy(() -> ByteMidpoint.between(b(0x45), b(0x45)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ByteMidpoint.between(b(0x46), b(0x45)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void preconditionRejectsInvalidUtf8Bounds() {
        // The contract requires valid-UTF-8 bounds (every real key is, every pivot is).
        // 0xFF never appears in valid UTF-8 ⇒ fail loud rather than silently corrupt.
        assertThatThrownBy(() -> ByteMidpoint.between(b(0x41), b(0x41, 0xFF)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ByteMidpoint.between(b(0x80), b(0x81)))   // lone continuation bytes
                .isInstanceOf(IllegalArgumentException.class);
    }

    // -------------------------------------------------------------------------
    // forwardReflect — the open-frontier pivot must also be safe (the U+000F bug).
    // -------------------------------------------------------------------------

    @Test
    void forwardReflectLowCursorCharNeverReflectsToControl() {
        // The production U+000F regression: c = [U+0007] (a low control), lo = ⊥. The old reflection
        // 2·idx(c) − idx(lo) = 2·7 − (−1) = 15 emitted U+000F (XML-illegal). The safe snap must lift
        // the synthesized scalar to the next safe scalar (U+0020) while staying > c.
        byte[] c = cp(0x0007);
        byte[] m = ByteMidpoint.forwardReflect(null, c);
        assertThat(m).containsExactly(0x20);                         // U+0020, not U+000F
        assertThat(Arrays.compareUnsigned(c, m)).as("c < m").isNegative();
        assertNoExcluded(m);
    }

    @Test
    void forwardReflectReflectionLandingOnDelSnapsPastIt() {
        // c[i] = U+007E, lo[i] = U+007D ⇒ reflected scalar 2·0x7E − 0x7D = 0x7F (DEL, unsafe).
        // The safe snap must skip DEL AND the C1 control block (U+0080..U+009F, also excluded)
        // to the next safe scalar (U+00A0), still > c.
        byte[] m = ByteMidpoint.forwardReflect(cp(0x007D), cp(0x007E));
        assertThat(m).isEqualTo(cp(0x00A0));
        assertThat(Arrays.compareUnsigned(cp(0x007E), m)).as("c < m").isNegative();
        assertNoExcluded(m);
    }

    @Test
    void forwardReflectMaxScalarRunAppendsSafeHighScalar() {
        // c[i] is already U+10FFFF — itself a real noncharacter (the plane-16 trailing pair) and
        // hence "unsafe", but it is c's OWN copied code point, not synthesized, so the asymmetry means
        // it's legitimately present in m. Only the APPENDED scalar (the one forwardReflect invents) must
        // be safe (never a raw unverified mid-scalar that could be a noncharacter) — cannot bump c[i] in
        // place, so forwardReflect appends a SAFE high scalar so m > c while sharing all of c.
        byte[] c = cp(0x10FFFF);
        byte[] m = ByteMidpoint.forwardReflect(c, c);   // lo == c ⇒ no in-place bump ⇒ append
        assertThat(Arrays.compareUnsigned(c, m)).as("c < m").isNegative();
        assertValidUtf8(m);
        assertThat(new String(m, StandardCharsets.UTF_8)).startsWith(new String(c, StandardCharsets.UTF_8));
        int[] appended = new String(m, StandardCharsets.UTF_8).codePoints().toArray();
        assertThat(isExcluded(appended[appended.length - 1]))
                .as("the appended (synthesized) scalar must be safe").isFalse();
    }

    @Test
    void forwardReflectMaxLengthMaxScalarTailNeverExceedsS3KeyLimit() {
        // Regression guard: the old append branch returned c ++ SAFE_HIGH unconditionally. For a
        // 1024-byte cursor ending in U+10FFFF, that produced a 1027-byte start-after boundary.
        byte[] c = concat("a".repeat(ByteMidpoint.MAX_KEY_LEN - cp(0x10FFFF).length)
                .getBytes(StandardCharsets.UTF_8), cp(0x10FFFF));
        assertThat(c).hasSize(ByteMidpoint.MAX_KEY_LEN);

        byte[] m = ByteMidpoint.forwardReflect(c, c);   // lo == c forces the append/fallback path

        if (m != null) {
            assertThat(m.length).isLessThanOrEqualTo(ByteMidpoint.MAX_KEY_LEN);
            assertThat(Arrays.compareUnsigned(c, m)).as("c < m").isNegative();
            assertValidUtf8(m);
            assertNoExcluded(m);
        }
    }
}
