/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static io.varve.swath.engine.StealMath.*;
import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.testkit.ScalarSafety;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Pure-math unit tests for {@link StealMath} (algorithms.md §3.1 and §3.2).
 * No concurrency, no I/O, no mocks — just byte-exact properties of the math.
 *
 * <p>DOES NOT cover PROP/RES/CONC adversarial tests (those are covered separately).
 */
class StealMathTest {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static byte[] b(int... v) {
        byte[] out = new byte[v.length];
        for (int i = 0; i < v.length; i++) out[i] = (byte) v[i];
        return out;
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[] x, byte[] y) {
        byte[] out = Arrays.copyOf(x, x.length + y.length);
        System.arraycopy(y, 0, out, x.length, y.length);
        return out;
    }

    private static boolean compareUnsignedLt(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b) < 0;
    }

    // -------------------------------------------------------------------------
    // prefixCeil
    // -------------------------------------------------------------------------

    @Test
    void prefixCeil_simpleAscii() {
        // "abc" → increment 'c' to 'd'
        assertThat(prefixCeil(utf8("abc"))).isEqualTo(utf8("abd"));
    }

    @Test
    void prefixCeil_singleByte() {
        assertThat(prefixCeil(b(0x41))).isEqualTo(b(0x42));
        assertThat(prefixCeil(b(0x00))).isEqualTo(b(0x01));
    }

    @Test
    void prefixCeil_trailingFF_dropsThemAndIncrements() {
        // [0x41, 0xFF] → drop trailing 0xFF, increment 0x41 → [0x42]
        assertThat(prefixCeil(b(0x41, 0xFF))).isEqualTo(b(0x42));
        // [0x41, 0xFF, 0xFF] → [0x42]
        assertThat(prefixCeil(b(0x41, 0xFF, 0xFF))).isEqualTo(b(0x42));
    }

    @Test
    void prefixCeil_allFF_returnsNull() {
        assertThat(prefixCeil(b(0xFF))).isNull();
        assertThat(prefixCeil(b(0xFF, 0xFF, 0xFF))).isNull();
    }

    @Test
    void prefixCeil_nullAndEmpty_returnNull() {
        assertThat(prefixCeil(null)).isNull();
        assertThat(prefixCeil(new byte[0])).isNull();
    }

    @Test
    void prefixCeil_highByteMiddle() {
        // [0x80, 0xFF] → drop trailing 0xFF → [0x80] → increment → [0x81]
        assertThat(prefixCeil(b(0x80, 0xFF))).isEqualTo(b(0x81));
    }

    @Test
    void prefixCeil_resultIsStrictlyGreaterThanAllKeysWithPrefix() {
        // Any key starting with the prefix is strictly less than the ceil.
        byte[] prefix = utf8("data/");
        byte[] ceil = prefixCeil(prefix);
        assertThat(ceil).isNotNull();
        // A key with the prefix is < ceil
        assertThat(compareUnsignedLt(utf8("data/a"), ceil)).isTrue();
        assertThat(compareUnsignedLt(utf8("data/zzz"), ceil)).isTrue();
        // ceil itself is NOT a key starting with the prefix
        assertThat(Arrays.compareUnsigned(ceil, prefix)).isPositive();
    }

    // -------------------------------------------------------------------------
    // fracIn / spanIn (window-relative)
    // -------------------------------------------------------------------------

    @Test
    void fracIn_keyShortThanCommonPrefix_returnsZero() {
        // lo and hi share a 4-byte prefix; a key of length 3 is "before" the window.
        byte[] lo = b(0xAA, 0xBB, 0xCC, 0xDD, 0x00);
        byte[] hi = b(0xAA, 0xBB, 0xCC, 0xDD, 0xFF);
        byte[] key = b(0xAA, 0xBB, 0xCC);   // length 3 < d=4
        assertThat(fracIn(key, lo, hi)).isEqualTo(0.0);
    }

    @Test
    void fracIn_nullKey_returnsZero() {
        byte[] lo = b(0x00);
        byte[] hi = b(0xFF);
        assertThat(fracIn(null, lo, hi)).isEqualTo(0.0);
    }

    @Test
    void fracIn_monotoneAcrossWindow() {
        // Within window [lo, hi], fracIn must be monotonically non-decreasing.
        byte[] lo = utf8("data/");
        byte[] hi = utf8("data0");   // prefixCeil("data/") = "data0"
        // d = commonPrefixLen("data/", "data0") = 4 (bytes 'd','a','t','a' are shared)
        // byte 4: '/' (0x2F) vs '0' (0x30) — differ

        String[] keys = {"data/", "data/a", "data/m", "data/z", "data/zz"};
        double prev = -1.0;
        for (String k : keys) {
            double f = fracIn(utf8(k), lo, hi);
            assertThat(f).isGreaterThanOrEqualTo(prev);
            prev = f;
        }
    }

    @Test
    void fracIn_windowRelative_distinguishesDensePrefix() {
        // Window [lo, hi] with a 6-byte shared prefix "prefix".
        // fracIn should span the full [0, 1) range over the 7th byte ('/' vs '0').
        byte[] lo = utf8("prefix/");
        byte[] hi = utf8("prefix0");   // 0x30 = '0', 0x2F = '/'
        // d = 6 (bytes 'p','r','e','f','i','x'), then '/' vs '0'

        // A key at the lo end
        double flo = fracIn(utf8("prefix/"), lo, hi);
        // A key at the hi end
        double fhi = fracIn(utf8("prefix0"), lo, hi);
        // Some key in between
        double fmid = fracIn(concat(utf8("prefix"), b(0x2F, 0x80)), lo, hi);  // "prefix/" + 0x80

        assertThat(flo).isLessThan(fmid);
        assertThat(fmid).isLessThan(fhi);
        // The span of the full window should be meaningful (close to 1/256 ≈ 0.00390625)
        assertThat(fhi - flo).isGreaterThan(1.0 / 512.0);
    }

    @Test
    void spanIn_loToHi_isPositive() {
        byte[] lo = b(0x00, 0x00, 0x00);
        byte[] hi = b(0x00, 0x00, 0xFF);
        // d = 2 (shared 0x00, 0x00), distinguishing byte: 0x00 vs 0xFF
        double span = spanIn(lo, hi, lo, hi);
        assertThat(span).isGreaterThan(0.0);
        assertThat(span).isLessThanOrEqualTo(1.0);
    }

    @Test
    void spanIn_samePoint_isZero() {
        byte[] lo = b(0x10);
        byte[] hi = b(0xF0);
        byte[] mid = b(0x80);
        assertThat(spanIn(mid, mid, lo, hi)).isEqualTo(0.0);
    }

    @Test
    void spanIn_clampedToNonNegative() {
        // spanIn(x, y, lo, hi) must never be negative.
        byte[] lo = b(0x00);
        byte[] hi = b(0xFF);
        byte[] a  = b(0x80);
        byte[] bb = b(0x40);
        // fracIn(bb) < fracIn(a) — but spanIn must clamp to 0.
        assertThat(spanIn(a, bb, lo, hi)).isEqualTo(0.0);
    }

    // -------------------------------------------------------------------------
    // estRemaining (victim scoring)
    // -------------------------------------------------------------------------

    @Test
    void estRemaining_nullHi_isPositiveInfinity() {
        assertThat(estRemaining(b(0x41), b(0x00), null, 100)).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    void estRemaining_cursorAtLo_ranksByWidth() {
        // consumed = spanIn(lo, lo, lo, hi) = 0 → return remaining span only.
        byte[] lo = b(0x00, 0x00);
        byte[] hi = b(0x00, 0xFF);
        // cursor == lo → consumed = 0
        double est = estRemaining(lo, lo, hi, 0);
        double expected = spanIn(lo, hi, lo, hi);
        assertThat(est).isEqualTo(expected);
    }

    @Test
    void estRemaining_densityEstimate() {
        // Window [0x00, 0xFF] at byte 0 (no shared prefix), cursor at 0x40.
        // consumed ≈ 0x40/256 = 0.25 span, remaining ≈ (0xFF-0x40)/256 = 0.748...
        // keysEmitted = 1000 → density = 1000/0.25 = 4000, est = 4000 * 0.748 ≈ 2992
        byte[] lo     = b(0x00);
        byte[] hi     = b(0xFF);
        byte[] cursor = b(0x40);
        long keys = 1000L;
        double est = estRemaining(cursor, lo, hi, keys);
        assertThat(est).isGreaterThan(1000.0);   // more than what we've seen so far
        // It must also be finite
        assertThat(est).isLessThan(Double.POSITIVE_INFINITY);
    }

    @Test
    void estRemaining_frontierScoresHigherThanBounded() {
        byte[] lo     = b(0x00);
        byte[] hi     = b(0xFF);
        byte[] cursor = b(0x80);
        // frontier (hi=null) must score above any bounded worker
        double bounded  = estRemaining(cursor, lo, hi, 500);
        double frontier = estRemaining(cursor, lo, null, 500);
        assertThat(frontier).isGreaterThan(bounded);
    }

    @Test
    void estRemaining_workerNearlyDone_scoresLow() {
        byte[] lo     = b(0x00);
        byte[] hi     = b(0xFF);
        byte[] cursor = b(0xFE);   // almost done
        double est = estRemaining(cursor, lo, hi, 100);
        // remaining span is very small
        assertThat(est).isLessThan(10.0);
    }

    // -------------------------------------------------------------------------
    // extrapolate (open frontier pivot) — UTF-8-safe forward reflection of (lo, c].
    // The pivot MUST always be valid UTF-8 (it is re-fed to byteMidpoint and stored as a
    // range bound); a raw non-UTF-8 pivot crashed the engine on deep low-ASCII prefixes.
    // It must also SHARE c's prefix so the thief's retry-nearer-cursor can refine it.
    // -------------------------------------------------------------------------

    @Test
    void extrapolate_isValidUtf8AndStrictlyBetween() {
        byte[] lo   = utf8("data/file0400");
        byte[] c    = utf8("data/file0500");
        byte[] ceil = prefixCeil(utf8("data/"));   // "data0"
        byte[] m    = extrapolate(lo, c, ceil);
        assertThat(m).isNotNull();
        assertThat(ByteMidpoint.isValidUtf8(m)).as("pivot must be valid UTF-8").isTrue();
        assertThat(Arrays.compareUnsigned(m, c)).isPositive();      // c < m
        assertThat(Arrays.compareUnsigned(m, ceil)).isNegative();   // m < ceil
    }

    @Test
    void extrapolate_sharesCursorPrefix_soRetryCanRefine() {
        // With lo sharing c's prefix (the cascade case after the first split), the reflected
        // pivot stays in c's namespace — it shares c's leading bytes, never jumps to a high byte.
        byte[] lo = utf8("flat/00000100");
        byte[] c  = utf8("flat/00000123");
        byte[] m  = extrapolate(lo, c, null);
        assertThat(m).isNotNull();
        assertThat(ByteMidpoint.isValidUtf8(m)).isTrue();
        assertThat(Arrays.compareUnsigned(m, c)).isPositive();
        // m shares the dense common prefix "flat/0000" rather than galloping out of it.
        assertThat(new String(m, StandardCharsets.UTF_8)).startsWith("flat/0000");
    }

    @Test
    void extrapolate_deepLowAsciiPrefix_yieldsValidUtf8_notRawBytes() {
        // A deep low-ASCII common prefix must not make extrapolate return raw unfrac bytes
        // (e.g. 0xC8 ...) that are not valid UTF-8.
        byte[] lo   = utf8("data/2024-01-01/part-00000");
        byte[] c    = utf8("data/2024-01-01/part-00100");
        byte[] ceil = prefixCeil(utf8("data/"));
        byte[] m    = extrapolate(lo, c, ceil);
        assertThat(m).isNotNull();
        assertThat(ByteMidpoint.isValidUtf8(m)).isTrue();
        assertThat(Arrays.compareUnsigned(m, c)).isPositive();
        assertThat(Arrays.compareUnsigned(m, ceil)).isNegative();
    }

    @Test
    void extrapolate_rootFrontier_loIsBottom_staysValidAndForward() {
        // The first steal: lo = ⊥ (null). c's first code point already differs from lo, so the
        // reflection bumps c's LAST code point instead of jumping out of c's namespace.
        byte[] c    = utf8("flat/00000006");
        byte[] ceil = prefixCeil(utf8("flat/"));   // "flat0"
        byte[] m    = extrapolate(null, c, ceil);
        assertThat(m).isNotNull();
        assertThat(ByteMidpoint.isValidUtf8(m)).isTrue();
        assertThat(Arrays.compareUnsigned(m, c)).isPositive();
        assertThat(Arrays.compareUnsigned(m, ceil)).isNegative();
        assertThat(new String(m, StandardCharsets.UTF_8)).startsWith("flat/");
    }

    @Test
    void extrapolate_cursorAtOrAfterCeil_returnsNull() {
        // No forward room: c >= ceil.
        assertThat(extrapolate(null, utf8("z"), utf8("t"))).isNull();     // c > ceil
        assertThat(extrapolate(null, utf8("t"), utf8("t"))).isNull();     // c == ceil
    }

    @Test
    void extrapolate_adjacentBounds_returnNull() {
        // c and ceil are UTF-8-adjacent (ceil == c ++ U+0000): nothing strictly between, and the
        // reflection overshoots the ceiling, so the midpoint fallback yields null.
        byte[] c    = utf8("a");
        byte[] ceil = concat(utf8("a"), b(0x00));
        assertThat(extrapolate(utf8("a"), c, ceil)).isNull();
    }

    @Test
    void extrapolate_nullCeil_usesMaxUtf8KeyAndStaysValid() {
        // Whole-bucket scope: ceil = null → implicit top = U+10FFFF (0xF4 0x8F 0xBF 0xBF).
        byte[] c   = utf8("hello");
        byte[] max = b(0xF4, 0x8F, 0xBF, 0xBF);
        byte[] m   = extrapolate(null, c, null);
        assertThat(m).isNotNull();
        assertThat(ByteMidpoint.isValidUtf8(m)).isTrue();
        assertThat(Arrays.compareUnsigned(m, c)).isPositive();               // c < m
        assertThat(Arrays.compareUnsigned(m, max)).isLessThanOrEqualTo(0);   // m <= U+10FFFF
    }

    @Test
    void extrapolate_invalidUtf8Ceil_fallsBackToMax_noThrow() {
        // A prefixCeil that bumped a continuation byte (0xCF 0xBF → 0xCF 0xC0) is NOT valid UTF-8.
        // extrapolate must NOT pass it to byteMidpoint; it falls back to the max-UTF-8 ceiling.
        byte[] badCeil = b(0xCF, 0xC0);
        assertThat(ByteMidpoint.isValidUtf8(badCeil)).isFalse();
        byte[] c = utf8("A");
        byte[] m = extrapolate(null, c, badCeil);   // must not throw
        assertThat(m).isNotNull();
        assertThat(ByteMidpoint.isValidUtf8(m)).isTrue();
        assertThat(Arrays.compareUnsigned(m, c)).isPositive();
    }

    @Test
    void extrapolate_alwaysValidUtf8_overRealisticCursors() {
        // Property-style sanity (not the PROP-1 critical case): every non-null pivot is valid UTF-8 and
        // strictly ordered, across realistic deep-prefix cursors and the corresponding ceiling,
        // both for the root frontier (lo = ⊥) and the cascade case (lo shares the prefix).
        String[] prefixes = {"data/", "crawl=2024-01/pid=000001/", "logs/", "a/b/c/"};
        String[] suffixes = {"", "0", "file_0500", "2024-01-01/part-00000", "zzz"};
        for (String p : prefixes) {
            byte[] ceil = prefixCeil(utf8(p));
            byte[] lo = utf8(p);   // a shared-prefix lo (the cascade case)
            for (String s : suffixes) {
                byte[] c = utf8(p + s);
                for (byte[] loArg : new byte[][]{null, lo}) {
                    byte[] m = extrapolate(loArg, c, ceil);
                    if (m != null) {
                        assertThat(ByteMidpoint.isValidUtf8(m))
                                .as("valid UTF-8 for cursor %s", p + s).isTrue();
                        assertThat(Arrays.compareUnsigned(m, c))
                                .as("c < m for cursor %s", p + s).isPositive();
                        if (ceil != null) {
                            assertThat(Arrays.compareUnsigned(m, ceil))
                                    .as("m < ceil for cursor %s", p + s).isNegative();
                        }
                    }
                }
            }
        }
    }

    @Test
    void extrapolate_pivotCarriesNoUnsafeCodePoint() {
        // With safe cursors, every extrapolate pivot must be an XML-safe start-after value: no code
        // point in E = {U+0000..U+001F} ∪ {U+007F} ∪ {U+FFFE, U+FFFF}. lo varies between ⊥ and a
        // shared-prefix bound; the synthesized scalar must always be safe.
        String[] cursors = {"a", "data/file0500", "flat/00000123", "z", " low", "~tilde",
                "data/2024-01-01/part-00100"};
        String[] prefixes = {null, "data/", "flat/", "a/b/c/"};
        for (String s : cursors) {
            byte[] c = utf8(s);
            for (String p : prefixes) {
                byte[] ceil = (p == null) ? null : prefixCeil(utf8(p));
                for (byte[] lo : new byte[][]{null, c}) {
                    byte[] m = extrapolate(lo, c, ceil);
                    if (m != null) {
                        assertNoUnsafe(m, "extrapolate(lo,%s,%s)".formatted(s, p));
                        assertThat(Arrays.compareUnsigned(m, c)).as("c < m for %s", s).isPositive();
                    }
                }
            }
        }
    }

    @Test
    void extrapolate_lowControlAdjacentCursor_reflectsToSafePivot() {
        // The U+000F-class regression at the engine boundary: a cursor whose last code point sits
        // just above the C0 block (U+0021 '!') with a lo one scalar below (U+0020) reflects to a
        // scalar that must be snapped clear of any control — never a raw U+000F-style pivot.
        byte[] lo = utf8(" ");
        byte[] c = utf8("!");
        byte[] m = extrapolate(lo, c, null);
        assertThat(m).isNotNull();
        assertNoUnsafe(m, "extrapolate low-adjacent");
        assertThat(Arrays.compareUnsigned(m, c)).as("c < m").isPositive();
    }

    private static void assertNoUnsafe(byte[] m, String where) {
        new String(m, StandardCharsets.UTF_8).codePoints().forEach(cp ->
                assertThat(ScalarSafety.isExcludedScalar(cp))
                        .as("%s: no unsafe code point U+%04X in %s", where, cp, Arrays.toString(m))
                        .isFalse());
    }

    // -------------------------------------------------------------------------
    // interpolate (far-ahead fractional pivot) — code-point-safe, bounded ranges only.
    // f=0.5 must coincide byte-for-byte with ByteMidpoint.between; larger f lands nearer hi.
    // -------------------------------------------------------------------------

    @Test
    void interpolate_halfCoincidesWithByteMidpoint() {
        // f = 0.5 must be byte-for-byte identical to the existing midpoint on a spread of shapes.
        String[][] pairs = {
                {"data/file0400", "data/file0900"},
                {"a", "z"},
                {"crawl=2024/pid=000001/", "crawl=2024/pid=000009/"},
                {"2022/03/05/aaaa", "2022/03/05/zzzz"},
                {"f/g/h/0000/obj", "f/g/h/9999/obj"},
        };
        for (String[] p : pairs) {
            byte[] a = utf8(p[0]);
            byte[] b = utf8(p[1]);
            assertThat(interpolate(a, b, 0.5))
                    .as("interpolate(%s,%s,0.5) == byteMidpoint", p[0], p[1])
                    .isEqualTo(ByteMidpoint.between(a, b));
        }
    }

    @Test
    void interpolate_largerFractionLandsNearerHi() {
        // Monotone in f: a bigger fraction places the pivot at/after the smaller fraction's pivot,
        // and every pivot stays strictly between and valid UTF-8.
        byte[] a = utf8("2022/03/05/00000000");
        byte[] b = utf8("2022/03/05/zzzzzzzz");
        byte[] lo = interpolate(a, b, 0.25);
        byte[] mid = interpolate(a, b, 0.5);
        byte[] hi = interpolate(a, b, 0.75);
        for (byte[] m : new byte[][]{lo, mid, hi}) {
            assertThat(m).isNotNull();
            assertThat(ByteMidpoint.isValidUtf8(m)).isTrue();
            assertThat(Arrays.compareUnsigned(a, m)).isNegative();
            assertThat(Arrays.compareUnsigned(m, b)).isNegative();
        }
        assertThat(Arrays.compareUnsigned(lo, mid)).isLessThanOrEqualTo(0);
        assertThat(Arrays.compareUnsigned(mid, hi)).isLessThanOrEqualTo(0);
        // With a wide divergence gap the 0.75 pivot is strictly past the 0.5 pivot (far-ahead).
        assertThat(Arrays.compareUnsigned(mid, hi)).isNegative();
    }

    @Test
    void interpolate_nullHi_returnsNull() {
        // Open frontier is handled by extrapolate, not interpolate.
        assertThat(interpolate(utf8("a"), null, 0.75)).isNull();
    }

    @Test
    void interpolate_loAtOrAfterHi_returnsNull() {
        assertThat(interpolate(utf8("z"), utf8("a"), 0.5)).isNull();
        assertThat(interpolate(utf8("a"), utf8("a"), 0.5)).isNull();
    }

    @Test
    void interpolate_adjacentBounds_returnsNullLikeByteMidpoint() {
        // b == a ++ U+0000 / U+0001 are the unsplittable slivers: null for any fraction (same
        // contract as byteMidpoint — fraction shifts only the synthesized scalar, never existence).
        byte[] a = utf8("a");
        for (double f : new double[]{0.25, 0.5, 0.75}) {
            assertThat(interpolate(a, concat(utf8("a"), b(0x00)), f)).isNull();
            assertThat(interpolate(a, concat(utf8("a"), b(0x01)), f)).isNull();
        }
    }

    @Test
    void interpolate_nullLo_treatedAsBottom() {
        // A null lo is the ⊥ sentinel (empty array); interpolate must still yield a strictly-between
        // valid-UTF-8 pivot below hi.
        byte[] m = interpolate(null, utf8("mmmm"), 0.75);
        assertThat(m).isNotNull();
        assertThat(ByteMidpoint.isValidUtf8(m)).isTrue();
        assertThat(Arrays.compareUnsigned(m, utf8("mmmm"))).isNegative();
    }

    @Test
    void interpolate_synthesizedScalarIsSafe() {
        // Every synthesized pivot scalar must avoid E (I12) across a spread of fractions and bounds.
        String[][] pairs = {{"a", "z"}, {"data/0", "data/9"}, {"2022/03/05/a", "2022/03/05/z"}};
        for (String[] p : pairs) {
            for (double f : new double[]{0.1, 0.25, 0.5, 0.75, 0.9}) {
                byte[] m = interpolate(utf8(p[0]), utf8(p[1]), f);
                if (m != null) {
                    assertNoUnsafe(m, "interpolate(%s,%s,%s)".formatted(p[0], p[1], f));
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Edge cases: 0xFF bytes, deep common prefix
    // -------------------------------------------------------------------------

    @Test
    void fracIn_windowWithAllHighBytes() {
        // Window: lo = [0xFF, 0x00], hi = [0xFF, 0xFF]
        // d = commonPrefixLen = 1 (shared 0xFF at byte 0)
        // fracIn(key, lo, hi) reads from byte 1
        byte[] lo  = b(0xFF, 0x00);
        byte[] hi  = b(0xFF, 0xFF);
        byte[] mid = b(0xFF, 0x80);
        double f = fracIn(mid, lo, hi);
        assertThat(f).isEqualTo(0.5);   // 0x80/256
    }

    @Test
    void spanIn_deepSharedPrefix_fullWindowIsUnity() {
        // lo = [0xAA, 0xBB, 0xCC, 0x00], hi = [0xAA, 0xBB, 0xCC, 0xFF]
        // d = 3, distinguishing byte at position 3.
        byte[] lo = b(0xAA, 0xBB, 0xCC, 0x00);
        byte[] hi = b(0xAA, 0xBB, 0xCC, 0xFF);
        double span = spanIn(lo, hi, lo, hi);
        // Should be (0xFF - 0x00)/256 = 255/256 ≈ 0.996, close to but less than 1.0
        assertThat(span).isGreaterThan(0.99);
        assertThat(span).isLessThan(1.0);
    }

    @Test
    void estRemaining_deepSharedPrefix_notCollapsedToZero() {
        // Two workers: one has a global prefix collision, the other doesn't.
        // Window-relative must NOT collapse to ~0 on a deep shared prefix.
        byte[] lo = utf8("crawl=2024-01/pid=000001/");
        byte[] hi = utf8("crawl=2024-01/pid=000001z");  // just past the pid range
        byte[] cursor = utf8("crawl=2024-01/pid=000001/file_0500");
        long keysEmitted = 500L;

        double est = estRemaining(cursor, lo, hi, keysEmitted);
        // Must be finite and greater than 0
        assertThat(est).isFinite();
        assertThat(est).isGreaterThan(0.0);
    }
}
