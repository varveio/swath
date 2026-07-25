/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * The bounded-range split pivot (algorithms.md §3.1): given valid-UTF-8
 * {@code a < b} (unsigned byte-lex), return an {@code m} with {@code a < m < b}
 * whose <b>synthesized</b> code point is drawn only from the {@linkplain #isSafe
 * safe set}, or {@code null} <b>iff</b> no such safe {@code m} exists strictly
 * between (the successor region up to {@code b} is a control/noncharacter sliver
 * — e.g. {@code b == a ++ [U+0000]} or {@code b == a ++ [U+0001]}).
 *
 * <p><b>Synthesized pivot scalars are XML-1.0-safe by construction (the
 * asymmetry).</b> Bound interpretation (decode / common-prefix / compare) ranges
 * over the FULL scalar space — real keys/cursors may contain any code point
 * including controls. But the ONE invented code point a pivot adds (the divergence
 * / append / reflect scalar) is drawn only from the {@link #isSafe safe set}
 * {@code E^c}, chosen at the synthesis point with strict-betweenness re-verified,
 * so the no-gap/no-overlap tiling is preserved (a rare control-only sliver becomes
 * a {@code null}, affecting only load balance, never coverage). See algorithms.md
 * §3.1 for the full rationale and {@code docs/internals/s3-implementation-compatibility.md}
 * for the per-endpoint detail behind {@code E}.
 *
 * <p>We compute over Unicode <b>code points</b>: every branch assembles {@code m}
 * only from scalar values, so it re-encodes to valid UTF-8 and transmits byte-exact
 * through the SDK's ordinary {@code startAfter(String)} — the SDK percent-encodes
 * it, and a valid-UTF-8 pivot round-trips. No {@code ExecutionInterceptor} and no
     * {@code encoding-type} change is needed. Property-tested by PROP-2 (incl. "every
     * output is valid UTF-8 and its synthesized scalar carries no code point in {@code E}").
 */
public final class ByteMidpoint {

    /** S3's maximum key length in bytes. A pivot longer than this is not a valid boundary. */
    public static final int MAX_KEY_LEN = 1024;

    /** First surrogate code point (U+D800); the surrogate block U+D800..U+DFFF is not scalar. */
    private static final int SURROGATE_LO = 0xD800;
    /** Size of the surrogate block (U+D800..U+DFFF) excluded from the scalar index space. */
    private static final int SURROGATE_SPAN = 0x800;
    /** Scalar-index sentinel for "below every code point" (lo bound of an open-left half). */
    private static final int BELOW = -1;
    /** Kernel sentinel: no safe scalar exists in the requested (open) interval. */
    private static final int NONE = -1;

    /**
     * Smallest SAFE scalar (U+0020). Everything below it is the C0 control block, all excluded
     * from {@code E}; the smallest 1-byte safe successor a pivot can append.
     */
    private static final int MIN_SAFE = 0x20;
    /**
     * A fixed mid-BMP safe scalar (U+8000), well clear of the controls and noncharacters, used
     * when a pivot must append a high scalar to stay {@code > c} while sharing all of {@code c}.
     */
    private static final int SAFE_HIGH = 0x8000;

    private ByteMidpoint() {
    }

    /**
     * Alphabet-aware pivot synthesis seam (algorithms.md §3.3, rank-space pivot synthesis): lets a caller supply
     * the invented divergence scalar from its own observed per-position alphabet instead of the
     * default safe-scalar midpoint. {@link #NO_SCALAR} falls back to the default synthesis. A
     * returned scalar is always independently re-validated ({@link #isSafe} and strictly
     * {@code loCp <_u scalar <_u hiCp}) before use, so betweenness / I12 safety hold regardless of
     * what the chooser returns.
     */
    @FunctionalInterface
    public interface ScalarChooser {
        /** Sentinel: no observed-alphabet scalar for this position — use the default synthesis. */
        int NO_SCALAR = -1;

        /**
         * Choose an observed safe scalar at code-point index {@code cpIndex}, strictly between
         * {@code loCp} (exclusive; {@code loCp < 0} means "no lower code point" / ⊥) and {@code hiCp}
         * (exclusive), at rank-fraction ≈ {@code fraction} of the observed alphabet, or
         * {@link #NO_SCALAR} when there is no clean alphabet signal at this position.
         */
        int chooseScalar(int cpIndex, int loCp, int hiCp, double fraction);
    }

    /**
     * The safe-scalar predicate: {@code cp} is drawn-able into a synthesized pivot iff it is NOT
     * in the excluded set {@code E = {U+0000..U+001F} ∪ {U+007F} ∪ {U+0080..U+009F} ∪
     * {U+FDD0..U+FDEF} ∪ {every plane's trailing pair xFFFE/xFFFF} ∪ {U+0025}} and not a
     * surrogate. {@code E} is a conservative superset of the XML-1.0-illegal code points, plus
     * the C1 block and every plane's noncharacters (both XML-legal, excluded because several S3
     * implementations 400 on them in a {@code start-after} cursor) and {@code '%'} (XML-legal and
     * accepted by real S3, excluded because some S3-compatible endpoints echo it back unescaped
     * and the AWS SDK's response interceptor then throws decoding it). See algorithms.md §3.1 and
     * {@code docs/internals/s3-implementation-compatibility.md} for the full rationale per member.
     * The outward scan in {@link #safeBetween(int, int, double)} terminates regardless of how
     * wide any excluded run is, since it never steps outside the open interval it is searching
     * (returning {@link #NONE} when the whole interval is excluded; the {@code a ++ MIN_SAFE}
     * fallback then guarantees overall progress).
     */
    static boolean isSafe(int cp) {
        if (cp < MIN_SAFE) {
            return false;                       // C0 control block U+0000..U+001F
        }
        if (cp == 0x7F) {
            return false;                       // DEL
        }
        if (cp >= 0x80 && cp <= 0x9F) {
            return false;                       // C1 control block U+0080..U+009F
        }
        if (cp >= 0xFDD0 && cp <= 0xFDEF) {
            return false;                       // standalone BMP noncharacter block
        }
        if ((cp & 0xFFFE) == 0xFFFE) {
            return false;                       // trailing xFFFE/xFFFF noncharacter pair, every plane
        }
        if (cp == 0x25) {
            return false;                       // '%' — verbatim-echo endpoint URLDecoder crash
        }
        return cp < SURROGATE_LO || cp >= SURROGATE_LO + SURROGATE_SPAN;   // not a surrogate
    }

    /**
     * @param a lower bound (exclusive); the worker's cursor — must be valid UTF-8
     * @param b upper bound (exclusive in the result sense); the worker's {@code hi} — must be valid UTF-8
     * @return {@code m} with {@code a < m < b} whose synthesized code point is {@linkplain #isSafe
     *         safe}, or {@code null} iff no such safe {@code m} exists strictly between (the region
     *         up to {@code b} is a control/noncharacter sliver, or the only candidate pivot would
     *         exceed {@link #MAX_KEY_LEN} bytes)
     * @throws IllegalArgumentException if {@code compareUnsigned(a, b) >= 0}, or if either argument is not valid UTF-8
     */
    public static byte[] between(byte[] a, byte[] b) {
        return between(a, b, 0.5);
    }

    /**
     * Fractional generalization of {@link #between(byte[], byte[])}: place the synthesized pivot
     * scalar at fraction {@code fraction ∈ (0,1)} of the way across the first divergent scalar gap
     * (in the contiguous scalar-index space), instead of at its midpoint. {@code fraction == 0.5}
     * reproduces {@link #between(byte[], byte[])} byte-for-byte. A larger fraction lands the pivot
     * nearer {@code b}, so a split placed there sits farther ahead of a fast drainer's cursor.
     * Every other branch (the {@code MIN_SAFE}/append and cap fallbacks, the {@code null}
     * unsplittable contract) is fraction-independent and identical to {@link #between(byte[], byte[])}
     * — so the valid-UTF-8 / safe-set (I12) guarantees are unchanged.
     */
    public static byte[] between(byte[] a, byte[] b, double fraction) {
        return between(a, b, fraction, null);
    }

    /**
     * Alphabet-aware generalization of {@link #between(byte[], byte[], double)} (algorithms.md
     * §3.3, rank-space pivot synthesis): identical in every branch except the single invented divergence scalar
     * may be drawn from {@code chooser}'s observed per-position alphabet. A {@code null} chooser,
     * or a {@link ScalarChooser#NO_SCALAR} result, reproduces {@link #between(byte[], byte[], double)}
     * byte-for-byte. Any scalar the chooser returns is re-validated ({@link #isSafe} + strict
     * betweenness), so the valid-UTF-8 / safe-set (I12) and {@code a <_u m <_u b} guarantees are
     * unchanged.
     */
    public static byte[] between(byte[] a, byte[] b, double fraction, ScalarChooser chooser) {
        if (Arrays.compareUnsigned(a, b) >= 0) {
            throw new IllegalArgumentException("byteMidpoint precondition violated: a must be < b");
        }
        int[] codeA = decode(a, "a");
        int[] codeB = decode(b, "b");
        int i = commonPrefixLength(codeA, codeB);

        if (i == codeA.length) {
            // Path B: a is a proper code-point prefix of b ⇒ codeB[i] exists.
            int v = pickScalar(chooser, i, BELOW, codeB[i], fraction);   // a safe scalar with 0x20 <= v < codeB[i]
            if (v != NONE) {
                byte[] mid = encode(append(codeA, v));  // a·v : MIN_SAFE <= v < codeB[i] ⇒ a < a·v < b
                if (mid.length <= MAX_KEY_LEN) {
                    return mid;
                }
                // a·v overflows only when v is multi-byte; v exists ⇒ codeB[i] > U+0020, so
                // a·MIN_SAFE (1-byte tail) is also < b and always fits (|a| ≤ |b|−1 ≤ 1023).
                return encode(append(codeA, MIN_SAFE));
            }
            // No safe scalar strictly below codeB[i] (codeB[i] ≤ U+0020). Recover only the case
            // where codeB[i] is itself safe and b is strictly longer than a·codeB[i] (e.g.
            // codeB[i] == U+0020 with a longer b): then a·codeB[i] < b is a valid safe pivot.
            if (isSafe(codeB[i]) && codeB.length > codeA.length + 1) {
                return encode(append(codeA, codeB[i]));
            }
            return null;   // genuinely unsplittable: only a control/noncharacter sliver lies between
        }

        // Path A: code points differ at i — codeA[i] < codeB[i].
        int v = pickScalar(chooser, i, codeA[i], codeB[i], fraction);
        if (v != NONE) {
            byte[] mid = encode(append(Arrays.copyOf(codeA, i), v));   // a[0..i-1]·v — shortest m
            if (mid.length <= MAX_KEY_LEN) {
                return mid;
            }
            // Fall through: a huge common prefix plus a multi-byte midpoint scalar overflows.
        } else {
            // No safe scalar strictly between codeA[i] and codeB[i]: extend a with MIN_SAFE.
            // a·MIN_SAFE > a (extension) and < b (at i, m[i] = codeA[i] < codeB[i] = b[i]).
            byte[] succ = encode(append(codeA, MIN_SAFE));
            if (succ.length <= MAX_KEY_LEN) {
                return succ;
            }
            // Fall through: a·MIN_SAFE overflows (a is already MAX_KEY_LEN bytes).
        }
        // The ideal pivot exceeds MAX_KEY_LEN. Find a shorter valid m by bumping a's right-most
        // bumpable scalar to the next SAFE scalar and dropping the tail (e.g. a = "A"+1023×U+0000,
        // b = "B" ⇒ "A"+1022×U+0000+U+0020, a valid 1024-byte pivot the naïve a·U+0000 would miss).
        return capFallback(codeA, b);
    }

    /** Convenience overload over {@link KeyBytes}; {@code null} when no key lies between. */
    public static KeyBytes between(KeyBytes a, KeyBytes b) {
        byte[] m = between(a.rawUnsafe(), b.rawUnsafe());
        return m == null ? null : KeyBytes.of(m);
    }

    /**
     * True iff {@code bytes} is valid UTF-8 — the precondition {@link #between} enforces on
     * its bounds. Lets callers screen a synthetic bound (e.g. a {@code prefixCeil} that bumped
     * a continuation byte) before passing it in, rather than catching the precondition throw.
     */
    public static boolean isValidUtf8(byte[] bytes) {
        try {
            strictDecoder().decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    /**
     * Open-frontier forward pivot (algorithms.md §3.1): reflect the consumed span {@code (lo, c]}
     * forward in <b>code-point</b> space to a valid-UTF-8 {@code m > c} that shares {@code c}'s
     * leading code points, so a retry-nearer-cursor can refine an overshoot by bisecting within
     * that shared prefix instead of collapsing to {@code c}'s immediate successor. At the first
     * divergence position {@code i} of {@code lo} and {@code c}, the reflected scalar index is
     * {@code 2·idx(c[i]) − idx(lo[i])} ({@code lo}'s side is {@code BELOW} when {@code lo} ends
     * at/before {@code i}). When {@code c}'s first code point already differs from {@code lo}
     * (e.g. {@code lo} is ⊥, the root frontier), reflecting there would leave {@code c}'s
     * namespace, so we instead reflect {@code c}'s <b>last</b> code point, keeping {@code m}
     * inside {@code c}'s prefix.
     *
     * @param lo the worker's range start (⊥ when {@code null}/empty); must be valid UTF-8
     * @param c  the worker's cursor; must be non-empty and valid UTF-8
     * @return a valid-UTF-8 {@code m} with {@code c < m} and length {@code <= MAX_KEY_LEN}, or
     *         {@code null} when {@code c} has no code points, no larger scalar exists, or no pivot fits
     *         S3's key-length cap
     */
    public static byte[] forwardReflect(byte[] lo, byte[] c) {
        int[] codeC = decode(c, "c");
        if (codeC.length == 0) {
            return null;
        }
        int[] codeL = (lo == null) ? new int[0] : decode(lo, "lo");
        int i = commonPrefixLength(codeL, codeC);
        if (i >= codeC.length) {
            i = codeC.length - 1;   // c == lo (or a prefix of it): reflect c's last code point
        } else if (i == 0) {
            i = codeC.length - 1;   // c leaves lo's namespace at code point 0: stay inside c's prefix
        }
        int jc = idx(codeC[i]);
        int jl = (i < codeL.length) ? idx(codeL[i]) : -1;   // BELOW when lo ends at/before i
        int maxIdx = idx(0x10FFFF);
        long jm = Math.min((long) jc + (jc - jl), maxIdx);  // 2·idx(c[i]) − idx(lo[i]), capped — reflect forward
        if (jm > jc) {
            int target = cp((int) jm);                       // target > c[i] (jm > jc)
            // Snap the reflected scalar UP to the nearest SAFE scalar (≥ target, so still > c[i]),
            // so m's invented divergence char is never a control/noncharacter.
            int snapped = isSafe(target) ? target : safeAbove(target);
            if (snapped != NONE) {
                int[] bumped = Arrays.copyOf(codeC, i + 1);
                bumped[i] = snapped;                         // snapped > c[i] ⇒ m > c, shared prefix c[0..i-1]
                byte[] enc = encode(bumped);
                if (enc.length <= MAX_KEY_LEN) {
                    return enc;
                }
                return capFallbackAfter(codeC);
            }
            // Fall through: no safe scalar at/above target (target near the top of the scalar space).
        }
        // c[i] is already the max scalar (cannot bump it in place, e.g. a U+10FFFF run): APPEND a
        // safe high scalar so m > c while still sharing ALL of c (m = c ++ SAFE_HIGH), keeping the
        // pivot inside c's namespace for the thief's retry-nearer-cursor.
        int[] appended = Arrays.copyOf(codeC, codeC.length + 1);
        appended[codeC.length] = SAFE_HIGH;
        byte[] enc = encode(appended);
        if (enc.length <= MAX_KEY_LEN) {
            return enc;
        }
        return capFallbackAfter(codeC);
    }

    /**
     * The {@linkplain #isSafe SAFE} scalar nearest the natural midpoint that lies STRICTLY between
     * {@code loCp} and {@code hiCp} over the contiguous scalar index space (the surrogate block is
     * excluded, so scalars index densely): {@code idx(x) = x < 0xD800 ? x : x - 0x800}. {@code loCp}
     * may be {@link #BELOW} (index −1, "no code point here"). The natural midpoint index is
     * {@code jm = floorDiv(idx(loCp) + idx(hiCp), 2)}; we scan OUTWARD from {@code jm}, checking
     * only indices strictly inside {@code (jl, jh)} — so the scan always terminates (it never steps
     * past either bound) regardless of how wide an excluded block is, returning {@link #NONE} once
     * both directions have left the open interval: the bounds are scalar-adjacent, or the whole
     * interval (however wide) is excluded. Never a surrogate, by construction.
     */
    private static int safeBetween(int loCp, int hiCp) {
        return safeBetween(loCp, hiCp, 0.5);
    }

    /**
     * Try the alphabet-aware {@code chooser} first (algorithms.md §3.3, rank-space pivot synthesis), falling
     * back to {@link #safeBetween(int, int, double)} when it declines or returns an unusable
     * scalar. A chooser result is accepted only when it is {@linkplain #isSafe safe} and strictly
     * between {@code loCp} and {@code hiCp} in the contiguous scalar-index space, so an alphabet
     * pivot tiles exactly like the default safe-scalar midpoint (I2/I3) even from a torn read of
     * the observed alphabet. A {@code null} chooser makes this identical to {@code safeBetween}.
     */
    private static int pickScalar(ScalarChooser chooser, int cpIndex, int loCp, int hiCp, double fraction) {
        if (chooser != null) {
            int v = chooser.chooseScalar(cpIndex, loCp, hiCp, fraction);
            if (v != ScalarChooser.NO_SCALAR && isSafe(v)) {
                int jl = (loCp < 0) ? -1 : idx(loCp);
                int jv = idx(v);
                int jh = idx(hiCp);
                if (jv > jl && jv < jh) {
                    return v;
                }
            }
        }
        return safeBetween(loCp, hiCp, fraction);
    }

    /**
     * Fractional variant of {@link #safeBetween(int, int)}: start the outward scan at the scalar
     * index {@code jl + ⌊fraction·(jh − jl)⌋} rather than the midpoint. {@code fraction == 0.5}
     * yields {@code jl + ⌊(jh − jl)/2⌋ == floorDiv(jl + jh, 2)} — the exact midpoint index the
     * plain {@link #safeBetween(int, int)} used — so it is byte-for-byte identical there. The start
     * index is clamped strictly inside {@code (jl, jh)}; the surrounding outward scan — which
     * terminates because it never steps outside that open interval, independent of how wide any
     * excluded block is — is otherwise unchanged.
     */
    private static int safeBetween(int loCp, int hiCp, double fraction) {
        int jl = (loCp < 0) ? -1 : idx(loCp);
        int jh = idx(hiCp);
        if (jh - jl <= 1) {
            return NONE;                          // scalar-adjacent: nothing strictly between
        }
        int jm = jl + (int) Math.floor(fraction * (jh - jl));   // fraction of the way from jl to jh
        if (jm <= jl) {
            jm = jl + 1;                          // clamp strictly inside (jl, jh)
        } else if (jm >= jh) {
            jm = jh - 1;
        }
        for (int delta = 0; ; delta++) {
            int down = jm - delta;
            int up = jm + delta;
            boolean any = false;
            if (down > jl && down < jh) {
                any = true;
                int c = cp(down);
                if (isSafe(c)) {
                    return c;
                }
            }
            if (up != down && up > jl && up < jh) {
                any = true;
                int c = cp(up);
                if (isSafe(c)) {
                    return c;
                }
            }
            if (!any) {
                return NONE;                       // exhausted the open interval — no safe scalar
            }
        }
    }

    /**
     * The smallest {@linkplain #isSafe SAFE} scalar strictly greater than {@code cp}, skipping the
     * surrogate block and any excluded scalar; {@link #NONE} when none exists (no safe scalar above).
     */
    private static int safeAbove(int cp) {
        int maxIdx = idx(0x10FFFF);
        for (int j = idx(cp) + 1; j <= maxIdx; j++) {
            int c = cp(j);
            if (isSafe(c)) {
                return c;
            }
        }
        return NONE;
    }

    private static int idx(int codePoint) {
        return codePoint < SURROGATE_LO ? codePoint : codePoint - SURROGATE_SPAN;
    }

    private static int cp(int scalarIndex) {
        return scalarIndex < SURROGATE_LO ? scalarIndex : scalarIndex + SURROGATE_SPAN;
    }

    private static int commonPrefixLength(int[] a, int[] b) {
        int n = Math.min(a.length, b.length);
        int i = 0;
        while (i < n && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    private static int[] append(int[] base, int codePoint) {
        int[] out = Arrays.copyOf(base, base.length + 1);
        out[base.length] = codePoint;
        return out;
    }

    /**
     * Cap fallback: the ideal pivot ({@code a·MIN_SAFE} or {@code a[0..i-1]·v}) exceeds
     * {@link #MAX_KEY_LEN} bytes. Scan {@code a}'s code points right-to-left for the right-most
     * one we can bump to the next SAFE scalar; the candidate {@code a[0..p-1]·safeAbove(a[p])}
     * (tail dropped) is always {@code > a} (larger scalar at {@code p}, identical prefix before it)
     * and carries only safe synthesized scalars, so we just need the first that also fits the cap
     * <b>and</b> stays {@code < b}. The right-most bump keeps the candidate shortest and, when
     * {@code a} and {@code b} differ within the dropped tail, guarantees {@code < b}. Returns
     * {@code null} only when no safe pivot fits {@link #MAX_KEY_LEN} (genuinely unsplittable —
     * balance-only, never a no-gap/no-overlap risk).
     */
    private static byte[] capFallback(int[] codeA, byte[] b) {
        for (int p = codeA.length - 1; p >= 0; p--) {
            int next = safeAbove(codeA[p]);
            if (next == NONE) {
                continue;                       // no safe scalar above codeA[p] — cannot bump here
            }
            int[] cand = Arrays.copyOf(codeA, p + 1);
            cand[p] = next;                     // cand > a: larger safe scalar at p, equal prefix before
            byte[] enc = encode(cand);
            if (enc.length <= MAX_KEY_LEN && Arrays.compareUnsigned(enc, b) < 0) {
                return enc;                     // a < enc < b, within the cap
            }
        }
        return null;
    }

    /**
     * Open-frontier cap fallback: if reflecting/appending would exceed {@link #MAX_KEY_LEN}, scan
     * the cursor right-to-left for the right-most scalar that can be bumped to the next SAFE scalar
     * and drop the tail. The candidate is always {@code > c} because it has an identical prefix and a
     * larger scalar at the bump position. Returns {@code null} when no safe pivot fits the cap.
     */
    private static byte[] capFallbackAfter(int[] codeC) {
        for (int p = codeC.length - 1; p >= 0; p--) {
            int next = safeAbove(codeC[p]);
            if (next == NONE) {
                continue;
            }
            int[] cand = Arrays.copyOf(codeC, p + 1);
            cand[p] = next;
            byte[] enc = encode(cand);
            if (enc.length <= MAX_KEY_LEN) {
                return enc;
            }
        }
        return null;
    }

    /** Encode code points to UTF-8 (no length cap; callers compare against {@link #MAX_KEY_LEN}). */
    private static byte[] encode(int[] codePoints) {
        StringBuilder sb = new StringBuilder(codePoints.length + 1);
        for (int cp : codePoints) {
            sb.appendCodePoint(cp);
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Strict UTF-8 → code points; the precondition is that {@code a} and {@code b} are valid UTF-8. */
    private static int[] decode(byte[] bytes, String field) {
        try {
            return strictDecoder().decode(ByteBuffer.wrap(bytes)).toString().codePoints().toArray();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(
                    "byteMidpoint precondition violated: bound " + field + " is not valid UTF-8", e);
        }
    }

    /** A fresh strict UTF-8 decoder (decoders are stateful, hence not shared). */
    private static CharsetDecoder strictDecoder() {
        return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
    }
}
