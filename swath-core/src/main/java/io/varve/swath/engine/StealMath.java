/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.engine.policy.Engagement;
import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.output.ControlCharEscaper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pure byte-exact pivot / victim-selection math for the {@code WorkStealingScan} engine
 * (algorithms.md §3.1 and §3.2). All methods are static and side-effect free.
 *
 * <p>Victim selection ({@link #estRemaining} via {@link #fracIn}) measures a key's position over
 * the suffix <em>after</em> the longest common prefix of {@code [lo, hi]}, so a deep shared prefix
 * cannot collapse the fraction the way a global absolute-byte fraction would. The open-frontier
 * split pivot ({@link #extrapolate}) reflects the consumed span forward in <b>code-point</b> space
 * ({@link ByteMidpoint#forwardReflect}): valid UTF-8, sharing the cursor's prefix. Reflecting over
 * a raw-byte fraction instead is unsafe — it can synthesize invalid UTF-8 and jump out of the
 * cursor's namespace, leaving no effective split. See algorithms.md §3.1/§3.2 for the derivation.
 */
public final class StealMath {

    private static final Logger log = LoggerFactory.getLogger(StealMath.class);

    /** Precision in bytes for the window-relative fraction ({@link #fracIn}). */
    static final int K = 12;

    /**
     * The maximum valid-UTF-8 key, U+10FFFF encoded ({@code 0xF4 0x8F 0xBF 0xBF}): the implicit
     * ceiling for an open frontier scoped to the whole bucket (no prefix ceiling), and the
     * fallback when a {@code prefixCeil} is not valid UTF-8.
     */
    private static final byte[] MAX_UTF8_KEY = {(byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF};

    private StealMath() {}

    // -------------------------------------------------------------------------
    // §3.2 — window-relative fracIn / spanIn / estRemaining (victim selection)
    // -------------------------------------------------------------------------

    /**
     * Position of {@code key} in the window defined by {@code [lo, hi]}, expressed
     * as a base-256 fraction over the <b>suffix that distinguishes {@code lo} from
     * {@code hi}</b> (bytes after their longest common prefix, up to {@value #K} bytes).
     *
     * <ul>
     *   <li>{@code null} key, or a key shorter than the common-prefix length, is treated
     *       as the bottom sentinel: 0.0.</li>
     *   <li>{@code null} lo or hi is treated as an empty array (no common prefix → offset
     *       0, so the full key is used).</li>
     * </ul>
     */
    public static double fracIn(byte[] key, byte[] lo, byte[] hi) {
        int d = commonPrefixLen(lo, hi);
        return fracFromOffset(key, d);
    }

    /**
     * Fraction of the window {@code [lo, hi]} spanned by the interval {@code [x, y]},
     * i.e. {@code fracIn(y, lo, hi) − fracIn(x, lo, hi)}, clamped to ≥ 0.
     */
    public static double spanIn(byte[] x, byte[] y, byte[] lo, byte[] hi) {
        return Math.max(0.0, fracIn(y, lo, hi) - fracIn(x, lo, hi));
    }

    /**
     * Estimated remaining work in {@code (cursor, hi]} for a worker with range
     * {@code [lo, hi]} that has emitted {@code keysEmitted} keys so far.
     *
     * <ul>
     *   <li>Returns {@link Double#POSITIVE_INFINITY} when {@code hi} is {@code null}
     *       (the open frontier always scores highest).</li>
     *   <li>Returns the raw remaining span when {@code consumed == 0} (cursor at lo —
     *       no density signal yet; rank by width alone).</li>
     *   <li>Otherwise returns {@code localDensity × remaining} where
     *       {@code localDensity = keysEmitted / consumed}.</li>
     * </ul>
     */
    public static double estRemaining(byte[] cursor, byte[] lo, byte[] hi, long keysEmitted) {
        if (hi == null) return Double.POSITIVE_INFINITY;
        double remaining = spanIn(cursor, hi, lo, hi);
        double consumed  = spanIn(lo, cursor, lo, hi);
        if (consumed <= 0.0) return remaining;
        return ((double) keysEmitted / consumed) * remaining;
    }

    /**
     * The <b>cursor-anchored</b> geometric factor: remaining span over consumed span, both read in a
     * window anchored at the cursor's OWN divergence from {@code lo} rather than at the divergence of
     * {@code [lo, hi]} that {@link #fracIn} reads. The ratio {@link #estRemaining} multiplies its
     * observed density by, measured where a deep-nested cursor actually moves.
     *
     * <p>Let {@code d = cpl(lo, cursor)} and {@code d0 = cpl(lo, hi)}; a cursor inside {@code (lo, hi]}
     * always has {@code d >= d0}. Where {@code d == d0} the anchor did not move and every term is
     * {@link #fracIn}'s own, digit for digit — the factor is exactly the shipped reading's
     * {@code remaining / consumed}. Where {@code d > d0} the cursor has descended into a subtree it
     * shares with {@code lo}: the window is read from byte {@code d} and its ceiling is the top of that
     * shared prefix (the constant 1.0 in that frame), which is inside {@code [lo, hi]}, so the frame
     * stays closed and both terms stay ordinary fractions. That deeper frame therefore reads
     * "at the rate this range has consumed the subtree its cursor is in, how much of that subtree is
     * left", and — its ceiling being 1.0 — drops below one only for a cursor diverging from {@code lo}
     * on a byte at or above {@code 0x80}, which object keys do not do.
     *
     * <p>Returns the neutral {@code 1.0} where there is no consumed evidence to anchor (an open
     * frontier, a cursor at or before {@code lo}, or a consumed span that underflows to zero), so a
     * caller that wants only the geometry gets a neutral answer rather than a degenerate one.
     */
    public static double anchoredGeometricFactor(byte[] cursor, byte[] lo, byte[] hi) {
        byte[] cur = (cursor == null) ? new byte[0] : cursor;
        byte[] loKey = (lo == null) ? new byte[0] : lo;
        if (hi == null || KeyBytes.compareUnsigned(cur, loKey) <= 0) return 1.0;
        int d  = commonPrefixLen(loKey, cur);
        int d0 = commonPrefixLen(loKey, hi);
        double consumed = fracFromOffset(cur, d) - fracFromOffset(loKey, d);
        if (consumed <= 0.0) return 1.0;
        double top = (d <= d0) ? fracFromOffset(hi, d) : 1.0;
        return Math.max(0.0, top - fracFromOffset(cur, d)) / consumed;
    }

    // -------------------------------------------------------------------------
    // §3.1 — extrapolate / prefixCeil (open frontier pivot)
    // -------------------------------------------------------------------------

    /**
     * Pick the split pivot for the open-frontier worker (victim's {@code hi == null}) by
     * reflecting the consumed span {@code (lo, c]} forward in code-point space, capped strictly
     * below the effective ceiling. Always valid UTF-8 and strictly between {@code c} and the
     * ceiling, so it transmits byte-exact through {@code startAfter} and never trips
     * {@code byteMidpoint}'s precondition — neither in the thief's retry-nearer-cursor nor when
     * it is later stored as a range bound.
     *
     * <p>The reflected pivot shares {@code c}'s prefix, so an overshoot is refined by the thief's
     * empty-probe retry. If the reflection has <b>no forward room</b> (cursor at {@code lo}, an
     * un-started frontier) the pivot is {@code null} — the owner finishes the frontier, never a
     * blind midpoint gallop (algorithms.md §3.1). If it only <b>overshoots the ceiling</b>, the
     * pivot caps to the plain code-point {@link ByteMidpoint#between midpoint} of
     * {@code (c, ceiling]} (still valid UTF-8). The effective ceiling is {@code ceil} when it is
     * non-{@code null} and valid UTF-8; a whole-bucket scope ({@code ceil == null}) or a
     * {@code prefixCeil} that bumped a byte into an invalid UTF-8 sequence falls back to
     * {@link #MAX_UTF8_KEY}.
     *
     * @param lo   the worker's range start (⊥ when {@code null}); a real, valid-UTF-8 key or ⊥
     * @param c    the worker's current cursor (a real, valid-UTF-8 emitted key, or {@code null} = ⊥)
     * @param ceil ceiling key = {@link #prefixCeil(byte[])} of the scan prefix, or {@code null}
     *             for a whole-bucket scope
     * @return a valid-UTF-8 pivot {@code m} with {@code c < m < ceil} (unsigned byte order),
     *         or {@code null} if {@code c} is at/after the ceiling or the bounds are
     *         UTF-8-adjacent (no key strictly between)
     */
    public static byte[] extrapolate(byte[] lo, byte[] c, byte[] ceil) {
        byte[] cForPivot = (c == null) ? new byte[0] : c;
        byte[] top = effectiveCeiling(ceil);
        if (KeyBytes.compareUnsigned(cForPivot, top) >= 0) {
            if (log.isDebugEnabled()) {
                log.debug("pivot_extrapolate_failed reason={} cursor={} ceiling={}",
                        "cursor_at_or_after_ceiling", describe(c), describe(top));
            }
            return null;   // cursor at/after the ceiling — no forward room
        }
        byte[] reflected = ByteMidpoint.forwardReflect(lo, cForPivot);
        if (reflected == null) {
            // No consumed span to reflect (cursor == lo, an un-started frontier): return null so
            // the owner finishes it rather than blind-galloping a split (algorithms.md §3.1).
            if (log.isDebugEnabled()) {
                log.debug("pivot_extrapolate_failed reason={} lo={} cursor={}",
                        "no_consumed_span", describe(lo), describe(c));
            }
            return null;
        }
        if (KeyBytes.compareUnsigned(reflected, top) < 0) {
            if (log.isDebugEnabled()) {
                log.debug("pivot_extrapolated lo={} cursor={} pivot={} ceiling={}",
                        describe(lo), describe(c), describe(reflected), describe(top));
            }
            return reflected;
        }
        byte[] capped = ByteMidpoint.between(cForPivot, top);   // reflection overshot the ceiling → cap below it
        if (log.isDebugEnabled()) {
            log.debug("pivot_extrapolated_capped lo={} cursor={} pivot={} ceiling={}",
                    describe(lo), describe(c), describe(capped), describe(top));
        }
        return capped;
    }

    /**
     * Bounded-range <b>far-ahead fractional pivot</b>. Returns a
     * valid-UTF-8 {@code m} with {@code lo <_u m <_u hi} placed at fraction {@code f} of the way
     * from {@code lo} to {@code hi} in Unicode <b>code-point</b> space (never raw-byte slicing —
     * I10/I12), reusing {@link ByteMidpoint#between(byte[], byte[], double)}'s safe-scalar synthesis
     * (the synthesized divergence scalar is drawn only from the safe set {@code E^c}, so an XML-safe
     * bound yields an XML-safe pivot — I12). {@code f == 0.5} coincides <b>byte-for-byte</b> with
     * {@link ByteMidpoint#between(byte[], byte[])}; a larger {@code f} places the split farther ahead
     * of the cursor, so a fast drainer cannot advance past the pivot before the split CAS commits
     * (algorithms.md §3).
     *
     * <p>Interpolation is for <b>bounded</b> ranges only: {@code hi == null} (the open frontier) is
     * balanced by {@link #extrapolate} density reflection and returns {@code null} here. Returns
     * {@code null} otherwise iff the range is unsplittable — the bounds are UTF-8-adjacent — which is
     * the <b>identical</b> {@code null} contract as {@link ByteMidpoint#between(byte[], byte[])} for
     * every {@code f ∈ (0,1)} (fraction shifts only the synthesized scalar, never whether one exists).
     *
     * @param lo  the lower bound (the cursor / {@code ⊥} when empty); valid UTF-8
     * @param hi  the upper bound; {@code null} = open frontier (returns {@code null})
     * @param f   the target fraction; clamped into {@code (0,1)}
     */
    public static byte[] interpolate(byte[] lo, byte[] hi, double f) {
        if (hi == null) {
            return null;                                   // open frontier — use extrapolate, not interpolate
        }
        byte[] a = (lo == null) ? new byte[0] : lo;
        if (KeyBytes.compareUnsigned(a, hi) >= 0) {
            return null;                                   // no room to split (lo at/after hi)
        }
        double frac = Math.min(1.0 - 1e-9, Math.max(1e-9, f));
        return ByteMidpoint.between(a, hi, frac);
    }

    /**
     * Alphabet-aware (rank-space) variant of {@link #interpolate(byte[], byte[], double)}: the
     * synthesized divergence scalar is drawn from the worker's observed per-position alphabet
     * ({@code digest}) so the pivot lands on a <b>populated</b> value (hex/UUID/base64/…) instead of a
     * sparse-alphabet dead zone. Identical in every other respect — same {@code null} unsplittable
     * contract, same {@code lo <_u m <_u hi} betweenness, same I12 safe-scalar guarantee. A {@code null}
     * {@code digest}, or a position with no clean alphabet signal (cold start / non-ASCII keyspace),
     * falls back <b>byte-for-byte</b> to {@link #interpolate(byte[], byte[], double)}.
     */
    public static byte[] interpolate(byte[] lo, byte[] hi, double f, AlphabetDigest.Snapshot digest) {
        if (hi == null) {
            return null;
        }
        byte[] a = (lo == null) ? new byte[0] : lo;
        if (KeyBytes.compareUnsigned(a, hi) >= 0) {
            return null;
        }
        double frac = Math.min(1.0 - 1e-9, Math.max(1e-9, f));
        return ByteMidpoint.between(a, hi, frac, digest);
    }

    /**
     * Alphabet-aware variant of {@link #interpolate(byte[], byte[], double, AlphabetDigest.Snapshot)} that
     * reports the digest's per-consult {@code ALPHABET.*} fallback reason (if any) into {@code
     * collector} — a caller-owned {@link Engagement} list, never {@code RunMetrics} directly (issue
     * #19's fix, contracts.md §2.1): {@code ThiefPolicy}'s pivot cascade and {@code
     * OwnerSplitGovernor}'s carve each pass their own pending-engagement list, so the fallback is
     * delivered to the executor exactly like every other engagement they already collect. Identical
     * in every other respect to the two-argument-fewer overload above; a {@code null} {@code digest}
     * or {@code collector} is exactly as inert as a {@code null} {@code digest} there.
     */
    public static byte[] interpolate(byte[] lo, byte[] hi, double f, AlphabetDigest.Snapshot digest,
                                      List<Engagement> collector) {
        if (hi == null) {
            return null;
        }
        byte[] a = (lo == null) ? new byte[0] : lo;
        if (KeyBytes.compareUnsigned(a, hi) >= 0) {
            return null;
        }
        double frac = Math.min(1.0 - 1e-9, Math.max(1e-9, f));
        ByteMidpoint.ScalarChooser chooser = (digest == null) ? null
                : (cpIndex, loCp, hiCp, fraction) -> digest.chooseScalar(cpIndex, loCp, hiCp, fraction, collector);
        return ByteMidpoint.between(a, hi, frac, chooser);
    }

    /**
     * Discriminate the <b>two distinct</b> {@code null} returns of {@link #extrapolate} for an
     * open-frontier victim (algorithms.md §3.1): is the victim merely <i>un-started</i> (cursor
     * still at {@code lo} — ⊥ for the root seed — so there is <b>no consumed span</b> to reflect
     * forward yet), as opposed to genuinely exhausted (cursor at/after the ceiling, or
     * UTF-8-adjacent to it)?
     *
     * <p>This is the <b>transient</b> case: the owner simply hasn't committed page 1 yet. The thief
     * must treat it as a RETRY and must <b>not</b> cache the victim {@code unsplittable} — otherwise
     * the seed victim, which owns the whole keyspace, is permanently excluded from every future
     * steal and the scan collapses to a single worker. Once the owner commits a page a consumed span
     * exists, {@link ByteMidpoint#forwardReflect} yields a real pivot and the steal succeeds; the
     * owner always makes progress, so the RETRY converges (algorithms.md §3.1).
     *
     * @param lo the victim's range start (⊥ when {@code null})
     * @param c  the victim's snapshot cursor (⊥ when {@code null})
     * @return {@code true} iff {@code c} is at/before {@code lo} (no consumed span — transient);
     *         {@code false} when the cursor has advanced (the {@code null} was the genuine
     *         terminal case)
     */
    public static boolean isUnstartedFrontier(byte[] lo, byte[] c) {
        byte[] loForCmp = (lo == null) ? new byte[0] : lo;
        byte[] cForCmp  = (c  == null) ? new byte[0] : c;
        return KeyBytes.compareUnsigned(cForCmp, loForCmp) <= 0;
    }

    /**
     * The valid-UTF-8 ceiling to split toward: {@code ceil} itself when usable, else
     * {@link #MAX_UTF8_KEY} (whole-bucket scope, or a {@code prefixCeil} that is not valid UTF-8).
     */
    private static byte[] effectiveCeiling(byte[] ceil) {
        return (ceil == null || !ByteMidpoint.isValidUtf8(ceil)) ? MAX_UTF8_KEY : ceil;
    }

    /**
     * The smallest key strictly greater than every key that starts with {@code prefix}:
     * drop trailing {@code 0xFF} bytes of {@code prefix}, then increment the last
     * remaining byte.
     *
     * @return the ceiling key, or {@code null} if {@code prefix} is {@code null},
     *         empty, or all-{@code 0xFF} bytes (ceiling is ⊤ / unbounded)
     */
    public static byte[] prefixCeil(byte[] prefix) {
        if (prefix == null || prefix.length == 0) return null;

        int last = prefix.length - 1;
        while (last >= 0 && (prefix[last] & 0xFF) == 0xFF) {
            last--;
        }
        if (last < 0) return null;    // all 0xFF — ceiling is unbounded

        byte[] ceil = new byte[last + 1];
        System.arraycopy(prefix, 0, ceil, 0, last + 1);
        ceil[last]++;
        return ceil;
    }

    // -------------------------------------------------------------------------
    // Owner-split pivot math ({@link OwnerSelfSplit#maybeOwnerSelfSplit}'s child-tail floor,
    // reflection clamp, and reflect-lift decisions — sibling arithmetic to extrapolate/interpolate
    // above, built on the same spanIn/extrapolate primitives, so it lives here rather than beside
    // the scan driver.)
    // -------------------------------------------------------------------------

    /**
     * The owner-split observed-mass child-tail floor. The child of an owner-split owns the far
     * {@code (m, H]} tail; if that tail's <i>realized</i> key mass is below two pages the carve
     * fissions into sub-two-page "confetti" that buys no parallelism, so it must be skipped.
     *
     * <p>A plain span floor ({@code (1-f)*est}, the drained density extrapolated uniformly to
     * {@code H}) over-passes on a skewed keyspace whose thinning tail is sparser than that average.
     * The correction uses the worker's observed {@code densityRatio = trailingEwmaDensity /
     * averageDensity} (see {@link WorkerState#observedDensityRatio()}): mass ahead reaches only about
     * {@code min(1, densityRatio)} of the remaining span, so the child tail's realized share of
     * {@code est} is {@code max(0, min(1, densityRatio) − f)}; block iff {@code est × that <=
     * 2*maxKeys}. On a uniform / still-dense keyspace ({@code densityRatio >= 1}, incl. the no-signal
     * fallback {@code +∞}) this reduces byte-for-byte to the plain {@code (1-f)*est} floor, so honest
     * splits are unaffected; on a thinning tail ({@code densityRatio < f}) the reachable tail
     * collapses to ~0 and the confetti carve is blocked. See metrics §5 for the derivation.
     *
     * <p>Public static (pure arithmetic, no engine state): a unit test can exercise the exact
     * boundary without driving the whole engine to a precise input combination, and
     * {@code io.varve.swath.engine.policy}'s owner-split governor calls this directly (source-agnostic
     * — no engine type crosses the package boundary).
     */
    public static boolean childTailBelowObservedMassFloor(double est, double f, double densityRatio, int maxKeys) {
        double reach = Math.min(1.0, densityRatio);       // thinning (ratio < 1) shrinks the reachable tail
        double realizedChildMass = est * Math.max(0.0, reach - f);
        return realizedChildMass <= 2.0 * (double) maxKeys;
    }

    /**
     * The owner-split reflection-clamp decision. Clamp the {@code f}-interpolated owner
     * pivot {@code m} down to the density-reflected pivot {@code mReflect = extrapolate(lo, cursor, H)}
     * iff (a) {@code mReflect} is a real pivot strictly in {@code (cursor, m)} — i.e. the interpolate
     * overshot the observed mass — AND (b) the clamped child tail {@code (mReflect, H]} still clears the
     * observed-mass {@link #childTailBelowObservedMassFloor floor}. On a uniform keyspace the
     * reflected pivot reaches at least as far as the far-ahead fraction ({@code mReflect >= m}), so the
     * clamp never engages (f's skew is load-bearing there — do not clamp blind). Public static
     * (pure arithmetic + byte compares, no engine state): the exact decision is unit-testable without
     * driving the whole engine to a precise pivot/density combination, and
     * {@code io.varve.swath.engine.policy}'s owner-split governor calls this directly.
     */
    public static boolean shouldClampToReflected(byte[] cursor, byte[] m, byte[] mReflect, byte[] lo, byte[] H,
                                          double est, double densityRatio, int maxKeys) {
        if (mReflect == null
                || KeyBytes.compareUnsigned(cursor, mReflect) >= 0
                || KeyBytes.compareUnsigned(mReflect, m) >= 0) {
            return false;                                  // no overshoot to clamp (reflection reached >= m)
        }
        double remainingFrac = spanIn(cursor, H, lo, H);
        double fReflect = (remainingFrac > 0.0)
                ? spanIn(cursor, mReflect, lo, H) / remainingFrac
                : 1.0;
        return !childTailBelowObservedMassFloor(est, fReflect, densityRatio, maxKeys);
    }

    /**
     * Whether the final post-clamp owner-split pivot {@code m} should be LIFTED up to the
     * density-reflected pivot {@code mReflect} (see {@link OwnerSelfSplit#maybeOwnerSelfSplit}'s
     * call-site javadoc for the design). Lifts only when ALL of:
     * <ol>
     *   <li>the owner's kept share {@code (cursorTo, m]}, measured in {@code est}'s OWN
     *       {@code [lo, H]} frame ({@code fKeptLo}, not a re-scoped {@code [cursorTo, H]} span), is
     *       below one page ({@code est * fKeptLo <= maxKeys});</li>
     *   <li>{@code mReflect} is real and moves {@code m} strictly UP ({@code m < mReflect}) — this
     *       method only ever lifts; {@link #shouldClampToReflected} owns the down direction and the
     *       two are mutually exclusive (this runs only after the clamp);</li>
     *   <li>{@code mReflect} strictly precedes {@code H} (room to lift into); and</li>
     *   <li>the LIFTED child tail {@code (mReflect, H]} still clears the floor
     *       ({@link #childTailBelowObservedMassFloor}) — never lift into a carve that would itself
     *       fission the child into confetti.</li>
     * </ol>
     * Any condition failing returns {@code false} and the caller publishes the unchanged carve at
     * {@code m}. Public static (pure arithmetic + byte compares, no engine state): the exact
     * boundary is unit-testable without driving the whole engine to a degenerate-pivot shape, and
     * {@code io.varve.swath.engine.policy}'s owner-split governor calls this directly.
     */
    public static boolean shouldLiftToReflected(byte[] cursorTo, byte[] m, byte[] mReflect, byte[] lo, byte[] H,
                                         double est, double densityRatio, int maxKeys) {
        double remSpan = spanIn(cursorTo, H, lo, H);
        double fKeptLo = spanIn(cursorTo, m, lo, H) / Math.max(1e-300, remSpan);
        if (est * fKeptLo > (double) maxKeys) {
            return false;   // owner already keeps at least one page in est's own frame — no lift needed
        }
        if (mReflect == null
                || KeyBytes.compareUnsigned(m, mReflect) >= 0
                || KeyBytes.compareUnsigned(mReflect, H) >= 0) {
            return false;   // nothing to lift to, or mReflect doesn't actually move m up with room to spare
        }
        double fReflectLifted = (remSpan > 0.0) ? spanIn(cursorTo, mReflect, lo, H) / remSpan : 1.0;
        return !childTailBelowObservedMassFloor(est, fReflectLifted, densityRatio, maxKeys);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Window-relative base-256 fraction: reads up to {@value #K} bytes of {@code key}
     * starting at byte {@code offset}.  A key shorter than {@code offset} → 0.0.
     */
    private static double fracFromOffset(byte[] key, int offset) {
        if (key == null || key.length <= offset) return 0.0;
        double f     = 0.0;
        double scale = 1.0 / 256.0;
        int    limit = Math.min(key.length, offset + K);
        for (int i = offset; i < limit; i++) {
            f += (key[i] & 0xFF) * scale;
            scale /= 256.0;
        }
        return f;
    }

    /**
     * Length (in bytes) of the longest common byte prefix of {@code a} and {@code b}.
     * {@code null} is treated as an empty array.
     */
    private static int commonPrefixLen(byte[] a, byte[] b) {
        if (a == null || b == null) return 0;
        int n = Math.min(a.length, b.length);
        int i = 0;
        while (i < n && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    /**
     * Debug-log rendering of a key: {@code "<none>"} for {@code null}, else the UTF-8 decode with
     * control characters escaped. Package-private (not {@code private}) so {@link WorkStealingScan}
     * and {@link Thief} share this one implementation instead of each carrying their own byte-identical
     * copy.
     */
    static String describe(byte[] raw) {
        if (raw == null) {
            return "<none>";
        }
        return ControlCharEscaper.escape(new String(raw, StandardCharsets.UTF_8));
    }
}
