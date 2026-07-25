/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import static io.varve.swath.testkit.CodePointKeys.b;
import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.testkit.CodePointKeys;
import io.varve.swath.testkit.ScalarSafety;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;

/**
 * PROP-2 adversarial guard: {@code ByteMidpoint} must never synthesize a pivot whose
 * INVENTED (divergence / append / reflect / cap-fallback) scalar is {@code U+0025} ('%'). A
 * lone or trailing '%' in a synthesized {@code start-after} cursor crashes verbatim-echo
 * S3-compatible endpoints (LocalStack, MinIO): those endpoints echo the request parameter back
 * un-re-percent-encoded, and the AWS SDK's own always-on response interceptor strict-decodes it
 * with {@link java.net.URLDecoder}, which throws {@code IllegalArgumentException("Incomplete
 * trailing escape (%) pattern")} on it. Real S3 re-encodes its echo and is unaffected. See
 * {@code ByteMidpoint#isSafe} and {@code docs/internals/s3-implementation-compatibility.md}.
 *
 * <p>This is deliberately NARROWER and more adversarial than the general whole-output safety
 * properties in {@link ByteMidpointPropertyTest} ({@code betweenPivotsAreAlwaysSafe} /
 * {@code forwardReflectPivotsAreAlwaysSafe}, which already cover ALL of {@code E} via
 * {@link ScalarSafety#isExcludedScalar} — but sample UNIFORMLY across the whole scalar space, so
 * a single-code-point target like {@code 0x25} out of ~1.1M candidates is essentially never hit
 * by chance in a few thousand tries). This class instead (a) pins the exact pre-fix regression
 * case, and (b) samples a NARROW band bracketing '%' so every branch of {@code between}/
 * {@code forwardReflect} that could invent a scalar is repeatedly forced to weigh {@code 0x25} as
 * a candidate.
 */
class ByteMidpointPercentPivotPropertyTest {

    /**
     * THE known regression trigger: {@code between({0x24}, {0x26})} ('$' .. '&', bracketing '%' with a
     * gap of exactly 2 — the only shape where the natural midpoint IS the excluded scalar).
     * Pre-fix this returned the single-scalar pivot {@code [0x25]} ('%') directly from the
     * divergence branch; post-fix the divergence branch finds no safe scalar in {@code (0x24,
     * 0x26)}, falls back to {@code a ++ MIN_SAFE}, and returns {@code [0x24, 0x20]}.
     */
    @Test
    void knownTriggerDollarAmpersandBracketNeverYieldsPercentPivot() {
        byte[] m = ByteMidpoint.between(b(0x24), b(0x26));
        assertThat(m).as("pre-fix regression: between($,&) returned bare '%'").isNotEqualTo(b(0x25));
        if (m != null) {
            assertNoPercentScalar(m);
        }
    }

    /**
     * Same bracket, but the divergence sits at a non-zero shared-prefix depth ("A$" vs "A&")
     * instead of at position 0 — exercises the {@code a[0..i-1]·MIN_SAFE} append-fallback branch
     * with a real copied prefix, not just the trivial single-code-point case above.
     */
    @Test
    void bracketAtNonZeroPrefixDepthNeverSynthesizesPercent() {
        byte[] a = b(0x41, 0x24);   // "A$"
        byte[] bb = b(0x41, 0x26);  // "A&"
        byte[] m = ByteMidpoint.between(a, bb);
        assertThat(m).as("pivot must exist: append-fallback always applies here").isNotNull();
        assertThat(Arrays.compareUnsigned(a, m)).as("a < m").isNegative();
        assertThat(Arrays.compareUnsigned(m, bb)).as("m < b").isNegative();
        assertNoPercentScalar(m);
    }

    /**
     * Cap-fallback branch, deterministically forced to bump the SAME '$'/'%'/'&' bracket:
     * {@code a} is 1024 code points — {@code '$'} (0x24) followed by 1023 copies of
     * {@code U+10FFFD} (a scalar with no safe scalar above it: the next two scalars,
     * {@code U+10FFFE}/{@code U+10FFFF}, are the plane-16 noncharacter pair, so
     * {@code safeAbove} exhausts) — against {@code b = [0x26]}. Every trailing position is
     * unbumpable, so {@code capFallback}'s right-to-left scan is forced all the way back to
     * position 0: {@code safeAbove(0x24)} must skip the excluded {@code 0x25} and land on
     * {@code 0x26}, which collides exactly with {@code b} — so the correct, safe answer is
     * "unsplittable" ({@code null}), never a pivot containing {@code 0x25}. Pre-fix, {@code
     * safeAbove(0x24)} would have returned {@code 0x25} directly (a bare '%' pivot, strictly
     * {@code < b}), so this genuinely regresses to null under the fix — a deliberate,
     * correctness-neutral (balance-only) trade documented on {@code ByteMidpoint#isSafe}.
     */
    @Test
    void capFallbackNeverBumpsUnbumpableTailToPercent() {
        StringBuilder sb = new StringBuilder();
        sb.appendCodePoint(0x24);
        for (int i = 0; i < 1023; i++) {
            sb.appendCodePoint(0x10FFFD);
        }
        byte[] a = sb.toString().getBytes(StandardCharsets.UTF_8);
        byte[] bb = b(0x26);

        byte[] m = ByteMidpoint.between(a, bb);
        if (m != null) {
            assertThat(Arrays.compareUnsigned(a, m)).as("a < m").isNegative();
            assertThat(Arrays.compareUnsigned(m, bb)).as("m < b").isNegative();
            assertNoPercentScalar(m);
        }
    }

    /**
     * Adversarial property: over a NARROW band bracketing '%' (U+0020..U+0040), for every pair
     * with {@code a < b}, the synthesized pivot never carries {@code 0x25}. {@code a} is drawn
     * percent-free (mirroring {@link ByteMidpointPropertyTest#betweenPivotsAreAlwaysSafe}'s
     * asymmetry: {@code m} copies {@code a}'s prefix verbatim, so a percent-carrying {@code a}
     * would legitimately — and harmlessly, per the documented interior-'%' limitation — reappear
     * in {@code m} without that being a synthesis bug); {@code b} is the arbitrary narrow-band
     * bound (may itself carry {@code 0x25}) that drives {@code between} to repeatedly weigh
     * {@code 0x25} as an invented-scalar candidate.
     */
    @Property(tries = 5000)
    void narrowBandAroundPercentNeverSynthesizesPercent(
            @ForAll("narrowBandSafeKeys") byte[] a, @ForAll("narrowBandKeys") byte[] b) {
        Assume.that(Arrays.compareUnsigned(a, b) < 0);

        byte[] m = ByteMidpoint.between(a, b);
        if (m != null) {
            assertNoPercentScalar(m);
        }
    }

    /** Same narrow-band adversarial pressure on the open-frontier forward-pivot path. */
    @Property(tries = 5000)
    void narrowBandForwardReflectNeverSynthesizesPercent(
            @ForAll("narrowBandKeys") byte[] lo, @ForAll("narrowBandSafeKeys") byte[] c) {
        Assume.that(c.length > 0);
        byte[] m = ByteMidpoint.forwardReflect(lo, c);
        if (m != null) {
            assertNoPercentScalar(m);
        }
    }

    /**
     * Keys drawn from a NARROW scalar band (U+0020..U+0040) bracketing '%' (U+0025) — may itself
     * carry the excluded '%' (an arbitrary bound, per the asymmetry), forcing {@code between}/
     * {@code forwardReflect} to repeatedly weigh {@code 0x25} as a synthesis candidate.
     */
    @Provide
    Arbitrary<byte[]> narrowBandKeys() {
        return CodePointKeys.narrowBandKeys();
    }

    /**
     * Narrow-band keys whose scalars are all SAFE (percent-free and otherwise unexcluded): the
     * role that gets copied verbatim into {@code m} ({@code forwardReflect}'s cursor {@code c}),
     * which must itself be percent-free for the "no percent anywhere in {@code m}" check below to
     * isolate the SYNTHESIZED scalar rather than a copied one.
     */
    @Provide
    Arbitrary<byte[]> narrowBandSafeKeys() {
        return CodePointKeys.narrowBandSafeKeys();
    }

    private static void assertNoPercentScalar(byte[] m) {
        new String(m, StandardCharsets.UTF_8).codePoints().forEach(c ->
                assertThat(c).as("synthesized pivot must never carry U+0025 (percent): %s", Arrays.toString(m))
                        .isNotEqualTo(0x25));
    }
}
