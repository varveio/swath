/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ByteMidpoint;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the observed-alphabet digest and its consumer
 * ({@link AlphabetDigest}, {@link StealMath#interpolate(byte[], byte[], double, AlphabetDigest)}).
 * The cross-cutting byte-exact / partition guard (PROP-1) and the acceptance test live in a
 * separate test class.
 */
final class AlphabetDigestTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // -------------------------------------------------------------------------
    // chooseScalar — alphabet-aware synthesis.
    // -------------------------------------------------------------------------

    @Test
    void chooseScalarPicksAPopulatedValueAtRankFraction() {
        // base = common prefix of ("h/0","h/f") = "h/" (len 2); the divergent position is index 2.
        AlphabetDigest d = new AlphabetDigest(b("h/0"), b("h/f"));
        for (String k : new String[] {"h/0", "h/3", "h/7", "h/a", "h/f"}) {
            d.observe(b(k));
        }
        // Between '0' and 'f' the observed alphabet in the OPEN interval is {'3','7','a'}; f=0.5 picks
        // the middle populated value '7' — never a dead-zone scalar.
        int v = d.chooseScalar(2, '0', 'f', 0.5);
        assertThat(v).as("mid-rank populated value between '0' and 'f'").isEqualTo('7');
        // A larger fraction lands nearer hi (still a populated value).
        assertThat(d.chooseScalar(2, '0', 'f', 0.99)).isEqualTo('a');
    }

    @Test
    void chooseScalarReturnsNoScalarInADeadZone() {
        AlphabetDigest d = new AlphabetDigest(b("h/0"), b("h/f"));
        for (String k : new String[] {"h/0", "h/3", "h/7", "h/a", "h/f"}) {
            d.observe(b(k));
        }
        // Nothing observed strictly between '7' and 'a' (the classic hex dead zone) → fall back.
        assertThat(d.chooseScalar(2, '7', 'a', 0.5)).isEqualTo(ByteMidpoint.ScalarChooser.NO_SCALAR);
        // Adjacent bounds — no scalar strictly between.
        assertThat(d.chooseScalar(2, '3', '4', 0.5)).isEqualTo(ByteMidpoint.ScalarChooser.NO_SCALAR);
    }

    @Test
    void chooseScalarWithNoSignalFallsBack() {
        AlphabetDigest cold = new AlphabetDigest(b("h/0"), b("h/f"));   // nothing observed yet
        assertThat(cold.chooseScalar(2, '0', 'f', 0.5))
                .as("cold start: no alphabet signal → default synthesis")
                .isEqualTo(ByteMidpoint.ScalarChooser.NO_SCALAR);

        AlphabetDigest d = new AlphabetDigest(b("h/0"), b("h/f"));
        d.observe(b("h/7"));
        // A position outside the tracked window has no signal.
        assertThat(d.chooseScalar(2 + AlphabetDigest.MAX_POSITIONS, '0', 'f', 0.5))
                .isEqualTo(ByteMidpoint.ScalarChooser.NO_SCALAR);
    }

    @Test
    void nonAsciiPositionYieldsNoSignal() {
        AlphabetDigest d = new AlphabetDigest(b("h/0"), b("h/z"));
        d.observe(b("h/7"));
        d.observe("h/é".getBytes(StandardCharsets.UTF_8));   // multi-byte scalar at index 2
        assertThat(d.chooseScalar(2, '0', 'z', 0.5))
                .as("a non-ASCII observation marks the position not-clean → fall back")
                .isEqualTo(ByteMidpoint.ScalarChooser.NO_SCALAR);
    }

    // -------------------------------------------------------------------------
    // StealMath.interpolate with the digest — populated pivots + byte-exact fallback.
    // -------------------------------------------------------------------------

    @Test
    void interpolateLandsOnAPopulatedValueInsteadOfADeadZone() {
        // Between '9'(0x39) and 'B'(0x42) the plain code-point midpoint is '='(0x3D) — a dead zone for
        // a keyspace whose populated alphabet at this position is {'9','A','B'}. The alphabet-aware
        // pivot must instead land on the observed 'A'.
        AlphabetDigest d = new AlphabetDigest(b("p9"), b("pB"));
        for (String k : new String[] {"p9", "pA", "pB"}) {
            d.observe(b(k));
        }
        byte[] plain = StealMath.interpolate(b("p9"), b("pB"), 0.5);
        byte[] aware = StealMath.interpolate(b("p9"), b("pB"), 0.5, d);

        assertThat(plain).as("plain midpoint lands in the dead zone").isEqualTo(b("p="));
        assertThat(aware).as("alphabet-aware pivot lands on the observed 'A'").isEqualTo(b("pA"));
        // Betweenness holds either way.
        assertThat(Arrays.compareUnsigned(b("p9"), aware)).isLessThan(0);
        assertThat(Arrays.compareUnsigned(aware, b("pB"))).isLessThan(0);
    }

    @Test
    void interpolateFallsBackByteForByteWhenGapUnpopulated() {
        AlphabetDigest d = new AlphabetDigest(b("p9"), b("pB"));
        d.observe(b("p9"));
        d.observe(b("pB"));   // nothing observed strictly between '9' and 'B'
        for (double f : new double[] {0.1, 0.5, 0.75, 0.9}) {
            assertThat(StealMath.interpolate(b("p9"), b("pB"), f, d))
                    .as("unpopulated gap → identical to the no-digest pivot at f=%s", f)
                    .isEqualTo(StealMath.interpolate(b("p9"), b("pB"), f));
        }
    }

    @Test
    void interpolateWithNullDigestIsByteForByteThePlainForm() {
        for (double f : new double[] {0.05, 0.5, 0.75, 0.95}) {
            assertThat(StealMath.interpolate(b("2022/03/05/1"), b("2022/03/05/9"), f, null))
                    .as("null digest reproduces the plain interpolate at f=%s", f)
                    .isEqualTo(StealMath.interpolate(b("2022/03/05/1"), b("2022/03/05/9"), f));
        }
    }

    // -------------------------------------------------------------------------
    // The step-back must drop the digest to reach a DISTINCT byte-midpoint.
    // -------------------------------------------------------------------------

    @Test
    void stepBackWithoutTheDigestReachesADistinctByteMidpoint() {
        // Sparse observed alphabet: the ONLY populated scalar in the open interval ('9','B') is 'A'
        // (0x41), so the digest-drawn pivot is 'pA' at EVERY fraction. A step-back that KEPT the digest
        // (f=0.5,d) returns the same 'pA' as the far-ahead pivot (f=0.75,d) — a no-op. The
        // true byte-midpoint (f=0.5, NO digest) instead reaches a DISTINCT scalar and is byte-identical
        // to the historical ByteMidpoint.between.
        AlphabetDigest d = new AlphabetDigest(b("p9"), b("pB"));
        for (String k : new String[] {"p9", "pA", "pB"}) {
            d.observe(b(k));
        }
        byte[] farAhead = StealMath.interpolate(b("p9"), b("pB"), 0.75, d);
        byte[] stepBackWithDigest = StealMath.interpolate(b("p9"), b("pB"), 0.5, d);
        byte[] stepBackNoDigest = StealMath.interpolate(b("p9"), b("pB"), 0.5);

        assertThat(stepBackWithDigest)
                .as("keeping the digest, the step-back is a NO-OP on a sparse alphabet")
                .isEqualTo(farAhead);
        assertThat(stepBackNoDigest)
                .as("the true byte-midpoint step-back reaches a DISTINCT pivot")
                .isNotEqualTo(farAhead);
        assertThat(stepBackNoDigest)
                .as("f=0.5 without the digest is byte-identical to the historical midpoint")
                .isEqualTo(ByteMidpoint.between(b("p9"), b("pB")));
    }

    // -------------------------------------------------------------------------
    // Publish the volatile generation on ANY observable state mutation.
    // -------------------------------------------------------------------------

    @Test
    void generationPublishesOnACleanToDirtyFlip() throws Exception {
        // A dirty-flip (clean→dirty + mask clear) with NO new printable bit set must STILL bump the
        // volatile generation, or a concurrent lock-free reader keeps seeing the stale clean mask and
        // chooses an ASCII scalar where the contract now says NO_SCALAR.
        AlphabetDigest d = new AlphabetDigest(b("h/0"), b("h/f"));
        d.observe(b("h/0"));                                     // pos0 sees '0' → a bit set, gen bumped
        int before = generation(d);

        d.observe("h/\n".getBytes(StandardCharsets.UTF_8));      // pos0 sees 0x0A (< 0x20): dirty, no new bit
        int after = generation(d);

        assertThat(after).as("generation republished on the clean→dirty flip").isGreaterThan(before);
        assertThat(d.chooseScalar(2, '0', 'f', 0.5))
                .as("the dirtied position now yields no alphabet signal")
                .isEqualTo(ByteMidpoint.ScalarChooser.NO_SCALAR);
    }

    private static int generation(AlphabetDigest d) throws Exception {
        Field f = AlphabetDigest.class.getDeclaredField("generation");
        f.setAccessible(true);
        return f.getInt(d);
    }
}
