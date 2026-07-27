/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.StealMath;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * What each candidate position sensor claims about itself, pinned as arithmetic rather than as a run.
 * The race measures what the variants do to a fleet; this measures what they are.
 *
 * <p>The worked example throughout is the one the defect was diagnosed on: two species directories
 * whose names diverge at byte ten, and a cursor a further twenty-odd bytes down inside the first of
 * them. That is the case where the shipped window reads a thousand committed keys as no movement at
 * all.
 */
class SensingEstimatorTest {

    private static final byte[] LO = key("species/Balearica_regulorum/bBalReg1/");
    private static final byte[] HI = key("species/Bathysaurus_mollis/fBatMol1/");
    private static final byte[] CURSOR_EARLY =
            key("species/Balearica_regulorum/bBalReg1/assembly_vgp/intermediates/00417");
    private static final byte[] CURSOR_LATER =
            key("species/Balearica_regulorum/bBalReg1/assembly_vgp/intermediates/09999");

    private static byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void theShippedWindowCannotSeeACursorMoveInsideTheSubtree() {
        // The premise the cures answer, restated here so the tests below are read against it: a real
        // advance of thousands of keys, and a position metric that does not move.
        assertThat(new WindowEstimator().advanceVisible(LO, CURSOR_EARLY, CURSOR_LATER, HI)).isFalse();
        assertThat(StealMath.spanIn(LO, CURSOR_LATER, LO, HI))
                .as("the consumed span the estimate divides by").isZero();
        assertThat(StealMath.estRemaining(CURSOR_LATER, LO, HI, 400_000L))
                .as("so the estimate is a raw width and the emitted keys are gone")
                .isEqualTo(StealMath.estRemaining(CURSOR_LATER, LO, HI, 0L));
    }

    @Test
    void theAnchoredWindowKeepsTheEmittedKeysAndSeesMovesWithinItsOwnWidth() {
        CursorAnchoredEstimator anchored = new CursorAnchoredEstimator();
        // The move the shipped window cannot see at all -- the cursor leaving lo -- is visible here.
        assertThat(anchored.advanceVisible(LO, LO, CURSOR_EARLY, HI)).isTrue();
        assertThat(new WindowEstimator().advanceVisible(LO, LO, CURSOR_EARLY, HI)).isFalse();
        // But re-anchoring buys a window, not unlimited resolution: this pair differs 26 bytes below
        // the anchor, past the window's own width, and reads as no movement here too. Pinned because
        // it is the honest limit of this variant and the reason a second window is a separate
        // candidate.
        assertThat(anchored.advanceVisible(LO, CURSOR_EARLY, CURSOR_LATER, HI)).isFalse();
        // What it does fix unconditionally: the consumed span is positive, so the emitted keys survive.
        assertThat(anchored.ignoresEmittedKeys(CURSOR_LATER, LO, HI)).isFalse();
        // And the estimate now responds to how much the range has produced, monotonically.
        assertThat(anchored.estRemaining(CURSOR_LATER, LO, HI, 400_000L))
                .isGreaterThan(anchored.estRemaining(CURSOR_LATER, LO, HI, 4_000L));
        // It stays in key units rather than exploding: a density extrapolated across the twenty-six
        // bytes between the two divergences would read in the 10^60 range, which no floor can use.
        assertThat(anchored.estRemaining(CURSOR_LATER, LO, HI, 400_000L)).isLessThan(1e9);
    }

    @Test
    void theAnchoredWindowIsTheShippedOneWhereverTheShippedOneCanSeeTheCursor() {
        // A cursor that diverges from lo exactly where hi does -- the healthy case, and every case on a
        // keyspace with no deep shared prefix. Re-anchoring is a no-op there, digit for digit, which is
        // what keeps a well-splitting bucket out of this variant's blast radius by construction.
        CursorAnchoredEstimator anchored = new CursorAnchoredEstimator();
        byte[][] cursors = {key("species/Bat"), key("species/Bathy"),
            key("species/Bathysaurus_mollis/f")};
        for (byte[] cursor : cursors) {
            assertThat(anchored.estRemaining(cursor, LO, HI, 12_345L))
                    .as("cursor %s", new String(cursor, StandardCharsets.UTF_8))
                    .isEqualTo(StealMath.estRemaining(cursor, LO, HI, 12_345L));
        }
        // Including the un-started range, where neither has any consumed evidence to anchor.
        assertThat(anchored.estRemaining(LO, LO, HI, 0L))
                .isEqualTo(StealMath.estRemaining(LO, LO, HI, 0L));
    }

    @Test
    void theRateEstimateIsExactlyWhatTheRangeHasProduced() {
        RateEstimator rate = new RateEstimator(100);
        assertThat(rate.estRemaining(CURSOR_LATER, LO, null, 7L))
                .as("an open frontier always outranks a bounded range, as it does today")
                .isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(rate.estRemaining(HI, LO, HI, 400_000L))
                .as("a cursor at the bound is done -- an exact comparison, not an inference").isZero();
        assertThat(rate.estRemaining(CURSOR_LATER, LO, HI, 400_000L)).isEqualTo(400_000.0);
        assertThat(rate.estRemaining(CURSOR_LATER, LO, HI, 0L))
                .as("and a range with no evidence yet is presumed to hold the page in flight")
                .isEqualTo(100.0);
        assertThat(rate.ignoresEmittedKeys(CURSOR_LATER, LO, HI)).isFalse();
        assertThat(rate.advanceVisible(LO, CURSOR_EARLY, CURSOR_LATER, HI)).isTrue();
    }

    @Test
    void theCombinedEstimateLetsGeometryAdjustTheRateWithinItsBand() {
        RateAnchoredEstimator combined = new RateAnchoredEstimator(100);
        double band = RateAnchoredEstimator.GEOMETRY_BAND;
        for (byte[] cursor : new byte[][] {CURSOR_EARLY, CURSOR_LATER, key("species/Bat")}) {
            assertThat(combined.estRemaining(cursor, LO, HI, 400_000L))
                    .as("cursor %s stays inside the band", new String(cursor, StandardCharsets.UTF_8))
                    .isBetween(400_000.0 / band, 400_000.0 * band);
        }
        assertThat(combined.estRemaining(HI, LO, HI, 400_000L)).isZero();
        assertThat(combined.estRemaining(CURSOR_LATER, LO, null, 400_000L))
                .isEqualTo(Double.POSITIVE_INFINITY);
    }
}
