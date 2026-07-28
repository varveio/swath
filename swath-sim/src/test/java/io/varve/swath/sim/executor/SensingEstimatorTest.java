/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.StealMath;
import io.varve.swath.engine.policy.OwnerSplitGovernor;
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
    void theAnchoredWindowIsTheShippedOneExactlyWhereTheCursorLeavesLoWhereHiDoes() {
        // The identity set, stated as the condition it actually is: cpl(lo, cursor) == cpl(lo, hi).
        // Re-anchoring is a no-op there, digit for digit. It is every case on a keyspace with no deep
        // shared prefix, and it is not one byte wider than that -- see the test below.
        CursorAnchoredEstimator anchored = new CursorAnchoredEstimator();
        int d0 = RemainingWorkEstimator.commonPrefixLen(LO, HI);
        byte[][] cursors = {key("species/Bat"), key("species/Bathy"),
            key("species/Bathysaurus_mollis/f")};
        for (byte[] cursor : cursors) {
            assertThat(RemainingWorkEstimator.commonPrefixLen(LO, cursor))
                    .as("cursor %s is in the identity set", new String(cursor, StandardCharsets.UTF_8))
                    .isEqualTo(d0);
            assertThat(anchored.estRemaining(cursor, LO, HI, 12_345L))
                    .as("cursor %s", new String(cursor, StandardCharsets.UTF_8))
                    .isEqualTo(StealMath.estRemaining(cursor, LO, HI, 12_345L));
        }
        // Including the un-started range, where neither has any consumed evidence to anchor.
        assertThat(anchored.estRemaining(LO, LO, HI, 0L))
                .isEqualTo(StealMath.estRemaining(LO, LO, HI, 0L));
    }

    @Test
    void theTwoReadingsDivergeOnceTheCursorLeavesLoDeeperThanHiDoes() {
        // The boundary of the identity above, pinned so the claim cannot quietly widen to "wherever the
        // shipped window can see the cursor". This cursor is one byte deeper than hi's divergence and
        // well inside the shipped window's own width, so the shipped reading is NOT degenerate -- its
        // consumed span is positive and it keeps the emitted keys. The two still disagree by more than
        // an order of magnitude, because each divides by a span measured at a different depth. Every
        // cursor with d0 < cpl(lo, cursor) < d0 + WINDOW_BYTES is this case.
        CursorAnchoredEstimator anchored = new CursorAnchoredEstimator();
        byte[] cursor = key("species/Balf");
        int d0 = RemainingWorkEstimator.commonPrefixLen(LO, HI);
        assertThat(RemainingWorkEstimator.commonPrefixLen(LO, cursor))
                .as("strictly deeper than hi's divergence, and inside the shipped window")
                .isBetween(d0 + 1, d0 + RemainingWorkEstimator.WINDOW_BYTES - 1);
        assertThat(new WindowEstimator().ignoresEmittedKeys(cursor, LO, HI))
                .as("the shipped window can see this cursor -- this is not the degenerate case")
                .isFalse();
        assertThat(anchored.estRemaining(cursor, LO, HI, 12_345L))
                .as("and the two readings are more than an order of magnitude apart anyway")
                .isLessThan(StealMath.estRemaining(cursor, LO, HI, 12_345L) / 10.0);
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
        RateAnchoredEstimator combined =
                new RateAnchoredEstimator(100, RateAnchoredEstimator.SYMMETRIC_MIN_GEOMETRY);
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

    /**
     * The lift-only band, pinned against the reading it exists to remove. The cursor here has crossed
     * nearly the whole byte-window from {@code lo} to {@code hi} — the shape a range takes while
     * draining a dated directory towards its bound, and the shape the diagnosed straggler had — so the
     * geometric factor is below the band's lower bound and the symmetric band cuts a range's proven
     * mass by the full sixteen: a range that has emitted sixty-four pages scores four, and is then
     * refused at an admission floor of four pages. Both halves are asserted: the cut is exact under the
     * symmetric band, and it is gone under the lift-only one with nothing else the estimate had lost.
     */
    @Test
    void theLiftOnlyBandNeverScoresARangeBelowTheMassItHasProduced() {
        byte[] mostlyDrained = key("species/Bat");
        assertThat(CursorAnchoredEstimator.geometricFactor(mostlyDrained, LO, HI))
                .as("the worked example's geometry is a cut past the band's lower bound")
                .isLessThan(RateAnchoredEstimator.SYMMETRIC_MIN_GEOMETRY);

        RateAnchoredEstimator symmetric =
                new RateAnchoredEstimator(1_000, RateAnchoredEstimator.SYMMETRIC_MIN_GEOMETRY);
        RateAnchoredEstimator liftOnly =
                new RateAnchoredEstimator(1_000, RateAnchoredEstimator.LIFT_ONLY_MIN_GEOMETRY);

        assertThat(symmetric.estRemaining(mostlyDrained, LO, HI, 64_000L))
                .as("the symmetric band scores sixty-four pages of proven mass as four")
                .isEqualTo(64_000.0 / RateAnchoredEstimator.GEOMETRY_BAND);
        assertThat(liftOnly.estRemaining(mostlyDrained, LO, HI, 64_000L))
                .as("the lift-only band scores it as the mass it has produced").isEqualTo(64_000.0);

        // What it keeps: the exact bound test, the open frontier, the no-evidence page floor, and the
        // whole upward half of the band -- a lifted estimate is identical under both.
        assertThat(liftOnly.estRemaining(HI, LO, HI, 400_000L)).isZero();
        assertThat(liftOnly.estRemaining(CURSOR_LATER, LO, null, 400_000L))
                .isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(liftOnly.estRemaining(mostlyDrained, LO, HI, 0L)).isEqualTo(1_000.0);
        assertThat(CursorAnchoredEstimator.geometricFactor(CURSOR_LATER, LO, HI))
                .as("a cursor still inside lo's own subtree is a lift").isGreaterThan(1.0);
        assertThat(liftOnly.estRemaining(CURSOR_LATER, LO, HI, 5_000L))
                .isEqualTo(symmetric.estRemaining(CURSOR_LATER, LO, HI, 5_000L));
        assertThat(liftOnly.ignoresEmittedKeys(mostlyDrained, LO, HI)).isFalse();
        assertThat(liftOnly.advanceVisible(LO, CURSOR_EARLY, CURSOR_LATER, HI)).isTrue();
    }

    /**
     * The same arithmetic read <b>against the gate that consumes it</b>, which is where the defect
     * actually lived: the owner's carve is refused while the estimate does not clear
     * {@code SELF_SPLIT_MIN_REMAINING_PAGES × page} ({@code EstimatorOwnerSplitPolicy}, the engine's
     * governor mirrored). The test above pins the cut; this pins the <b>consequence</b> — sixty-four
     * pages of proven mass is exactly the boundary, so the trace's sixty-four refusals on a range's
     * committed pages are the floor's own arithmetic and not a coincidence of that fixture.
     *
     * <p>Both constants are referenced rather than written out: a floor of four pages at a 1,000-key
     * page is 4,000 keys only until somebody changes either, and a literal here would then pin a
     * boundary that had moved.
     */
    @Test
    void theSymmetricBandRefusesSixtyFourPagesOfProvenMassAtTheOwnersFloorAndTheLiftOnlyBandAdmitsIt() {
        byte[] mostlyDrained = key("species/Bat");
        int page = 1_000;
        double floor = (double) OwnerSplitGovernor.SELF_SPLIT_MIN_REMAINING_PAGES * page;
        RateAnchoredEstimator symmetric =
                new RateAnchoredEstimator(page, RateAnchoredEstimator.SYMMETRIC_MIN_GEOMETRY);
        RateAnchoredEstimator liftOnly =
                new RateAnchoredEstimator(page, RateAnchoredEstimator.LIFT_ONLY_MIN_GEOMETRY);

        assertThat(symmetric.estRemaining(mostlyDrained, LO, HI, 64L * page))
                .as("sixty-four pages, cut by sixteen, does not clear the floor — the carve is refused")
                .isLessThanOrEqualTo(floor);
        assertThat(symmetric.estRemaining(mostlyDrained, LO, HI, 65L * page))
                .as("and sixty-five is the first page that does, which is where the refusals stop")
                .isGreaterThan(floor);
        assertThat(liftOnly.estRemaining(mostlyDrained, LO, HI, 64L * page))
                .as("under the lift-only band the same range clears it by its proven mass alone")
                .isGreaterThan(floor);
    }

    /**
     * <b>The floors the sweep races are the floors the protocol registered</b>, each read against the
     * gate that consumes it. The two settled ends bracket the ladder at the same reading: sixty-four
     * pages of proven mass before the owner's carve is admitted under the symmetric band, four under
     * the lift-only one. Every interior floor is that boundary at its own reciprocal, so what the
     * sweep's arms differ by is exactly one number — pinned here, at the arms rather than at the
     * constants, because installing the wrong floor into an arm is the one way this round could
     * measure a ladder it never ran.
     */
    @Test
    void eachInteriorArmInstallsItsRegisteredFloorAndAdmitsTheCarveAtThatFloorsReciprocal() {
        byte[] mostlyDrained = key("species/Bat");
        int page = 1_000;
        double floor = (double) OwnerSplitGovernor.SELF_SPLIT_MIN_REMAINING_PAGES * page;
        SensingVariant[] arms = {SensingVariant.RATE_ANCHORED_FLOOR_EIGHTH,
            SensingVariant.RATE_ANCHORED_FLOOR_QUARTER, SensingVariant.RATE_ANCHORED_FLOOR_HALF};
        assertThat(arms.length).as("one arm per registered floor")
                .isEqualTo(GeometryFloorSweepProtocol.FLOORS.length);

        for (int i = 0; i < arms.length; i++) {
            double registered = GeometryFloorSweepProtocol.FLOORS[i];
            RemainingWorkEstimator estimator = arms[i].estimator(page);
            String at = arms[i] + " at a floor of " + registered;
            assertThat(estimator.estRemaining(mostlyDrained, LO, HI, 64L * page))
                    .as("%s: a geometry past the band cuts proven mass by exactly that floor", at)
                    .isEqualTo(64.0 * page * registered);

            long boundary = Math.round(OwnerSplitGovernor.SELF_SPLIT_MIN_REMAINING_PAGES / registered);
            assertThat(estimator.estRemaining(mostlyDrained, LO, HI, boundary * page))
                    .as("%s: %d pages is the last mass the owner's floor still refuses", at, boundary)
                    .isLessThanOrEqualTo(floor);
            assertThat(estimator.estRemaining(mostlyDrained, LO, HI, (boundary + 1) * page))
                    .as("%s: and one page more is the first it admits", at).isGreaterThan(floor);
        }
    }
}
