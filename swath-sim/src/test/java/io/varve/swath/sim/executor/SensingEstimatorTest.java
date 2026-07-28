/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

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

    /**
     * <b>The killed E2 arm's frame is still the engine's.</b> {@link CursorAnchoredEstimator} measures
     * its own consumed and remaining spans, and the shared {@code geometricFactor} that used to hold
     * those two readings and {@link StealMath#anchoredGeometricFactor} together was deleted when the
     * promoted arm moved into the engine. The identity it asserted is unchanged — this arm's estimate
     * IS {@code keysEmitted × anchoredGeometricFactor} wherever the cursor has consumed evidence to
     * anchor — so pin it directly, or an edit to either frame leaves this arm's race records
     * describing arithmetic nothing computes. Both branches are covered: the anchored boundary itself
     * ({@code d == d0}, where the frame is the incumbent's), and the deeper frame either side of the
     * {@code 0x80} divergence byte where it stops lifting.
     */
    @Test
    void theAnchoredArmsFrameIsStillTheEnginesAnchoredGeometricFactor() {
        CursorAnchoredEstimator anchored = new CursorAnchoredEstimator();
        long keys = 12_345L;
        int d0 = RemainingWorkEstimator.commonPrefixLen(LO, HI);
        byte[][] cursors = {key("species/Bat"), key("species/Balf"), CURSOR_EARLY, CURSOR_LATER,
            deeper((byte) 0xC0)};
        assertThat(RemainingWorkEstimator.commonPrefixLen(LO, cursors[0]))
                .as("the boundary case is in the roster").isEqualTo(d0);

        for (byte[] cursor : cursors) {
            double factor = StealMath.anchoredGeometricFactor(cursor, LO, HI);
            double estimate = anchored.estRemaining(cursor, LO, HI, keys);
            // The arm divides then multiplies and the engine's factor divides last, so the only slack
            // allowed is floating-point association.
            assertThat(estimate)
                    .as("cursor %s", new String(cursor, StandardCharsets.UTF_8))
                    .isCloseTo(keys * factor, within(Math.abs(keys * factor) * 1e-12));
        }
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
        RateAnchoredArm combined =
                new RateAnchoredArm(100, RateAnchoredArm.SYMMETRIC_MIN_GEOMETRY);
        double band = RateAnchoredArm.GEOMETRY_BAND;
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
     * <b>The promoted arm reads exactly what the engine reads.</b> {@code RATE_ANCHORED_FLOOR_QUARTER}
     * is the arm the corpus race promoted, and the composition it is made of now lives in the engine
     * ({@code io.varve.swath.engine.RateAnchoredEstimator}) with this arm delegating to it. Every value
     * below is pinned digit for digit in swath-core's own {@code RateAnchoredEstimatorTest}, and both
     * sides were green on these numbers before the delegation existed — so a port that drifted from
     * the implementation this race measured fails here, on the arm, rather than quietly becoming a
     * different algorithm with the same race table attached to it.
     */
    @Test
    void thePromotedArmsReadingsAreTheEnginesToTheDigit() {
        int page = 1_000;
        RemainingWorkEstimator quarter = SensingVariant.RATE_ANCHORED_FLOOR_QUARTER.estimator(page);
        assertThat(quarter.estRemaining(CURSOR_LATER, LO, HI, 5_000L)).isEqualTo(8134.808965781418);
        assertThat(quarter.estRemaining(CURSOR_LATER, LO, HI, 0L)).isEqualTo(1626.9617931562836);
        assertThat(quarter.estRemaining(key("species/Bat"), LO, HI, 64L * page)).isEqualTo(16_000.0);
        assertThat(quarter.estRemaining(key("species/Bat"), LO, HI, 0L)).isEqualTo(250.0);
        assertThat(quarter.estRemaining(key("species/Baq"), LO, HI, 5_000L)).isEqualTo(3701.256127880026);
        assertThat(quarter.estRemaining(key("species/Balf"), LO, HI, 5_000L)).isEqualTo(80_000.0);
        assertThat(quarter.estRemaining(LO, LO, HI, 5_000L)).isEqualTo(5_000.0);
        assertThat(quarter.estRemaining(HI, LO, HI, 400_000L)).isZero();
        assertThat(quarter.estRemaining(CURSOR_LATER, LO, null, 5_000L))
                .isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(quarter.ignoresEmittedKeys(CURSOR_LATER, LO, HI)).isFalse();
        assertThat(quarter.advanceVisible(LO, CURSOR_EARLY, CURSOR_LATER, HI)).isTrue();
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
        assertThat(StealMath.anchoredGeometricFactor(mostlyDrained, LO, HI))
                .as("the worked example's geometry is a cut past the band's lower bound")
                .isLessThan(RateAnchoredArm.SYMMETRIC_MIN_GEOMETRY);

        RateAnchoredArm symmetric =
                new RateAnchoredArm(1_000, RateAnchoredArm.SYMMETRIC_MIN_GEOMETRY);
        RateAnchoredArm liftOnly =
                new RateAnchoredArm(1_000, RateAnchoredArm.LIFT_ONLY_MIN_GEOMETRY);

        assertThat(symmetric.estRemaining(mostlyDrained, LO, HI, 64_000L))
                .as("the symmetric band scores sixty-four pages of proven mass as four")
                .isEqualTo(64_000.0 / RateAnchoredArm.GEOMETRY_BAND);
        assertThat(liftOnly.estRemaining(mostlyDrained, LO, HI, 64_000L))
                .as("the lift-only band scores it as the mass it has produced").isEqualTo(64_000.0);

        // What it keeps: the exact bound test, the open frontier, the no-evidence page floor, and the
        // whole upward half of the band -- a lifted estimate is identical under both.
        assertThat(liftOnly.estRemaining(HI, LO, HI, 400_000L)).isZero();
        assertThat(liftOnly.estRemaining(CURSOR_LATER, LO, null, 400_000L))
                .isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(liftOnly.estRemaining(mostlyDrained, LO, HI, 0L)).isEqualTo(1_000.0);
        assertThat(StealMath.anchoredGeometricFactor(CURSOR_LATER, LO, HI))
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
        RateAnchoredArm symmetric =
                new RateAnchoredArm(page, RateAnchoredArm.SYMMETRIC_MIN_GEOMETRY);
        RateAnchoredArm liftOnly =
                new RateAnchoredArm(page, RateAnchoredArm.LIFT_ONLY_MIN_GEOMETRY);

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

    /**
     * <b>A floor conditioned on which frame the reading came from has no population to act on.</b> The
     * candidate was to keep the symmetric band where the frame is the incumbent's own
     * ({@code cpl(lo, cursor) == cpl(lo, hi)}, where the reading is a measurement) and to lift the floor
     * only in the deeper frame, which {@link CursorAnchoredEstimator} itself discloses as a local
     * under-statement. The deeper frame cannot produce the reading such a floor would remove.
     *
     * <p>Why, as arithmetic: in that frame the remaining span is measured to the top of the prefix the
     * cursor shares with {@code lo}, the constant 1.0, so the factor is {@code (1 − f) / (f − f_lo)} for
     * the cursor's own fraction {@code f} read from its divergence. That is below one only where
     * {@code f > (1 + f_lo) / 2 ≥ 1/2}, which needs a divergence byte at or above {@code 0x80}. Object
     * keys diverge on printable bytes, so the deeper frame lifts and never cuts — and a floor applied
     * only there returns the symmetric band's own estimate at every call.
     *
     * <p>Measured, not only argued: over the geometry-floor sweep's fourteen-fixture roster, at two
     * seeds under both ends of the band, the deeper frame carried 14% of 495,968 estimate calls and
     * <b>none of the 349,005 cut readings, nor any of the 44,614 refusals the lift-only floor frees</b>
     * — every one of those came from the {@code d == d0} frame, which is the branch the candidate
     * leaves alone. Pinned here so the candidate is refuted rather than re-proposed.
     */
    @Test
    void theDeeperFrameOnlyLiftsAtTheByteValuesObjectKeysDivergeOn() {
        int page = 1_000;
        int d0 = RemainingWorkEstimator.commonPrefixLen(LO, HI);
        RateAnchoredArm symmetric =
                new RateAnchoredArm(page, RateAnchoredArm.SYMMETRIC_MIN_GEOMETRY);
        RateAnchoredArm liftOnly =
                new RateAnchoredArm(page, RateAnchoredArm.LIFT_ONLY_MIN_GEOMETRY);
        RateAnchoredArm quarter =
                new RateAnchoredArm(page, RateAnchoredArm.QUARTER_MIN_GEOMETRY);

        for (int b = 0x20; b < 0x80; b++) {
            byte[] cursor = deeper((byte) b);
            String at = "a cursor diverging from lo on byte 0x" + Integer.toHexString(b);
            assertThat(RemainingWorkEstimator.commonPrefixLen(LO, cursor))
                    .as("%s is in the deeper frame", at).isGreaterThan(d0);
            assertThat(StealMath.anchoredGeometricFactor(cursor, LO, HI))
                    .as("%s reads as a lift, so no floor of any height binds", at)
                    .isGreaterThanOrEqualTo(RateAnchoredArm.LIFT_ONLY_MIN_GEOMETRY);
            assertThat(liftOnly.estRemaining(cursor, LO, HI, 64L * page))
                    .as("%s: conditioning the floor on this frame returns the symmetric estimate", at)
                    .isEqualTo(symmetric.estRemaining(cursor, LO, HI, 64L * page));
            assertThat(quarter.estRemaining(cursor, LO, HI, 64L * page))
                    .as("%s: and so does conditioning it at any interior height", at)
                    .isEqualTo(symmetric.estRemaining(cursor, LO, HI, 64L * page));
        }

        // The bound is the byte range and not the frame: past 0x80 the deeper frame does cut, and there
        // the two floors part company. Keyspaces are what make the branch empty, so it is measured.
        byte[] highByte = deeper((byte) 0xC0);
        assertThat(StealMath.anchoredGeometricFactor(highByte, LO, HI)).isLessThan(1.0);
        assertThat(liftOnly.estRemaining(highByte, LO, HI, 64L * page))
                .isGreaterThan(symmetric.estRemaining(highByte, LO, HI, 64L * page));

        // And the cut the candidate exists to remove is in the other branch: the worked example, whose
        // sixteen-fold cut refuses the owner's carve above, diverges from lo exactly where hi does.
        byte[] mostlyDrained = key("species/Bat");
        assertThat(RemainingWorkEstimator.commonPrefixLen(LO, mostlyDrained))
                .as("the cut population sits in the frame the candidate leaves symmetric").isEqualTo(d0);
        assertThat(StealMath.anchoredGeometricFactor(mostlyDrained, LO, HI))
                .isLessThan(RateAnchoredArm.SYMMETRIC_MIN_GEOMETRY);
    }

    /** A cursor inside {@code lo}'s own subtree, diverging from it on {@code first} — the deeper frame. */
    private static byte[] deeper(byte first) {
        byte[] tail = key("assembly_vgp/intermediates/00417");
        byte[] cursor = new byte[LO.length + 1 + tail.length];
        System.arraycopy(LO, 0, cursor, 0, LO.length);
        cursor[LO.length] = first;
        System.arraycopy(tail, 0, cursor, LO.length + 1, tail.length);
        return cursor;
    }
}
