/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import io.varve.swath.engine.policy.Engagement;
import io.varve.swath.engine.policy.OwnerSplitGovernor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * <b>The ported sensor's readings, pinned as exact arithmetic.</b> Every value here was produced by
 * the simulator's own {@code RATE_ANCHORED_FLOOR_QUARTER} arm before this class existed, and the
 * simulator's {@code SensingEstimatorTest} pins the identical numbers on the other side of the
 * delegation — so the port cannot drift from the implementation the race measured without one of the
 * two failing.
 *
 * <p>The worked example throughout is the one the defect was diagnosed on, and the one the sim's own
 * estimator tests use: two species directories whose names diverge at byte ten, and cursors a further
 * twenty-odd bytes down inside the first of them — the case where the shipped window reads a thousand
 * committed keys as no movement at all.
 */
class RateAnchoredEstimatorTest {

    private static final byte[] LO = key("species/Balearica_regulorum/bBalReg1/");
    private static final byte[] HI = key("species/Bathysaurus_mollis/fBatMol1/");
    /** A cursor deep inside {@code lo}'s own subtree: the anchored frame, which lifts. */
    private static final byte[] CURSOR_DEEP =
            key("species/Balearica_regulorum/bBalReg1/assembly_vgp/intermediates/09999");
    /** A cursor most of the way to {@code hi} in the shipped window's own frame: geometry cuts hard. */
    private static final byte[] MOSTLY_DRAINED = key("species/Bat");
    /** One byte deeper than {@code hi}'s divergence, inside the shipped window: geometry lifts hugely. */
    private static final byte[] JUST_PAST_DIVERGENCE = key("species/Balf");
    /** In the shipped window's frame, a cursor whose geometry is an ordinary in-band cut. */
    private static final byte[] IN_BAND_CUT = key("species/Baq");

    private static final int PAGE = 1_000;

    /** The two decision sites' categories, spelled as {@code OwnerSplitGovernor}/{@code ThiefPolicy} pass them. */
    private static final String OWNER = "SENSING_OWNER";
    private static final String STEAL = "SENSING_STEAL";

    private static final RateAnchoredEstimator QUARTER =
            new RateAnchoredEstimator(PAGE, RateAnchoredEstimator.QUARTER_MIN_GEOMETRY);

    private static byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void theEstimateIsTheRangesProvenMassLiftedByTheAnchoredGeometry() {
        // The deep frame the shipped window cannot see at all: its consumed span underflows to zero
        // there, so it discards the emitted keys entirely -- stated here as the premise the pins below
        // are read against.
        assertThat(StealMath.spanIn(LO, CURSOR_DEEP, LO, HI)).isZero();
        assertThat(StealMath.estRemaining(CURSOR_DEEP, LO, HI, 400_000L))
                .isEqualTo(StealMath.estRemaining(CURSOR_DEEP, LO, HI, 0L));

        assertThat(StealMath.anchoredGeometricFactor(CURSOR_DEEP, LO, HI))
                .as("the anchored frame reads this cursor as a lift").isEqualTo(1.6269617931562836);
        assertThat(QUARTER.estRemaining(CURSOR_DEEP, LO, HI, 5_000L)).isEqualTo(8134.808965781418);
        assertThat(QUARTER.estRemaining(CURSOR_DEEP, LO, HI, 0L))
                .as("and with no evidence yet the magnitude is the page in flight, not zero")
                .isEqualTo(1626.9617931562836);
    }

    /**
     * WHICH rung the toggle installs, not merely that it installs one. The floor is the single number
     * the corpus race's arms differed by, so an edit to {@link EngineToggles#remainingWorkEstimator}
     * handing a run a losing rung — the eighth or the half — would otherwise leave every test green:
     * both are the same class, both classify, both wire. They read this fixture at half and at double
     * the quarter's number, which is what this pins.
     */
    @Test
    void theToggleInstallsThePromotedQuarterRungAndNotAnotherOfTheLadder() {
        RemainingWorkEstimator installed =
                EngineToggles.DEFAULT.withRateAnchoredSensing(true).remainingWorkEstimator(PAGE);

        assertThat(installed.estRemaining(MOSTLY_DRAINED, LO, HI, 64L * PAGE))
                .as("the promoted quarter's own reading of the mostly-drained fixture")
                .isEqualTo(16_000.0);
    }

    @Test
    void theQuarterFloorStopsTheGeometryCuttingProvenMassPastAFactorOfFour() {
        assertThat(StealMath.anchoredGeometricFactor(MOSTLY_DRAINED, LO, HI))
                .as("the worked example's geometry is a cut far past the floor")
                .isEqualTo(0.053669669382911074);
        assertThat(QUARTER.estRemaining(MOSTLY_DRAINED, LO, HI, 64L * PAGE))
                .as("64 pages of proven mass is cut by exactly the floor, and no further")
                .isEqualTo(64.0 * PAGE * RateAnchoredEstimator.QUARTER_MIN_GEOMETRY)
                .isEqualTo(16_000.0);
        assertThat(QUARTER.estRemaining(MOSTLY_DRAINED, LO, HI, 5_000L)).isEqualTo(1_250.0);
        assertThat(QUARTER.estRemaining(MOSTLY_DRAINED, LO, HI, 0L)).isEqualTo(250.0);
    }

    /**
     * The floor read against the gate that consumes it — the owner's carve is refused while the
     * estimate does not clear {@code SELF_SPLIT_MIN_REMAINING_PAGES × page}, and a floor of a quarter
     * puts that boundary at its reciprocal: sixteen pages of proven mass. Both constants are
     * referenced rather than written out, so a change to either moves this boundary instead of leaving
     * a literal pinning one that had moved.
     */
    @Test
    void sixteenPagesOfProvenMassIsTheLastMassTheOwnersFloorRefusesUnderTheQuarter() {
        double floor = (double) OwnerSplitGovernor.SELF_SPLIT_MIN_REMAINING_PAGES * PAGE;
        long boundary = Math.round(OwnerSplitGovernor.SELF_SPLIT_MIN_REMAINING_PAGES
                / RateAnchoredEstimator.QUARTER_MIN_GEOMETRY);
        assertThat(boundary).isEqualTo(16L);
        assertThat(QUARTER.estRemaining(MOSTLY_DRAINED, LO, HI, boundary * PAGE))
                .as("sixteen pages does not clear the floor — the carve is refused")
                .isLessThanOrEqualTo(floor);
        assertThat(QUARTER.estRemaining(MOSTLY_DRAINED, LO, HI, (boundary + 1) * PAGE))
                .as("and seventeen is the first that does, which is where the refusals stop")
                .isGreaterThan(floor);
    }

    /**
     * Where the two readings agree, stated as the condition it actually is: a cursor in the shipped
     * window's own frame ({@code cpl(lo, cursor) == cpl(lo, hi)}, where the anchored factor IS the
     * shipped reading's {@code remaining / consumed}), a geometry inside the band, and at least a page
     * of proven mass. The estimate is then {@code keysEmitted × geometry}, which is what the shipped
     * reading computes as {@code (keysEmitted / consumed) × remaining} — the same quantity, to the
     * order the division is taken in, and no further.
     */
    @Test
    void inTheShippedWindowsOwnFrameAnInBandReadingIsTheShippedEstimate() {
        assertThat(StealMath.anchoredGeometricFactor(IN_BAND_CUT, LO, HI))
                .as("an ordinary cut, above the floor and below the band")
                .isEqualTo(0.7402512255760052)
                .isBetween(RateAnchoredEstimator.QUARTER_MIN_GEOMETRY, 1.0);
        assertThat(QUARTER.estRemaining(IN_BAND_CUT, LO, HI, 5_000L)).isEqualTo(3701.256127880026);
        assertThat(QUARTER.estRemaining(IN_BAND_CUT, LO, HI, 5_000L))
                .as("the shipped reading of the same range, to floating-point association")
                .isCloseTo(StealMath.estRemaining(IN_BAND_CUT, LO, HI, 5_000L), within(1e-9));
    }

    @Test
    void theBandCapsHowFarGeometryMayLiftAndTheExactBoundsAreUntouched() {
        assertThat(StealMath.anchoredGeometricFactor(JUST_PAST_DIVERGENCE, LO, HI))
                .as("a cursor one byte past hi's divergence reads far above the band")
                .isEqualTo(248.64859230921192);
        assertThat(QUARTER.estRemaining(JUST_PAST_DIVERGENCE, LO, HI, 5_000L))
                .as("so the lift is capped at the band")
                .isEqualTo(5_000.0 * RateAnchoredEstimator.GEOMETRY_BAND)
                .isEqualTo(80_000.0);

        assertThat(QUARTER.estRemaining(LO, LO, HI, 5_000L))
                .as("an un-started range has no consumed evidence to anchor: geometry is neutral")
                .isEqualTo(5_000.0);
        assertThat(QUARTER.estRemaining(LO, LO, HI, 0L)).isEqualTo(1_000.0);
        assertThat(QUARTER.estRemaining(HI, LO, HI, 400_000L))
                .as("a cursor at the bound is done — an exact comparison, not an inference").isZero();
        assertThat(QUARTER.estRemaining(key("species/Z"), LO, HI, 400_000L)).isZero();
        assertThat(QUARTER.estRemaining(CURSOR_DEEP, LO, null, 5_000L))
                .as("an open frontier always outranks a bounded range, as it does today")
                .isEqualTo(Double.POSITIVE_INFINITY);
    }

    /**
     * The sensor's own engagement counters (AGENTS.md's instrument-every-algo-path rule): one
     * mutually-exclusive geometry reading per classified estimate, plus the no-evidence page floor, so
     * post-analysis can tell from the metrics alone which way the band bit on a real bucket.
     */
    @Test
    void everyClassifiedReadingReportsWhatTheBandDidToIt() {
        assertThat(classify(OWNER, CURSOR_DEEP, 5_000L)).containsExactly("SENSING_OWNER.geometry_lift");
        assertThat(classify(OWNER, JUST_PAST_DIVERGENCE, 5_000L))
                .containsExactly("SENSING_OWNER.geometry_capped");
        assertThat(classify(OWNER, IN_BAND_CUT, 5_000L)).containsExactly("SENSING_OWNER.geometry_cut");
        assertThat(classify(OWNER, MOSTLY_DRAINED, 5_000L)).containsExactly("SENSING_OWNER.geometry_floored");
        assertThat(classify(OWNER, LO, 5_000L)).containsExactly("SENSING_OWNER.geometry_neutral");
        assertThat(classify(OWNER, CURSOR_DEEP, 0L))
                .as("a range that has not produced a page yet says so alongside its geometry")
                .containsExactly("SENSING_OWNER.geometry_lift", "SENSING_OWNER.page_floor");
        assertThat(classify(OWNER, HI, 5_000L))
                .as("a finished range's zero is the shipped contract's").isEmpty();

        List<Engagement> openFrontier = new ArrayList<>();
        QUARTER.classify(OWNER, CURSOR_DEEP, LO, null, 5_000L, openFrontier);
        assertThat(openFrontier).as("as is the open frontier's +INF").isEmpty();
    }

    /**
     * The reading is the sensor's; the NAMESPACE is the caller's. The two decision sites count over
     * incompatible denominators — an owner-gate consult per qualifying page commit, a steal
     * classification per attempt with a winner — so the identical reading must land under whichever
     * category the site passed, and never in one pooled family that no ratio could be drawn from.
     */
    @Test
    void theReadingIsFiledUnderTheCallingSitesOwnCategory() {
        assertThat(classify(STEAL, CURSOR_DEEP, 5_000L)).containsExactly("SENSING_STEAL.geometry_lift");
        assertThat(classify(STEAL, MOSTLY_DRAINED, 5_000L)).containsExactly("SENSING_STEAL.geometry_floored");
        assertThat(classify(STEAL, CURSOR_DEEP, 0L))
                .containsExactly("SENSING_STEAL.geometry_lift", "SENSING_STEAL.page_floor");
    }

    @Test
    void theShippedReadingIsTheDefaultAndReportsNothingOfItsOwn() {
        assertThat(RemainingWorkEstimator.WINDOW.estRemaining(CURSOR_DEEP, LO, HI, 400_000L))
                .isEqualTo(StealMath.estRemaining(CURSOR_DEEP, LO, HI, 400_000L));
        List<Engagement> collected = new ArrayList<>();
        RemainingWorkEstimator.WINDOW.classify(OWNER, CURSOR_DEEP, LO, HI, 400_000L, collected);
        assertThat(collected).isEmpty();
    }

    /** The page floor is the magnitude of an un-started range, so it cannot be zero. */
    @Test
    void aPageSizeBelowOneIsRefusedRatherThanScoringEveryRangeZero() {
        assertThatThrownBy(() -> new RateAnchoredEstimator(0, RateAnchoredEstimator.QUARTER_MIN_GEOMETRY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pageSize");
        assertThat(new RateAnchoredEstimator(1, RateAnchoredEstimator.QUARTER_MIN_GEOMETRY)
                .estRemaining(CURSOR_DEEP, LO, HI, 0L))
                .as("one key is the smallest page that still keeps an un-started range in selection")
                .isPositive();
    }

    /**
     * A {@code minGeometry} outside the ladder's own {@code (0, 1]} range would make estimates
     * either {@code NaN} (a range's own arithmetic breaking selection for every other range too) or
     * would contradict the documented "cut floor" semantics ({@code minGeometry > 1.0} would mean
     * geometry may inflate a range past what a lift is defined to do).
     */
    @Test
    void aMinGeometryOutsideTheLaddersOwnRangeIsRefusedRatherThanCorruptingEveryEstimate() {
        assertThatThrownBy(() -> new RateAnchoredEstimator(PAGE, Double.NaN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minGeometry");
        assertThatThrownBy(() -> new RateAnchoredEstimator(PAGE, Double.POSITIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minGeometry");
        assertThatThrownBy(() -> new RateAnchoredEstimator(PAGE, 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minGeometry");
        assertThatThrownBy(() -> new RateAnchoredEstimator(PAGE, -0.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minGeometry");
        assertThatThrownBy(() -> new RateAnchoredEstimator(PAGE, 1.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minGeometry");
        assertThat(new RateAnchoredEstimator(PAGE, 1.0)
                .estRemaining(CURSOR_DEEP, LO, HI, 0L))
                .as("the lift-only end of the ladder, minGeometry == 1.0, stays a valid construction")
                .isPositive();
    }

    /** {@code QUARTER}'s classification of {@code cursor} in {@code [LO, HI]}, as {@code category.reason}. */
    private static List<String> classify(String category, byte[] cursor, long keysEmitted) {
        List<Engagement> collected = new ArrayList<>();
        QUARTER.classify(category, cursor, LO, HI, keysEmitted, collected);
        return collected.stream().map(e -> e.category() + "." + e.reason()).toList();
    }
}
