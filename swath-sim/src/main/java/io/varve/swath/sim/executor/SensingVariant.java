/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

/**
 * Which position sensor a run steers on. {@link #CURRENT} is the algorithm as it ships and is what a
 * run gets when it does not ask for anything else; the others are candidate cures for the sensor's
 * blindness on deep-nested keyspaces, raced against it under the protocol in
 * {@code SensingRaceProtocol}.
 *
 * <p>Running a variant here changes nothing about the engine: each is an estimator installed into the
 * engine's own thief and owner-split policies through the seam they already have, so what a race
 * compares is one quantity, measured differently, driving identical decision logic. The arm a race
 * promoted is a different matter — {@link #RATE_ANCHORED_FLOOR_QUARTER}'s composition now lives in
 * {@code swath-core} and this arm delegates to it, so the engine and the simulator cannot hold two
 * versions of the reading that was measured.
 */
public enum SensingVariant {

    /** The shipped estimator: local density times remaining span over the {@code [lo, hi]} window. */
    CURRENT,
    /** E1: remaining work estimated from the keys a range has emitted, with no key-shape inference. */
    RATE,
    /** E2: the same density-times-span reading, in a window anchored at the cursor's own divergence. */
    CURSOR_ANCHORED,
    /** E1 and E2 together: the rate estimate, modulated by the anchored geometry within a band. */
    RATE_CURSOR_ANCHORED,
    /**
     * E4b: the same combination with the band's lower half shortened to an eighth rather than removed.
     * Raced under {@code GeometryFloorSweepProtocol}, against the finding that the lower half cuts a
     * measured shortfall and an inferred one with one factor, and that only removing both is what E4's
     * cures are bought with.
     */
    RATE_ANCHORED_FLOOR_EIGHTH,
    /**
     * E4b: the lower half shortened to a quarter — <b>the arm the corpus race promoted</b>, and the one
     * whose reading is the engine's own {@code RateAnchoredEstimator} rather than a copy of it.
     * @see #RATE_ANCHORED_FLOOR_EIGHTH
     */
    RATE_ANCHORED_FLOOR_QUARTER,
    /** E4b: the lower half shortened to a half. @see #RATE_ANCHORED_FLOOR_EIGHTH */
    RATE_ANCHORED_FLOOR_HALF,
    /**
     * E4: the same combination with the band's lower half removed, so the anchored geometry may lift a
     * range's proven mass but never cut it. Raced under {@code CarveAdmissionRaceProtocol}, against the
     * finding that a proven-mass estimate cut by the band is what refuses the owner's carve on the one
     * range holding a fleet.
     */
    RATE_ANCHORED_LIFT_ONLY;

    /** This variant's estimator, sized to the run's page. */
    RemainingWorkEstimator estimator(int pageSize) {
        return switch (this) {
            case CURRENT -> new WindowEstimator();
            case RATE -> new RateEstimator(pageSize);
            case CURSOR_ANCHORED -> new CursorAnchoredEstimator();
            case RATE_CURSOR_ANCHORED ->
                    new RateAnchoredArm(pageSize, RateAnchoredArm.SYMMETRIC_MIN_GEOMETRY);
            case RATE_ANCHORED_FLOOR_EIGHTH ->
                    new RateAnchoredArm(pageSize, RateAnchoredArm.EIGHTH_MIN_GEOMETRY);
            case RATE_ANCHORED_FLOOR_QUARTER ->
                    new RateAnchoredArm(pageSize, RateAnchoredArm.QUARTER_MIN_GEOMETRY);
            case RATE_ANCHORED_FLOOR_HALF ->
                    new RateAnchoredArm(pageSize, RateAnchoredArm.HALF_MIN_GEOMETRY);
            case RATE_ANCHORED_LIFT_ONLY ->
                    new RateAnchoredArm(pageSize, RateAnchoredArm.LIFT_ONLY_MIN_GEOMETRY);
        };
    }
}
