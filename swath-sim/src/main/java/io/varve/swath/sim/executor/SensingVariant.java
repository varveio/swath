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
 * <p>Nothing here changes the engine. Each variant is an estimator installed into the engine's own
 * thief and owner-split policies through the seam they already have, so what a race compares is one
 * quantity, measured differently, driving identical decision logic.
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
                    new RateAnchoredEstimator(pageSize, RateAnchoredEstimator.SYMMETRIC_MIN_GEOMETRY);
            case RATE_ANCHORED_LIFT_ONLY ->
                    new RateAnchoredEstimator(pageSize, RateAnchoredEstimator.LIFT_ONLY_MIN_GEOMETRY);
        };
    }
}
