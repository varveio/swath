/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

/**
 * Simulator sensing controls and experimental arms. {@link #CURRENT} (and a {@code null} estimator)
 * selects the legacy WINDOW control and remains the simulator's ordinary default seam; it is not the
 * production default. Since 0.2.0, production defaults to rate-anchored sensing with the promoted
 * quarter floor, which {@link #RATE_ANCHORED_FLOOR_QUARTER} delegates to in core.
 */
public enum SensingVariant {

    /** Legacy WINDOW control: density times remaining span over the range-bound window. */
    CURRENT,
    /** Emitted-mass experiment without key-shape inference. */
    RATE,
    /** Density-times-span experiment anchored at the cursor's divergence. */
    CURSOR_ANCHORED,
    /** Emitted mass adjusted by symmetrically banded cursor-anchored geometry. */
    RATE_CURSOR_ANCHORED,
    /** Rate-anchored experiment with a one-eighth geometry floor. */
    RATE_ANCHORED_FLOOR_EIGHTH,
    /** Production-delegating rate-anchored arm with the promoted quarter geometry floor. */
    RATE_ANCHORED_FLOOR_QUARTER,
    /** Rate-anchored experiment with a one-half geometry floor. */
    RATE_ANCHORED_FLOOR_HALF,
    /** Rate-anchored experiment in which geometry may lift emitted mass but never cut it. */
    RATE_ANCHORED_LIFT_ONLY;

    /** This simulator variant's estimator, sized to the run's page. */
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
