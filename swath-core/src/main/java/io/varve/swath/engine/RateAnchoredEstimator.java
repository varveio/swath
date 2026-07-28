/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.engine.policy.Engagement;
import io.varve.swath.model.KeyBytes;
import java.util.List;

/**
 * <b>A range's proven mass, which cursor-anchored geometry may adjust but not overrule.</b> The
 * candidate position sensor the simulator raced against the shipped {@link RemainingWorkEstimator#WINDOW}
 * reading over a captured-listing corpus and promoted out of that race (the sim's
 * {@code SensingVariant.RATE_ANCHORED_FLOOR_QUARTER} arm, which now delegates here — one implementation,
 * raced and shipped). <b>Not the default:</b> a run steers on this only under {@code --engine-toggle
 * rate_anchored_sensing=on}, so a real-bucket A/B can run both arms from one binary.
 *
 * <p>The estimate is {@code max(keysEmitted, pageSize) × banded(geometry)}:
 * <ul>
 *   <li><b>The magnitude is what the range has already produced.</b> Under a heavy-tailed size law the
 *       expected remaining mass of a range known to exceed {@code t} grows with {@code t}, so a range
 *       that has emitted a million keys and has not finished is evidence of a big range, not a nearly
 *       finished one. Taking the estimate to be exactly {@code keysEmitted} is the mean residual life
 *       of a Pareto law with exponent 2, and it stays in the key units the owner-side floors compare
 *       against multiples of a page. {@code pageSize} is the no-evidence floor: a range that has
 *       emitted nothing yet holds at least the page in flight, so it is not scored zero and dropped
 *       out of victim selection.</li>
 *   <li><b>The geometry is {@link StealMath#anchoredGeometricFactor}</b>, the same remaining-over-consumed
 *       ratio the shipped reading multiplies its density by, but measured in the window anchored at the
 *       cursor's own divergence — the term the shipped reading loses to a deep shared prefix, where its
 *       consumed span underflows to zero and a range's emitted keys drop out of the estimate entirely.</li>
 *   <li><b>The band is what keeps geometry an adjustment.</b> {@link #GEOMETRY_BAND} caps how far
 *       measured position may lift the proven mass; {@code minGeometry} is where it stops cutting it.
 *       A factor below one asserts that less remains than has already come out, which is the inference
 *       the proven-mass magnitude exists to refuse — and it is what refused a straggler's carve at the
 *       owner's remaining-work floor until the range had emitted 64 pages. The exact
 *       {@code cursor >= hi} test below still scores a finished range zero, so what a floor drops is
 *       the <em>inferred</em> shortfall, not the measured one.</li>
 * </ul>
 *
 * <p><b>Why {@code minGeometry} is a constructor argument rather than the promoted constant inlined.</b>
 * The floor is the one number the race's arms differed by — a ladder swept from "geometry may cut by
 * sixteen" to "geometry may never cut" — and {@link #QUARTER_MIN_GEOMETRY} is the rung that won. The
 * simulator still runs the losing rungs to keep those race records reproducible, and it runs them
 * through THIS object rather than through a copy of this arithmetic, which is what a parameter buys and
 * an inlined constant would not.
 *
 * <p>What a floor decides, in the units the gate consuming it uses: a range clears the owner's
 * admission floor once its emitted mass reaches {@code SELF_SPLIT_MIN_REMAINING_PAGES / minGeometry}
 * pages — sixteen pages at the quarter.
 *
 * <p>Pure and allocation-free on the estimate call, exactly as {@link StealMath} is; one instance
 * serves a whole run's fleet.
 */
public final class RateAnchoredEstimator implements RemainingWorkEstimator {

    /** How far the anchored geometry may lift the proven mass: a factor of sixteen. */
    public static final double GEOMETRY_BAND = 16.0;

    /** The promoted floor: geometry may cut a range's proven mass by four, and no further. */
    public static final double QUARTER_MIN_GEOMETRY = 1.0 / 4.0;

    /** The ⊥ sentinel (cursor at range start) as bytes, for the exact bound comparisons below. */
    private static final byte[] BOTTOM = new byte[0];

    private final int pageSize;
    private final double minGeometry;

    /**
     * @param pageSize    the run's page size {@code maxKeys}, used as the no-evidence floor
     * @param minGeometry how far the anchored geometry may cut the proven mass — {@link
     *                    #QUARTER_MIN_GEOMETRY} for the promoted reading, a losing rung of the same
     *                    ladder for a simulator arm re-running the race
     */
    public RateAnchoredEstimator(int pageSize, double minGeometry) {
        this.pageSize = pageSize;
        this.minGeometry = minGeometry;
    }

    @Override
    public double estRemaining(byte[] cursor, byte[] lo, byte[] hi, long keysEmitted) {
        if (hi == null) {
            return Double.POSITIVE_INFINITY;   // the open frontier always scores highest, as it does today
        }
        if (KeyBytes.compareUnsigned(cursor == null ? BOTTOM : cursor, hi) >= 0) {
            return 0.0;                        // the cursor has reached the bound: exact, not inferred
        }
        double geometry = StealMath.anchoredGeometricFactor(cursor, lo, hi);
        double banded = Math.min(GEOMETRY_BAND, Math.max(minGeometry, geometry));
        return Math.max(keysEmitted, pageSize) * banded;
    }

    @Override
    public void classify(byte[] cursor, byte[] lo, byte[] hi, long keysEmitted,
                         List<Engagement> collector) {
        if (hi == null || KeyBytes.compareUnsigned(cursor == null ? BOTTOM : cursor, hi) >= 0) {
            // The two readings that never consult geometry: the open frontier's +INF and a finished
            // range's exact zero. Both are the shipped contract's own answers, so there is nothing
            // this sensor did to report.
            return;
        }
        double geometry = StealMath.anchoredGeometricFactor(cursor, lo, hi);
        // Which way the band bit, as one mutually-exclusive reading per classified estimate: whether
        // the measured position lifted the proven mass, passed through it, cut it within what the
        // floor allows, or was refused by the floor. Read against OWNER_SPLIT.remaining_est_floor and
        // NO_VICTIM.all_no_remaining_span, the two gates the estimate feeds.
        collector.add(new Engagement("SENSING",
                geometry > GEOMETRY_BAND ? "geometry_capped"
                        : geometry > 1.0 ? "geometry_lift"
                        : geometry == 1.0 ? "geometry_neutral"
                        : geometry < minGeometry ? "geometry_floored" : "geometry_cut"));
        if (keysEmitted < pageSize) {
            // The estimate is the no-evidence page floor rather than proven mass — the range has not
            // yet produced a page, so its magnitude is an assumption and not a measurement.
            collector.add(new Engagement("SENSING", "page_floor"));
        }
    }
}
