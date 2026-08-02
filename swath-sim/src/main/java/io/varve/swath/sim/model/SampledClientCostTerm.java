/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.SimRng;

/**
 * A {@link ClientCostTerm} charged as a scalar or sampled per page from measured quantiles.
 *
 * <p>The empirical inverse-CDF ladder interpolates between strictly ascending quantiles and is flat
 * outside them: it avoids inventing an unmeasured floor or tail, at the cost of overstating below the
 * first point and understating above the last. The pinned worker ladder consequently averages about
 * 6.36 ms/page rather than its published 5.85 ms/page (+8.8%). Inputs are validated and copied;
 * per-key cost remains unsampled.
 */
public final class SampledClientCostTerm {

    private final ClientCostTerm term;
    private final double[] fractions;
    private final long[] nanos;

    private SampledClientCostTerm(ClientCostTerm term, double[] fractions, long[] nanos) {
        this.term = term;
        this.fractions = fractions;
        this.nanos = nanos;
    }

    /** A term charged at its scalar cost, consuming no random draw. */
    public static SampledClientCostTerm scalar(ClientCostTerm term) {
        if (term == null) {
            throw new MissingSimDependencyException("client cost term (per-page client service cost)");
        }
        return new SampledClientCostTerm(term, null, null);
    }

    /**
     * A term sampled from measured quantiles.
     *
     * @param term      the same term (its {@code perPageNanos} is the published mean, kept for the run
     *                  record and for the provenance that travels with it)
     * @param fractions the quantile positions, strictly ascending in {@code (0, 1)}
     * @param nanos     the measured value at each position, ascending, same length
     */
    public static SampledClientCostTerm quantiles(ClientCostTerm term, double[] fractions, long[] nanos) {
        if (term == null) {
            throw new MissingSimDependencyException("client cost term (per-page client service cost)");
        }
        if (fractions == null || nanos == null || fractions.length == 0 || fractions.length != nanos.length) {
            throw new IllegalArgumentException("a quantile ladder needs one measured value per position, got "
                    + (fractions == null ? "null" : fractions.length) + " positions and "
                    + (nanos == null ? "null" : nanos.length) + " values");
        }
        for (int i = 0; i < fractions.length; i++) {
            if (fractions[i] <= 0.0 || fractions[i] >= 1.0) {
                throw new IllegalArgumentException("quantile positions must lie strictly inside (0, 1), got "
                        + fractions[i]);
            }
            if (nanos[i] < 0) {
                throw new IllegalArgumentException("a quantile value must be >= 0, got " + nanos[i]);
            }
            if (i > 0 && (fractions[i] <= fractions[i - 1] || nanos[i] < nanos[i - 1])) {
                throw new IllegalArgumentException("a quantile ladder must ascend in both position and "
                        + "value; position " + i + " does not");
            }
        }
        return new SampledClientCostTerm(term, fractions.clone(), nanos.clone());
    }

    /** The underlying term and its provenance. */
    public ClientCostTerm term() {
        return term;
    }

    /** Whether this term has a quantile ladder. */
    public boolean isSampled() {
        return fractions != null;
    }

    /** Draws this page's cost when sampled; otherwise returns its scalar cost. */
    public long drawNanos(int keys, SimRng rng) {
        if (fractions == null) {
            return term.costNanos(keys);
        }
        long perPage = sample(rng.nextDouble());
        return Math.addExact(perPage, term.costNanos(keys) - term.perPageNanos());
    }

    /** Returns the ladder value at {@code u}; package-private for interpolation tests. */
    long sample(double u) {
        if (u <= fractions[0]) {
            return nanos[0];
        }
        for (int i = 1; i < fractions.length; i++) {
            if (u <= fractions[i]) {
                double span = fractions[i] - fractions[i - 1];
                double position = (u - fractions[i - 1]) / span;
                return nanos[i - 1] + Math.round(position * (nanos[i] - nanos[i - 1]));
            }
        }
        return nanos[nanos.length - 1];
    }
}
