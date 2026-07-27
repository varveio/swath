/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.kernel.SimRng;

/**
 * A {@link ClientCostTerm} that can be <b>drawn from</b> rather than only averaged: the same
 * provenance-carrying magnitude, plus an optional ladder of measured quantiles a run samples through
 * its own client-cost tape.
 *
 * <p><b>Why a mean is not always enough.</b> Two of the measured client-side stages have a heavy
 * upper tail — a short common case with occasional work an order of magnitude larger (a rotation, a
 * flush, a stalled commit). Their mean sits several times above their median, so charging every page
 * the mean makes every page equally slow and erases the tail entirely: the run's total is roughly
 * right while its distribution is wrong, and it is the distribution that decides whether a policy
 * that bursts pays for the burst. Sampling reproduces both.
 *
 * <p><b>What the ladder is, exactly.</b> An empirical inverse CDF through the published quantiles,
 * linear between neighbours, flat below the lowest and flat above the highest. It deliberately does
 * <b>not</b> extrapolate past the largest measured value: nothing was measured out there, and a
 * simulator that invents its own tail is making a claim the data does not support. The consequence is
 * stated rather than hidden — a sampled term understates the extreme tail beyond its last quantile,
 * and its own drawn mean will not equal the published mean, because a mean is not recoverable from a
 * handful of quantiles.
 *
 * <p>Sampling is per <b>page</b>. The per-key component of the underlying term (if any) is added
 * unsampled: it is a linear cost of the page's size, not a random quantity.
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

    /**
     * A term charged at its mean, drawing nothing — the right form for a stage whose measured
     * distribution is tight, and the only form available for one published as a mean alone.
     */
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

    /** The underlying term, with the provenance a result has to be read against. */
    public ClientCostTerm term() {
        return term;
    }

    /** Whether this term draws (has a ladder) or is charged at its mean. */
    public boolean isSampled() {
        return fractions != null;
    }

    /**
     * This term's cost for one page of {@code keys} keys, drawn from {@code rng} when the term is
     * sampled and returned as the flat mean when it is not.
     */
    public long drawNanos(int keys, SimRng rng) {
        if (fractions == null) {
            return term.costNanos(keys);
        }
        long perPage = sample(rng.nextDouble());
        return Math.addExact(perPage, term.costNanos(keys) - term.perPageNanos());
    }

    /** The ladder's value at {@code u}; package-private so the interpolation is directly testable. */
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
