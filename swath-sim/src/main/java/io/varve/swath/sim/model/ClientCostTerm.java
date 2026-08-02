/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

/**
 * Per-page and per-key client cost, with provenance for interpreting results.
 *
 * <p>Terms are explicit. A zero term is allowed only for an exact-mode check and is not predictive.
 *
 * @param provenance  how much weight a result computed with this term can bear
 * @param perPageNanos client-side cost charged once per page returned
 * @param perKeyNanos  client-side cost charged per key in that page
 * @param sourceLabel  a short human label for where the numbers came from, for the run record
 */
public record ClientCostTerm(Provenance provenance, long perPageNanos, long perKeyNanos, String sourceLabel) {

    /** What claims a run using this term can support. */
    public enum Provenance {
        /** Measured, cross-checked, and within its predeclared acceptance band. */
        FINAL,
        /** Measured interim value; usable for development, not conclusions. */
        PROVISIONAL,
        /** Measured value with a failed cross-check; not quotable as a finding. */
        PROVISIONAL_WITH_DEFECT,
        /** Deliberately zero for an exact-mode check; never predictive. */
        ZEROED_FOR_EXACT_MODE
    }

    public ClientCostTerm {
        if (provenance == null) {
            throw new IllegalArgumentException("a client cost term must carry its provenance; a term "
                    + "without one would be read as predictive by default");
        }
        if (perPageNanos < 0 || perKeyNanos < 0) {
            throw new IllegalArgumentException("client cost must be >= 0, got perPage=" + perPageNanos
                    + " perKey=" + perKeyNanos);
        }
        if (sourceLabel == null || sourceLabel.isBlank()) {
            throw new IllegalArgumentException("a client cost term must say where it came from");
        }
        if (provenance == Provenance.ZEROED_FOR_EXACT_MODE && (perPageNanos != 0 || perKeyNanos != 0)) {
            throw new IllegalArgumentException("a ZEROED_FOR_EXACT_MODE term must actually be zero, got "
                    + "perPage=" + perPageNanos + " perKey=" + perKeyNanos);
        }
        if (provenance != Provenance.ZEROED_FOR_EXACT_MODE && perPageNanos == 0 && perKeyNanos == 0) {
            throw new IllegalArgumentException("a measured client cost term of exactly zero is almost "
                    + "certainly an unset input; use zeroedForExactMode(...) to zero it deliberately");
        }
    }

    /**
     * Returns a deliberate zero term for an exact-mode check.
     *
     * @param why the checked invariant, retained in the run record
     */
    public static ClientCostTerm zeroedForExactMode(String why) {
        return new ClientCostTerm(Provenance.ZEROED_FOR_EXACT_MODE, 0, 0, why);
    }

    /** Whether this term models real-system cost rather than exact-mode arithmetic; see its provenance for confidence. */
    public boolean isPredictive() {
        return provenance != Provenance.ZEROED_FOR_EXACT_MODE;
    }

    /**
     * Returns the client-side cost of a page with {@code keys} keys.
     *
     * <p>Checked arithmetic prevents overflow from becoming a negative virtual-time delay.
     */
    public long costNanos(int keys) {
        if (keys < 0) {
            throw new IllegalArgumentException("keys must be >= 0, got " + keys);
        }
        try {
            return Math.addExact(perPageNanos, Math.multiplyExact(perKeyNanos, (long) keys));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("client cost overflows a nanosecond count: perPage="
                    + perPageNanos + " perKey=" + perKeyNanos + " keys=" + keys, overflow);
        }
    }
}
