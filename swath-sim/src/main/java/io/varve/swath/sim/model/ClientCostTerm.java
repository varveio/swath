/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

/**
 * What a page costs the client after it arrives: parsing it, emitting its rows, and committing the
 * cursor that covers them. A first-class term of the model, on the same footing as network latency.
 *
 * <p><b>Why it is first-class, and why it may not default.</b> A listing client that is fast enough
 * on the wire becomes bound by what it does with each page, and at that point the ranking of two
 * strategies is decided almost entirely by this term. Omit it and the simulator will happily
 * "discover" strategies that are impossible — ones whose whole advantage is fetching pages the client
 * could never have processed. A term is therefore always supplied explicitly, and always carries
 * where it came from ({@link Provenance}), so a result can be read together with the confidence of
 * the input that produced it.
 *
 * <p>Zeroing the term is legal and sometimes necessary — the kernel's exact-mode invariants require
 * it — but only through {@link #zeroedForExactMode(String)}, which records that the zero was chosen
 * rather than defaulted, and marks any run using it as an arithmetic check rather than a prediction.
 *
 * @param provenance  how much weight a result computed with this term can bear
 * @param perPageNanos client-side cost charged once per page returned
 * @param perKeyNanos  client-side cost charged per key in that page
 * @param sourceLabel  a short human label for where the numbers came from, for the run record
 */
public record ClientCostTerm(Provenance provenance, long perPageNanos, long perKeyNanos, String sourceLabel) {

    /** How far a term can be trusted, and what a run using it may be used to claim. */
    public enum Provenance {
        /** Measured, cross-checked, and inside its predeclared acceptance band. */
        FINAL,
        /** Measured, but published as an interim value — usable to build against, not to conclude from. */
        PROVISIONAL,
        /** Measured and failed at least one of its own cross-checks; the failure travels with the term. */
        PROVISIONAL_WITH_DEFECT,
        /** Deliberately zero, so that an analytic invariant is exact. Never a prediction. */
        ZEROED_FOR_EXACT_MODE
    }

    public ClientCostTerm {
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
     * A deliberately zero term, for a run whose point is a closed-form equality rather than a
     * prediction.
     *
     * @param why the invariant being checked, recorded so a stray zeroed run is identifiable later
     */
    public static ClientCostTerm zeroedForExactMode(String why) {
        return new ClientCostTerm(Provenance.ZEROED_FOR_EXACT_MODE, 0, 0, why);
    }

    /** Whether a result computed with this term may be quoted as a prediction about the real system. */
    public boolean isPredictive() {
        return provenance != Provenance.ZEROED_FOR_EXACT_MODE;
    }

    /** The total client-side cost of one page of {@code keys} keys. */
    public long costNanos(int keys) {
        if (keys < 0) {
            throw new IllegalArgumentException("keys must be >= 0, got " + keys);
        }
        return perPageNanos + perKeyNanos * keys;
    }
}
