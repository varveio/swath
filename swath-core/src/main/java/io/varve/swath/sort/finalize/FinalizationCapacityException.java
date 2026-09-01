/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import java.io.IOException;

/**
 * A finalization refusal raised when a configured budget cannot seat the minimum resources the
 * merge needs. Every staged input the merge reads is still durable and nothing has been published,
 * so the run defers rather than fails: raising the budget the subtype names and resuming re-enters
 * the merge with no new listing.
 *
 * <p>The supertype is what keeps that disposition consistent across layers. A caller classifies and
 * defers on this one type, so a new capacity refusal cannot reach the runtime as an unclassified
 * fatal I/O failure merely because someone forgot to add a branch for it.
 */
public abstract sealed class FinalizationCapacityException extends IOException
        permits CascadeCapacityExhaustedException, MergeMemoryExhaustedException {

    private final String errorClass;
    private final String deferral;

    FinalizationCapacityException(String errorClass, String deferral, String message) {
        super(message);
        this.errorClass = errorClass;
        this.deferral = deferral;
    }

    /** The classified fingerprint a refused run reports as its {@code error_class}. */
    public final String errorClass() {
        return errorClass;
    }

    /** What the run is waiting on, phrased for the deferral an operator reads and acts on. */
    public final String deferral() {
        return deferral;
    }
}
