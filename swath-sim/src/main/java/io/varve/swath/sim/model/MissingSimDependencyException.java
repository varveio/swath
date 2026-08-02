/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

/** Missing a required simulation input; an unmeasured value is distinct from a measured zero. */
public final class MissingSimDependencyException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    /** @param dependency the missing input */
    public MissingSimDependencyException(String dependency) {
        super("missing simulation dependency: " + dependency);
    }
}
