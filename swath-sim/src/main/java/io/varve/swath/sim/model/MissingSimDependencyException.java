/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

/**
 * A scenario was constructed without an input the model cannot honestly default.
 *
 * <p>This exists so that a missing measurement fails loudly at construction instead of quietly
 * becoming a zero. A simulator whose client-side per-page cost defaults to zero will report that the
 * fastest strategy is the one that pulls the most pages per second, because in that model pages are
 * free once they arrive — a conclusion that is not merely imprecise but the opposite of what a
 * client-bound system does. "We have not measured this yet" and "we measured it and it is zero" are
 * different states of the world, and only the second one may be simulated.
 */
public final class MissingSimDependencyException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    /**
     * @param dependency what is missing, named the way a reader who has to go find it would name it
     */
    public MissingSimDependencyException(String dependency) {
        super("missing simulation dependency: " + dependency);
    }
}
