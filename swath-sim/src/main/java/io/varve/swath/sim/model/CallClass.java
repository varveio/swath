/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

/** Store-call classes with distinct latency distributions. */
public enum CallClass {

    /** A worker's page request. */
    WORKER_PAGE,
    /** A split's single-key pivot probe. */
    PIVOT_PROBE,
    /** A delimited probe for common prefixes. */
    STRUCTURE_PROBE,
    /** A probe issued during seed descent. */
    SEED_PROBE
}
