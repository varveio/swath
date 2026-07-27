/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

/**
 * The kinds of store call a run makes, distinguished because their latency distributions are not the
 * same one. A worker page asks for up to a thousand keys and pays for them; a pivot probe asks for
 * exactly one and is dominated by the round trip; a structure probe rolls a whole directory level up
 * and is dominated by the server-side walk. Fitting a single distribution across all three and then
 * asking which policy issues fewer calls would answer the wrong question.
 */
public enum CallClass {

    /** A worker listing its own range: a full page request. */
    WORKER_PAGE,
    /** A split's single-key probe, used to place a pivot. */
    PIVOT_PROBE,
    /** A delimited probe that rolls up a level's common prefixes. */
    STRUCTURE_PROBE,
    /** A probe issued by the seed descent, before any worker starts. */
    SEED_PROBE
}
