/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * The one random draw the thief brain's pivot cascade needs (issue #20: the structure-probe
 * suppression recovery's 1-in-{@code STRUCTURE_SUPPRESS_RETRY_DIVISOR} escape hatch, {@code
 * ThiefPolicy#structureProbingEnabled}) — a policy-owned seam, not a {@code java.util.random} type
 * crossing into this package. Reaching for ambient randomness directly from inside a policy made
 * that one decision unreproducible from its view alone; injecting this instead makes {@code
 * ThiefPolicy} deterministic given its inputs, the same as every other decision in the cascade.
 *
 * <p>{@code Thief} supplies the engine's live default ({@code bound ->
 * ThreadLocalRandom.current().nextInt(bound)}) — deliberately the SAME ambient source the engine
 * drew from before this fix, so live-run behavior is unchanged and thread-safety is inherited
 * (many workers share one {@code Thief}, hence one {@code ThiefPolicy}, hence one draw source).
 * Ambient-ness becomes an executor concern outside the policy package instead of living inside it.
 * A per-worker seeded generator for reproducible LIVE runs is a separate decision, not this fix.
 * Tests and a future simulator inject a seeded or scripted implementation instead.
 */
@FunctionalInterface
public interface DecisionRng {

    /** A uniformly distributed {@code int} in {@code [0, bound)}, {@code bound > 0}. */
    int nextInt(int bound);
}
