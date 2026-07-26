/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * Which cascade step a {@link RequestKeyProbe} belongs to (algorithms.md §3) — not needed to
 * construct the probe request itself (every key probe is the identical single-key
 * {@code start_after} shape), but needed by the executor to attribute the {@code
 * swath.empty_upper_bisections} mechanical counter to exactly the bisection-loop probes, the same
 * way the current engine does (never on the initial/step-back/reflect probes).
 */
public enum KeyProbePhase {
    /** The far-ahead (or plain midpoint) placement's first probe. */
    INITIAL,
    /** The far-ahead step-back's re-probe at the plain code-point midpoint. */
    STEP_BACK,
    /** The density-reflected pivot's probe. */
    REFLECT,
    /** One retry-nearer-cursor bisection halving. */
    BISECT
}
