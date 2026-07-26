/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * One seed run's descent in progress (algorithms.md §8), holding whatever state the descent needs
 * between probes — the cut-point set and frontier assembled so far, the probe/sample budget spent,
 * which classification a truncated level is mid-disambiguating. A fresh instance is created per run by
 * {@link SeedPlanner#beginDescent()}; never reused across runs.
 *
 * <p>The executor drives it to the terminal {@link SeedPlan}: call {@link #start()}, and for every
 * {@link RequestSeedProbe} it returns, issue the described RPC and feed the distilled
 * {@link SeedProbeOutcome} back via {@link #onProbeResult}, repeating until the {@link SeedPlan} comes
 * back.
 */
public interface SeedDescent {

    /** The first probe this descent needs: the top-level structure probe of the scan prefix. */
    SeedAction start();

    /** Continue the descent with the result of the most recently requested probe. */
    SeedAction onProbeResult(SeedProbeOutcome outcome);
}
