/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.EngineToggles;
import org.junit.jupiter.api.Test;

/**
 * Pins {@link HybridSeedPlanner}'s tuned, load-bearing constants to their LITERAL values —
 * {@code SeedStepTest}/{@code SeedMassAwareDescentTest} and neighbors reference several of these
 * only symbolically (or as a comment), so a change to any one of them is invisible to those suites —
 * they would stay green testing the NEW value against itself (the same gap {@code
 * ThiefPolicyConstantsTest}/{@code OwnerSplitGovernorTest}/{@code FutilityPacingPolicyTest} close for
 * their own packages). {@code DELIMITER} is deliberately NOT pinned here — it is a store convention
 * ({@code /}), not a tuning knob.
 */
class HybridSeedPlannerConstantsTest {

    @Test
    void tunedConstantsArePinnedToTheirLiteralValues() {
        assertThat(HybridSeedPlanner.PROBE_PAGE).isEqualTo(1000);
        assertThat(HybridSeedPlanner.SAMPLE_WIDTH).isEqualTo(3);
        assertThat(HybridSeedPlanner.SAMPLE_BUDGET).isEqualTo(32);
        assertThat(HybridSeedPlanner.SAMPLE_DENSE_MIN_OBJECTS).isEqualTo(8);
        assertThat(HybridSeedPlanner.MIN_WEIGHT_SAMPLES).isEqualTo(8);
    }

    /**
     * The probe-budget cap ({@code maxProbes <= 256}) is not a bare constant — it is
     * {@code min(256, targetSeeds)} — so it is pinned through {@link HybridSeedPlanner#probeBudget()}'s
     * observable behavior instead: a worker count large enough to drive {@code targetSeeds} (
     * {@code min(1000, 4*workerCount)}) past 256 must still be capped at exactly 256.
     */
    @Test
    void probeBudgetCapsAtTwoHundredFiftySixEvenForALargeWorkerCount() {
        HybridSeedPlanner planner = new HybridSeedPlanner(null, 1000, EngineToggles.DEFAULT);
        assertThat(planner.probeBudget()).isEqualTo(256);
    }
}
