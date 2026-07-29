/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.policy.Carve;
import io.varve.swath.engine.policy.OwnerSplitDecision;
import io.varve.swath.engine.policy.OwnerSplitMutation;
import io.varve.swath.engine.policy.OwnerSplitSkipReason;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Regression coverage for {@link OwnerSelfSplit#resolveProbeClaims} — the executor-level claim
 * resolution a dual-claim {@link Carve} (both {@code CLAIM_CONFETTI_PROBE_SLOT} and {@code
 * CLAIM_CARVE_BRAKE_PROBE_SLOT}) must go through. A prior version of this method resolved
 * confetti's claim first and returned immediately on a loss, before the carve brake's claim was
 * ever even attempted — silently dropping the brake's claim (its sequence never advanced) whenever
 * confetti's claim lost. Drives the REAL {@link ConfettiFeedbackGate} directly (a "minimal harness
 * around the claim-resolution logic" — the method itself is the production code, extracted static
 * and package-private specifically so this suite can call it without a fully-simulated engine run)
 * so this is a genuine regression guard against that exact defect, not a re-implementation of it.
 */
final class OwnerSelfSplitClaimResolutionTest {

    private static final byte[] PIVOT = {0x61};

    private static Carve dualClaimCarve() {
        return new Carve(PIVOT, List.of(),
                List.of(OwnerSplitMutation.CLAIM_CONFETTI_PROBE_SLOT, OwnerSplitMutation.CLAIM_CARVE_BRAKE_PROBE_SLOT));
    }

    /**
     * <b>The exact defect this test guards against.</b> The confetti slot is ALREADY TAKEN (another
     * consult won it first, advancing {@code probeSeq} past the value this decision's own snapshot
     * was built from) before {@code resolveProbeClaims} is called with a decision that ALSO carries
     * a live carve-brake claim. The confetti claim must lose (stale expectation); the carve brake's
     * claim must STILL be attempted and its sequence must STILL advance by exactly one — a broken
     * resolver that bails after the first lost claim would leave {@code carveBrakeProbeSeq} at 0
     * forever for this consult, which is exactly what the prior version of this method did.
     */
    @Test
    void confettiClaimAlreadyTakenStillResolvesAndAdvancesTheBrakesOwnClaim() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        assertThat(gate.claimProbeSlot(0)).as("fixture: someone else wins the confetti slot first").isTrue();
        ConfettiFeedbackGate.Snapshot staleSnapshot = new ConfettiFeedbackGate.Snapshot(8, 8, 0, 0);

        OwnerSplitDecision decision = dualClaimCarve();
        OwnerSelfSplit.ClaimResolution result = OwnerSelfSplit.resolveProbeClaims(gate, decision, staleSnapshot);

        assertThat(result.probeSlotClaimed()).as("the confetti claim must lose (stale expectation)").isFalse();
        assertThat(result.bailReason()).isEqualTo(OwnerSplitSkipReason.CONFETTI_SUPPRESSED);
        assertThat(result.carveBrakeProbeSlotClaimed())
                .as("the brake's claim was actually attempted (untouched carveBrakeProbeSeq == 0 matches "
                        + "the snapshot, so it wins)")
                .isTrue();
        assertThat(gate.snapshot().carveBrakeProbeSeq())
                .as("THE REGRESSION: the brake's own independent sequence still advanced by exactly one "
                        + "for this consult, even though the confetti claim (resolved first) lost -- a "
                        + "broken resolver that bails after the first lost claim would leave this at 0")
                .isEqualTo(1L);
        assertThat(gate.snapshot().probeSeq())
                .as("confetti's sequence: the pre-take win (0->1) plus this losing attempt's own "
                        + "fallback increment (1->2)")
                .isEqualTo(2L);
    }

    /** The symmetric case: the carve brake's slot is already taken, confetti's is free. */
    @Test
    void carveBrakeClaimAlreadyTakenStillResolvesAndAdvancesConfettisOwnClaim() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        assertThat(gate.claimCarveBrakeProbeSlot(0))
                .as("fixture: someone else wins the carve-brake slot first").isTrue();
        ConfettiFeedbackGate.Snapshot staleSnapshot = new ConfettiFeedbackGate.Snapshot(8, 8, 0, 0);

        OwnerSplitDecision decision = dualClaimCarve();
        OwnerSelfSplit.ClaimResolution result = OwnerSelfSplit.resolveProbeClaims(gate, decision, staleSnapshot);

        assertThat(result.probeSlotClaimed())
                .as("confetti's claim was actually attempted and won (untouched probeSeq == 0 matches)")
                .isTrue();
        assertThat(result.carveBrakeProbeSlotClaimed()).as("the brake claim must lose (stale expectation)")
                .isFalse();
        assertThat(result.bailReason())
                .as("confetti's claim won, so the brake's loss is the terminal reason")
                .isEqualTo(OwnerSplitSkipReason.CARVE_BRAKED);
        assertThat(gate.snapshot().probeSeq())
                .as("confetti's own sequence still advanced by exactly one via its genuine win")
                .isEqualTo(1L);
    }

    /** Both slots already taken: exactly one terminal reason is reported, and BOTH sequences advance. */
    @Test
    void bothClaimsAlreadyTakenReportsConfettisReasonAndAdvancesBothSequences() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        assertThat(gate.claimProbeSlot(0)).isTrue();
        assertThat(gate.claimCarveBrakeProbeSlot(0)).isTrue();
        ConfettiFeedbackGate.Snapshot staleSnapshot = new ConfettiFeedbackGate.Snapshot(8, 8, 0, 0);

        OwnerSplitDecision decision = dualClaimCarve();
        OwnerSelfSplit.ClaimResolution result = OwnerSelfSplit.resolveProbeClaims(gate, decision, staleSnapshot);

        assertThat(result.probeSlotClaimed()).isFalse();
        assertThat(result.carveBrakeProbeSlotClaimed()).isFalse();
        assertThat(result.bailReason())
                .as("confetti's own reason takes priority when both lost -- exactly one reason reported")
                .isEqualTo(OwnerSplitSkipReason.CONFETTI_SUPPRESSED);
        assertThat(gate.snapshot().probeSeq()).as("pre-take win + this losing attempt's fallback").isEqualTo(2L);
        assertThat(gate.snapshot().carveBrakeProbeSeq())
                .as("the brake's claim was still attempted (and lost) even though confetti's reason wins")
                .isEqualTo(2L);
    }

    /** Neither slot contested: both claims win, no bail, and both sequences advance by one each. */
    @Test
    void neitherClaimContestedBothWinAndNeitherBails() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        ConfettiFeedbackGate.Snapshot freshSnapshot = gate.snapshot();

        OwnerSplitDecision decision = dualClaimCarve();
        OwnerSelfSplit.ClaimResolution result = OwnerSelfSplit.resolveProbeClaims(gate, decision, freshSnapshot);

        assertThat(result.probeSlotClaimed()).isTrue();
        assertThat(result.carveBrakeProbeSlotClaimed()).isTrue();
        assertThat(result.bailReason()).isNull();
        assertThat(gate.snapshot().probeSeq()).isEqualTo(1L);
        assertThat(gate.snapshot().carveBrakeProbeSeq()).isEqualTo(1L);
    }
}
