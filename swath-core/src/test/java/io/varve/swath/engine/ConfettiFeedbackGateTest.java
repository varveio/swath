/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Direct unit coverage of {@link ConfettiFeedbackGate}'s bookkeeping — {@link
 * ConfettiFeedbackGate#recordCompletion}/{@link ConfettiFeedbackGate#snapshot} and {@link
 * ConfettiFeedbackGate#consumeProbeSlot} — plus {@link OwnerSelfSplit#isConfettiChild}, the
 * confetti-vs-substantial classification predicate the gate's realized-mass evidence is built
 * from: a tagged child is confetti only if it BOTH has a small own tally AND never itself split.
 * Exercised directly (pure counters, no engine machinery). Issue #22's fix moved the
 * warmup/threshold/probe-cycle DECISION logic out of this class entirely — that boundary coverage
 * now lives in {@code io.varve.swath.engine.policy.OwnerSplitGovernorTest}, which exercises it as
 * pure arithmetic over a {@code ConfettiObservation} view field, not against a live gate.
 */
final class ConfettiFeedbackGateTest {

    // -------------------------------------------------------------------------------------------
    // recordCompletion / snapshot: the tagged-completion tallies accumulate correctly.
    // -------------------------------------------------------------------------------------------

    @Test
    void freshGateSnapshotsAllZero() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        assertThat(gate.snapshot()).isEqualTo(new ConfettiFeedbackGate.Snapshot(0, 0, 0));
    }

    @Test
    void recordCompletionAccumulatesTotalAndConfettiSeparately() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        gate.recordCompletion(true);
        gate.recordCompletion(true);
        gate.recordCompletion(false);
        ConfettiFeedbackGate.Snapshot snap = gate.snapshot();
        assertThat(snap.taggedTotal()).as("every completion counts toward the total").isEqualTo(3);
        assertThat(snap.taggedConfetti()).as("only the confetti-classified ones count here").isEqualTo(2);
    }

    @Test
    void recordCompletionNeverTouchesProbeSeq() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        for (int i = 0; i < 20; i++) {
            gate.recordCompletion(i % 2 == 0);
        }
        assertThat(gate.snapshot().probeSeq())
                .as("completions and the probe sequence are independent counters")
                .isZero();
    }

    // -------------------------------------------------------------------------------------------
    // consumeProbeSlot: advances probeSeq, independent of the completion tallies.
    // -------------------------------------------------------------------------------------------

    @Test
    void consumeProbeSlotAdvancesProbeSeqByOneEachCall() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        gate.consumeProbeSlot();
        assertThat(gate.snapshot().probeSeq()).isEqualTo(1);
        gate.consumeProbeSlot();
        gate.consumeProbeSlot();
        assertThat(gate.snapshot().probeSeq()).isEqualTo(3);
    }

    @Test
    void consumeProbeSlotNeverTouchesTheCompletionTallies() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        gate.recordCompletion(true);
        for (int i = 0; i < 5; i++) {
            gate.consumeProbeSlot();
        }
        ConfettiFeedbackGate.Snapshot snap = gate.snapshot();
        assertThat(snap.taggedTotal()).isEqualTo(1);
        assertThat(snap.taggedConfetti()).isEqualTo(1);
        assertThat(snap.probeSeq()).isEqualTo(5);
    }

    // -------------------------------------------------------------------------------------------
    // isConfettiChild: the classification predicate. Confetti requires BOTH a small own tally AND
    // never-split; a child that split further is ALWAYS substantial, regardless of how tiny its own
    // tally turns out (it shed a productive subtree onward).
    // -------------------------------------------------------------------------------------------

    private static final int MAX_KEYS = 100;

    @Test
    void neverSplitAndSmallTallyIsConfetti() {
        // The genuine terminal-leaf pathology: a 1-page child that never split further and stayed
        // small.
        assertThat(OwnerSelfSplit.isConfettiChild(1L, false, MAX_KEYS))
                .as("never split, tally << 2*maxKeys -> confetti")
                .isTrue();
        assertThat(OwnerSelfSplit.isConfettiChild(2L * MAX_KEYS, false, MAX_KEYS))
                .as("never split, tally == 2*maxKeys exactly (boundary inclusive) -> confetti")
                .isTrue();
    }

    @Test
    void splitFurtherIsAlwaysSubstantialEvenWithATinyOwnTally() {
        // The classification case: a healthy intermediate node in a deep owner-split recursion that
        // shed several further tails onward and therefore finished with a tiny own tally -- this
        // must NOT be confetti; shedding a productive subtree IS the signal the gate wants,
        // regardless of the final tally size.
        assertThat(OwnerSelfSplit.isConfettiChild(1L, true, MAX_KEYS))
                .as("split further, tiny own tally -> still substantial (hasSplit wins)")
                .isFalse();
        assertThat(OwnerSelfSplit.isConfettiChild(0L, true, MAX_KEYS))
                .as("split further, zero own tally -> still substantial")
                .isFalse();
    }

    @Test
    void neverSplitButLargeTallyIsSubstantial() {
        assertThat(OwnerSelfSplit.isConfettiChild(2L * MAX_KEYS + 1, false, MAX_KEYS))
                .as("never split but tally just above the floor -> substantial")
                .isFalse();
    }

    @Test
    void splitFurtherAndLargeTallyIsSubstantial() {
        assertThat(OwnerSelfSplit.isConfettiChild(1_000_000L, true, MAX_KEYS))
                .as("split further and a large own tally -> substantial (both conditions agree)")
                .isFalse();
    }
}
