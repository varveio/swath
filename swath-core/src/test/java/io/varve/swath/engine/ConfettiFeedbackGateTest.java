/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Direct unit coverage of {@link ConfettiFeedbackGate}'s bookkeeping — the warmup
 * floor, the threshold boundary, and the every-{@code PROBE_K}-th probe escape — plus
 * {@link OwnerSelfSplit#isConfettiChild}, the confetti-vs-substantial classification predicate the
 * gate's realized-mass evidence is built from: a tagged child is confetti only if it BOTH has a
 * small own tally AND never itself split. Exercised directly (package-private, pure arithmetic +
 * atomics, no engine machinery) so the exact decision sequence is pinned without driving the whole
 * engine to a precise realized-mass distribution. This is an ordinary unit guard of the gate's
 * bookkeeping, not the adversarial skewed-keyspace regression, which lives in a separate suite.
 */
final class ConfettiFeedbackGateTest {

    // -------------------------------------------------------------------------------------------
    // Warmup: below MIN_SAMPLE, always CARVE regardless of how confetti-heavy the observed rate is.
    // -------------------------------------------------------------------------------------------

    @Test
    void belowMinSampleAlwaysCarvesEvenAtOneHundredPercentConfetti() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        // MIN_SAMPLE - 1 all-confetti completions: still warming up, no basis to suppress yet.
        for (long i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE - 1; i++) {
            gate.recordCompletion(true);
            assertThat(gate.decide())
                    .as("warmup sample %d/%d: must still carve", i + 1, ConfettiFeedbackGate.MIN_SAMPLE)
                    .isEqualTo(ConfettiFeedbackGate.Decision.CARVE);
        }
    }

    @Test
    void exactlyMinSampleAllConfettiTripsTheGate() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        for (long i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE; i++) {
            gate.recordCompletion(true);
        }
        // rate = 1.0 > 0.5 threshold, warmup satisfied (total == MIN_SAMPLE) -> the gate engages
        // (first over-threshold decide() is attempt #1, not a multiple of PROBE_K -> SUPPRESSED).
        assertThat(gate.decide()).isEqualTo(ConfettiFeedbackGate.Decision.SUPPRESSED);
    }

    // -------------------------------------------------------------------------------------------
    // Threshold boundary: rate <= SUPPRESS_THRESHOLD (0.5) never suppresses; strictly above does.
    // -------------------------------------------------------------------------------------------

    @Test
    void rateAtExactlyThresholdDoesNotSuppress() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        // 4 confetti / 8 total = 0.5 == SUPPRESS_THRESHOLD exactly -> "<= threshold" carves.
        for (int i = 0; i < 4; i++) {
            gate.recordCompletion(true);
        }
        for (int i = 0; i < 4; i++) {
            gate.recordCompletion(false);
        }
        assertThat(gate.decide())
                .as("rate == threshold exactly must still carve (strictly-greater-than triggers suppression)")
                .isEqualTo(ConfettiFeedbackGate.Decision.CARVE);
    }

    @Test
    void rateJustAboveThresholdSuppresses() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        // 5 confetti / 8 total = 0.625 > 0.5 -> the gate engages.
        for (int i = 0; i < 5; i++) {
            gate.recordCompletion(true);
        }
        for (int i = 0; i < 3; i++) {
            gate.recordCompletion(false);
        }
        assertThat(gate.decide()).isEqualTo(ConfettiFeedbackGate.Decision.SUPPRESSED);
    }

    // -------------------------------------------------------------------------------------------
    // Probe escape: every PROBE_K-th would-be-suppressed decide() is let through as a PROBE.
    // -------------------------------------------------------------------------------------------

    @Test
    void everyProbeKthSuppressedAttemptIsAProbe() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        for (int i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE; i++) {
            gate.recordCompletion(true);   // 100% confetti -> always over threshold from here on
        }
        long probeK = ConfettiFeedbackGate.PROBE_K;
        for (long attempt = 1; attempt <= probeK * 3; attempt++) {
            ConfettiFeedbackGate.Decision decision = gate.decide();
            if (attempt % probeK == 0) {
                assertThat(decision).as("attempt %d is the %d-th probe slot", attempt, probeK)
                        .isEqualTo(ConfettiFeedbackGate.Decision.PROBE);
            } else {
                assertThat(decision).as("attempt %d is not a probe slot", attempt)
                        .isEqualTo(ConfettiFeedbackGate.Decision.SUPPRESSED);
            }
        }
    }

    @Test
    void probeCounterIsIndependentOfRecordedCompletionsAfterWarmup() {
        // Once suppressing, decide() alone (no further recordCompletion calls) still advances the
        // probe counter -- the K-th call is a probe regardless of whether the run has recorded any
        // MORE completions since warmup (the probe counter and the completion tally are separate).
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        for (int i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE; i++) {
            gate.recordCompletion(true);
        }
        for (int i = 0; i < ConfettiFeedbackGate.PROBE_K - 1; i++) {
            assertThat(gate.decide()).isEqualTo(ConfettiFeedbackGate.Decision.SUPPRESSED);
        }
        assertThat(gate.decide()).isEqualTo(ConfettiFeedbackGate.Decision.PROBE);
    }

    // -------------------------------------------------------------------------------------------
    // Recovery: a run of substantial (non-confetti) completions after the gate engaged brings the
    // observed rate back at/under the threshold, and CARVE resumes without needing a probe to win.
    // -------------------------------------------------------------------------------------------

    @Test
    void recordingEnoughSubstantialCompletionsRecoversTheRate() {
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        for (int i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE; i++) {
            gate.recordCompletion(true);   // 8/8 confetti -> suppressing
        }
        assertThat(gate.decide()).isEqualTo(ConfettiFeedbackGate.Decision.SUPPRESSED);

        // 8 more substantial completions -> 8 confetti / 16 total = 0.5 == threshold -> recovers.
        for (int i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE; i++) {
            gate.recordCompletion(false);
        }
        assertThat(gate.decide())
                .as("rate recovered to exactly the threshold -> carving resumes")
                .isEqualTo(ConfettiFeedbackGate.Decision.CARVE);
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
