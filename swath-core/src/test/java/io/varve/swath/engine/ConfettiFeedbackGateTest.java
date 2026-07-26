/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CyclicBarrier;
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

    /**
     * A tuned constant, pinned by LITERAL. {@code OwnerSplitGovernorTest}'s gate tests reference it
     * only symbolically (via comments spelling out "8", never an assertion against the constant
     * itself), so a change to the warmup floor is invisible to them. This is the confetti-feedback
     * gate's warmup sample floor (issue #22) — an accidental edit should fail a build, not silently
     * retune when the gate starts trusting its own ground truth.
     */
    @Test
    void minSampleIsPinnedToItsLiteralValue() {
        assertThat(ConfettiFeedbackGate.MIN_SAMPLE).isEqualTo(8);
    }

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
    // Concurrency (issue #22's disclosed relaxation): snapshot-then-consumeProbeSlot is two
    // separate calls, so two racing workers can share a pre-increment snapshot. The two tests below
    // split the disclosure's two claims apart, because a single test claiming BOTH turned out to
    // prove only one of them: a mutated, non-atomic consumeProbeSlot() (probeSeq.set(probeSeq.get()
    // + 1) in place of incrementAndGet()) still passed the forced-race test below 20/20 runs -- its
    // own "no increment lost" assertion never caught the mutant, because racing to increment right
    // after one barrier release rarely produces enough genuine contention to lose one.
    //   - concurrentConsumeProbeSlotSharesThePreIncrementReadUnderAForcedRace demonstrates ONLY the
    //     DETERMINISTIC half: a CyclicBarrier forces every racer's snapshot() read to happen-before
    //     every racer's consumeProbeSlot() call, so all racers PROVABLY observe the same
    //     pre-increment probeSeq (the shared-slot drift the disclosure describes, reproduced on
    //     demand rather than hoped for) -- never a scheduler-luck interleaving assertion (issue #18).
    //   - concurrentConsumeProbeSlotConservesEveryIncrementUnderGeneralLoad is the PROBABILISTIC half:
    //     plain concurrent stress, no forced ordering, asserting only the final conserved total.
    //     Measured against the same non-atomic mutant above: 100% (20/20) detection at this test's
    //     32 threads x 5,000 calls; a materially weaker 5% (1/20) at the previously-committed 16 x 200.
    // Neither test is a proof of atomicity -- no racing test can establish that to certainty (issue
    // #18 again, one level up: don't let a test imply a proof it can't deliver). The AUTHORITATIVE
    // conservation guarantee is that consumeProbeSlot() is a plain AtomicLong#incrementAndGet(), BY
    // INSPECTION -- see that method's own javadoc in ConfettiFeedbackGate.
    // -------------------------------------------------------------------------------------------

    @Test
    void concurrentConsumeProbeSlotSharesThePreIncrementReadUnderAForcedRace() throws Exception {
        // One barrier forces EVERY racer's snapshot() read to happen-before ANY racer's
        // consumeProbeSlot() call: the barrier only releases once all `racers` threads have
        // reached it, i.e. once every read has already completed -- so this is not hoping for a
        // lucky interleaving, it is structurally guaranteed by CyclicBarrier's own happens-before
        // semantics that all reads observe probeSeq == 0.
        int racers = 8;
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        CyclicBarrier allReadsDone = new CyclicBarrier(racers);
        long[] observedBeforeIncrement = new long[racers];
        Thread[] threads = new Thread[racers];
        for (int i = 0; i < racers; i++) {
            int idx = i;
            threads[i] = new Thread(() -> {
                observedBeforeIncrement[idx] = gate.snapshot().probeSeq();
                await(allReadsDone);
                gate.consumeProbeSlot();
            });
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        assertThat(observedBeforeIncrement)
                .as("every racer's read happened before any racer's increment (barrier-forced), so "
                        + "all %d racers observed the SAME pre-increment probeSeq -- the exact shared-slot "
                        + "drift this gate's javadoc discloses, reproduced on demand rather than hoped for",
                        racers)
                .containsOnly(0L);
        // Incidentally still conserved here too -- but this SPECIFIC forced setup does not reliably
        // catch a non-atomic consumeProbeSlot() (a mutated implementation passed this exact check
        // 20/20 runs): see concurrentConsumeProbeSlotConservesEveryIncrementUnderGeneralLoad below for
        // the probabilistic coverage that does, and consumeProbeSlot's own javadoc for the
        // by-inspection guarantee neither test can substitute for.
        assertThat(gate.snapshot().probeSeq())
                .as("incidentally conserved under this forced setup too, though not what it is designed "
                        + "to catch (see the comment above)")
                .isEqualTo(racers);
    }

    @Test
    void concurrentConsumeProbeSlotConservesEveryIncrementUnderGeneralLoad() throws Exception {
        // No forced ordering here -- plain concurrent stress, asserting only the final conserved
        // total (never an interleaving-dependent check). PROBABILISTIC coverage, not a proof: this
        // is the test that actually caught the non-atomic mutant described in the comment block
        // above, at a measured 100% (20/20) detection rate at these thread/call counts -- a
        // materially weaker 5% (1/20) at the previously-committed 16 x 200. Raised here so this
        // test carries real (if still imperfect, per issue #18's "no racing test can prove
        // atomicity" caveat) probabilistic weight rather than a mostly-decorative one.
        int threadCount = 32;
        int callsPerThread = 5000;
        ConfettiFeedbackGate gate = new ConfettiFeedbackGate();
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int c = 0; c < callsPerThread; c++) {
                    gate.consumeProbeSlot();
                }
            });
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        assertThat(gate.snapshot().probeSeq())
                .as("every consumeProbeSlot() call across every thread is counted exactly once")
                .isEqualTo((long) threadCount * callsPerThread);
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
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
