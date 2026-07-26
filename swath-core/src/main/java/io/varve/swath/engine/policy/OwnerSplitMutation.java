/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * A durable-state change the owner-split governor decided but does not itself apply — mirrors
 * {@link VictimMutation}'s shape, minus the addressing: owner-split concerns only the one worker
 * being considered (never a pool of candidates), so there is no {@code victimNodeId} to carry.
 * Returned from {@link OwnerSplitDecision#mutations()}; the executor ({@code OwnerSelfSplit})
 * applies it against the real collaborator.
 */
public enum OwnerSplitMutation {
    /**
     * Advance the confetti feedback gate's probe sequence unconditionally ({@code
     * ConfettiFeedbackGate#consumeProbeSlot()}) — the over-threshold consult happened and consumed a
     * slot, whatever it decided. Carried by the {@code CONFETTI_SUPPRESSED} {@link Skip}, and by a
     * probe consult the governor then abandoned on its own pivot checks (an {@code
     * UNSPLITTABLE_PIVOT}/floor {@link Skip} downstream of the probe branch). Mirrors the gate's own
     * pre-#22 {@code decide()}, which incremented its probe counter unconditionally once over
     * threshold, before checking which outcome it landed on.
     */
    CONSUME_CONFETTI_PROBE_SLOT,

    /**
     * Claim the periodic probe slot this decision landed on ({@code
     * ConfettiFeedbackGate#claimProbeSlot(long)}), and carve only if the claim wins — <b>issue
     * #31</b>. Carried by the {@code confetti_probe}-engaged {@link Carve} in place of {@link
     * #CONSUME_CONFETTI_PROBE_SLOT}.
     *
     * <p>The governor decides {@code PROBE} from the {@code probeSeq} its view was built from, so N
     * owners that all snapshot the same value before any of them advances it all decide {@code PROBE}
     * — and, before this mutation existed, all CARVED, multiplying exactly the confetti-sized carves
     * the gate exists to suppress. The claim restores "at most one carve per slot" without putting the
     * decision back inside the gate: {@code decide(view)} stays a pure function of its view, and the
     * conditionality is explicit IN the decision rather than an executor override — the same shape as
     * the split CAS's own {@code SPLIT_ABORTED} path, where a decided carve can still be declined by
     * an atomic the executor owns. A losing claim is suppressed exactly as pre-#22 (and still consumes
     * a slot), which is why the executor resolves it BEFORE recording any of the carve's engagements:
     * on the suppressed path none of them ever fired.
     */
    CLAIM_CONFETTI_PROBE_SLOT
}
