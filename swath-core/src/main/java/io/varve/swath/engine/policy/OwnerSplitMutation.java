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
     * Advance the confetti feedback gate's probe sequence ({@code ConfettiFeedbackGate#
     * consumeProbeSlot()}). Carried by BOTH outcomes of the confetti check's over-threshold branch
     * ({@code CONFETTI_SUPPRESSED} and the {@code confetti_probe}-engaged {@link Carve}) — mirrors
     * the gate's own pre-#22 {@code decide()}, which incremented its probe counter unconditionally
     * once over threshold, before checking which of the two outcomes it landed on.
     */
    CONSUME_CONFETTI_PROBE_SLOT
}
