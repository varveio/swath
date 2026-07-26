/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * A coherent read of the confetti feedback gate's counters at one page-commit (algorithms.md
 * §3.3, issue #22): plain counts, so source-agnostic like every other {@link OwnerSplitView}
 * field — no {@code ConfettiFeedbackGate} reference crosses into this package. Mirrors {@code
 * ConfettiFeedbackGate.Snapshot} field-for-field; the executor maps one to the other when
 * building the view.
 *
 * @param taggedTotal    tagged owner-split children completed so far this run
 * @param taggedConfetti of those, how many classified confetti (realized mass below the floor,
 *                       never itself split)
 * @param probeSeq       the run's current probe sequence number — how many times the governor's
 *                        confetti check has previously crossed into its over-threshold branch,
 *                        regardless of outcome
 */
public record ConfettiObservation(long taggedTotal, long taggedConfetti, long probeSeq) {
}
