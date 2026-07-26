/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The realized-child-mass feedback gate for owner self-split. The floor
 * ({@link StealMath#childTailBelowObservedMassFloor}) already reasons about the child tail's realized
 * mass, but only from UPSTREAM estimates ({@code est}/{@code densityRatio}, both extrapolated
 * from the in-cluster density already emitted) — on a keyspace whose code-point tail thins out
 * hard past the drained cluster, those estimates still pass carves whose
 * REALIZED emitted mass turns out confetti-sized (typically a single page), because the estimate
 * has no way to see the mostly-empty tail it is extrapolating across.
 *
 * <p>This gate closes that loop with GROUND TRUTH: it tracks, across every owner-split child
 * this run has tagged, how many turned out confetti versus substantial. A tagged child is
 * classified confetti only when BOTH hold at its completion: (1) its own emitted tally is
 * {@code <= 2*maxKeys} (the same threshold the floor uses), AND (2) it never itself split
 * during its lifetime — neither an owner self-split nor a successful thief steal ever carved a
 * child off it in turn ({@link WorkerState#hasSplit()}). Condition (2) is load-bearing, not an
 * edge case: on a dense/uniform range (this mechanism's OWN target shape) owner self-split
 * recurses deliberately deep — "each child recursively re-drains and self-splits again" (see
 * {@link OwnerSelfSplit#SELF_SPLIT_MIN_PAGES_BETWEEN}'s javadoc) — so a healthy intermediate node routinely
 * finishes with a small own tally purely because it shed its own further tail(s) onward; that is
 * proof the carve was worthwhile, not evidence of a thinning tail, and must never count against
 * the observed rate. Only a node that never split AND still finished tiny is a genuine terminal
 * confetti leaf — exactly the pathology this gate targets (there the 1-page children
 * never split further; they just end).
 *
 * <p>Once at least {@link #MIN_SAMPLE} tagged children have completed (a warmup — too few
 * samples is not evidence), a confetti rate above {@link #SUPPRESS_THRESHOLD} suppresses further
 * owner-split carving outright. A {@code PROBE_K}-th would-be-suppressed carve is let through
 * anyway (a cheap round-robin counter, not gated on anything else) so the feedback loop cannot
 * starve itself permanently: if the keyspace later turns genuinely dense again, that probe's own
 * realized mass folds back in and the observed rate falls, resuming ordinary carving without any
 * external intervention.
 *
 * <p>{@link #MIN_SAMPLE}/{@link #SUPPRESS_THRESHOLD}/{@link #PROBE_K} are hand-picked constants,
 * not yet tuned by a sweep. One instance per {@link WorkStealingScan} (run-scoped); every
 * method is safe for concurrent callers (workers race to both complete tagged children and
 * attempt owner-splits).
 *
 * <p>Public so {@code io.varve.swath.engine.policy}'s owner-split governor can hold this same
 * run-scoped instance as a collaborator and consult {@link #decide()} as part of its own pure
 * per-call decision (a deterministic read of this gate's already-recorded atomics, no I/O). The
 * tagged-child lifecycle — {@link #recordCompletion}, and {@link OwnerSelfSplit}'s
 * claim/drain/remove-before-tag bookkeeping that feeds it — stays executor-owned: classification
 * happens at node completion, an event entirely outside the per-carve decision this gate is
 * consulted from, tied to real node ids and {@link WorkerState} the policy package never sees
 * (seam-notes.md's source-agnostic rule).
 */
public final class ConfettiFeedbackGate {

    /** Warmup floor: below this many tagged-child completions there is no basis to suppress. */
    public static final long MIN_SAMPLE = 8;
    /** Suppress once the observed confetti rate is STRICTLY above this fraction. */
    static final double SUPPRESS_THRESHOLD = 0.5;
    /** Let every K-th would-be-suppressed carve through anyway, to keep the feedback alive. */
    static final long PROBE_K = 16;

    private final AtomicLong taggedTotal = new AtomicLong();
    private final AtomicLong taggedConfetti = new AtomicLong();
    private final AtomicLong probeCounter = new AtomicLong();

    /** The carve-time decision {@link #decide()} returns. */
    public enum Decision {
        /** Carve normally — below warmup, or the observed rate is at/under the threshold. */
        CARVE,
        /** The rate is over threshold, but this is the {@code PROBE_K}-th attempt — let it through. */
        PROBE,
        /** The rate is over threshold and this attempt is not the probe slot — suppress the carve. */
        SUPPRESSED
    }

    /**
     * Fold one tagged owner-split child's completion classification (ground truth) into the
     * run-level rate the NEXT {@link #decide()} call reads. Called at most once per tagged
     * child (the caller removes it from the tagged set before calling, so double-completion is
     * impossible). Public so a test outside {@code io.varve.swath.engine} can warm this
     * collaborator up directly (e.g. the owner-split governor's own table-driven tests).
     */
    public void recordCompletion(boolean confetti) {
        taggedTotal.incrementAndGet();
        if (confetti) {
            taggedConfetti.incrementAndGet();
        }
    }

    /** The carve-time decision, evaluated fresh against the current observed rate. */
    public Decision decide() {
        long total = taggedTotal.get();
        if (total < MIN_SAMPLE) {
            return Decision.CARVE;   // warmup: not enough realized-mass evidence yet
        }
        double rate = (double) taggedConfetti.get() / (double) total;
        if (rate <= SUPPRESS_THRESHOLD) {
            return Decision.CARVE;   // observed confetti rate is acceptable
        }
        // Over threshold: every PROBE_K-th such attempt is let through as a probe so the gate
        // cannot starve its own feedback signal (a keyspace that turns dense again recovers).
        long n = probeCounter.incrementAndGet();
        return (n % PROBE_K == 0) ? Decision.PROBE : Decision.SUPPRESSED;
    }
}
