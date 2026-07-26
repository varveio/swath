/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The realized-child-mass feedback gate for owner self-split: pure MEASUREMENT state, not
 * decision logic. The floor ({@link StealMath#childTailBelowObservedMassFloor}) already reasons
 * about the child tail's realized mass, but only from UPSTREAM estimates ({@code est}/{@code
 * densityRatio}, both extrapolated from the in-cluster density already emitted) — on a keyspace
 * whose code-point tail thins out hard past the drained cluster, those estimates still pass
 * carves whose REALIZED emitted mass turns out confetti-sized (typically a single page), because
 * the estimate has no way to see the mostly-empty tail it is extrapolating across.
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
 * <p><b>Issue #22's fix moved the classification math out of this class.</b> Once at least {@link
 * #MIN_SAMPLE} tagged children have completed (a warmup — too few samples is not evidence),
 * {@code io.varve.swath.engine.policy}'s {@code OwnerSplitGovernor} reads a {@link #snapshot()} of
 * this gate's counters and decides, as a pure function of the view, whether the observed confetti
 * rate suppresses further owner-split carving — with a periodic probe so a keyspace that later
 * turns genuinely dense recovers on its own. This class now only ever MEASURES (accumulates
 * {@link #recordCompletion} calls and hands back a coherent {@link #snapshot()}) and, at the
 * executor's direction, {@linkplain #consumeProbeSlot() advances the probe sequence} — it makes no
 * decision of its own. {@link #MIN_SAMPLE} is still declared here (rather than moved alongside
 * {@code SUPPRESS_THRESHOLD}/{@code PROBE_K}, both of which did move) because it is referenced by
 * engine-level tests that construct realistic warmup scenarios against this gate directly, with no
 * reason to know about the policy package.
 *
 * <p>One instance per {@link WorkStealingScan} (run-scoped); every method is safe for concurrent
 * callers (workers race to both complete tagged children and attempt owner-splits).
 *
 * <p>Public so {@code io.varve.swath.engine.policy}'s owner-split governor package can read a
 * {@link #snapshot()} of this same run-scoped instance. The tagged-child lifecycle — {@link
 * #recordCompletion}, and {@link OwnerSelfSplit}'s claim/drain/remove-before-tag bookkeeping that
 * feeds it — stays executor-owned: classification happens at node completion, an event entirely
 * outside the per-carve decision this gate's measurements feed, tied to real node ids and {@link
 * WorkerState} the policy package never sees (seam-notes.md's source-agnostic rule).
 */
public final class ConfettiFeedbackGate {

    /** Warmup floor: below this many tagged-child completions there is no basis to suppress. */
    public static final long MIN_SAMPLE = 8;

    private final AtomicLong taggedTotal = new AtomicLong();
    private final AtomicLong taggedConfetti = new AtomicLong();
    private final AtomicLong probeSeq = new AtomicLong();

    /**
     * A coherent read of this gate's counters at one instant, in the same order {@code decide()}
     * used to read them (total, then confetti) before issue #22's fix moved the classification math
     * to {@code OwnerSplitGovernor}. {@code probeSeq} is the run's current probe sequence number —
     * the count of {@link #consumeProbeSlot()} calls so far, i.e. the number of PRIOR over-threshold
     * consults, whatever their outcome. Three independent counters, not a single atomic snapshot: a
     * torn read across them only nudges which page-commit sees the rate/probe-slot transition,
     * never correctness (mirrors the density EWMA's own lock-free read tolerance).
     */
    public record Snapshot(long taggedTotal, long taggedConfetti, long probeSeq) {
    }

    /**
     * Fold one tagged owner-split child's completion classification (ground truth) into the
     * run-level rate the NEXT {@link #snapshot()} reflects. Called at most once per tagged
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

    /** A coherent read of this run's tagged-completion tallies and current probe sequence. */
    public Snapshot snapshot() {
        return new Snapshot(taggedTotal.get(), taggedConfetti.get(), probeSeq.get());
    }

    /**
     * Advance the probe sequence: called by the executor exactly when {@code
     * OwnerSplitGovernor}'s decision carries a {@code CONSUME_CONFETTI_PROBE_SLOT} mutation — i.e.
     * every time the governor's confetti check crossed into its over-threshold branch, regardless
     * of whether that consult landed on {@code SUPPRESSED} or the periodic probe (mirrors {@code
     * decide()}'s old unconditional {@code probeCounter.incrementAndGet()} in that branch, byte-for-
     * byte). Two workers racing this call can read the SAME {@link #snapshot()} before either
     * advances it and so land on the same probe slot — the gate is run-scoped while {@code
     * WorkerState#lock()} is per-worker, so nothing serializes two owners' consults against each
     * other the way {@code decide()}'s single call used to. The counter itself never loses an
     * increment (it is still a plain {@code incrementAndGet}); only which SPECIFIC consult lands on
     * a shared slot can vary under a race. See {@code OwnerSplitGovernor}'s commit message (the
     * fix for issue #22) for why this relaxation is acceptable: nothing in I1-I12 touches probe
     * cadence, and this class's own javadoc already called the mechanism "a cheap round-robin
     * counter, not gated on anything else".
     *
     * <p><b>What is and isn't tested for this race.</b> {@code ConfettiFeedbackGateTest}'s
     * {@code concurrentConsumeProbeSlotNeverLosesAnIncrementUnderAForcedRace} test proves both
     * halves of the claim above directly against this class, under a barrier-forced worst case (all
     * racers' reads provably happen-before any of their increments): the racers' {@link #snapshot()}
     * reads DO share the same pre-increment {@code probeSeq} value (the drift this javadoc
     * describes, reproduced on demand rather than hoped for), and {@link #probeSeq} still ends up
     * exactly at the number of {@link #consumeProbeSlot()} calls made — no increment lost. No
     * engine-level test exercises this race: every {@code run(...)} in {@code
     * ConfettiFeedbackContractTest} uses {@code workers=1} (structurally single-threaded, so it
     * cannot race a shared {@code probeSeq}), and {@code ConfettiFeedbackWiringTest} (the one
     * {@code workers=4} confetti scenario) drives a dense/uniform keyspace that, by its own
     * assertion, never crosses {@code SUPPRESS_THRESHOLD} — so it never reaches this branch under
     * concurrency either. (An earlier version of this fix's commit message claimed {@code
     * ConfettiFeedbackContractTest} covered this race; it does not, for the {@code workers=1} reason
     * above — corrected here since that commit's history cannot be amended.)
     */
    public void consumeProbeSlot() {
        probeSeq.incrementAndGet();
    }
}
