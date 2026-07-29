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
 * executor's direction, advances the probe sequence — either unconditionally
 * ({@link #consumeProbeSlot()}) or as the winner-take-one claim a decided probe carve needs
 * ({@link #claimProbeSlot(long)}, issue #31). Neither is a decision of its own: WHICH outcome a
 * consult is entitled to stays the governor's call over its view, and the claim only resolves which
 * of several equally-entitled consults gets the one slot they are contending for. {@link #MIN_SAMPLE} is still declared here (rather than moved alongside
 * {@code SUPPRESS_THRESHOLD}/{@code PROBE_K}, both of which did move) because it is referenced by
 * engine-level tests that construct realistic warmup scenarios against this gate directly, with no
 * reason to know about the policy package.
 *
 * <p>One instance per {@link WorkStealingScan} (run-scoped); every method is safe for concurrent
 * callers (workers race to both complete tagged children and attempt owner-splits).
 *
 * <p><b>The carve brake's completion window is fed independently of confetti's own counters</b>
 * (E-20's decoupling fix, punch-list row 26): {@link #recordConfettiClassification} touches ONLY
 * {@link #taggedTotal}/{@link #taggedConfetti} (confetti's own rate signal, gated by the executor on
 * {@code confetti_feedback}), and {@link #recordWindowCompletion} touches ONLY the {@link
 * #completionWindow} (the brake's signal, gated by the executor on {@code carve_brake != off}) — a
 * completed child can feed either, both, or (if neither toggle is on) neither, with no shared
 * counter to couple them. {@code OwnerSplitGovernor} reads the window's {@link
 * CarveCompletionWindow#windowAverage(long, int)} through {@link
 * #carveBrakeWindowAverage(long, int)}, resolving the run's chosen K at that call, not at record
 * time (one window instance serves every {@code carve_brake} mode). The brake's own periodic probe
 * (every {@code CARVE_BRAKE_PROBE_K}-th would-be-braked consult) needs its OWN sequence — {@link
 * #carveBrakeProbeSeq} — independent of confetti's {@link #probeSeq}, since the two gates'
 * over-threshold populations are distinct; {@link #consumeCarveBrakeProbeSlot()}/{@link
 * #claimCarveBrakeProbeSlot(long)} mirror {@link #consumeProbeSlot()}/{@link
 * #claimProbeSlot(long)} exactly, for the identical issue #31 reason (N owners sharing a
 * pre-increment snapshot all decide PROBE, so only a CAS-claimed carve may actually publish).
 *
 * <p>Public so {@code io.varve.swath.engine.policy}'s owner-split governor package can read a
 * {@link #snapshot()} of this same run-scoped instance. The tagged-child lifecycle — {@link
 * #recordCompletion}, and {@link OwnerSelfSplit}'s claim/drain/remove-before-tag bookkeeping that
 * feeds it — stays executor-owned: classification happens at node completion, an event entirely
 * outside the per-carve decision this gate's measurements feed, tied to real node ids and {@link
 * WorkerState} the policy package never sees (contracts.md §2.1's source-agnostic rule).
 */
public final class ConfettiFeedbackGate {

    /** Warmup floor: below this many tagged-child completions there is no basis to suppress. */
    public static final long MIN_SAMPLE = 8;

    private final AtomicLong taggedTotal = new AtomicLong();
    private final AtomicLong taggedConfetti = new AtomicLong();
    private final AtomicLong probeSeq = new AtomicLong();
    private final AtomicLong carveBrakeProbeSeq = new AtomicLong();
    private final CarveCompletionWindow completionWindow = new CarveCompletionWindow();

    /**
     * A coherent read of this gate's counters at one instant, in the same order {@code decide()}
     * used to read them (total, then confetti) before issue #22's fix moved the classification math
     * to {@code OwnerSplitGovernor}. {@code probeSeq} is the run's current probe sequence number —
     * the count of {@link #consumeProbeSlot()} calls so far, i.e. the number of PRIOR over-threshold
     * confetti consults, whatever their outcome. {@code carveBrakeProbeSeq} is the brake's OWN probe
     * sequence, independent of {@code probeSeq} (see this class's javadoc). The carve brake's window
     * average is deliberately NOT one of this snapshot's fields — see {@link
     * #carveBrakeWindowAverage(long, int)}, whose {@code k}/{@code maxKeys} parameters this
     * no-argument snapshot has no way to carry. Four independent counters, not a single atomic
     * snapshot: a torn read across them only nudges which page-commit sees a rate/probe-slot
     * transition, never correctness (mirrors the density EWMA's own lock-free read tolerance).
     */
    public record Snapshot(long taggedTotal, long taggedConfetti, long probeSeq, long carveBrakeProbeSeq) {
    }

    /**
     * Fold one tagged owner-split child's completion classification (ground truth) into the
     * run-level confetti rate the NEXT {@link #snapshot()} reflects. Called at most once per tagged
     * child (the caller removes it from the tagged set before calling, so double-completion is
     * impossible), and ONLY when {@code confetti_feedback} is on — the carve brake's window is fed
     * separately by {@link #recordWindowCompletion}, on its OWN gate ({@code carve_brake != off}),
     * so a completed child can feed either counter, both, or neither (E-20's decoupling fix). Public
     * so a test outside {@code io.varve.swath.engine} can warm this collaborator up directly (e.g.
     * the owner-split governor's own table-driven tests).
     */
    public void recordConfettiClassification(boolean confetti) {
        taggedTotal.incrementAndGet();
        if (confetti) {
            taggedConfetti.incrementAndGet();
        }
    }

    /**
     * Fold one tagged owner-split child's realized mass and split status into the carve brake's
     * completion window — independent of {@link #recordConfettiClassification} (E-20's decoupling
     * fix): the executor calls this whenever {@code carve_brake != off}, regardless of whether
     * {@code confetti_feedback} is also on. See {@link CarveCompletionWindow}'s javadoc for the
     * window's own coherence/split-awareness/zero-warmup contract.
     *
     * @param mass  the child's realized emitted key count (keys emitted by completion)
     * @param split whether the child itself split further (owner self-split or a successful thief
     *              steal) during its own lifetime
     */
    public void recordWindowCompletion(long mass, boolean split) {
        completionWindow.record(mass, split);
    }

    /** A coherent read of this run's tagged-completion tallies and probe sequences. */
    public Snapshot snapshot() {
        return new Snapshot(taggedTotal.get(), taggedConfetti.get(), probeSeq.get(), carveBrakeProbeSeq.get());
    }

    /**
     * The carve brake's split-aware effective-mass window average, resolved at THIS call against
     * {@code k}/{@code maxKeys} (not baked into storage — see {@link CarveCompletionWindow}'s
     * javadoc) — {@link Double#NaN} iff no completion has fed the window yet (zero warmup).
     *
     * @param k       the run's {@code carve_brake} mode multiplier ({@code CarveBrakeMode#k()});
     *                the caller passes {@code 0} when the mode is {@code off} (the window is empty
     *                in that case regardless, since {@link #recordWindowCompletion} is never called)
     * @param maxKeys the run's page size — the effective-mass floor's unit
     */
    public double carveBrakeWindowAverage(long k, int maxKeys) {
        return completionWindow.windowAverage(k, maxKeys);
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
     * increment (it is still a plain {@code incrementAndGet}).
     *
     * <p><b>Issue #31 corrected what that race costs.</b> An earlier version of this javadoc said
     * "only which SPECIFIC consult lands on a shared slot can vary" — an understatement, and it is
     * retracted here: N owners sharing a snapshot at a slot boundary all decided {@code PROBE} and so
     * all CARVED, multiplying the confetti-sized carves this gate exists to suppress, rather than
     * merely shifting cadence. That is fixed by {@link #claimProbeSlot(long)}, which the executor now
     * calls for a decided probe carve; this method remains the unconditional advance for a consult
     * that was suppressed outright (and for a probe consult whose carve the governor then abandoned
     * on its own pivot checks).
     *
     * <p><b>The authoritative conservation guarantee is BY INSPECTION, not by any racing test:</b>
     * {@link #probeSeq} is a plain {@link java.util.concurrent.atomic.AtomicLong}, and {@link
     * #consumeProbeSlot()} calls only its {@code incrementAndGet()} — no read-modify-write gap for a
     * race to land in. No concurrent test can *prove* atomicity to certainty (issue #18: don't let a
     * test imply a proof it can't deliver); the type is the proof.
     *
     * <p><b>What is and isn't tested for this race.</b> {@code ConfettiFeedbackGateTest}'s
     * {@code concurrentConsumeProbeSlotSharesThePreIncrementReadUnderAForcedRace} proves only the
     * DETERMINISTIC half directly against this class, under a barrier-forced worst case (all racers'
     * reads provably happen-before any of their increments): the racers' {@link #snapshot()} reads DO
     * share the same pre-increment {@code probeSeq} value — the drift this javadoc describes,
     * reproduced on demand rather than hoped for. It does <b>not</b> reliably catch a non-atomic
     * {@code consumeProbeSlot()} — a mutated implementation ({@code probeSeq.set(probeSeq.get() + 1)})
     * still passed that test's own "no increment lost" assertion 20/20 runs, because racing to
     * increment right after one barrier release rarely produces enough genuine contention to lose
     * one. {@code concurrentConsumeProbeSlotConservesEveryIncrementUnderGeneralLoad}'s plain
     * concurrent-stress final-total check is the one that actually caught that same mutant — at a
     * measured 100% (20/20) detection rate at its 32-threads x 5,000-calls contention level — but
     * that is PROBABILISTIC coverage, same caveat as above. No engine-level test exercises this race:
     * every {@code run(...)} in {@code ConfettiFeedbackContractTest} uses {@code workers=1}
     * (structurally single-threaded, so it cannot race a shared {@code probeSeq}), and {@code
     * ConfettiFeedbackWiringTest} (the one {@code workers=4} confetti scenario) drives a
     * dense/uniform keyspace that, by its own assertion, never crosses {@code SUPPRESS_THRESHOLD} —
     * so it never reaches this branch under concurrency either. (An earlier version of this fix's
     * commit message claimed {@code ConfettiFeedbackContractTest} covered this race; it does not, for
     * the {@code workers=1} reason above — corrected here since that commit's history cannot be
     * amended.)
     */
    public void consumeProbeSlot() {
        probeSeq.incrementAndGet();
    }

    /**
     * Claim the periodic probe slot the governor's decision landed on — <b>issue #31's fix</b>, and
     * the step that restores "at most one carve per probe slot".
     *
     * <p>The governor decides {@code PROBE} from {@code (probeSeq + 1) % PROBE_K == 0} over the
     * {@link #snapshot()} the executor took, so N owners that all snapshot the same {@code
     * expectedProbeSeq} before any of them advances it ALL compute a probe hit and would all carve —
     * multiplying exactly the confetti-sized carves this gate exists to prevent. Pre-#22 the fused
     * {@code incrementAndGet()} handed each caller a distinct value, so exactly one carved and the
     * other N−1 were suppressed. This restores that, without putting the decision back inside the
     * gate: the governor still decides purely over its view, and the executor asks here whether its
     * consult actually won the slot it decided on.
     *
     * <p>Exactly one concurrent caller can win a given {@code expectedProbeSeq} — a single
     * {@code compareAndSet}. A loser still <b>consumes</b> a slot ({@code incrementAndGet}), because
     * its consult happened: the sequence therefore advances once per over-threshold consult, winner
     * or loser, which is byte-for-byte what the pre-#22 fused increment did (N racers at {@code s}
     * left the counter at {@code s + N}, with only the caller that got {@code s + 1} probing).
     *
     * @param expectedProbeSeq the {@code probeSeq} the deciding view was built from
     * @return {@code true} iff this consult won the slot and its carve may proceed; {@code false} iff
     *         a concurrent consult took it, in which case this consult is suppressed exactly as the
     *         pre-#22 code would have suppressed it
     */
    public boolean claimProbeSlot(long expectedProbeSeq) {
        if (probeSeq.compareAndSet(expectedProbeSeq, expectedProbeSeq + 1)) {
            return true;
        }
        probeSeq.incrementAndGet();   // lost the slot, but this consult still consumed one
        return false;
    }

    /**
     * The carve brake's OWN unconditional probe-sequence advance — {@link #consumeProbeSlot()}'s
     * exact twin, for the brake's independent sequence. Called by the executor whenever the
     * governor's decision carries a carve-brake {@code CONSUME_CARVE_BRAKE_PROBE_SLOT} mutation:
     * every consult that crossed into the brake's over-threshold branch, regardless of outcome.
     */
    public void consumeCarveBrakeProbeSlot() {
        carveBrakeProbeSeq.incrementAndGet();
    }

    /**
     * The carve brake's OWN probe-slot claim — {@link #claimProbeSlot(long)}'s exact twin, against
     * {@link #carveBrakeProbeSeq} instead of {@link #probeSeq} (issue #31's fix, mirrored: N owners
     * sharing the same pre-increment {@code carveBrakeProbeSeq} snapshot would all decide {@code
     * carve_brake_probe} and so all carve, multiplying exactly the confetti-sized carves the brake
     * exists to suppress). Exactly one concurrent caller wins a given {@code
     * expectedCarveBrakeProbeSeq}; a loser still consumes a slot, so the sequence advances once per
     * over-threshold consult, winner or loser — the same accounting {@link #claimProbeSlot(long)}
     * gives confetti's sequence.
     *
     * @param expectedCarveBrakeProbeSeq the {@code carveBrakeProbeSeq} the deciding view was built from
     * @return {@code true} iff this consult won the slot and its carve may proceed
     */
    public boolean claimCarveBrakeProbeSlot(long expectedCarveBrakeProbeSeq) {
        if (carveBrakeProbeSeq.compareAndSet(expectedCarveBrakeProbeSeq, expectedCarveBrakeProbeSeq + 1)) {
            return true;
        }
        carveBrakeProbeSeq.incrementAndGet();   // lost the slot, but this consult still consumed one
        return false;
    }
}
