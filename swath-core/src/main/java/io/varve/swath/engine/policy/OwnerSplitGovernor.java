/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import io.varve.swath.engine.ConfettiFeedbackGate;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.RemainingWorkEstimator;
import io.varve.swath.engine.StealMath;
import io.varve.swath.engine.TailFloorMode;
import io.varve.swath.model.KeyBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The owner-split governor: the owner-side proactive self-split's gate chain (algorithms.md §3.3)
 * as an {@link OwnerSplitPolicy}. Byte ordering is S3-lexicographic throughout — {@link StealMath}
 * is called directly as the concrete thing it is; this extraction deliberately does not introduce
 * an ordering/comparator/key-codec abstraction for a hypothetical second source (rule of three —
 * see contracts.md §2.1).
 *
 * <p><b>The confetti feedback gate is pure measurement, not a collaborator consulted for a
 * decision (issue #22's fix).</b> This class owns the classification math entirely: {@code
 * SUPPRESS_THRESHOLD}/{@code PROBE_K} live here, and {@link #decide} reads the run's tagged-child
 * tallies and probe sequence off {@link OwnerSplitView#confetti()} — a plain value the executor
 * snapshots off {@code ConfettiFeedbackGate} once per call — instead of consulting a live gate
 * reference. {@code decide(view)} is now a genuine pure function of its argument: nothing here
 * mutates shared state, so the over-threshold branch instead RETURNS a {@link
 * OwnerSplitMutation#CONSUME_CONFETTI_PROBE_SLOT} mutation for the executor to apply to the real
 * gate, on both outcomes ({@code SUPPRESSED} and the periodic probe) — mirroring the gate's own
 * former {@code decide()}, which incremented its probe counter unconditionally once over
 * threshold, before checking which of the two outcomes it landed on.
 */
public final class OwnerSplitGovernor implements OwnerSplitPolicy {

    /**
     * Owner-side proactive self-split. A draining worker carves its OWN range at
     * page-commit only when its estimated remaining work exceeds this many pages (× {@code maxKeys}
     * keys) — i.e. a large dense drain, not a range about to finish. Small so a genuine mega-day
     * (~110 pages) qualifies while ordinary short ranges never do.
     */
    public static final long SELF_SPLIT_MIN_REMAINING_PAGES = 4;
    /**
     * ...and at most once per this many committed non-empty pages, so a fast tail sheds <b>O(1)</b>
     * self-splits per drain (each child recursively re-drains and self-splits again — an O(log) ramp)
     * rather than shattering into a child per page ("confetti"). Paired with the progress-gate
     * ({@code WorkerState#markStolen()}) which already bounds carves to ≤1 per emitted page.
     */
    public static final long SELF_SPLIT_MIN_PAGES_BETWEEN = 32;
    /**
     * Suppress once the observed confetti rate is STRICTLY above this fraction (moved here from
     * {@code ConfettiFeedbackGate} by issue #22's fix — this is the classification decision, the
     * gate now only measures).
     */
    static final double SUPPRESS_THRESHOLD = 0.5;
    /** Let every K-th would-be-suppressed carve through anyway, to keep the feedback alive. */
    static final long PROBE_K = 16;

    private final EngineToggles toggles;
    private final int workerCount;
    private final int maxKeys;
    private final RemainingWorkEstimator estimator;

    /**
     * @param toggles     the {@code --engine-toggle} ablation namespace this run was constructed with
     * @param workerCount the fixed configured worker count {@code Tmax} (the demand gate's threshold)
     * @param maxKeys     the page size (the remaining-est floor's and the observed-mass floor's unit)
     * @param estimator   the position sensor the remaining-work gates read; {@code null}-defaults to
     *                    {@link RemainingWorkEstimator#WINDOW}, the shipped reading, so a caller that
     *                    does not steer this seam gets exactly the arithmetic it always had
     */
    public OwnerSplitGovernor(EngineToggles toggles, int workerCount, int maxKeys,
                              RemainingWorkEstimator estimator) {
        this.toggles = toggles == null ? EngineToggles.DEFAULT : toggles;
        this.workerCount = workerCount;
        this.maxKeys = maxKeys;
        this.estimator = estimator == null ? RemainingWorkEstimator.WINDOW : estimator;
    }

    @Override
    public OwnerSplitDecision decide(OwnerSplitView view) {
        byte[] H = view.hi();
        if (H == null) {
            return new Skip(OwnerSplitSkipReason.OPEN_FRONTIER, List.of(), List.of());
        }
        byte[] cursorTo = view.cursorTo();
        byte[] lo = view.lo();
        // Plain code-point estRemaining (NOT the rank-space variant): an owner-split is zero-probe
        // (interpolate is pure math, no LIST), so under-firing here trades cheap owner-splits for
        // costlier thief structure-probes on the un-split tail — strictly worse. The rank-space
        // deflation belongs only to the pivot synthesis below (interpolate(..., alphabetDigest())),
        // which needs no estRemaining change to land on a populated value.
        double est = estimator.estRemaining(cursorTo, lo, H, view.keysEmitted());
        // What the sensor's reading did (§5 discipline), collected before the gates it feeds so a
        // refusal carries its own reading's classification. Under this site's OWN category: one
        // reading per qualifying page commit, which is the denominator the OWNER_SPLIT.* skip
        // reasons below are already counted against. Inert for the shipped reading.
        List<Engagement> engagements = new ArrayList<>();
        estimator.classify("SENSING_OWNER", cursorTo, lo, H, view.keysEmitted(), engagements);
        // The rate limit's input, hoisted so every decision below can report it (§7's
        // owner_split_decision event) — read, not computed, so it is known at every gate.
        long pagesSinceLastSelfSplit = view.committed() - view.lastSelfSplitPage();
        if (est <= (double) SELF_SPLIT_MIN_REMAINING_PAGES * maxKeys) {
            // Remaining work too small to be worth a proactive carve — issue #16.
            engagements.add(new Engagement("OWNER_SPLIT", OwnerSplitSkipReason.REMAINING_EST_FLOOR.code()));
            return new Skip(OwnerSplitSkipReason.REMAINING_EST_FLOOR, engagements, List.of(),
                    gateInputs(OwnerSplitSkipReason.REMAINING_EST_FLOOR.code(), view, est, pagesSinceLastSelfSplit,
                            OwnerSplitGateInputs.NOT_COMPUTED, OwnerSplitGateInputs.NOT_COMPUTED));
        }
        if (pagesSinceLastSelfSplit < SELF_SPLIT_MIN_PAGES_BETWEEN) {
            // Rate-limit: O(1) self-splits per drain, not one per page.
            engagements.add(new Engagement("OWNER_SPLIT", OwnerSplitSkipReason.RATE_LIMITED.code()));
            return new Skip(OwnerSplitSkipReason.RATE_LIMITED, engagements, List.of(),
                    gateInputs(OwnerSplitSkipReason.RATE_LIMITED.code(), view, est, pagesSinceLastSelfSplit,
                            OwnerSplitGateInputs.NOT_COMPUTED, OwnerSplitGateInputs.NOT_COMPUTED));
        }
        // Owner-split DEMAND GATE. On a SATURATED bucket the ready queue already holds enough live
        // nodes to keep every worker busy, so an extra child buys ZERO parallelism and only costs a
        // wasted page (its bounded final page is fetched full, then trimmed per key). Suppress the
        // carve once live nodes reach T; below T (during ramp) the gate stays open so the engine
        // still ramps to T busy workers. {@code workerCount > 1} guards a T=1 run: with no thief at
        // all, "buys zero parallelism" is moot, and gating would only shrink the durable checkpoint
        // granularity for the lone worker without ever saving an S3 call — it drains the un-split
        // range at the identical cost either way. See docs/internals/metrics-internals.md §5.
        if (workerCount > 1 && view.outstanding() >= (long) workerCount) {
            engagements.add(new Engagement("OWNER_SPLIT", OwnerSplitSkipReason.DEMAND_GATED.code()));
            return new Skip(OwnerSplitSkipReason.DEMAND_GATED, engagements, List.of(),
                    gateInputs(OwnerSplitSkipReason.DEMAND_GATED.code(), view, est, pagesSinceLastSelfSplit,
                            OwnerSplitGateInputs.NOT_COMPUTED, OwnerSplitGateInputs.NOT_COMPUTED));
        }
        // Far-ahead pivot fraction from the worker's own zero-cost density (>= 0.5 ⇒ >= byteMidpoint).
        // The child owns the far tail — so floor that tail above two pages: even below the demand gate an
        // owner-split must never fission into a ~1-page "confetti" child whose bounded final page is
        // fetched full then trimmed per key. The floor measures the tail in OBSERVED-density terms —
        // the plain span share (1-f)*est over-states a thinning tail on skewed keyspaces (see
        // StealMath.childTailBelowObservedMassFloor's math).
        double f = toggles.farAheadFraction(view.densityFraction());
        double densityRatio = toggles.observedDensityRatio(view.observedDensityRatio());
        if (tailFloorBlocks(est, f, densityRatio, engagements)) {
            engagements.add(new Engagement("OWNER_SPLIT", OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED.code()));
            return new Skip(OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED, engagements, List.of(),
                    gateInputs(OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED.code(), view, est,
                            pagesSinceLastSelfSplit, f, densityRatio));
        }
        // CONFETTI FEEDBACK GATE. The gates above reason from upstream estimates (est/densityRatio);
        // on a keyspace whose tail thins out over most of the observed span those estimates still
        // pass a carve whose REALIZED mass turns out confetti-sized. Once the run has accumulated
        // enough tagged-child evidence (MIN_SAMPLE), a high observed confetti rate suppresses further
        // carving directly from that ground truth — with a periodic probe so a keyspace that later
        // turns genuinely dense recovers on its own. This is now pure arithmetic over the view's own
        // ConfettiObservation snapshot (issue #22) — see ConfettiFeedbackGate's javadoc and
        // docs/internals/metrics-internals.md §5.
        List<OwnerSplitMutation> mutations = List.of();
        // The terminal reason a CARVE reports (§7): the last gate the carve path engaged, or the
        // plain published carve when it engaged none.
        String carveReason = "self_published";
        if (toggles.confettiFeedback()) {
            ConfettiObservation obs = view.confetti();
            if (obs.taggedTotal() >= ConfettiFeedbackGate.MIN_SAMPLE) {
                double rate = (double) obs.taggedConfetti() / (double) obs.taggedTotal();
                if (rate > SUPPRESS_THRESHOLD) {
                    List<OwnerSplitMutation> probeSlot = List.of(OwnerSplitMutation.CONSUME_CONFETTI_PROBE_SLOT);
                    if ((obs.probeSeq() + 1) % PROBE_K == 0) {
                        carveReason = "confetti_probe";
                        engagements.add(new Engagement("OWNER_SPLIT", carveReason));
                        // CLAIM, not CONSUME (issue #31): this decision is a pure function of the
                        // probeSeq the view was built from, so every owner sharing that snapshot
                        // decides PROBE here -- and all of them would carve. The executor resolves the
                        // claim against the run-scoped gate and lets exactly one through, suppressing
                        // the rest exactly as the pre-#22 fused increment did. If the pivot checks
                        // below then abandon this carve, the Skip they return carries the
                        // unconditional CONSUME instead, since no carve is left to serialize.
                        mutations = List.of(OwnerSplitMutation.CLAIM_CONFETTI_PROBE_SLOT);
                    } else {
                        engagements.add(new Engagement("OWNER_SPLIT", OwnerSplitSkipReason.CONFETTI_SUPPRESSED.code()));
                        return new Skip(OwnerSplitSkipReason.CONFETTI_SUPPRESSED, engagements, probeSlot,
                                gateInputs(OwnerSplitSkipReason.CONFETTI_SUPPRESSED.code(), view, est,
                                        pagesSinceLastSelfSplit, f, densityRatio));
                    }
                }
            }
        }
        // Synthesize the pivot at fraction f in the observed-alphabet rank space so it lands on a
        // populated value.
        byte[] m = toggles.interpolate(cursorTo, H, f, view.alphabetDigest(), engagements);
        if (m == null
                || KeyBytes.compareUnsigned(cursorTo, m) >= 0
                || KeyBytes.compareUnsigned(m, H) > 0) {
            // Unsplittable, or pivot not strictly in (cursorTo, H] — skip this page. Recurs (not a
            // one-off): estRemaining's span heuristic can diverge from true byte-adjacency on a deep
            // shared prefix, the same measurement/reality gap algorithms.md §3.2 documents for the
            // thief side — see OwnerSplitSkipReason#UNSPLITTABLE_PIVOT's javadoc.
            engagements.add(new Engagement("OWNER_SPLIT", OwnerSplitSkipReason.UNSPLITTABLE_PIVOT.code()));
            // A probe consult that gets here has no carve left to serialize, so it degrades to the
            // unconditional advance (issue #31): the slot was granted and spent, exactly as the
            // pre-#22 fused increment spent it, and no other owner's carve is being excluded.
            return new Skip(OwnerSplitSkipReason.UNSPLITTABLE_PIVOT, engagements,
                    consumeInsteadOfClaim(mutations),
                    gateInputs(OwnerSplitSkipReason.UNSPLITTABLE_PIVOT.code(), view, est, pagesSinceLastSelfSplit,
                            f, densityRatio));
        }
        // Engagement (§5): did the observed-alphabet chooser land the owner-split pivot on a
        // populated value (differs from the plain code-point interpolate at the same fraction)? Recorded
        // about the INTERPOLATED pivot (the alphabet chooser's own output), before the reflection clamp below.
        byte[] plainPivot = StealMath.interpolate(cursorTo, H, f);
        engagements.add(new Engagement("ALPHABET",
                !Arrays.equals(m, plainPivot) ? "alphabet_chosen" : "alphabet_fallback"));
        // Owner-split reflection clamp (gated by `reflect`). On a skewed keyspace the
        // f-interpolated pivot can overshoot the observed mass into vacuum (a near-empty child); the
        // density-reflected pivot m_r = extrapolate(lo, cursor, H) marks where the drained mass reflects
        // to. Clamp the pivot DOWN to m_r ONLY when m_r is strictly below the interpolated pivot (the
        // interpolate overshot) AND the clamped child tail (m_r, H] still clears the observed-mass
        // floor — so the split lands inside the mass instead of carving vacuum. On a uniform keyspace
        // m_r >= m (reflection reaches at least as far as f), so the clamp never engages (f's skew is
        // load-bearing there — don't clamp blind). Tiling is preserved: extrapolate guarantees
        // cursor <_u m_r <_u H, and the executor's CAS re-validates under the lock.
        if (toggles.reflect()) {
            byte[] mReflect = StealMath.extrapolate(lo, cursorTo, H);
            if (clampsToReflected(cursorTo, m, mReflect, lo, H, est, densityRatio, engagements)) {
                m = mReflect;
                carveReason = "pivot_reflect_clamped";
                engagements.add(new Engagement("OWNER_SPLIT", carveReason));
            }
        }
        // REFLECT-LIFT. When the FINAL post-clamp pivot would leave the owner a sub-one-page kept
        // share (measured in est's OWN [lo, H] frame — fKeptLo — never a re-scoped span), LIFT m to
        // the density-reflected pivot instead of carving at cursorTo's degenerate successor: the
        // owner then keeps ~one page of REAL mass (its final page partial-trims instead of coming
        // back empty) and the child still gets the far tail. Lift only UP (the clamp above owns the
        // down direction) and only if the lifted child tail still clears the observed-mass floor;
        // any condition failing falls through to the unchanged carve, with the confetti feedback
        // gate above as the realized-mass backstop.
        //
        // Gate on BOTH toggles: the lift is a density-reflection application — the SAME
        // StealMath.extrapolate the thief's reflection and the clamp above use, into the SAME
        // reflected-pivot family — so `reflect=off` disables the thief's reflection, this method's
        // clamp, and the lift together (full reflection ablation, matching docs/usage.md's
        // "reflect=off restores exact pre-reflection placement"); `reflect_lift=off` alone disables
        // only this lift.
        if (toggles.reflect() && toggles.reflectLift()) {
            byte[] mReflect = StealMath.extrapolate(lo, cursorTo, H);
            if (liftsToReflected(cursorTo, m, mReflect, lo, H, est, densityRatio, engagements)) {
                m = mReflect;
                carveReason = "pivot_reflect_lifted";
                engagements.add(new Engagement("OWNER_SPLIT", carveReason));
            }
        }
        return new Carve(m, engagements, mutations,
                gateInputs(carveReason, view, est, pagesSinceLastSelfSplit, f, densityRatio));
    }

    /**
     * The observed-mass child-tail floor at the run's {@code tail_floor} mode, with the mode's
     * effect on THIS gate measured (see {@link #noteTailFloorDivergence}). The gate is the site the
     * wide-flat blindness was measured at (algorithms.md §3.3: 5,326 of 5,326 tail refusals), so its
     * divergence counters are the live A/B's primary reading; their denominator is this gate's own
     * population — the qualifying page commits the {@code OWNER_SPLIT.*} skip reasons count against.
     */
    private boolean tailFloorBlocks(double est, double f, double densityRatio, List<Engagement> engagements) {
        TailFloorMode mode = toggles.tailFloor();
        boolean blocked = StealMath.childTailBelowObservedMassFloor(est, f, densityRatio, maxKeys, mode);
        if (mode != TailFloorMode.CURRENT) {
            boolean blockedUnderCurrent = StealMath.childTailBelowObservedMassFloor(
                    est, f, densityRatio, maxKeys, TailFloorMode.CURRENT);
            noteTailFloorDivergence("gate", !blocked, !blockedUnderCurrent, engagements);
        }
        return blocked;
    }

    /**
     * {@link StealMath#shouldClampToReflected} at the run's {@code tail_floor} mode, with the mode's
     * effect on the CLAMP measured under its own reason prefix — a carve that reached pivot
     * synthesis is a different population from the gate above, and pooling the two would make either
     * ratio uninterpretable (metrics-internals.md §5's denominator rule).
     */
    private boolean clampsToReflected(byte[] cursorTo, byte[] m, byte[] mReflect, byte[] lo, byte[] H,
                                      double est, double densityRatio, List<Engagement> engagements) {
        TailFloorMode mode = toggles.tailFloor();
        boolean clamps = StealMath.shouldClampToReflected(cursorTo, m, mReflect, lo, H, est, densityRatio,
                maxKeys, mode);
        if (mode != TailFloorMode.CURRENT) {
            boolean clampsUnderCurrent = StealMath.shouldClampToReflected(cursorTo, m, mReflect, lo, H, est,
                    densityRatio, maxKeys, TailFloorMode.CURRENT);
            noteTailFloorDivergence("clamp", clamps, clampsUnderCurrent, engagements);
        }
        return clamps;
    }

    /**
     * {@link StealMath#shouldLiftToReflected} at the run's {@code tail_floor} mode, instrumented as
     * {@link #clampsToReflected} is — again its own population (post-clamp carves), so its own
     * reason prefix.
     */
    private boolean liftsToReflected(byte[] cursorTo, byte[] m, byte[] mReflect, byte[] lo, byte[] H,
                                     double est, double densityRatio, List<Engagement> engagements) {
        TailFloorMode mode = toggles.tailFloor();
        boolean lifts = StealMath.shouldLiftToReflected(cursorTo, m, mReflect, lo, H, est, densityRatio,
                maxKeys, mode);
        if (mode != TailFloorMode.CURRENT) {
            boolean liftsUnderCurrent = StealMath.shouldLiftToReflected(cursorTo, m, mReflect, lo, H, est,
                    densityRatio, maxKeys, TailFloorMode.CURRENT);
            noteTailFloorDivergence("lift", lifts, liftsUnderCurrent, engagements);
        }
        return lifts;
    }

    /**
     * Record that a non-{@code current} {@code tail_floor} mode CHANGED this consult's verdict
     * (§5 discipline): {@code TAIL_FLOOR.<site>_admit_current_blocks} when the arm admits a carve the
     * shipped floor refuses — the cure's intended direction, and the count that attributes any
     * split-rate difference in a live A/B to this toggle rather than to the run — and {@code
     * TAIL_FLOOR.<site>_would_block_current_admits} for the opposite direction. Both arms are
     * monotonically more permissive than {@code current}, so the second reason is expected to stay
     * at zero; it is recorded anyway, because an assumption that costs one counter to falsify should
     * not be left as a comment. Agreement is silent (nothing happened), and a {@code current} run
     * never reaches here at all — one extra pure-arithmetic evaluation on the arms only.
     */
    private static void noteTailFloorDivergence(String site, boolean admitsUnderMode, boolean admitsUnderCurrent,
                                                List<Engagement> engagements) {
        if (admitsUnderMode == admitsUnderCurrent) {
            return;
        }
        engagements.add(new Engagement("TAIL_FLOOR",
                site + (admitsUnderMode ? "_admit_current_blocks" : "_would_block_current_admits")));
    }

    /**
     * The {@link OwnerSplitGateInputs} one {@link #decide} outcome reports: {@code reason} is the
     * terminal gate's own code, and {@code f}/{@code densityRatio} are {@link
     * OwnerSplitGateInputs#NOT_COMPUTED} for a gate that terminated ABOVE where the chain computes
     * them. Pure construction — no state, no side effect (the whole point of returning it rather
     * than emitting it here: the governor never holds the trace sink).
     */
    private OwnerSplitGateInputs gateInputs(String reason, OwnerSplitView view, double est,
                                            long pagesSinceLastSelfSplit, double f, double densityRatio) {
        return new OwnerSplitGateInputs(reason, est, pagesSinceLastSelfSplit, view.outstanding(), workerCount,
                f, densityRatio, view.keysEmitted());
    }

    /**
     * Downgrade a probe-slot CLAIM to the unconditional CONSUME, for a probe consult that ends in a
     * {@link Skip} rather than a {@link Carve} (issue #31). A claim only means anything when there is a
     * carve to admit or exclude; a skip has spent its slot either way. Any other mutation list — the
     * empty one, or the {@code CONFETTI_SUPPRESSED} skip's own CONSUME — passes through unchanged.
     */
    private static List<OwnerSplitMutation> consumeInsteadOfClaim(List<OwnerSplitMutation> mutations) {
        return mutations.contains(OwnerSplitMutation.CLAIM_CONFETTI_PROBE_SLOT)
                ? List.of(OwnerSplitMutation.CONSUME_CONFETTI_PROBE_SLOT)
                : mutations;
    }
}
