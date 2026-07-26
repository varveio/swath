/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import io.varve.swath.engine.ConfettiFeedbackGate;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.StealMath;
import io.varve.swath.model.KeyBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The owner-split governor: the owner-side proactive self-split's gate chain (algorithms.md §3.3)
 * as an {@link OwnerSplitPolicy}. Byte ordering is S3-lexicographic throughout — {@link StealMath}
 * is called directly as the concrete thing it is; this extraction deliberately does not introduce
 * an ordering/comparator/key-codec abstraction for a hypothetical second source (rule of three —
 * see seam-notes.md).
 *
 * <p><b>The confetti feedback gate is a collaborator, not owned state here.</b> {@link
 * ConfettiFeedbackGate#decide()} is consulted mid-chain as ordinary decision logic (a deterministic
 * read of its own already-recorded atomics — no I/O), but its tagged-child completion
 * classification ({@code recordCompletion}) is an event fired from a totally different call site
 * (node completion, engine-wide) tied to real node ids and {@code WorkerState} this package never
 * sees — so the executor ({@code OwnerSelfSplit}) owns constructing the one run-scoped instance and
 * feeding it completions; this governor only ever reads it.
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

    private final EngineToggles toggles;
    private final int workerCount;
    private final int maxKeys;
    private final ConfettiFeedbackGate confettiFeedback;

    /**
     * @param toggles          the {@code --engine-toggle} ablation namespace this run was constructed with
     * @param workerCount      the fixed configured worker count {@code Tmax} (the demand gate's threshold)
     * @param maxKeys          the page size (the remaining-est floor's and the observed-mass floor's unit)
     * @param confettiFeedback the run-scoped realized-child-mass feedback gate this run shares with the
     *                         executor's tagged-child completion classification
     */
    public OwnerSplitGovernor(EngineToggles toggles, int workerCount, int maxKeys,
                              ConfettiFeedbackGate confettiFeedback) {
        this.toggles = toggles == null ? EngineToggles.DEFAULT : toggles;
        this.workerCount = workerCount;
        this.maxKeys = maxKeys;
        this.confettiFeedback = confettiFeedback;
    }

    @Override
    public OwnerSplitDecision decide(OwnerSplitView view) {
        byte[] H = view.hi();
        if (H == null) {
            return new Skip(OwnerSplitSkipReason.OPEN_FRONTIER, List.of());
        }
        byte[] cursorTo = view.cursorTo();
        byte[] lo = view.lo();
        // Plain code-point estRemaining (NOT the rank-space variant): an owner-split is zero-probe
        // (interpolate is pure math, no LIST), so under-firing here trades cheap owner-splits for
        // costlier thief structure-probes on the un-split tail — strictly worse. The rank-space
        // deflation belongs only to the pivot synthesis below (interpolate(..., alphabetDigest())),
        // which needs no estRemaining change to land on a populated value.
        double est = StealMath.estRemaining(cursorTo, lo, H, view.keysEmitted());
        if (est <= (double) SELF_SPLIT_MIN_REMAINING_PAGES * maxKeys) {
            // Remaining work too small to be worth a proactive carve — issue #16.
            return new Skip(OwnerSplitSkipReason.REMAINING_EST_FLOOR,
                    List.of(new Engagement("OWNER_SPLIT", OwnerSplitSkipReason.REMAINING_EST_FLOOR.code())));
        }
        if (view.committed() - view.lastSelfSplitPage() < SELF_SPLIT_MIN_PAGES_BETWEEN) {
            // Rate-limit: O(1) self-splits per drain, not one per page.
            return new Skip(OwnerSplitSkipReason.RATE_LIMITED,
                    List.of(new Engagement("OWNER_SPLIT", OwnerSplitSkipReason.RATE_LIMITED.code())));
        }
        List<Engagement> engagements = new ArrayList<>();
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
            return new Skip(OwnerSplitSkipReason.DEMAND_GATED, engagements);
        }
        // Far-ahead pivot fraction from the worker's own zero-cost density (>= 0.5 ⇒ >= byteMidpoint).
        // The child owns the far tail — so floor that tail above two pages: even below the demand gate an
        // owner-split must never fission into a ~1-page "confetti" child whose bounded final page is
        // fetched full then trimmed per key. The floor measures the tail in OBSERVED-density terms —
        // the plain span share (1-f)*est over-states a thinning tail on skewed keyspaces (see
        // StealMath.childTailBelowObservedMassFloor's math).
        double f = toggles.farAheadFraction(view.densityFraction());
        double densityRatio = toggles.observedDensityRatio(view.observedDensityRatio());
        if (StealMath.childTailBelowObservedMassFloor(est, f, densityRatio, maxKeys)) {
            engagements.add(new Engagement("OWNER_SPLIT", OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED.code()));
            return new Skip(OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED, engagements);
        }
        // CONFETTI FEEDBACK GATE. The gates above reason from upstream estimates (est/densityRatio);
        // on a keyspace whose tail thins out over most of the observed span those estimates still
        // pass a carve whose REALIZED mass turns out confetti-sized. Once the run has accumulated
        // enough tagged-child evidence (MIN_SAMPLE), a high observed confetti rate suppresses further
        // carving directly from that ground truth — with a periodic probe so a keyspace that later
        // turns genuinely dense recovers on its own. See ConfettiFeedbackGate's javadoc and
        // docs/internals/metrics-internals.md §5.
        if (toggles.confettiFeedback()) {
            ConfettiFeedbackGate.Decision decision = confettiFeedback.decide();
            if (decision == ConfettiFeedbackGate.Decision.SUPPRESSED) {
                engagements.add(new Engagement("OWNER_SPLIT", OwnerSplitSkipReason.CONFETTI_SUPPRESSED.code()));
                return new Skip(OwnerSplitSkipReason.CONFETTI_SUPPRESSED, engagements);
            }
            if (decision == ConfettiFeedbackGate.Decision.PROBE) {
                engagements.add(new Engagement("OWNER_SPLIT", "confetti_probe"));
            }
        }
        // Synthesize the pivot at fraction f in the observed-alphabet rank space so it lands on a
        // populated value.
        byte[] m = toggles.interpolate(cursorTo, H, f, view.alphabetDigest());
        if (m == null
                || KeyBytes.compareUnsigned(cursorTo, m) >= 0
                || KeyBytes.compareUnsigned(m, H) > 0) {
            // Unsplittable, or pivot not strictly in (cursorTo, H] — skip this page. Recurs (not a
            // one-off): estRemaining's span heuristic can diverge from true byte-adjacency on a deep
            // shared prefix, the same measurement/reality gap algorithms.md §3.2 documents for the
            // thief side — see OwnerSplitSkipReason#UNSPLITTABLE_PIVOT's javadoc.
            engagements.add(new Engagement("OWNER_SPLIT", OwnerSplitSkipReason.UNSPLITTABLE_PIVOT.code()));
            return new Skip(OwnerSplitSkipReason.UNSPLITTABLE_PIVOT, engagements);
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
            if (StealMath.shouldClampToReflected(cursorTo, m, mReflect, lo, H, est, densityRatio, maxKeys)) {
                m = mReflect;
                engagements.add(new Engagement("OWNER_SPLIT", "pivot_reflect_clamped"));
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
            if (StealMath.shouldLiftToReflected(cursorTo, m, mReflect, lo, H, est, densityRatio, maxKeys)) {
                m = mReflect;
                engagements.add(new Engagement("OWNER_SPLIT", "pivot_reflect_lifted"));
            }
        }
        return new Carve(m, engagements);
    }
}
