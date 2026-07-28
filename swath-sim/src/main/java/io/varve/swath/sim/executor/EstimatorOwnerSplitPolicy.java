/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.ConfettiFeedbackGate;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.StealMath;
import io.varve.swath.engine.policy.Carve;
import io.varve.swath.engine.policy.ConfettiObservation;
import io.varve.swath.engine.policy.Engagement;
import io.varve.swath.engine.policy.OwnerSplitDecision;
import io.varve.swath.engine.policy.OwnerSplitGovernor;
import io.varve.swath.engine.policy.OwnerSplitMutation;
import io.varve.swath.engine.policy.OwnerSplitPolicy;
import io.varve.swath.engine.policy.OwnerSplitSkipReason;
import io.varve.swath.engine.policy.OwnerSplitView;
import io.varve.swath.engine.policy.Skip;
import io.varve.swath.model.KeyBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The engine's owner-split governor with its position sensor swapped out. The gate chain is mirrored
 * from {@code OwnerSplitGovernor#decide} in order — the remaining-work floor, the page rate limit,
 * the demand gate, the observed-mass child-tail floor, the confetti feedback gate, then pivot
 * synthesis, the reflection clamp and the reflect-lift — with <b>one</b> substitution: {@code est}
 * comes from a {@link RemainingWorkEstimator}. Every constant that governs a gate is the engine's own
 * public one, except the two the engine keeps package-private, which are reproduced below.
 *
 * <p><b>Why the owner side is in the race at all.</b> The estimate is not only a victim-ranking
 * quantity: two of the owner's gates compare it against multiples of a page, so a degenerate estimate
 * refuses proactive carves as well as misdirecting steals. A variant raced on victim selection alone
 * would leave half the mechanism it is meant to fix running on the incumbent sensor.
 *
 * <p><b>The mirror is held to the original mechanically.</b> {@code SensingVariantParityTest} drives
 * this class and the engine's governor over the same views with the incumbent estimator installed and
 * requires identical decisions, including the confetti-gate branches the two duplicated constants
 * govern — so a constant that changes in the engine and not here fails a test instead of quietly
 * becoming a race result.
 */
final class EstimatorOwnerSplitPolicy implements OwnerSplitPolicy {

    /**
     * The category every engagement this policy fires is filed under, which is
     * {@link SimExecutor#OWNER_SPLIT_CATEGORY}: the gate chain fires nine of them, and the counters
     * they become are read back by name elsewhere in the module.
     */
    private static final String OWNER_SPLIT = SimExecutor.OWNER_SPLIT_CATEGORY;

    /**
     * The engine's own {@code OwnerSplitGovernor.SUPPRESS_THRESHOLD}, which is package-private there.
     * Duplicated rather than widened: the engine is not modified for a simulator experiment, and the
     * parity test is what keeps the copy honest.
     */
    private static final double SUPPRESS_THRESHOLD = 0.5;

    /** The engine's own {@code OwnerSplitGovernor.PROBE_K}, duplicated for the same reason. */
    private static final long PROBE_K = 16;

    private final RemainingWorkEstimator estimator;
    private final EngineToggles toggles;
    private final int workerCount;
    private final int maxKeys;

    /**
     * @param estimator   the sensor the est-driven gates read
     * @param toggles     the ablation namespace, as the engine's governor takes it
     * @param workerCount the configured fleet size (the demand gate's threshold)
     * @param maxKeys     the page size (the two floors' unit)
     */
    EstimatorOwnerSplitPolicy(RemainingWorkEstimator estimator, EngineToggles toggles, int workerCount,
                              int maxKeys) {
        this.estimator = estimator;
        this.toggles = toggles == null ? EngineToggles.DEFAULT : toggles;
        this.workerCount = workerCount;
        this.maxKeys = maxKeys;
    }

    @Override
    public OwnerSplitDecision decide(OwnerSplitView view) {
        byte[] hi = view.hi();
        if (hi == null) {
            return new Skip(OwnerSplitSkipReason.OPEN_FRONTIER, List.of(), List.of());
        }
        byte[] cursorTo = view.cursorTo();
        byte[] lo = view.lo();
        double est = estimator.estRemaining(cursorTo, lo, hi, view.keysEmitted());
        if (est <= (double) OwnerSplitGovernor.SELF_SPLIT_MIN_REMAINING_PAGES * maxKeys) {
            return new Skip(OwnerSplitSkipReason.REMAINING_EST_FLOOR,
                    List.of(new Engagement(OWNER_SPLIT, OwnerSplitSkipReason.REMAINING_EST_FLOOR.code())),
                    List.of());
        }
        if (view.committed() - view.lastSelfSplitPage() < OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN) {
            return new Skip(OwnerSplitSkipReason.RATE_LIMITED,
                    List.of(new Engagement(OWNER_SPLIT, OwnerSplitSkipReason.RATE_LIMITED.code())),
                    List.of());
        }
        List<Engagement> engagements = new ArrayList<>();
        if (workerCount > 1 && view.outstanding() >= (long) workerCount) {
            engagements.add(new Engagement(OWNER_SPLIT, OwnerSplitSkipReason.DEMAND_GATED.code()));
            return new Skip(OwnerSplitSkipReason.DEMAND_GATED, engagements, List.of());
        }
        double f = toggles.farAheadFraction(view.densityFraction());
        double densityRatio = toggles.observedDensityRatio(view.observedDensityRatio());
        if (StealMath.childTailBelowObservedMassFloor(est, f, densityRatio, maxKeys)) {
            engagements.add(new Engagement(OWNER_SPLIT,
                    OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED.code()));
            return new Skip(OwnerSplitSkipReason.FLOOR_REFLECTED_BLOCKED, engagements, List.of());
        }
        List<OwnerSplitMutation> mutations = List.of();
        if (toggles.confettiFeedback()) {
            ConfettiObservation obs = view.confetti();
            if (obs.taggedTotal() >= ConfettiFeedbackGate.MIN_SAMPLE) {
                double rate = (double) obs.taggedConfetti() / (double) obs.taggedTotal();
                if (rate > SUPPRESS_THRESHOLD) {
                    List<OwnerSplitMutation> probeSlot =
                            List.of(OwnerSplitMutation.CONSUME_CONFETTI_PROBE_SLOT);
                    if ((obs.probeSeq() + 1) % PROBE_K == 0) {
                        engagements.add(new Engagement(OWNER_SPLIT, "confetti_probe"));
                        mutations = List.of(OwnerSplitMutation.CLAIM_CONFETTI_PROBE_SLOT);
                    } else {
                        engagements.add(new Engagement(OWNER_SPLIT,
                                OwnerSplitSkipReason.CONFETTI_SUPPRESSED.code()));
                        return new Skip(OwnerSplitSkipReason.CONFETTI_SUPPRESSED, engagements, probeSlot);
                    }
                }
            }
        }
        byte[] m = toggles.interpolate(cursorTo, hi, f, view.alphabetDigest(), engagements);
        if (m == null
                || KeyBytes.compareUnsigned(cursorTo, m) >= 0
                || KeyBytes.compareUnsigned(m, hi) > 0) {
            engagements.add(new Engagement(OWNER_SPLIT, OwnerSplitSkipReason.UNSPLITTABLE_PIVOT.code()));
            return new Skip(OwnerSplitSkipReason.UNSPLITTABLE_PIVOT, engagements,
                    consumeInsteadOfClaim(mutations));
        }
        byte[] plainPivot = StealMath.interpolate(cursorTo, hi, f);
        engagements.add(new Engagement("ALPHABET",
                !Arrays.equals(m, plainPivot) ? "alphabet_chosen" : "alphabet_fallback"));
        if (toggles.reflect()) {
            byte[] mReflect = StealMath.extrapolate(lo, cursorTo, hi);
            if (StealMath.shouldClampToReflected(cursorTo, m, mReflect, lo, hi, est, densityRatio, maxKeys)) {
                m = mReflect;
                engagements.add(new Engagement(OWNER_SPLIT, "pivot_reflect_clamped"));
            }
        }
        if (toggles.reflect() && toggles.reflectLift()) {
            byte[] mReflect = StealMath.extrapolate(lo, cursorTo, hi);
            if (StealMath.shouldLiftToReflected(cursorTo, m, mReflect, lo, hi, est, densityRatio, maxKeys)) {
                m = mReflect;
                engagements.add(new Engagement(OWNER_SPLIT, "pivot_reflect_lifted"));
            }
        }
        return new Carve(m, engagements, mutations);
    }

    /** The engine's own downgrade of a probe-slot claim on a skip, mirrored. */
    private static List<OwnerSplitMutation> consumeInsteadOfClaim(List<OwnerSplitMutation> mutations) {
        return mutations.contains(OwnerSplitMutation.CLAIM_CONFETTI_PROBE_SLOT)
                ? List.of(OwnerSplitMutation.CONSUME_CONFETTI_PROBE_SLOT)
                : mutations;
    }
}
