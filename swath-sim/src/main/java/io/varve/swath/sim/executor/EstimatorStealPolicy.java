/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.policy.Engagement;
import io.varve.swath.engine.policy.NoVictim;
import io.varve.swath.engine.policy.NoVictimReason;
import io.varve.swath.engine.policy.Selected;
import io.varve.swath.engine.policy.Selection;
import io.varve.swath.engine.policy.StealAttempt;
import io.varve.swath.engine.policy.StealAttemptView;
import io.varve.swath.engine.policy.StealPolicy;
import io.varve.swath.engine.policy.VictimMutation;
import io.varve.swath.engine.policy.VictimScan;
import io.varve.swath.engine.policy.VictimView;
import java.util.ArrayList;
import java.util.List;

/**
 * The engine's thief with its position sensor swapped out. Victim selection is mirrored from
 * {@code ThiefPolicy#selectVictim} line for line — the same skip order, the same refusal reasons, the
 * same mutations and engagements — with <b>one</b> substitution: the score comes from a
 * {@link RemainingWorkEstimator} rather than from the engine's own arithmetic. Everything downstream
 * of selection, the whole pivot cascade with its probes, back-outs, reflections and bisections, is
 * the engine's own object, called through the same interface the executor would have called it
 * through.
 *
 * <p>Mirroring rather than delegating is forced by the seam's shape: selection's contract is a
 * {@link Selection}, so the estimate is not something a wrapper can intercept. The mirror is small,
 * and {@code SensingVariantParityTest} holds it to the original by running both over the same pools
 * with the incumbent estimator installed — a divergence in the copy fails that test rather than
 * silently becoming part of a race result.
 */
final class EstimatorStealPolicy implements StealPolicy {

    private final RemainingWorkEstimator estimator;
    private final StealPolicy cascade;

    /**
     * @param estimator the sensor selection scores with
     * @param cascade   the engine's own thief, which owns every decision after a victim is chosen
     */
    EstimatorStealPolicy(RemainingWorkEstimator estimator, StealPolicy cascade) {
        this.estimator = estimator;
        this.cascade = cascade;
    }

    @Override
    public Selection selectVictim(List<VictimView> pool) {
        List<Engagement> engagements = new ArrayList<>();
        List<VictimMutation> mutations = new ArrayList<>();
        VictimView chosen = null;
        double best = Double.NEGATIVE_INFINITY;
        int seen = 0;
        int skippedUnsplittable = 0;
        int skippedPaced = 0;
        int skippedNoSpan = 0;
        for (VictimView w : pool) {
            seen++;
            if (w.unsplittable()) {
                skippedUnsplittable++;
                continue;
            }
            if (w.pacingSkipAvailable()) {
                skippedPaced++;
                mutations.add(new VictimMutation(w.nodeId(), VictimMutation.Kind.CONSUME_PACING_SKIP));
                engagements.add(new Engagement("STEAL", "futility_paced"));
                continue;
            }
            double est = estimator.estRemaining(w.cursor(), w.lo(), w.hi(), w.keysEmitted());
            if (est <= 0.0) {
                skippedNoSpan++;
                continue;
            }
            if (est > best) {
                best = est;
                chosen = w;
            }
        }
        VictimScan scan = new VictimScan(seen, skippedUnsplittable, skippedPaced, skippedNoSpan, best);
        if (chosen == null) {
            NoVictimReason reason;
            if (seen == 0) {
                reason = NoVictimReason.POOL_EMPTY;
            } else if (skippedNoSpan == seen) {
                reason = NoVictimReason.ALL_NO_REMAINING_SPAN;
            } else if (skippedPaced == seen) {
                reason = NoVictimReason.ALL_FUTILITY_PACED;
            } else if (skippedUnsplittable == seen) {
                reason = NoVictimReason.ALL_UNSPLITTABLE;
            } else {
                reason = NoVictimReason.MIXED_SKIPS;
            }
            return new NoVictim(reason, engagements, mutations, scan);
        }
        return new Selected(chosen.nodeId(), engagements, mutations, scan);
    }

    @Override
    public StealAttempt beginAttempt(StealAttemptView view) {
        return cascade.beginAttempt(view);
    }
}
