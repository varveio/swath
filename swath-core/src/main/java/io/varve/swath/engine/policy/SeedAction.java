/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * What the descent wants next: issue one more bounded {@code delimiter=/} probe and continue
 * ({@link RequestSeedProbe}), or the terminal {@link SeedPlan} — the finished cut set plus the
 * decision trace, ready for the executor to tile into ranges.
 *
 * <p>Every variant carries its own {@link Engagement} marks (possibly empty) rather than deferring
 * them to the terminal plan, because a classification mark (a heavy-cut confirmation, a
 * frontier-ordering engagement, a fan-out-tiling decision) fires the moment the descent makes that
 * call — which is very often mid-descent, on a step that also requests the next probe, not only once
 * the whole run concludes.
 *
 * <p>Unlike the thief's {@code StealAction}, there is no mutation list here: the descent runs once,
 * single-threaded, entirely before any worker starts — no live shared state (no {@code WorkerState},
 * no lock) for it to mutate. Its frontier and probe/sample budget bookkeeping are private to the
 * {@link SeedDescent} instance and never observed by another thread, so there is nothing for the
 * executor to apply on the descent's behalf.
 *
 * @see RequestSeedProbe
 * @see SeedPlan
 */
public sealed interface SeedAction permits RequestSeedProbe, SeedPlan {

    /** Engagement marks to record for this step (empty when none fired). */
    List<Engagement> engagements();
}
