/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * What a {@link StealAttempt} wants next: issue one more probe and continue the cascade
 * ({@link RequestKeyProbe}/{@link RequestStructureProbe}/{@link RequestFloorProbe}), or a terminal
 * outcome ({@link Commit}/{@link Retry}/{@link MarkUnsplittable} — algorithms.md §3's three
 * cascade-owned results; {@code CHILD_CREATED} vs. the lock-guarded {@code RETRY}s are the
 * executor's own translation of {@link Commit}, applied under the victim's lock after the CAS
 * re-validate — outside this type, since the policy never sees the lock or the CAS).
 *
 * <p>Every variant carries its own {@link Engagement} marks and {@link VictimMutation}s (both
 * possibly empty) rather than deferring them to the attempt's terminal action, because a mark or a
 * durable-state change (e.g. the structure probe's zero-fan-out streak) can fire on an
 * <b>intermediate</b> step — a probe request that continues the cascade — not only at the end.
 *
 * @see RequestKeyProbe
 * @see RequestStructureProbe
 * @see RequestFloorProbe
 * @see Commit
 * @see Retry
 * @see MarkUnsplittable
 */
public sealed interface StealAction
        permits RequestKeyProbe, RequestStructureProbe, RequestFloorProbe, Commit, Retry, MarkUnsplittable {

    /** Engagement marks to record for this step (empty when none fired). */
    List<Engagement> engagements();

    /** Per-victim durable-state mutations to apply for this step (empty when none apply). */
    List<VictimMutation> mutations();
}
