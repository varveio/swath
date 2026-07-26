/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * The victim has a terminal {@code null} pivot — no safe key strictly between its bounds
 * (algorithms.md §3). Always carries a {@code VictimMutation.Kind#SET_UNSPLITTABLE} mutation so
 * the executor caches it on the real {@code WorkerState} and never re-probes a dead range.
 */
public record MarkUnsplittable(UnsplittableReason reason, List<Engagement> engagements,
                               List<VictimMutation> mutations) implements StealAction {
}
