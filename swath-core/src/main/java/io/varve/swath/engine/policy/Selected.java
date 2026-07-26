/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/** A victim was chosen: {@code argmax estRemaining} over the eligible pool (algorithms.md §3.2). */
public record Selected(long victimNodeId, List<Engagement> engagements, List<VictimMutation> mutations)
        implements Selection {
}
