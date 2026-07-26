/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/** The cascade itself (pre-lock) gave up this attempt; re-steal later (algorithms.md §3). */
public record Retry(RetryReason reason, List<Engagement> engagements, List<VictimMutation> mutations)
        implements StealAction {
}
