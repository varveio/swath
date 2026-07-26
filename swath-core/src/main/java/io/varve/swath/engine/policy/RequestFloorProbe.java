/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * Issue the flat-leaf bootstrap floor probe {@code ListObjectsV2(prefix=leafPrefix, max_keys=1,
 * start_after=null)} (algorithms.md §3.1's {@code flatLeafDensityPivot}) and resume the cascade via
 * {@link StealAttempt#onProbeResult(ProbeOutcome)} with a {@link FloorProbeOutcome}.
 *
 * @param leafPrefix the cursor's deepest {@code /}-terminated directory prefix
 */
public record RequestFloorProbe(byte[] leafPrefix, List<Engagement> engagements, List<VictimMutation> mutations)
        implements StealAction {
}
