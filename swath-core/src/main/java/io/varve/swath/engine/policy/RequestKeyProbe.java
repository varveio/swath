/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * Issue the single-key speculative probe {@code ListObjectsV2(prefix=P, start_after=pivot,
 * max_keys=1)} (algorithms.md §3) and resume the cascade via
 * {@link StealAttempt#onProbeResult(ProbeOutcome)} with a {@link KeyProbeOutcome}.
 *
 * @param pivot the candidate pivot {@code m} to probe
 * @param hi    the coherent snapshot bound {@code H} ({@code null} = open frontier — accepts any
 *              returned key)
 */
public record RequestKeyProbe(byte[] pivot, byte[] hi, List<Engagement> engagements, List<VictimMutation> mutations)
        implements StealAction {
}
