/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * Issue one bounded {@code ListObjectsV2(prefix=probePrefix, delimiter=/, start_after=startAfter,
 * max_keys=STRUCTURE_PROBE_MAX_KEYS)} (algorithms.md §3) and resume the cascade via
 * {@link StealAttempt#onProbeResult(ProbeOutcome)} with a {@link StructureProbeOutcome}. The
 * {@code max_keys} cap is a policy-owned cascade constant (not carried here — the executor issues
 * exactly what the policy's probe budget calls for, mirroring how the pivot itself is policy-owned
 * while the RPC construction stays executor mechanics).
 *
 * @param probePrefix the directory prefix {@code Q} to list (the empty-upper funnel's
 *                     {@code lo∧cursor} directory, or the parent-empty sliver's coarse→fine
 *                     back-out level)
 * @param startAfter   the {@code start_after} cursor for this probe
 */
public record RequestStructureProbe(byte[] probePrefix, byte[] startAfter, List<Engagement> engagements,
                                    List<VictimMutation> mutations) implements StealAction {
}
