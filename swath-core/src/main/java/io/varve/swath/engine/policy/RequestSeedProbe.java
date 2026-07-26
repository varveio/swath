/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import java.util.List;

/**
 * Issue one bounded {@code ListObjectsV2(prefix=probePrefix, delimiter=/, start_after=startAfter,
 * max_keys=<the store's page cap>)} (algorithms.md §8) and resume the descent via
 * {@link SeedDescent#onProbeResult(SeedProbeOutcome)}. The descent issues every probe it ever needs
 * through this one action shape — the top-level probe, the bounded +1-page top-level pagination, each
 * frontier-poll descent probe, the second-level heavy/1:1 disambiguation samples, and the post-descent
 * cut-weight samples are all the same RPC shape, just at different prefixes and cursors — unlike the
 * thief's cascade, which needs three distinct probe shapes ({@code RequestKeyProbe}/
 * {@code RequestStructureProbe}/{@code RequestFloorProbe}) for three genuinely different RPCs.
 *
 * @param probePrefix the directory prefix to list
 * @param startAfter  the {@code start_after} cursor for this probe, or {@code null} (used only by the
 *                    bounded +1-page top-level pagination)
 */
public record RequestSeedProbe(byte[] probePrefix, byte[] startAfter, List<Engagement> engagements)
        implements SeedAction {
}
