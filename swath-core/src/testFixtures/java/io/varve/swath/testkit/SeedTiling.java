/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.CheckpointException;
import java.util.function.IntFunction;

/**
 * Seeds {@code ranges} equal {@code (lo, hi]} worklist nodes tiling a dense flat keyspace of
 * {@code n} keys addressed by {@code key(i)} (0-indexed, monotonically increasing in
 * unsigned-byte order) — the multi-range worklist {@code SeedStep} produces for a large bucket
 * (radix bands / scatter scouts), so a saturated run starts with many live nodes
 * (outstanding &asymp; ranges), not a single seed. Boundary-belongs-left exact tiling: seed
 * {@code j = (key(j*step-1), key((j+1)*step-1)]}, no gap, no overlap.
 */
public final class SeedTiling {

    private SeedTiling() {
    }

    /**
     * @param lowestPrefix the {@code lo} of seed 0 (below every {@code key(i)}, typically the
     *                     keyspace's shared prefix)
     * @param key          the {@code i}-th key of the {@code n}-key dense flat keyspace being seeded
     */
    public static void seedTiled(SqliteCheckpointStore store, long runId, int n, int ranges,
            byte[] lowestPrefix, IntFunction<byte[]> key) throws CheckpointException {
        int step = n / ranges;
        for (int j = 0; j < ranges; j++) {
            byte[] lo = j == 0 ? lowestPrefix : key.apply(j * step - 1);
            byte[] hi = j == ranges - 1 ? key.apply(n - 1) : key.apply((j + 1) * step - 1);
            // cursor = lo so the scanner starts start_after=lo (a fresh non-root range resumes at its
            // own lower bound); a null cursor would restart every seed from ⊥ and re-emit the whole space.
            store.insertNode(new NodeSpec(runId, null, NodeKind.RANGE, lo, hi, lo, null));
        }
    }
}
