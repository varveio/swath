/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.error.CheckpointException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link CheckpointStore} decorator that records every <b>committed</b> split
 * ({@code SplitNode} that returned a real child id, not {@link CheckpointStore#SPLIT_ABORTED}).
 * Used by PROP-1 to reconstruct the durable range-set partition and prove it tiles
 * {@code (⊥, ⊤]} with no gap / no overlap (I2/I3), independent of the emitted-key set.
 *
 * <p>Records are appended in commit order: a split holds the victim lock across
 * {@link #splitNode}, so two splits of the <i>same</i> victim are strictly ordered here,
 * and a child can only be split after its creating split has already been recorded —
 * so {@link RangePartition} can replay them in arrival order (causal order is preserved).
 */
public final class RecordingSplitStore extends ForwardingCheckpointStore {

    private final List<RangePartition.Split> splits = new ArrayList<>();

    public RecordingSplitStore(CheckpointStore delegate) {
        super(delegate);
    }

    @Override
    public long splitNode(SplitSpec s) throws CheckpointException {
        long childId = delegate.splitNode(s);
        if (childId != CheckpointStore.SPLIT_ABORTED) {
            synchronized (splits) {
                splits.add(new RangePartition.Split(s.victimId(), s.pivot(), s.oldHi(), childId));
            }
        }
        return childId;
    }

    /** An immutable snapshot of the committed splits in arrival order. */
    public List<RangePartition.Split> splits() {
        synchronized (splits) {
            return List.copyOf(splits);
        }
    }
}
