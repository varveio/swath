/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.error.CheckpointException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

/**
 * A {@link CheckpointStore} decorator that injects the <b>split-guard ABORT</b> path
 * (algorithms.md §3/§4.3): for a chosen split it returns {@link CheckpointStore#SPLIT_ABORTED}
 * <b>without touching the delegate</b> — exactly a rowcount-0 CAS (the victim's
 * {@code range_end} is left unchanged and no child row is inserted). The thief must then
 * restore {@code victim.hi = H} and RETRY, leaving the durable range set still tiling.
 *
 * <p>Reusable by CONC-3 (the late-loser {@code splitTxn → ABORT} path) and RES-3.
 */
public final class AbortingCheckpointStore extends ForwardingCheckpointStore {

    /** {@code (zeroBasedSplitIndex, spec) -> true} ⇒ abort this split (no delegate call). */
    private final BiPredicate<Integer, SplitSpec> abortWhen;
    private final AtomicInteger splitIndex = new AtomicInteger();
    private final AtomicInteger aborts = new AtomicInteger();

    public AbortingCheckpointStore(CheckpointStore delegate, BiPredicate<Integer, SplitSpec> abortWhen) {
        super(delegate);
        this.abortWhen = abortWhen;
    }

    /** Abort exactly the first split attempt; delegate all others. */
    public static AbortingCheckpointStore abortFirst(CheckpointStore delegate) {
        return new AbortingCheckpointStore(delegate, (idx, spec) -> idx == 0);
    }

    @Override
    public long splitNode(SplitSpec s) throws CheckpointException {
        int idx = splitIndex.getAndIncrement();
        if (abortWhen.test(idx, s)) {
            aborts.incrementAndGet();
            return CheckpointStore.SPLIT_ABORTED;   // rowcount-0: nothing changes durably
        }
        return delegate.splitNode(s);
    }

    /** How many split attempts have been forced to ABORT so far. */
    public int aborts() {
        return aborts.get();
    }
}
