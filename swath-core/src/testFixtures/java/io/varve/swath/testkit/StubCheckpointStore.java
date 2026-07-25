/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.error.CheckpointException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link CheckpointStore} test double whose only implemented method is {@link #splitNode} —
 * every other method throws {@link UnsupportedOperationException}. The shared fixture for the
 * Thief/pivot-mechanics unit cluster (single-attempt {@code Thief.steal} tests that never drive a
 * real run/commit/resume path and only need to program or observe one split).
 *
 * <p>Every {@link #splitNode} call is recorded ({@link #lastSplit}, {@link #splitCalls})
 * regardless of what the caller-supplied {@link SplitResult} returns or throws, so a caller
 * needing only a subset of that bookkeeping can simply ignore the rest.
 */
public class StubCheckpointStore implements CheckpointStore {

    /** What {@link #splitNode} returns (or throws) for a given {@link SplitSpec}. */
    public interface SplitResult {
        long apply(SplitSpec s) throws CheckpointException;
    }

    /** The most recent {@link #splitNode} argument, or {@code null} if never called. */
    public SplitSpec lastSplit;
    /** The number of {@link #splitNode} calls so far. */
    public int splitCalls;

    private final SplitResult result;

    /** A store whose {@link #splitNode} throws {@link UnsupportedOperationException} (never used). */
    public StubCheckpointStore() {
        this(s -> {
            throw new UnsupportedOperationException();
        });
    }

    public StubCheckpointStore(SplitResult result) {
        this.result = result;
    }

    /** Always returns the same fixed child id, regardless of {@code s}. */
    public static StubCheckpointStore returning(long childId) {
        return new StubCheckpointStore(s -> childId);
    }

    /** Always rejects the split CAS ({@link CheckpointStore#SPLIT_ABORTED}). */
    public static StubCheckpointStore alwaysAborts() {
        return new StubCheckpointStore(s -> CheckpointStore.SPLIT_ABORTED);
    }

    @Override
    public long splitNode(SplitSpec s) throws CheckpointException {
        splitCalls++;
        lastSplit = s;
        return result.apply(s);
    }

    @Override
    public RunMeta openRun(RunKey k, boolean resume, boolean restart) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long insertNode(NodeSpec spec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Node> loadResumable(long runId, boolean fileSink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitPage(PageCommit c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> commitPageAsync(PageCommit c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void partFinalized(PartFinalize f) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PartRef> finalizedParts(long runId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void markOutputComplete(long runId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void markRunFinished(long runId, RunStatus status) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }
}
