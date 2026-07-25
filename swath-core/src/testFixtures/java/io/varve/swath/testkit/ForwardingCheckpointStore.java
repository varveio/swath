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
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.InvalidArgsException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link CheckpointStore} that forwards every call to a delegate — the base for the
 * adversarial-test decorators ({@link RecordingSplitStore}, {@link AbortingCheckpointStore})
 * that intercept exactly one method (the split). Reusable by RES-3 / CONC-3.
 */
public class ForwardingCheckpointStore implements CheckpointStore {

    protected final CheckpointStore delegate;

    public ForwardingCheckpointStore(CheckpointStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public RunMeta openRun(RunKey key, boolean resume, boolean restart)
            throws CheckpointException, InvalidArgsException {
        return delegate.openRun(key, resume, restart);
    }

    @Override
    public RunMeta openRun(RunKey key, boolean resume, boolean restart, boolean overwrite)
            throws CheckpointException, InvalidArgsException {
        return delegate.openRun(key, resume, restart, overwrite);
    }

    @Override
    public long insertNode(NodeSpec spec) throws CheckpointException {
        return delegate.insertNode(spec);
    }

    @Override
    public List<Long> insertNodes(List<NodeSpec> specs) throws CheckpointException {
        return delegate.insertNodes(specs);
    }

    @Override
    public long countNodes(long runId) throws CheckpointException {
        return delegate.countNodes(runId);
    }

    @Override
    public List<Node> loadResumable(long runId, boolean fileSink) throws CheckpointException {
        return delegate.loadResumable(runId, fileSink);
    }

    @Override
    public void commitPage(PageCommit c) throws CheckpointException {
        delegate.commitPage(c);
    }

    @Override
    public CompletableFuture<Void> commitPageAsync(PageCommit c) throws CheckpointException {
        return delegate.commitPageAsync(c);
    }

    @Override
    public long splitNode(SplitSpec s) throws CheckpointException {
        return delegate.splitNode(s);
    }

    @Override
    public void partFinalized(PartFinalize f) throws CheckpointException {
        delegate.partFinalized(f);
    }

    @Override
    public List<PartRef> finalizedParts(long runId) throws CheckpointException {
        return delegate.finalizedParts(runId);
    }

    @Override
    public void markOutputComplete(long runId) throws CheckpointException {
        delegate.markOutputComplete(runId);
    }

    @Override
    public void markRunFinished(long runId, RunStatus status) throws CheckpointException {
        delegate.markRunFinished(runId, status);
    }

    @Override
    public void markRunFatalUnlessFinished(long runId) throws CheckpointException {
        delegate.markRunFatalUnlessFinished(runId);
    }

    @Override
    public void markRunUnresumable(long runId) throws CheckpointException {
        delegate.markRunUnresumable(runId);
    }

    @Override
    public SortPhase sortPhase(long runId) throws CheckpointException {
        return delegate.sortPhase(runId);
    }

    @Override
    public void setSortPhase(long runId, SortPhase phase) throws CheckpointException {
        delegate.setSortPhase(runId, phase);
    }

    @Override
    public void close() throws CheckpointException {
        delegate.close();
    }
}
