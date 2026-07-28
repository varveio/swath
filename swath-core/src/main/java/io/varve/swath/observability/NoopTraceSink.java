/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

/** {@link TraceSink#NONE} — every method is a free no-op; never allocates, never touches I/O. */
final class NoopTraceSink implements TraceSink {

    static final NoopTraceSink INSTANCE = new NoopTraceSink();

    private NoopTraceSink() {
    }

    @Override
    public boolean enabled() {
        return false;
    }

    @Override
    public void seeded(long nodeId, byte[] lo, byte[] hi) {
    }

    @Override
    public void claimed(long workerId, long nodeId, byte[] lo, byte[] cursor, byte[] hi) {
    }

    @Override
    public void pageCommitted(long workerId, long nodeId, int keysEmitted, byte[] cursor, boolean completed) {
    }

    @Override
    public void stealAttempt(long workerId, String outcome, String reason) {
    }

    @Override
    public void ownerSplitDecision(long workerId, long nodeId, String reason, double est,
            long pagesSinceLastSelfSplit, long outstanding, int workerCount, double farAheadFraction,
            double densityRatio, long keysEmitted) {
        // Intentionally empty: TraceSink.NONE discards trace events.
    }

    @Override
    public void victimScan(long workerId, int seen, int skippedUnsplittable, int skippedPaced, int skippedNoSpan,
            long chosenNodeId, double bestEst, String reason) {
        // Intentionally empty: TraceSink.NONE discards trace events.
    }

    @Override
    public void split(long workerId, long parentNodeId, long childNodeId, String mechanism, byte[] pivot, byte[] hi) {
    }

    @Override
    public void ownerSplit(long workerId, long parentNodeId, long childNodeId, String mechanism, byte[] pivot, byte[] hi) {
    }

    @Override
    public void completed(long workerId, long nodeId) {
    }

    @Override
    public void failed(long workerId, long nodeId, String reason) {
    }

    @Override
    public void close() {
    }
}
