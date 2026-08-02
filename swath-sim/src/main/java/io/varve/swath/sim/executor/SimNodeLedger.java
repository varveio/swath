/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.model.KeyBytes;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Single-event-loop, in-memory stand-in for claimable ranges, outstanding work, and the guarded
 * durable-split outcome. It models neither persistence nor resume.
 *
 * <p>A split proposal is accepted only when its expected bound still matches, the cursor remains
 * before the pivot, and the parent is incomplete. Thief proposals arrive after explicit executor
 * revalidation; owner proposals rely on invariants established in the same event body.
 * {@link #splitsAborted()} counts every ledger-guard rejection, not all lost races.
 *
 * <p>Serial event execution supplies ordering here; this class does not model the thread-safe CAS
 * implementation used by the production executor.
 */
final class SimNodeLedger {

    /** Returned when the split guard rejects a proposal. */
    static final long SPLIT_ABORTED = -1L;

    /**
     * A claimable range. {@code nodeId} is its identity.
     *
     * <p>Its byte arrays make generated equality and hashing reference-based.
     */
    record Claim(long nodeId, byte[] lo, byte[] cursor, byte[] hi) {
    }

    private static final class Node {
        private final byte[] lo;
        private byte[] cursor;
        private byte[] hi;
        private boolean completed;

        private Node(byte[] lo, byte[] cursor, byte[] hi) {
            this.lo = lo;
            this.cursor = cursor;
            this.hi = hi;
        }
    }

    private final Map<Long, Node> nodes = new HashMap<>();
    private final Deque<Claim> ready = new ArrayDeque<>();
    private long nextNodeId = 1L;
    private long outstanding;
    private long splitsCommitted;
    private long splitsAborted;

    /** Creates one ready seed and counts it as outstanding. */
    long addSeed(byte[] lo, byte[] hi) {
        long id = nextNodeId++;
        nodes.put(id, new Node(lo, lo, hi));
        ready.add(new Claim(id, lo, lo, hi));
        outstanding++;
        return id;
    }

    /** Removes the next ready claim, or returns {@code null}. */
    Claim poll() {
        return ready.poll();
    }

    /** Live nodes not yet accounted complete; this is the quiescence counter. */
    long outstanding() {
        return outstanding;
    }

    /** Whether no live nodes remain. */
    boolean quiescent() {
        return outstanding == 0L;
    }

    /** Accounts one completed node and returns the remaining live-node count. */
    long decrement() {
        outstanding--;
        return outstanding;
    }

    /** Advances the simulated committed cursor and optionally marks the node complete. */
    void commitPage(long nodeId, byte[] cursorTo, boolean completed) {
        Node node = nodes.get(nodeId);
        if (cursorTo != null) {
            node.cursor = cursorTo;
        }
        if (completed) {
            node.completed = true;
        }
    }

    /**
     * Narrows the parent and creates a child only if {@code expectedHi} still matches, the cursor is
     * before {@code pivot}, and the parent is incomplete.
     *
     * @return the new child's id, or {@link #SPLIT_ABORTED} when the guard rejects the proposal
     */
    long splitNode(long nodeId, byte[] pivot, byte[] expectedHi) {
        Node node = nodes.get(nodeId);
        if (node == null || node.completed
                || !Arrays.equals(node.hi, expectedHi)
                || (node.cursor != null && KeyBytes.compareUnsigned(node.cursor, pivot) >= 0)) {
            splitsAborted++;
            return SPLIT_ABORTED;
        }
        node.hi = pivot;
        long childId = nextNodeId++;
        nodes.put(childId, new Node(pivot, pivot, expectedHi));
        splitsCommitted++;
        return childId;
    }

    /**
     * Publishes a split child as ready and outstanding. The caller performs this after
     * {@link #splitNode} and before parent completion can expose a false quiescent gap.
     */
    void enqueueChild(long childId, byte[] lo, byte[] hi) {
        ready.add(new Claim(childId, lo, lo, hi));
        outstanding++;
    }

    /** Accepted split proposals; each narrowed a parent and created one child. */
    long splitsCommitted() {
        return splitsCommitted;
    }

    /** Proposals rejected by the ledger guard. */
    long splitsAborted() {
        return splitsAborted;
    }

    /** Seed and split-child nodes ever created. */
    long nodesCreated() {
        return nextNodeId - 1L;
    }
}
