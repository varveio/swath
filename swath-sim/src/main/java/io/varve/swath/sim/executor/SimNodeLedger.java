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
 * The simulator's stand-in for the two things a real run's durable checkpoint provides: the ready
 * queue of claimable ranges, and the compare-and-set that decides whether a proposed split is allowed
 * to become a node.
 *
 * <p><b>The CAS is the back-stop, not the race.</b> A thief picks its pivot against a snapshot it read
 * earlier, without holding anything; between that read and the split the victim may have advanced past
 * the pivot, or another thief may have narrowed the same bound. The durable split is guarded on all
 * three facts — the bound is still the one the proposal was made against, the cursor has not reached
 * the pivot, and the node has not completed — and rejects the proposal otherwise.
 *
 * <p>The engine checks the first two of those <em>again</em>, immediately before proposing, and that
 * earlier check is where a lost race is normally observed ({@code RETRY.cursor_passed_pivot} /
 * {@code bound_moved}). What reaches this guard is only what survived it, so a rejection here needs a
 * change between the two — a second proposer, or a node that completed in between. The fleet admits one
 * steal attempt at a time, so {@link #splitsAborted()} stays at or near zero on runs that are losing
 * most of their steals: it is the late loser, and reading it as the run's footrace record is a mistake
 * (see {@code PolicyRunResult#splitsLostAtRevalidation()}). The guard is reproduced anyway because the
 * engine has it and because the completion case is genuinely reachable.
 *
 * <p>What is deliberately <b>not</b> modelled: durability itself. Writing a row costs time, and that
 * time is charged by the client-cost model's checkpoint stage, which is where it belongs; the ledger
 * only decides outcomes. Nothing here is persisted, so nothing here can be resumed — a simulated run
 * is one process's lifetime by construction.
 *
 * <p>Single-threaded, like everything else the kernel dispatches: the ordering that would need locks in
 * a real executor is provided here by the fact that an event body runs alone.
 */
final class SimNodeLedger {

    /** What {@link #splitNode} returns when the guard rejects the proposal. */
    static final long SPLIT_ABORTED = -1L;

    /** A claimable range: the node's id, its immutable start, its resume cursor, and its bound. */
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

    /** Adds one seed range and counts it outstanding. */
    long addSeed(byte[] lo, byte[] hi) {
        long id = nextNodeId++;
        nodes.put(id, new Node(lo, lo, hi));
        ready.add(new Claim(id, lo, lo, hi));
        outstanding++;
        return id;
    }

    /** The next claimable range, or {@code null} when none is ready right now. */
    Claim poll() {
        return ready.poll();
    }

    /** Ranges neither completed nor yet created-and-completed — the quiescence counter. */
    long outstanding() {
        return outstanding;
    }

    /** Whether the run is finished: nothing ready and nothing outstanding. */
    boolean quiescent() {
        return outstanding == 0L;
    }

    /** One range finished; returns what remains. */
    long decrement() {
        outstanding--;
        return outstanding;
    }

    /** A node's durable cursor advance, and its completion when its last page lands. */
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
     * The guarded durable split of {@code nodeId} at {@code pivot}, proposed against the bound
     * {@code expectedHi} the proposer read earlier.
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
     * Publishes a freshly split child to the ready queue and counts it outstanding. Separate from
     * {@link #splitNode} because the real engine publishes under the parent's lock, after the durable
     * split returns and before the lock is released — the ordering that stops quiescence from ever
     * observing a moment where a parent has given up its tail and the child is uncounted.
     */
    void enqueueChild(long childId, byte[] lo, byte[] hi) {
        ready.add(new Claim(childId, lo, lo, hi));
        outstanding++;
    }

    /** Durable splits that committed. */
    long splitsCommitted() {
        return splitsCommitted;
    }

    /** Split proposals the guard rejected — the late loser; see the class note for why it is rare. */
    long splitsAborted() {
        return splitsAborted;
    }

    /** Nodes ever created, seeds included. */
    long nodesCreated() {
        return nextNodeId - 1L;
    }
}
