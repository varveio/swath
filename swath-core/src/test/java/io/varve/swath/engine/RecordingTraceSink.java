/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.varve.swath.observability.TraceSink;
import java.util.ArrayList;
import java.util.List;

/**
 * A test-scoped {@link TraceSink} that records every call as an ordered {@link ObjectNode}
 * instead of writing JSONL to disk — the decision-trace golden recorder's tap on the {@code
 * stealAttempt}/{@code split}/{@code ownerSplit} events {@link io.varve.swath.engine.Thief} and
 * {@link OwnerSelfSplit} already emit through the production {@link TraceSink} seam (B1,
 * {@code docs/ops/dev/decision-trace-goldens.md}). {@code enabled()} is always {@code true} so a
 * caller that gates a split's return value on it (see {@link
 * OwnerSelfSplit#maybeOwnerSelfSplit}) always gets the trace payload.
 *
 * <p>Not thread-safe (single-thief/single-victim deterministic drivers only, per the goldens'
 * determinism requirement) — no synchronization, unlike {@code JsonlTraceSink}.
 */
final class RecordingTraceSink implements TraceSink {

    private final List<ObjectNode> events = new ArrayList<>();

    List<ObjectNode> events() {
        return events;
    }

    void clear() {
        events.clear();
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public void seeded(long nodeId, byte[] lo, byte[] hi) {
        ObjectNode e = GoldenTrace.newNode();
        e.put("event", "seeded");
        e.put("node_id", nodeId);
        GoldenTrace.putHex(e, "lo", lo);
        GoldenTrace.putHex(e, "hi", hi);
        events.add(e);
    }

    @Override
    public void claimed(long workerId, long nodeId, byte[] lo, byte[] cursor, byte[] hi) {
        ObjectNode e = GoldenTrace.newNode();
        e.put("event", "claimed");
        e.put("worker_id", workerId);
        e.put("node_id", nodeId);
        GoldenTrace.putHex(e, "lo", lo);
        GoldenTrace.putHex(e, "cursor", cursor);
        GoldenTrace.putHex(e, "hi", hi);
        events.add(e);
    }

    @Override
    public void pageCommitted(long workerId, long nodeId, int keysEmitted, byte[] cursor, boolean completed) {
        ObjectNode e = GoldenTrace.newNode();
        e.put("event", "page_committed");
        e.put("worker_id", workerId);
        e.put("node_id", nodeId);
        e.put("keys", keysEmitted);
        GoldenTrace.putHex(e, "cursor", cursor);
        e.put("completed", completed);
        events.add(e);
    }

    @Override
    public void stealAttempt(long workerId, String outcome, String reason) {
        ObjectNode e = GoldenTrace.newNode();
        e.put("event", "steal_attempt");
        e.put("worker_id", workerId);
        e.put("outcome", outcome);
        e.put("reason", reason);
        events.add(e);
    }

    @Override
    public void split(long workerId, long parentNodeId, long childNodeId, String mechanism, byte[] pivot, byte[] hi) {
        events.add(splitLike("split", workerId, parentNodeId, childNodeId, mechanism, pivot, hi));
    }

    @Override
    public void ownerSplit(long workerId, long parentNodeId, long childNodeId, String mechanism, byte[] pivot,
                            byte[] hi) {
        events.add(splitLike("owner_split", workerId, parentNodeId, childNodeId, mechanism, pivot, hi));
    }

    private static ObjectNode splitLike(String event, long workerId, long parentNodeId, long childNodeId,
                                         String mechanism, byte[] pivot, byte[] hi) {
        ObjectNode e = GoldenTrace.newNode();
        e.put("event", event);
        e.put("worker_id", workerId);
        e.put("parent_node_id", parentNodeId);
        e.put("child_node_id", childNodeId);
        e.put("mechanism", mechanism);
        GoldenTrace.putHex(e, "pivot", pivot);
        GoldenTrace.putHex(e, "hi", hi);
        return e;
    }

    @Override
    public void completed(long workerId, long nodeId) {
        ObjectNode e = GoldenTrace.newNode();
        e.put("event", "completed");
        e.put("worker_id", workerId);
        e.put("node_id", nodeId);
        events.add(e);
    }

    @Override
    public void failed(long workerId, long nodeId, String reason) {
        ObjectNode e = GoldenTrace.newNode();
        e.put("event", "failed");
        e.put("worker_id", workerId);
        e.put("node_id", nodeId);
        e.put("reason", reason);
        events.add(e);
    }

    @Override
    public void close() {
        // no-op: nothing is ever flushed to disk
    }
}
