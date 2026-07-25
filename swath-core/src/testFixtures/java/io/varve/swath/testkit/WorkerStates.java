/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.engine.WorkerState;

/**
 * A terse, metrics-less {@link WorkerState} factory for engine tests: the effective construction
 * the (now removed) 4-arg {@link WorkerState} constructor overload built. A test that needs a
 * metrics sink constructs {@link WorkerState} directly.
 */
public final class WorkerStates {

    private WorkerStates() {
    }

    /** A metrics-less {@link WorkerState} — a hand-built victim for a unit test driving a {@code Thief} directly. */
    public static WorkerState of(long nodeId, byte[] lo, byte[] cursor, byte[] hi) {
        return new WorkerState(nodeId, lo, cursor, hi, null);
    }
}
