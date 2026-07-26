/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.engine.WorkerState;

/**
 * A terse {@link WorkerState} factory for engine tests, named so a call site reads plainly as
 * "a hand-built victim" rather than a bare {@code new WorkerState(...)}.
 */
public final class WorkerStates {

    private WorkerStates() {
    }

    /** A hand-built {@link WorkerState} victim for a unit test driving a {@code Thief} directly. */
    public static WorkerState of(long nodeId, byte[] lo, byte[] cursor, byte[] hi) {
        return new WorkerState(nodeId, lo, cursor, hi);
    }
}
