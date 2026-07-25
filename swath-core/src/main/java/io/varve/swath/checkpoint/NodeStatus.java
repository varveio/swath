/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

/** Node state machine: PENDING → IN_PROGRESS → COMPLETED (or back to PENDING on resume). */
public enum NodeStatus {
    PENDING,
    IN_PROGRESS,
    COMPLETED
}
