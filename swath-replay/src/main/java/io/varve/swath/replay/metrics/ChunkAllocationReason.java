/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.metrics;

/** Why a response allocated another charged output chunk. */
public enum ChunkAllocationReason {
    INITIAL, INITIAL_CAP_PARTIAL, CONTINUATION, CAP_PARTIAL
}
