/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.metrics;

/** Stable request-shape labels retained for S3 latency and meter compatibility. */
public enum RequestShape {
    WORKER_PAGE, PIVOT_PROBE, STRUCTURE_PROBE
}
