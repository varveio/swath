/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

/**
 * The {@code --sort} run's phase, recorded in {@code run_meta.sort_phase}.
 * Drives resume dispatch: {@code LISTING} nodes are re-listed (durable segments kept), {@code
 * MERGING} re-runs the merge from staging with zero new LIST fetches, {@code PUBLISHED} means the
 * final {@code manifest.json} exists (idempotent clean-up on re-entry). {@code null} in {@code
 * run_meta} means a non-sort run.
 */
public enum SortPhase {
    LISTING,
    MERGING,
    PUBLISHED
}
