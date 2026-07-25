/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

/**
 * A per-page checkpoint commit (algorithms.md §4.2, I1 commit-before-emit).
 *
 * <p>{@code advanceTo} = the last emitted key when the batch was non-empty;
 * {@code null} means an <b>empty</b> batch — {@code cursor} is left unchanged
 * (§4.2; includes the post-narrow terminal case). {@code completed} sets the
 * node {@code COMPLETED} (reached its bound or the listing ended), else
 * {@code IN_PROGRESS}.
 */
public record PageCommit(long nodeId, byte[] advanceTo, boolean completed) {
}
