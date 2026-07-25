/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

/**
 * A split request (algorithms.md §4.3, I4). Narrows the victim's {@code range_end}
 * to {@code pivot} and inserts a child owning {@code (pivot, oldHi]} — one
 * transaction, CAS-guarded by {@code (cursor < pivot AND range_end IS oldHi AND
 * status <> COMPLETED)}. {@code oldHi} null = the open frontier (matched via
 * SQLite null-safe {@code IS}).
 */
public record SplitSpec(long runId, long victimId, byte[] pivot, byte[] oldHi) {
}
