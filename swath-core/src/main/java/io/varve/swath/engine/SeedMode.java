/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

/**
 * How the {@code WorkStealingScan} worklist is seeded before stealing begins
 * (algorithms.md §8, {@code --seed}).
 *
 * <ul>
 *   <li>{@link #NONE} — one root range {@code (⊥, null]}; rely on stealing alone.</li>
 *   <li>{@link #SHALLOW} — a bounded {@code delimiter=/} structure probe that tiles
 *       {@code (⊥, null]} on the top-level common prefixes so a deep-nested keyspace
 *       parallelizes immediately (the default).</li>
 *   <li>{@link #HINTS} — cut-points from a {@code --hints} file (not yet wired).</li>
 * </ul>
 */
public enum SeedMode {
    NONE,
    SHALLOW,
    HINTS
}
