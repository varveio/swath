/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * The result of the flat-leaf bootstrap floor probe {@code ListObjectsV2(prefix=leafDir,
 * max_keys=1)} (algorithms.md §3.1's {@code flatLeafDensityPivot}): the leaf's first key, if any —
 * used to seed a density reflection when the worker's own {@code lo} is the leaf directory itself
 * rather than a real key inside it.
 *
 * @param firstKey the first key at/after the leaf prefix, or {@code null} if the leaf is empty
 */
public record FloorProbeOutcome(byte[] firstKey) implements ProbeOutcome {
}
