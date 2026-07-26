/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * The result of the single-key speculative probe {@code ListObjectsV2(prefix=P, start_after=m,
 * max_keys=1)} (algorithms.md §3): whether it returned a key {@code <= H} ({@code H == null}
 * accepts any returned key) — the ONE fact every {@link RequestKeyProbe} caller branches on.
 */
public record KeyProbeOutcome(boolean nonEmpty) implements ProbeOutcome {
}
