/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/** Mirrors {@code WorkerState#resetZeroFanoutStructureProbes()}. */
public record ResetZeroFanoutStructureProbes(long victimNodeId) implements VictimMutation {
}
