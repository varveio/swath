/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/** Mirrors {@code WorkerState#markNonProductiveSteal(Snapshot)}. */
public record MarkNonProductive(long victimNodeId) implements VictimMutation {
}
