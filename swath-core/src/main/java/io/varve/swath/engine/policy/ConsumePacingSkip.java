/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/** Mirrors {@code WorkerState#stealPaced()}'s mutating decrement of the futility cooldown. */
public record ConsumePacingSkip(long victimNodeId) implements VictimMutation {
}
