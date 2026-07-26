/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * A metrics engagement mark the policy fired while deciding — mid-cascade (e.g. the far-ahead
 * step-back, a density-reflection hit/miss, a suppressed structure probe) or at a terminal
 * decision. The executor records it verbatim via {@code RunMetrics#recordStealReason(category,
 * reason)} (AGENTS.md's instrument-every-algo-path counter-per-path law); the policy never touches
 * {@code RunMetrics} itself (source-agnostic / no I/O — contracts.md §2.1).
 *
 * <p>{@link PivotMechanism}/{@link RetryReason}/{@link NoVictimReason}/{@link UnsplittableReason}
 * give type-safe closed vocabularies for the four headline decision categories; this generic
 * {@code (category, reason)} pair covers everything else the cascade already instruments today
 * ({@code STRUCTURE.*}, {@code ALPHABET.*}, {@code STEAL.futility_paced}) without inventing a
 * parallel enum per secondary counter.
 *
 * @param category the {@code RunMetrics#recordStealReason} category (e.g. {@code "STRUCTURE"})
 * @param reason   the reason within that category (e.g. {@code "suppressed_zero_fanout"})
 */
public record Engagement(String category, String reason) {
}
