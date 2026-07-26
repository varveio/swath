/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * <b>The policy seam</b> (contracts.md §2.1): the seed step's descent — the
 * span-priority frontier, probe-budget accounting, per-level classification (narrow / partition
 * fan-out / flat-wide radix banding / tiny-leaf explosion vs. heavy-cut), and cut-set assembly plus
 * the mass-weighted subsample to the target seed count (algorithms.md §8) — as a deterministic state
 * machine over observed probe results, with every RPC, page decode, and node insertion left to the
 * executor ({@code SeedStep}). The engine and a future discrete-event simulator share this; a future
 * seed-diet policy and a hints-file-driven planner are meant to slot in as alternative implementations
 * behind this same interface (that interface compatibility is the point of this extraction) — so
 * nothing here is specific to how the current HYBRID descent happens to explore the keyspace.
 *
 * <p><b>Source-agnostic (contracts.md §2.1):</b> no S3/protocol type (no SDK response class, no XML/wire
 * type, no {@code io.varve.swath.store.ListPage}) crosses into this package's descent state,
 * decisions, or probe outcomes — keys as raw bytes, counts, and policy-domain records only.
 *
 * <p>{@link #probeBudget()} is available immediately at construction — the executor records it
 * (metrics, the seed phase's exact denominator) once per run regardless of which seed mode the run
 * ends up using, including the modes ({@code none}, {@code hints}) that never call
 * {@link #beginDescent()} at all. {@link #beginDescent()} — called only for the shallow/HYBRID mode —
 * starts the actual descent, driven to a terminal {@link SeedPlan} via {@link SeedDescent}.
 */
public interface SeedPlanner {

    /**
     * The total probe budget this run's descent may spend (structure probes plus every sample
     * sub-budget combined) — computed once, from the run's target concurrency, and fixed for the
     * planner's lifetime.
     */
    int probeBudget();

    /** Begin a fresh descent. Never reused — call once per run. */
    SeedDescent beginDescent();
}
