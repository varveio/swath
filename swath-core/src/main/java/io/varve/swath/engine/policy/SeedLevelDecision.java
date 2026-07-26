/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * One probed level's own entry in the seed decision trace (algorithms.md §8, §5's classification
 * signal) — the policy-domain shape the executor promotes, field-for-field, into
 * {@code RunMetrics.SeedProbeDecision} for the run summary's {@code seed} block. Kept distinct from
 * that observability type so this package never imports {@code io.varve.swath.observability}
 * (a decision-trace record is a policy output, not a metrics-sink type).
 *
 * @param prefix         the prefix probed (the scan prefix itself for the top-level entry)
 * @param fanout         the raw common-prefix count this probe observed
 * @param truncated      whether this probe's page was truncated
 * @param classification this level's disposition — e.g. {@code "narrow"}, {@code "flat_wide"},
 *                       {@code "tiny_leaf_explosion"}, {@code "fanout_tiled"},
 *                       {@code "heavy_cut_banded"}, {@code "top_probe_paginated"} — plus the two
 *                       run-level-only rewrites ({@code "dense_root_radix_banded"}/
 *                       {@code "delimiter_seeded"}) the terminal {@link SeedPlan} applies only after
 *                       the whole descent completes, since only then is the run's overall shape known
 * @param cutsKept       how many of this level's common prefixes were newly added to the global cut set
 * @param cutsDiscarded  how many were already present (e.g. two probed levels sharing a boundary)
 */
public record SeedLevelDecision(byte[] prefix, int fanout, boolean truncated, String classification,
                                int cutsKept, int cutsDiscarded) {
}
