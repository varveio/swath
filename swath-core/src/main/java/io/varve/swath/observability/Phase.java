/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

/**
 * §3.2 {@code swath.phase} live gauge — dashboard readability (and the self-throttle/tail boards)
 * only, NOT the correctness gate for "is this run stuck": a gauge is sampled at push time, so a
 * phase shorter than the export interval can emit zero samples for it. The stuck-detection signal
 * is {@link RunMetrics#recordProgress(long)} ({@code swath.progress.units}), a monotonic counter
 * that advances in every phase by construction. {@link #code()} is the value the gauge publishes —
 * an EXPLICIT field, not {@link #ordinal()}: a future enum reorder/insert must not silently
 * renumber the wire values a dashboard/alert already depends on; {@code -1} (unset, before the
 * first {@link RunMetrics#setPhase}) maps to {@code NaN}, which is emitted as a {@code NaN}
 * datapoint at OTLP export and dropped downstream by the collector/backend (not omitted by
 * Micrometer itself) — the series is effectively absent until a phase is set.
 */
public enum Phase {
    /**
     * The pre-listing seed step ({@code SeedStep}): bounded structure probes, zero emitted entries.
     * Declared FIRST (lifecycle order) but coded {@code 4} — the explicit codes above are already
     * published, and inserting a constant must never renumber them.
     */
    SEEDING(4),
    LISTING(0),
    MERGING(1),
    WRITING(2),
    COMPLETE(3);

    private final int code;

    Phase(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
