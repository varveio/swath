/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

/**
 * Why a steal attempt committed {@code UNSPLITTABLE} (algorithms.md §3). {@link #code()} is the
 * exact {@code RunMetrics#recordStealReason("UNSPLITTABLE", code())} reason string the current
 * engine already emits. Only one case exists today (a terminal {@code null} pivot — no safe key
 * strictly between the bounds); kept as an enum rather than a bare boolean/constant for symmetry
 * with the other three decision categories and so a future terminal case has somewhere to go
 * without widening the {@link MarkUnsplittable} shape.
 */
public enum UnsplittableReason {
    /** {@code byteMidpoint}/{@code interpolate} found no safe key strictly between the bounds. */
    NO_PIVOT("no_pivot");

    private final String code;

    UnsplittableReason(String code) {
        this.code = code;
    }

    /** The exact {@code RunMetrics#recordStealReason("UNSPLITTABLE", ...)} reason string. */
    public String code() {
        return code;
    }
}
