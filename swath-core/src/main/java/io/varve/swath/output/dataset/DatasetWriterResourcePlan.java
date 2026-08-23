/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

/**
 * Format-owned, static resource context for a parallel dataset writer pool. Fields are nullable
 * when the format has no comparable fixed buffer or heap-admission model.
 */
public record DatasetWriterResourcePlan(
        Long rowGroupTargetBytesPerWriter,
        Integer rowGroupAllowanceMultiplier,
        Long plannedHeapBytes,
        Boolean heapAdmissionApplied) {

    public static final DatasetWriterResourcePlan NONE =
            new DatasetWriterResourcePlan(null, null, null, null);
}
