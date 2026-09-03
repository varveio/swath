/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.output.parquet.PartListener;
import java.util.List;

/** Optional durability, rotation, and observation wiring for a dataset writer pool. */
public record DatasetWriterPoolConfig(
        String sinkName,
        String bucket,
        PartListener partListener,
        List<PartInfo> existingParts,
        long rotationIntervalNanos,
        long rotationMaxRows,
        DatasetWriterObserver observer,
        LaneRouting routing) {

    public DatasetWriterPoolConfig {
        if (sinkName == null || sinkName.isBlank()) {
            throw new IllegalArgumentException("sinkName is required");
        }
        bucket = bucket == null ? "" : bucket;
        partListener = partListener == null ? PartListener.NONE : partListener;
        existingParts = existingParts == null ? List.of() : List.copyOf(existingParts);
        observer = observer == null ? DatasetWriterObserver.NONE : observer;
        routing = routing == null ? LaneRouting.STICKY : routing;
    }
}
