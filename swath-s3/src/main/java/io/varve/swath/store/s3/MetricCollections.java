/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import java.util.List;
import java.util.function.Consumer;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.SdkMetric;

/**
 * The recursive {@link MetricCollection} tree-walk shared by {@link S3PoolMetricPublisher}
 * and {@link S3CallClassLatencyPublisher} — both bridge SDK metrics that may land on any
 * descendant of the per-request {@link MetricCollection} (an attempt-level or deeper child, not
 * necessarily the root), so both must visit the WHOLE tree rather than read only the top level.
 * Package-private: an internal collaborator of the two publishers, not a public store-layer
 * surface.
 */
final class MetricCollections {

    private MetricCollections() {
    }

    /**
     * Visits {@code root} and every descendant, depth-first, root/parent before its children —
     * the same order each publisher's own hand-rolled walk used before this extraction, so a
     * "last sibling wins" caller (see {@code S3PoolMetricPublisher}'s multi-attempt test) keeps
     * seeing the same last-write-wins result.
     */
    static void walk(MetricCollection root, Consumer<MetricCollection> visitor) {
        visitor.accept(root);
        for (MetricCollection child : root.children()) {
            walk(child, visitor);
        }
    }

    /** The last recorded value of {@code metric} on {@code collection}, or {@code null} if absent. */
    static <T> T last(MetricCollection collection, SdkMetric<T> metric) {
        List<T> values = collection.metricValues(metric);
        return values.isEmpty() ? null : values.get(values.size() - 1);
    }
}
