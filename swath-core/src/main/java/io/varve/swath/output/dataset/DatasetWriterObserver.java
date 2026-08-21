/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

/** Format-owned observation hooks for the shared writer lifecycle. */
public interface DatasetWriterObserver {
    DatasetWriterObserver NONE = new DatasetWriterObserver() { };

    default void recordLaneWork(long elapsedNanos) { }
    default void recordRotation(String reason) { }
    default Object startFinalize() { return null; }
    default void recordFinalize(Object sample) { }
    default void recordPart(String result) { }
    default void markProgress() { }
}
