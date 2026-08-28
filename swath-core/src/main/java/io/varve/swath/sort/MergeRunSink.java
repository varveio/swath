/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Receives one raw merger's source-run classification when it closes. */
@FunctionalInterface
interface MergeRunSink {

    MergeRunSink NO_OP = (copyableSegments, interleavedSegments) -> { };

    void accept(long copyableSegments, long interleavedSegments);
}
