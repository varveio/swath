/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/**
 * Receives one raw merger's source-run classification after successful logical completion and
 * close-time validation.
 *
 * <p>This reports actual emitted source runs, not a proof of disjoint key ranges. Comparator-equal
 * rows may remain contiguous from one source under either merger's existing heap tie behavior; the
 * sink never changes that ordering merely to make classifications match.
 */
@FunctionalInterface
interface MergeRunSink {

    MergeRunSink NO_OP = (copyableSegments, interleavedSegments) -> { };

    void accept(long copyableSegments, long interleavedSegments);
}
