/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

/** A recorded part file ({@code part_file}) — used on resume to retain finalized parts for
 * completion publication and discard every non-finalized part. */
public record PartRef(long id, int writerId, String path, String format, boolean finalized, long rows, long bytes) {
}
