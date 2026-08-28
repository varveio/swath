/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

/**
 * A recorded part file ({@code part_file}) — used on resume to retain finalized parts for
 * completion publication and discard every non-finalized part. Nullable format metadata means
 * unrecorded legacy metadata, not numeric version/type zero.
 */
public record PartRef(long id, int writerId, String path, String format,
                      Integer formatVersion, Integer extensionType,
                      boolean finalized, long rows, long bytes) {
}
