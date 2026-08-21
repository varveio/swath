/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.util.Objects;

/**
 * Immutable facts captured while writing one final sorted part. An instance exists only after the
 * part's footer and fsync have completed successfully; callers must never treat an open or
 * unsuccessfully-closed writer as carrying publishable metadata.
 *
 * @param rows exact number of emitted rows
 * @param bytes exact number of emitted file bytes, including the Parquet footer
 * @param md5 lowercase MD5 of those exact emitted bytes
 * @param minKey exact first key as UTF-8 text, or {@code null} for an empty part
 * @param maxKey exact last key as UTF-8 text, or {@code null} for an empty part
 * @param closeNanos wall time spent writing the footer and fsyncing the file/directory
 * @param md5Nanos CPU wall time spent updating/finalizing the incremental digest
 * @param boundsBytes exact key bytes observed while retaining first/last bounds
 */
public record FinalPartMetadata(long rows, long bytes, String md5, String minKey, String maxKey,
                                long closeNanos, long md5Nanos, long boundsBytes) {

    public FinalPartMetadata {
        if (rows < 0 || bytes < 0 || closeNanos < 0 || md5Nanos < 0 || boundsBytes < 0) {
            throw new IllegalArgumentException("final part counts/timings must be non-negative");
        }
        Objects.requireNonNull(md5, "md5");
        if ((minKey == null) != (maxKey == null)) {
            throw new IllegalArgumentException("minKey/maxKey must both be null or both be present");
        }
        if (rows == 0 && minKey != null) {
            throw new IllegalArgumentException("empty parts must have no bounds");
        }
        if (rows > 0 && minKey == null) {
            throw new IllegalArgumentException("non-empty parts must have bounds");
        }
    }
}
