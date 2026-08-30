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
 * @param rawMinKey exact first key bytes, or {@code null} for an empty part
 * @param rawMaxKey exact last key bytes, or {@code null} for an empty part
 */
public record FinalPartMetadata(long rows, long bytes, String md5, String minKey, String maxKey,
                                long closeNanos, long md5Nanos, long boundsBytes,
                                byte[] rawMinKey, byte[] rawMaxKey) {

    /** Compatibility constructor for callers whose bounds are known to be UTF-8 text. */
    public FinalPartMetadata(long rows, long bytes, String md5, String minKey, String maxKey,
            long closeNanos, long md5Nanos, long boundsBytes) {
        this(rows, bytes, md5, minKey, maxKey, closeNanos, md5Nanos, boundsBytes,
                minKey == null ? null : minKey.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                maxKey == null ? null : maxKey.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    public FinalPartMetadata {
        if (rows < 0 || bytes < 0 || closeNanos < 0 || md5Nanos < 0 || boundsBytes < 0) {
            throw new IllegalArgumentException("final part counts/timings must be non-negative");
        }
        Objects.requireNonNull(md5, "md5");
        if ((minKey == null) != (maxKey == null)) {
            throw new IllegalArgumentException("minKey/maxKey must both be null or both be present");
        }
        if ((rawMinKey == null) != (rawMaxKey == null)
                || (minKey == null) != (rawMinKey == null)) {
            throw new IllegalArgumentException(
                    "text and raw min/max bounds must all be absent or all be present");
        }
        if (rows == 0 && minKey != null) {
            throw new IllegalArgumentException("empty parts must have no bounds");
        }
        if (rows > 0 && minKey == null) {
            throw new IllegalArgumentException("non-empty parts must have bounds");
        }
        rawMinKey = rawMinKey == null ? null : rawMinKey.clone();
        rawMaxKey = rawMaxKey == null ? null : rawMaxKey.clone();
    }

    @Override
    public byte[] rawMinKey() {
        return rawMinKey == null ? null : rawMinKey.clone();
    }

    @Override
    public byte[] rawMaxKey() {
        return rawMaxKey == null ? null : rawMaxKey.clone();
    }
}
