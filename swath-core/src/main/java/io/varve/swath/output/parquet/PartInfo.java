/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

/**
 * Immutable metadata for a finalized (footer-fsynced) part retained for completion publication (I6).
 *
 * @param path   {@code data/}-prefixed relative path (relative to the dataset root), the canonical
 *               form shared by the consumer manifest {@code files[].key}, the checkpoint
 *               {@code part_file.path}, and the resume disk-sweep
 * @param md5    lowercase hex-encoded MD5 of the part's bytes (the consumer manifest's
 *               {@code MD5checksum}); may be empty only for a defensively-constructed part with no
 *               computed checksum
 * @param minKey the part's true first key, UTF-8 text — {@code null}
 *               for an unsorted part (row order carries no key-range meaning there)
 * @param maxKey the part's true last key, UTF-8 text — {@code null} for an unsorted part
 */
public record PartInfo(String path, int writerId, long rows, long bytes, String md5,
                       String minKey, String maxKey) {

    /** Unsorted-part convenience: no {@code minKey}/{@code maxKey}. */
    public PartInfo(String path, int writerId, long rows, long bytes, String md5) {
        this(path, writerId, rows, bytes, md5, null, null);
    }
}
