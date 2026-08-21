/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

/** One listed object row, as returned by a {@code ListingStore} and rendered into S3 XML. */
public record ListedObject(
        byte[] key,
        long size,
        long lastModifiedEpochMicros,
        String etag,
        String storageClass,
        String ownerId,
        String ownerDisplayName,
        String checksumAlgorithm,
        String checksumType) {
}
