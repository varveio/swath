/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import java.time.format.DateTimeParseException;

/**
 * A listed object. {@code versionId} is non-null only in versioned listings;
 * {@code ownerId} only with {@code --fetch-owner}; {@code checksumAlgorithm}
 * only when present; {@code ownerDisplayName}/{@code checksumType} only when
 * present ({@code ownerDisplayName} also requires {@code --fetch-owner}).
 * {@code etag} is stored with surrounding quotes stripped (contract §4).
 */
public record ObjectEntry(
        KeyBytes key,
        long size,
        String lastModifiedText,
        String etag,
        String storageClass,
        String versionId,         // nullable
        boolean isLatest,
        String ownerId,           // nullable
        String ownerDisplayName,  // nullable
        String checksumAlgorithm, // nullable
        String checksumType       // nullable
) implements ListEntry {

    public ObjectEntry {
        if (lastModifiedText == null) {
            lastModifiedText = "";
        }
    }

    /** Compatibility constructor for typed stores, fixtures and sorted spill readers. */
    public ObjectEntry(
            KeyBytes key,
            long size,
            long lastModifiedEpochMicros,
            String etag,
            String storageClass,
            String versionId,
            boolean isLatest,
            String ownerId,
            String ownerDisplayName,
            String checksumAlgorithm,
            String checksumType
    ) {
        this(key, size, LastModified.textFromEpochMicros(lastModifiedEpochMicros), etag, storageClass,
                versionId, isLatest, ownerId, ownerDisplayName, checksumAlgorithm, checksumType);
    }

    /** Parse the source text for a typed consumer, with an entry-attributed failure. */
    public long lastModifiedEpochMicros() {
        try {
            return LastModified.epochMicrosFromText(lastModifiedText);
        } catch (DateTimeParseException e) {
            throw new LastModifiedParseException(key, lastModifiedText, e);
        }
    }

    /**
     * Creates an entry without owner display-name or checksum-type metadata.
     */
    public static ObjectEntry withoutOwnerDisplayNameAndChecksumType(
            KeyBytes key,
            long size,
            long lastModifiedEpochMicros,
            String etag,
            String storageClass,
            String versionId,
            boolean isLatest,
            String ownerId,
            String checksumAlgorithm
    ) {
        return new ObjectEntry(key, size, lastModifiedEpochMicros, etag, storageClass, versionId,
                isLatest, ownerId, null, checksumAlgorithm, null);
    }
}
