/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import java.time.format.DateTimeParseException;
import java.util.Objects;

/**
 * A listed object. {@code versionId} is non-null only in versioned listings;
 * {@code ownerId} only with {@code --fetch-owner}; {@code checksumAlgorithm}
 * only when present; {@code ownerDisplayName}/{@code checksumType} only when
 * present ({@code ownerDisplayName} also requires {@code --fetch-owner}).
 * {@code etag} is stored with surrounding quotes stripped (contract §4).
 *
 * <p>This is a class rather than a record so the parsed timestamp can remain private lazy state;
 * value equality, hashing, and text representation still cover exactly the eleven source fields.
 */
public final class ObjectEntry implements ListEntry {

    private final KeyBytes key;
    private final long size;
    private final String lastModifiedText;
    private long lastModifiedEpochMicros;
    private volatile boolean lastModifiedParsed;
    private final String etag;
    private final String storageClass;
    private final String versionId;
    private final boolean isLatest;
    private final String ownerId;
    private final String ownerDisplayName;
    private final String checksumAlgorithm;
    private final String checksumType;

    public ObjectEntry(
            KeyBytes key,
            long size,
            String lastModifiedText,
            String etag,
            String storageClass,
            String versionId,
            boolean isLatest,
            String ownerId,
            String ownerDisplayName,
            String checksumAlgorithm,
            String checksumType
    ) {
        this.key = key;
        this.size = size;
        this.lastModifiedText = lastModifiedText == null ? "" : lastModifiedText;
        this.lastModifiedEpochMicros = 0L;
        this.lastModifiedParsed = false;
        this.etag = etag;
        this.storageClass = storageClass;
        this.versionId = versionId;
        this.isLatest = isLatest;
        this.ownerId = ownerId;
        this.ownerDisplayName = ownerDisplayName;
        this.checksumAlgorithm = checksumAlgorithm;
        this.checksumType = checksumType;
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
        this.key = key;
        this.size = size;
        this.lastModifiedText = LastModified.textFromEpochMicros(lastModifiedEpochMicros);
        this.lastModifiedEpochMicros = lastModifiedEpochMicros;
        this.lastModifiedParsed = true;
        this.etag = etag;
        this.storageClass = storageClass;
        this.versionId = versionId;
        this.isLatest = isLatest;
        this.ownerId = ownerId;
        this.ownerDisplayName = ownerDisplayName;
        this.checksumAlgorithm = checksumAlgorithm;
        this.checksumType = checksumType;
    }

    @Override
    public KeyBytes key() {
        return key;
    }

    public long size() {
        return size;
    }

    public String lastModifiedText() {
        return lastModifiedText;
    }

    /** Lazily parse and cache source text on the typed path, attributing malformed text to this entry. */
    public long lastModifiedEpochMicros() {
        if (lastModifiedParsed) {
            return lastModifiedEpochMicros;
        }
        try {
            long parsed = LastModified.epochMicrosFromText(lastModifiedText);
            lastModifiedEpochMicros = parsed;
            lastModifiedParsed = true;
            return parsed;
        } catch (DateTimeParseException e) {
            throw new LastModifiedParseException(key, lastModifiedText, e);
        }
    }

    public String etag() {
        return etag;
    }

    public String storageClass() {
        return storageClass;
    }

    public String versionId() {
        return versionId;
    }

    public boolean isLatest() {
        return isLatest;
    }

    public String ownerId() {
        return ownerId;
    }

    public String ownerDisplayName() {
        return ownerDisplayName;
    }

    public String checksumAlgorithm() {
        return checksumAlgorithm;
    }

    public String checksumType() {
        return checksumType;
    }

    /** Creates an entry without owner display-name or checksum-type metadata. */
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

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ObjectEntry that)) {
            return false;
        }
        return size == that.size
                && isLatest == that.isLatest
                && Objects.equals(key, that.key)
                && Objects.equals(lastModifiedText, that.lastModifiedText)
                && Objects.equals(etag, that.etag)
                && Objects.equals(storageClass, that.storageClass)
                && Objects.equals(versionId, that.versionId)
                && Objects.equals(ownerId, that.ownerId)
                && Objects.equals(ownerDisplayName, that.ownerDisplayName)
                && Objects.equals(checksumAlgorithm, that.checksumAlgorithm)
                && Objects.equals(checksumType, that.checksumType);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(key);
        result = 31 * result + Long.hashCode(size);
        result = 31 * result + Objects.hashCode(lastModifiedText);
        result = 31 * result + Objects.hashCode(etag);
        result = 31 * result + Objects.hashCode(storageClass);
        result = 31 * result + Objects.hashCode(versionId);
        result = 31 * result + Boolean.hashCode(isLatest);
        result = 31 * result + Objects.hashCode(ownerId);
        result = 31 * result + Objects.hashCode(ownerDisplayName);
        result = 31 * result + Objects.hashCode(checksumAlgorithm);
        return 31 * result + Objects.hashCode(checksumType);
    }

    @Override
    public String toString() {
        return "ObjectEntry[key=" + key
                + ", size=" + size
                + ", lastModifiedText=" + lastModifiedText
                + ", etag=" + etag
                + ", storageClass=" + storageClass
                + ", versionId=" + versionId
                + ", isLatest=" + isLatest
                + ", ownerId=" + ownerId
                + ", ownerDisplayName=" + ownerDisplayName
                + ", checksumAlgorithm=" + checksumAlgorithm
                + ", checksumType=" + checksumType + ']';
    }
}
