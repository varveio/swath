/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.testkit;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import java.nio.charset.StandardCharsets;

/**
 * Test-only data builder for {@link ObjectEntry} (Object-Mother pattern, per the settled construction
 * idiom): a canonical bare default plus fluent per-field overrides, replacing the positional 11-arg
 * {@code new ObjectEntry(...)} calls that had drifted into ~14 near-identical shapes across the
 * capture-fixture tests — each caller now expresses only its delta from the default.
 */
public final class ObjectEntries {

    private ObjectEntries() {
    }

    public static Builder key(String key) {
        return new Builder(KeyBytes.ofUtf8(key));
    }

    public static Builder key(byte[] key) {
        return new Builder(KeyBytes.of(key));
    }

    /** The all-default shape: size 1, epoch-0 mtime, no etag/storage-class/owner/checksum metadata. */
    public static ObjectEntry bare(String key) {
        return key(key).build();
    }

    /** size=7, a fixed mtime, an owner and CRC32/FULL_OBJECT checksum — the differential-suite shape. */
    public static ObjectEntry withOwner(byte[] key, String etag) {
        return key(key).size(7L).lastModifiedEpochMicros(1_700_000_000_000_000L).etag(etag)
                .storageClass("STANDARD").isLatest(true).owner("owner-id", "owner-display")
                .checksum("CRC32", "FULL_OBJECT").build();
    }

    public static ObjectEntry withOwner(String key, String etag) {
        return withOwner(key.getBytes(StandardCharsets.UTF_8), etag);
    }

    public static final class Builder {
        private final KeyBytes key;
        private long size = 1L;
        private long lastModifiedEpochMicros;
        private String etag;
        private String storageClass;
        private String versionId;
        private boolean isLatest;
        private String ownerId;
        private String ownerDisplayName;
        private String checksumAlgorithm;
        private String checksumType;

        private Builder(KeyBytes key) {
            this.key = key;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder lastModifiedEpochMicros(long value) {
            this.lastModifiedEpochMicros = value;
            return this;
        }

        public Builder etag(String etag) {
            this.etag = etag;
            return this;
        }

        public Builder storageClass(String storageClass) {
            this.storageClass = storageClass;
            return this;
        }

        public Builder versionId(String versionId) {
            this.versionId = versionId;
            return this;
        }

        public Builder isLatest(boolean isLatest) {
            this.isLatest = isLatest;
            return this;
        }

        public Builder owner(String ownerId, String ownerDisplayName) {
            this.ownerId = ownerId;
            this.ownerDisplayName = ownerDisplayName;
            return this;
        }

        public Builder checksum(String checksumAlgorithm, String checksumType) {
            this.checksumAlgorithm = checksumAlgorithm;
            this.checksumType = checksumType;
            return this;
        }

        public ObjectEntry build() {
            return new ObjectEntry(key, size, lastModifiedEpochMicros, etag, storageClass, versionId,
                    isLatest, ownerId, ownerDisplayName, checksumAlgorithm, checksumType);
        }
    }
}
