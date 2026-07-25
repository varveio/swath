/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;

/** An object in the {@link MockPageFetcher}'s in-memory keyspace. */
public record MockObject(byte[] key, long size, long lastModifiedEpochMicros,
                         String etag, String storageClass) {

    public ObjectEntry toEntry() {
        return ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.of(key), size, lastModifiedEpochMicros,
                etag, storageClass, null, true, null, null);
    }
}
