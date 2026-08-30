/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import software.amazon.awssdk.services.s3.model.S3Object;

/** Shared field normalization for the SDK-model and direct-page object paths. */
final class S3ObjectEntryMapper {

    private S3ObjectEntryMapper() {
    }

    /** Map an SDK-model object whose timestamp has already been parsed. */
    static ObjectEntry map(S3Object object, KeyBytes key, long lastModifiedEpochMicros) {
        String checksumAlgorithm = object.hasChecksumAlgorithm()
                && !object.checksumAlgorithmAsStrings().isEmpty()
                ? object.checksumAlgorithmAsStrings().getFirst()
                : null;
        String checksumType = object.checksumTypeAsString();
        return new ObjectEntry(
                key,
                object.size() != null ? object.size() : 0L,
                lastModifiedEpochMicros,
                S3PageFetcher.stripEtagQuotes(object.eTag()),
                object.storageClassAsString(),
                null,
                true,
                object.owner() == null ? null : object.owner().id(),
                object.owner() == null ? null : object.owner().displayName(),
                checksumAlgorithm,
                checksumType == null || checksumType.isBlank() ? null : checksumType);
    }

    /** Map a direct-page object while retaining its source timestamp text. */
    static ObjectEntry map(S3Object object, KeyBytes key, String lastModifiedText) {
        String checksumAlgorithm = object.hasChecksumAlgorithm()
                && !object.checksumAlgorithmAsStrings().isEmpty()
                ? object.checksumAlgorithmAsStrings().getFirst()
                : null;
        String checksumType = object.checksumTypeAsString();
        return new ObjectEntry(
                key,
                object.size() != null ? object.size() : 0L,
                lastModifiedText,
                S3PageFetcher.stripEtagQuotes(object.eTag()),
                object.storageClassAsString(),
                null,
                true,
                object.owner() == null ? null : object.owner().id(),
                object.owner() == null ? null : object.owner().displayName(),
                checksumAlgorithm,
                checksumType == null || checksumType.isBlank() ? null : checksumType);
    }
}
