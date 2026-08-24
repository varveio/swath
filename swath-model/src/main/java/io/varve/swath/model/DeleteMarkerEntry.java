/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

/** A delete-marker row (versioned listings). Emitted only with {@code --all-versions}. */
public record DeleteMarkerEntry(
        KeyBytes key,
        String versionId,
        boolean isLatest,
        String lastModifiedText,
        String ownerId           // nullable
) implements ListEntry {

    public DeleteMarkerEntry {
        if (lastModifiedText == null) {
            lastModifiedText = "";
        }
    }

    /** Compatibility constructor for typed stores, fixtures and sorted spill readers. */
    public DeleteMarkerEntry(
            KeyBytes key,
            String versionId,
            boolean isLatest,
            long lastModifiedEpochMicros,
            String ownerId
    ) {
        this(key, versionId, isLatest, LastModified.textFromEpochMicros(lastModifiedEpochMicros), ownerId);
    }

    public long lastModifiedEpochMicros() {
        return LastModified.epochMicrosFromText(lastModifiedText);
    }
}
