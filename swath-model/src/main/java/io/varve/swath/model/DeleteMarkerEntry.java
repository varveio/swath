/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import java.time.format.DateTimeParseException;

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

    /** Parse the source text for a typed consumer, with an entry-attributed failure. */
    public long lastModifiedEpochMicros() {
        try {
            return LastModified.epochMicrosFromText(lastModifiedText);
        } catch (DateTimeParseException e) {
            throw new LastModifiedParseException(key, lastModifiedText, e);
        }
    }
}
