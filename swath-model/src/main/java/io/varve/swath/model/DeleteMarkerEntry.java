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
        long lastModifiedEpochMicros,
        String ownerId           // nullable
) implements ListEntry {
}
