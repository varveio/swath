/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.util.Comparator;

/**
 * Checkpoint-visible identity of one page-run segment's on-disk format.
 */
public record PageRunFormat(int formatVersion, int extensionType) {

    /** The {@code part_file.format} namespace for page-run staging. */
    public static final String NAME = "page-run";

    /** Current page-run header format version. */
    public static final int CURRENT_FORMAT_VERSION = 4;

    /** A readable page run with no extension before the fixed EOF tail. */
    public static final int ABSENT_EXTENSION = 0;

    private static final PageRunFormat CURRENT_LISTING =
            new PageRunFormat(CURRENT_FORMAT_VERSION, ABSENT_EXTENSION);

    public PageRunFormat {
        if (formatVersion < 0 || extensionType < 0) {
            throw new IllegalArgumentException("page-run format metadata must be non-negative");
        }
    }

    /** Metadata actually emitted for a newly checkpointed listing segment. */
    public static PageRunFormat currentListing() {
        return CURRENT_LISTING;
    }

    /**
     * PageRun v4 persists the canonical swath ordering without a comparator identifier. Reject an
     * alternate comparator before it can write or merge bytes under an unstated format contract.
     */
    static void requireCanonicalComparator(Comparator<ListEntry> comparator) {
        if (!(comparator instanceof ListEntryComparator)) {
            throw new IllegalArgumentException(
                    "page-run format v4 requires ListEntryComparator");
        }
    }

    /** Classify nullable checkpoint metadata without treating a pre-column row as version zero. */
    public static Compatibility compatibility(Integer formatVersion, Integer extensionType) {
        if (formatVersion == null && extensionType == null) {
            return Compatibility.LEGACY_UNRECORDED;
        }
        if (formatVersion == null || extensionType == null) {
            return Compatibility.INCOMPLETE;
        }
        if (formatVersion != CURRENT_FORMAT_VERSION) {
            return Compatibility.UNKNOWN_FORMAT_VERSION;
        }
        if (extensionType != ABSENT_EXTENSION) {
            return Compatibility.UNKNOWN_EXTENSION_TYPE;
        }
        return Compatibility.SUPPORTED;
    }

    /** Resume disposition for the two nullable {@code part_file} compatibility columns. */
    public enum Compatibility {
        LEGACY_UNRECORDED,
        SUPPORTED,
        INCOMPLETE,
        UNKNOWN_FORMAT_VERSION,
        UNKNOWN_EXTENSION_TYPE
    }
}
