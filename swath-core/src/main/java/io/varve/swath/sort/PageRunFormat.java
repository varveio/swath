/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.util.Comparator;

/**
 * Checkpoint-visible identity of one page-run segment's on-disk format and trailer extension.
 *
 * <p>The page-run header version and extension type are separate compatibility axes: a reader may
 * understand the segment body while not understanding metadata embedded before the fixed EOF tail.
 * Original listing segments written by this build always carry the type-3 page index; extension-free,
 * legacy type-1 minima, and legacy type-2 page-index runs remain readable for compatibility but are never produced as
 * newly checkpointed listing segments. A writer that emits a new listing extension updates this
 * value in the same change, so checkpoint metadata cannot describe different bytes.
 */
public record PageRunFormat(int formatVersion, int extensionType) {

    /** The {@code part_file.format} namespace for page-run staging. */
    public static final String NAME = "page-run";

    /** Current page-run header format version. */
    public static final int CURRENT_FORMAT_VERSION = 2;

    /** A readable page run with no extension before the fixed EOF tail. */
    public static final int ABSENT_EXTENSION = 0;

    /** The legacy page-minimum boundary-sample extension. */
    public static final int LEGACY_MINIMA_EXTENSION = 1;

    /** The legacy sparse page-offset index without decoded-page residency metadata. */
    public static final int LEGACY_PAGE_INDEX_EXTENSION = 2;

    /** The current sparse page-offset index with decoded-page residency metadata. */
    public static final int PAGE_INDEX_EXTENSION = 3;

    private static final PageRunFormat CURRENT_LISTING =
            new PageRunFormat(CURRENT_FORMAT_VERSION, PAGE_INDEX_EXTENSION);

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
     * PageRun v2 persists the canonical swath ordering without a comparator identifier. Reject an
     * alternate comparator before it can write or merge bytes under an unstated format contract.
     */
    static void requireCanonicalComparator(Comparator<ListEntry> comparator) {
        if (!(comparator instanceof ListEntryComparator)) {
            throw new IllegalArgumentException(
                    "page-run format v2 requires ListEntryComparator");
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
        if (extensionType != ABSENT_EXTENSION && extensionType != LEGACY_MINIMA_EXTENSION
                && extensionType != LEGACY_PAGE_INDEX_EXTENSION
                && extensionType != PAGE_INDEX_EXTENSION) {
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
