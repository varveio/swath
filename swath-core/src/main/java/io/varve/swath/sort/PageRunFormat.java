/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/**
 * Checkpoint-visible identity of one page-run segment's on-disk format and trailer extension.
 *
 * <p>The page-run header version and extension type are separate compatibility axes: a reader may
 * understand the segment body while not understanding metadata embedded before the fixed EOF tail.
 * Original listing segments written by this build always carry the boundary-sample extension;
 * extension-free page runs remain readable for legacy/cascade compatibility but are never produced
 * as newly checkpointed listing segments.
 */
public record PageRunFormat(int formatVersion, int extensionType) {

    /** The {@code part_file.format} namespace for page-run staging. */
    public static final String NAME = "page-run";

    /** Current page-run header format version. */
    public static final int CURRENT_FORMAT_VERSION = 1;

    /** A readable page run with no extension before the fixed EOF tail. */
    public static final int NO_EXTENSION = 0;

    /** The legacy/current page-minimum boundary-sample extension. */
    public static final int BOUNDARY_SAMPLE_EXTENSION = 1;

    private static final PageRunFormat CURRENT_LISTING =
            new PageRunFormat(CURRENT_FORMAT_VERSION, BOUNDARY_SAMPLE_EXTENSION);

    public PageRunFormat {
        if (formatVersion < 0 || extensionType < 0) {
            throw new IllegalArgumentException("page-run format metadata must be non-negative");
        }
    }

    /** Metadata actually emitted for a newly checkpointed listing segment. */
    public static PageRunFormat currentListing() {
        return CURRENT_LISTING;
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
        if (extensionType != NO_EXTENSION && extensionType != BOUNDARY_SAMPLE_EXTENSION) {
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
