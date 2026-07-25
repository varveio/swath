/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

/**
 * What a store supports — the router picks the engine from these declared
 * capabilities, not a hardcoded S3 assumption. The v1.0 S3 fetcher
 * supports object listings only; versions stay false until ListObjectVersions
 * lands. S3 uses {@code maxKeysCap = 1000}, {@code paginationKind = KEY}.
 */
public record StoreCapabilities(
        boolean supportsStartKey,
        boolean supportsRangeBounds,
        boolean supportsDelimiter,
        boolean supportsVersions,
        boolean guaranteesLexOrder,
        int maxKeysCap,
        PaginationKind paginationKind) {

    public StoreCapabilities withMaxKeysCap(int maxKeysCap) {
        return new StoreCapabilities(supportsStartKey, supportsRangeBounds, supportsDelimiter, supportsVersions,
                guaranteesLexOrder, maxKeysCap, paginationKind);
    }

    /** The S3 general-purpose-bucket capability set. */
    public static StoreCapabilities s3() {
        return new StoreCapabilities(true, true, true, false, true, 1000, PaginationKind.KEY);
    }
}
