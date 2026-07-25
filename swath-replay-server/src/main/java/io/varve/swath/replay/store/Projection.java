/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

/**
 * Which columns a {@link ListingStore} read should materialise. Owner columns are only needed when
 * the request asked for {@code fetch-owner=true}; a store may skip them otherwise to save I/O
 * (the DuckDB store always projects them — it is behavior-cheap there — but a sorted-Parquet store
 * honors it to avoid decoding owner columns).
 */
public record Projection(boolean owner) {

    public static final Projection KEYS_ONLY = new Projection(false);
    public static final Projection WITH_OWNER = new Projection(true);

    public static Projection of(boolean owner) {
        return owner ? WITH_OWNER : KEYS_ONLY;
    }
}
