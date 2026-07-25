/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

/**
 * The list seam the handler drives: given a parsed request, produce a page of results. This is
 * pure protocol (no server/transport dependency) — the production implementation is
 * {@link ListObjectsV2Pager} (over a {@code ListingStore}); tests may supply a lambda.
 */
public interface ListingFixture extends AutoCloseable {

    S3ListResult list(S3ListRequest request);

    @Override
    default void close() {
    }
}
