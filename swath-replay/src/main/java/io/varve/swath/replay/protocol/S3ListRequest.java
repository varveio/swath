/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

public record S3ListRequest(
        String bucket,
        byte[] prefix,
        byte[] delimiter,
        byte[] startAfter,
        String continuationToken,
        int maxKeys,
        int pageSize,
        boolean encodingTypeUrl,
        boolean fetchOwner) {

    private static final int S3_MAX_KEYS = 1000;

    public S3ListRequest(String bucket, byte[] prefix, byte[] delimiter, byte[] startAfter, String continuationToken,
                         int maxKeys, boolean encodingTypeUrl, boolean fetchOwner) {
        this(bucket, prefix, delimiter, startAfter, continuationToken, maxKeys,
                Math.min(Math.max(maxKeys, 0), S3_MAX_KEYS), encodingTypeUrl, fetchOwner);
    }

    /**
     * Whether the request carries a continuation token that actually resumes something. A blank
     * {@code continuation-token=} query value is not a resume point — {@link ContinuationToken#decode}
     * reads it as absent — so it must not suppress {@code start-after} either, in the boundary the
     * pager resolves or in the response the renderer echoes. Both ask here so the two cannot drift.
     */
    public boolean hasContinuationToken() {
        return continuationToken != null && !continuationToken.isBlank();
    }
}
