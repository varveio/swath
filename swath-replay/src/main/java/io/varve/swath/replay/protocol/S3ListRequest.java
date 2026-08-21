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
}
