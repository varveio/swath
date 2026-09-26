/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

/** Parsed GCS JSON v1 objects.list request; strings are Unicode, never S3 query aliases. */
public record GcsListRequest(String bucket, String prefix, String delimiter, String startOffset,
                             String endOffset, int pageSize, String pageToken, boolean fullProjection,
                             boolean pageSizeClamped) {
    public GcsListRequest(String bucket, String prefix, String delimiter, String startOffset,
                          String endOffset, int pageSize, String pageToken, boolean fullProjection) {
        this(bucket, prefix, delimiter, startOffset, endOffset, pageSize, pageToken, fullProjection, false);
    }
}
