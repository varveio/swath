/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

/** Parsed Azure Blob XML List Blobs request for the flat synthetic-v1 profile. */
public record AzureListRequest(String account, String container, String version,
                               String prefix, String delimiter, String startFrom, String marker,
                               int pageSize, String requestedMaxResults,
                               boolean prefixSupplied, boolean delimiterSupplied,
                               boolean markerSupplied, boolean maxResultsSupplied,
                               String clientRequestId) {
}
