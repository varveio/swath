/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import java.util.List;
import java.util.Map;

/** A scrape-time view of serving settings and live response bounds. */
record ServingMetadata(List<String> protocols, String bucket, String azureAccount,
                       String fixtureIdentity, String orderingProfile, String metadataPolicy,
                       int maxConcurrentRequests, int maxResponses, int readPermitLimit,
                       long responseBufferBudget, int maxResponseBytes, int outputChunkBytes,
                       long chargedResponseBytes, long peakChargedResponseBytes, int activeResponses,
                       long stopTimeoutMs, long idleTimeoutMs, long writeTimeoutMs,
                       String latencyInjection, String paginationProfile, Map<String, String> profiles) {
    static final String ORDERING_PROFILE = "unsigned-utf8-byte-order";

    static ServingMetadata legacy(String mode) {
        return new ServingMetadata(List.of("s3"), "unknown", null, "unknown", ORDERING_PROFILE,
                "fixture", ReplayServer.DEFAULT_MAX_CONCURRENT_REQUESTS,
                2 * ReplayServer.DEFAULT_MAX_CONCURRENT_REQUESTS, 0,
                ServeConfig.DEFAULT_RESPONSE_BUFFER_BUDGET, ServeConfig.DEFAULT_MAX_RESPONSE_BYTES,
                BudgetedOutput.configuredChunkBytes(), 0, 0, 0, 10_000, 30_000, 30_000, "off", "default",
                Map.of("s3", "list-objects-v2"));
    }
}
