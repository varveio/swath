/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import io.varve.swath.observability.RunMetrics;
import java.time.Duration;

/**
 * The optional wiring for an {@link S3PageFetcher} beyond its required {@code S3Client} and bucket:
 * the listing flags, the probe attempt-timeout, and the metrics sink. Production wires the clump
 * once; tests pass {@link #DEFAULT} with any single knob derived via a {@code withX}, or use the
 * bare {@link S3PageFetcher#S3PageFetcher(software.amazon.awssdk.services.s3.S3Client, String)}
 * convenience for the (dominant) no-option case. Connection config — region, endpoint, credentials,
 * client-level timeouts — is the separate {@link S3Config}.
 *
 * @param fetchOwner request the Owner field (ListObjectsV2 {@code FetchOwner=true}; §4 {@code owner_id})
 * @param requestPayer send {@code x-amz-request-payer: requester} on every request
 *                     ({@code --request-payer requester}; requester-pays buckets)
 * @param metrics the run metrics sink; {@code null} (the default) means the fetcher installs a fresh
 *                no-op {@code SimpleMeterRegistry} sink so it never records against a shared registry
 * @param probeApiCallAttemptTimeout per-request attempt-timeout override applied to probe call
 *                                   classes only (see {@link S3PageFetcher#probeApiCallAttemptTimeout})
 */
public record S3PageFetcherConfig(
        boolean fetchOwner,
        boolean requestPayer,
        RunMetrics metrics,
        Duration probeApiCallAttemptTimeout) {

    /**
     * The canonical default: no owner fetch, no requester-pays, a fresh no-op metrics sink (via the
     * {@code null} sentinel), and the {@link S3Config#DEFAULT_PROBE_ATTEMPT_TIMEOUT} probe budget.
     */
    public static final S3PageFetcherConfig DEFAULT =
            new S3PageFetcherConfig(false, false, null, S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);

    public S3PageFetcherConfig withFetchOwner(boolean fetchOwner) {
        return new S3PageFetcherConfig(fetchOwner, requestPayer, metrics, probeApiCallAttemptTimeout);
    }

    public S3PageFetcherConfig withRequestPayer(boolean requestPayer) {
        return new S3PageFetcherConfig(fetchOwner, requestPayer, metrics, probeApiCallAttemptTimeout);
    }

    public S3PageFetcherConfig withMetrics(RunMetrics metrics) {
        return new S3PageFetcherConfig(fetchOwner, requestPayer, metrics, probeApiCallAttemptTimeout);
    }

    public S3PageFetcherConfig withProbeApiCallAttemptTimeout(Duration probeApiCallAttemptTimeout) {
        return new S3PageFetcherConfig(fetchOwner, requestPayer, metrics, probeApiCallAttemptTimeout);
    }
}
