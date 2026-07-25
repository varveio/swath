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
 * @param probeApiCallAttemptTimeout per-attempt budget for the POINT-probe call class, applied as a
 *                                   per-request override (see
 *                                   {@link S3PageFetcher#probeApiCallAttemptTimeout})
 * @param scanApiCallAttemptTimeout the per-attempt BASE budget for the SCAN call classes (worker
 *                                  page, {@code delimiter=/} structure probe). Never applied as an
 *                                  override at escalation level 0 — the client already enforces it —
 *                                  but it is the base {@link S3PageFetcher#attemptTimeoutForLevel}
 *                                  doubles once the engine escalates. Should be the same value the
 *                                  client was built with, so level 0 and level 1 describe one ladder.
 */
public record S3PageFetcherConfig(
        boolean fetchOwner,
        boolean requestPayer,
        RunMetrics metrics,
        Duration probeApiCallAttemptTimeout,
        Duration scanApiCallAttemptTimeout) {

    /**
     * The canonical default: no owner fetch, no requester-pays, a fresh no-op metrics sink (via the
     * {@code null} sentinel), the {@link S3Config#DEFAULT_PROBE_ATTEMPT_TIMEOUT} point-probe budget,
     * and the {@link S3Config#DEFAULT_ATTEMPT_TIMEOUT} scan budget.
     */
    public static final S3PageFetcherConfig DEFAULT = new S3PageFetcherConfig(
            false, false, null, S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT, S3Config.DEFAULT_ATTEMPT_TIMEOUT);

    public S3PageFetcherConfig withFetchOwner(boolean fetchOwner) {
        return new S3PageFetcherConfig(
                fetchOwner, requestPayer, metrics, probeApiCallAttemptTimeout, scanApiCallAttemptTimeout);
    }

    public S3PageFetcherConfig withRequestPayer(boolean requestPayer) {
        return new S3PageFetcherConfig(
                fetchOwner, requestPayer, metrics, probeApiCallAttemptTimeout, scanApiCallAttemptTimeout);
    }

    public S3PageFetcherConfig withMetrics(RunMetrics metrics) {
        return new S3PageFetcherConfig(
                fetchOwner, requestPayer, metrics, probeApiCallAttemptTimeout, scanApiCallAttemptTimeout);
    }

    public S3PageFetcherConfig withProbeApiCallAttemptTimeout(Duration probeApiCallAttemptTimeout) {
        return new S3PageFetcherConfig(
                fetchOwner, requestPayer, metrics, probeApiCallAttemptTimeout, scanApiCallAttemptTimeout);
    }

    public S3PageFetcherConfig withScanApiCallAttemptTimeout(Duration scanApiCallAttemptTimeout) {
        return new S3PageFetcherConfig(
                fetchOwner, requestPayer, metrics, probeApiCallAttemptTimeout, scanApiCallAttemptTimeout);
    }
}
