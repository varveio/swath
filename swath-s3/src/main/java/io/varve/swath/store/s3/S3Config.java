/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import java.net.URI;
import java.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/**
 * Connection config for {@link S3ClientFactory}. {@code endpointOverride} +
 * {@code forcePathStyle} support LocalStack/MinIO and custom endpoints
 * ({@code --force-path-style} defaults on when an endpoint is set).
 * {@code region} is nullable: absent {@code --region}, the SDK's default region
 * provider chain is left in charge.
 */
public record S3Config(
        Region region,                    // nullable
        URI endpointOverride,            // nullable
        boolean forcePathStyle,
        int maxParallelListings,         // T — drives maxConnections = T + 16
        int maxAttempts,                 // --aws-max-attempts (default 1: swath is the sole retrier)
        Duration apiCallAttemptTimeout,  // per-attempt page timeout budget (WORKER pages; client-level)
        Duration apiCallTimeout,         // overall per-logical-call ceiling (bounds a hung fetch)
        AwsCredentialsProvider credentials,
        Duration probeApiCallAttemptTimeout   // short per-attempt budget for probe call classes
) {

    /**
     * Per-attempt SDK timeout: a single LIST attempt must not hang. At
     * 10 s a stalled read is abandoned quickly and, since it is not classified as a throttle, simply
     * retried without strangling concurrency. Applies at the CLIENT level (every call class) unless
     * overridden per-request — see {@link #DEFAULT_PROBE_ATTEMPT_TIMEOUT} for the probe-class override.
     */
    public static final Duration DEFAULT_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
    /**
     * The per-attempt budget for a PROBE call class (thief 1-key pivot probe /
     * delimiter=/ structure probe — {@code S3PageFetcher#callClass}), applied as a per-request {@code
     * apiCallAttemptTimeout} override on top of the {@link #DEFAULT_ATTEMPT_TIMEOUT} client-level
     * default. A probe carries no S3-backpressure signal and already caps its transient retries
     * ({@code PROBE_TRANSIENT_RETRY_CAP=1}), so a stuck probe should be abandoned and retried quickly
     * rather than holding an attempt open for the full 10 s WORKER-page budget — the long fuse behind
     * a probe-timeout spiral. Worker pages are unaffected: they keep the 10 s client-level
     * budget with no per-request override.
     */
    public static final Duration DEFAULT_PROBE_ATTEMPT_TIMEOUT = Duration.ofSeconds(3);
    /**
     * Overall per-logical-call ceiling: the PRIMARY liveness guarantee. Even if an attempt
     * wedges below the socket layer, {@code apiCallTimeout} unblocks the logical call at 60 s so no
     * worker is parked in the SDK forever (the in-JVM watchdog is the secondary guard).
     */
    public static final Duration DEFAULT_API_CALL_TIMEOUT = Duration.ofSeconds(60);
    /**
     * SDK retry budget: 1 = no SDK-internal retries. swath's own bounded/jittered
     * gauge-aware loop is the sole retrier, so the AIMD gauge sees every real 503 immediately
     * instead of after the SDK silently swallowed several behind its own backoff.
     */
    public static final int DEFAULT_MAX_ATTEMPTS = 1;
    public static final int DEFAULT_MAX_PARALLEL = 64;
}
