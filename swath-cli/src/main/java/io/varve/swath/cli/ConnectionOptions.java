/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.SafeInput;
import io.varve.swath.store.ApiRateLimiter;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.RateLimitedPageFetcher;
import io.varve.swath.store.s3.BearerTokenSupplier;
import io.varve.swath.store.s3.ProcessBearerTokenSupplier;
import io.varve.swath.store.s3.S3Config;
import java.net.URI;
import java.time.Duration;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

/**
 * S3 connection controls: endpoint/addressing, credentials (profile / anonymous / requester-pays),
 * region, target concurrency, the listing-queue budget, and the client-side API rate cap. Owns
 * building the resolved {@link S3Config} and wrapping a fetcher in the optional rate limiter.
 */
final class ConnectionOptions {

    @Resume(ResumeClass.IDENTITY)
    @Option(names = "--endpoint-url", paramLabel = "URL", description = "Custom S3 endpoint (LocalStack/MinIO).")
    String endpointUrl;

    @Resume(ResumeClass.FREE)
    @Option(names = "--force-path-style", negatable = true,
            description = "Path-style addressing (defaults on when --endpoint-url is set).")
    Boolean forcePathStyle;

    @Resume(ResumeClass.STICKY)
    @Option(names = "--no-sign-request", description = "Anonymous requests (public buckets).")
    boolean noSignRequest;

    @Resume(ResumeClass.STICKY)
    @Option(names = "--requester-pays", paramLabel = "requester",
            description = "Send the requester-pays header (accepted value: requester).")
    String requestPayer;

    /**
     * The resolved {@code --requester-pays} flag, set once by {@link #resolveRequestPayer()} at
     * the top of the run — non-{@code args_hash}, soft-restored by {@code swath resume}, and passed to the
     * S3 fetcher so it attaches the header to every ListObjectsV2 request the run issues.
     */
    boolean requestPayerEnabled;

    @Resume(value = ResumeClass.IDENTITY, restored = true)
    @Option(names = "--fetch-owner", description = "Request object owner fields from S3.")
    boolean fetchOwner;

    @Resume(ResumeClass.STICKY)
    @Option(names = "--profile", paramLabel = "NAME", description = "AWS profile for credentials.")
    String profile;

    @Resume(ResumeClass.STICKY)
    @Option(names = "--region", paramLabel = "REGION", description = "AWS region (else resolved from the environment).")
    String region;

    // FREE, not STICKY like the auth flags above it, and shared with `swath resume` rather than
    // re-declared there — see BearerTokenOptions for why both of those matter.
    @ArgGroup(exclusive = false, validate = false)
    final BearerTokenOptions bearer = new BearerTokenOptions();

    @Resume(ResumeClass.FREE)
    @Option(names = "--concurrency", paramLabel = "N", description = "Maximum concurrent listing requests (default: 64).")
    int maxParallelListings = S3Config.DEFAULT_MAX_PARALLEL;

    @Resume(ResumeClass.FREE)
    @Option(names = "--object-listing-queue-size", paramLabel = "N", description = "Listing queue size in entries (default 50000).")
    int queueSizeEntries = 50_000;

    @Resume(ResumeClass.FREE)
    @Option(names = "--request-rate", paramLabel = "N",
            description = "Cap aggregate S3 requests per second (default/0: unlimited).")
    Double rateLimitApi;

    /** Sanity ceiling for {@code --concurrency}: catches a fat-fingered/programmatic value
     * (e.g. an extra zero, or {@code 0}/negative from a driving sweep script) well before it would
     * matter for real concurrency; virtual threads are cheap so this is generous. */
    static final int MAX_MAX_PARALLEL_LISTINGS = 100_000;

    /**
     * Resolve {@code --requester-pays} (mirrors the AWS CLI): absent (the default) means "do not
     * send the header"; the only accepted explicit value is {@code requester} (case-insensitive).
     * Pure validation (no I/O), run before any network/store work so a typo'd value fails fast at
     * exit 2 rather than after a checkpoint DB or S3 client is open.
     */
    boolean resolveRequestPayer() throws InvalidArgsException {
        if (requestPayer == null) {
            return false;
        }
        if (!requestPayer.equalsIgnoreCase("requester")) {
            throw new InvalidArgsException(
                    "--requester-pays must be 'requester' (got " + requestPayer + ")");
        }
        return true;
    }

    /**
     * Resolve {@code --concurrency} (the AIMD ceiling {@code Tmax}). Must be
     * {@code >= 1} — {@code T < 1} (e.g. {@code 0} or negative from a programmatic sweep) sizes the
     * worker pool and concurrency semaphore to nothing/negative and misbehaves. Bounded above at
     * {@link #MAX_MAX_PARALLEL_LISTINGS} as a sanity ceiling, not a resource-budget limit.
     */
    static int resolveMaxParallelListings(int maxParallelListings) throws InvalidConfigException {
        if (maxParallelListings < 1 || maxParallelListings > MAX_MAX_PARALLEL_LISTINGS) {
            throw new InvalidConfigException("--concurrency must be between 1 and "
                    + MAX_MAX_PARALLEL_LISTINGS + " (got " + maxParallelListings + ")");
        }
        return maxParallelListings;
    }

    /**
     * Resolve the listing queue cap (I11): the channel admits a page while the <b>in-flight
     * entry count</b> is below this value — it is an entry budget, not a batch-slot count.
     * Must be {@code >= 1}.
     */
    static int resolveQueueEntries(int queueSizeEntries) throws InvalidConfigException {
        if (queueSizeEntries < 1) {
            throw new InvalidConfigException(
                    "--object-listing-queue-size must be >= 1 (got " + queueSizeEntries + ")");
        }
        return queueSizeEntries;
    }

    /**
     * Resolve {@code --request-rate}: a client-side cap on the run's aggregate S3 API
     * request rate, requests/sec. Unset (the default) or an explicit {@code 0} both mean "no cap".
     * A negative value is rejected at exit 2, as are {@code NaN}/{@code Infinity} (picocli's
     * {@code Double} converter accepts both literal strings) — left to reach {@code ApiRateLimiter}
     * they would throw an undocumented exit 1 deep inside {@link #maybeRateLimited}, after {@code
     * openRun} has already inserted the run's {@code RUNNING} row.
     */
    static Double resolveRateLimitApi(Double rateLimitApi) throws InvalidConfigException {
        if (rateLimitApi == null || rateLimitApi == 0) {
            return null;
        }
        if (rateLimitApi < 0 || !Double.isFinite(rateLimitApi)) {
            throw new InvalidConfigException(
                    "--request-rate must be a positive, finite number (or 0/unset to disable), got "
                            + rateLimitApi);
        }
        return rateLimitApi;
    }

    /**
     * Wrap {@code fetcher} in the {@code --request-rate} client-side cap when configured — a no-op
     * passthrough (the exact same instance) when unset, so the default hot path pays zero added
     * overhead. Called once per fetcher construction, so every caller sharing that {@link PageFetcher}
     * instance shares the same limiter: the cap is on the run's aggregate rate, never per-thread.
     */
    PageFetcher maybeRateLimited(PageFetcher fetcher, RunMetrics metrics) {
        if (rateLimitApi == null) {
            return fetcher;
        }
        return new RateLimitedPageFetcher(fetcher, ApiRateLimiter.perSecond(rateLimitApi), metrics);
    }

    /** Default {@code --bearer-token-refresh-interval}: comfortably under a typical ~1h Google OAuth
     * access-token TTL, without so short a cadence that ordinary listing runs mint tokens needlessly
     * often. */
    static final Duration DEFAULT_BEARER_TOKEN_REFRESH_INTERVAL = Duration.ofMinutes(45);

    /** Build the resolved S3 connection config (region defaulting included) from the flags. */
    S3Config buildConfig() throws InvalidConfigException {
        URI endpoint;
        try {
            endpoint = SafeInput.endpoint("--endpoint-url", endpointUrl);
        } catch (InvalidArgsException e) {
            throw new InvalidConfigException(e.getMessage());
        }
        boolean pathStyle = forcePathStyle != null ? forcePathStyle : (endpoint != null);
        return new S3Config(
                resolveRegion(endpoint),
                endpoint,
                pathStyle,
                maxParallelListings,
                S3Config.DEFAULT_MAX_ATTEMPTS,
                S3Config.DEFAULT_ATTEMPT_TIMEOUT,
                S3Config.DEFAULT_API_CALL_TIMEOUT,
                resolveCredentials(),
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT,
                resolveBearerTokenSupplier());
    }

    /**
     * Resolve {@code --bearer-token-command}/{@code --bearer-token-refresh-interval} into a {@link
     * BearerTokenSupplier}, or {@code null} when unset (the overwhelmingly common case: normal
     * SigV4/{@code --profile} signing).
     */
    private BearerTokenSupplier resolveBearerTokenSupplier() throws InvalidConfigException {
        if (bearer.command == null) {
            return null;
        }
        Duration refreshInterval = bearer.refreshInterval == null
                ? DEFAULT_BEARER_TOKEN_REFRESH_INTERVAL
                : DurationParser.parse(bearer.refreshInterval, "bearer-token-refresh-interval", false);
        return new ProcessBearerTokenSupplier(bearer.command, refreshInterval);
    }

    /**
     * Resolve the AWS region. Explicit {@code --region} wins. With a custom {@code --endpoint-url}
     * (LocalStack/MinIO) the region is irrelevant to the server, so default to US_EAST_1 rather than
     * forcing the user to set one. Otherwise defer to the SDK default chain (AWS_REGION /
     * AWS_DEFAULT_REGION / profile); if it cannot resolve, surface a typed {@link
     * InvalidConfigException} (exit 2) instead of letting an unchecked SDK exception escape as an
     * unexpected exit 1.
     */
    private Region resolveRegion(URI endpoint) throws InvalidConfigException {
        if (region != null) {
            return Region.of(region);
        }
        if (endpoint != null) {
            return Region.US_EAST_1;
        }
        try {
            return new DefaultAwsRegionProviderChain().getRegion();
        } catch (RuntimeException e) {
            throw new InvalidConfigException(
                    "no AWS region: pass --region or set AWS_REGION / AWS_DEFAULT_REGION", e);
        }
    }

    private AwsCredentialsProvider resolveCredentials() {
        if (noSignRequest) {
            return AnonymousCredentialsProvider.create();
        }
        if (profile != null) {
            return ProfileCredentialsProvider.create(profile);
        }
        return DefaultCredentialsProvider.builder().build();
    }
}
