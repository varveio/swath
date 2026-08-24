/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import io.varve.swath.observability.RunMetrics;
import java.time.Duration;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * Builds the sync {@link S3Client} for the listing engine (sync SDK on
 * virtual threads).
 *
 * <p><b>{@code maxConnections = T + 16}.</b> The Apache pool must exceed the
 * target concurrency {@code T} or it silently caps it. The
 * {@code +16} headroom covers probes, the seed pass, and retries running
 * alongside the {@code T} range workers.
 *
 * <p>SDK-internal retry is disabled on the SDK's {@code standard} strategy (never
 * {@code adaptive}) — see {@link S3Config#DEFAULT_MAX_ATTEMPTS} for why swath's own
 * retry loop is the sole retrier. {@code apiCallAttemptTimeout} caps a single attempt
 * (sizing: next paragraph) and {@code apiCallTimeout} caps the whole logical call; see
 * {@link S3Config#DEFAULT_API_CALL_TIMEOUT} for why it is sized as it is.
 *
 * <p>The client-level {@code apiCallAttemptTimeout} configured here ({@link
 * S3Config#DEFAULT_ATTEMPT_TIMEOUT}, 10 s) is the SCAN-class budget — worker pages AND
 * {@code delimiter=/} structure probes, both of whose cost tracks the keyspace they cross. The
 * POINT-probe call class (pivot) gets a SHORTER per-request override applied by
 * {@link S3PageFetcher} directly ({@link S3Config#DEFAULT_PROBE_ATTEMPT_TIMEOUT}, 3 s) — this
 * factory/client-level value is never overridden for that; only the per-request override on the
 * outgoing {@code ListObjectsV2Request} differs.
 */
public final class S3ClientFactory {

    /** The connection-pool headroom over the concurrency target. */
    public static final int CONNECTION_HEADROOM = 16;

    // Connection-freshness hedge: an idle ESTABLISHED socket can be silently reaped by a
    // NAT/conntrack table, then reused, blackholing the next read to the attempt timeout. Evicting
    // idle/aged connections and keeping TCP liveness on reduces those hangs if that is the
    // mechanism; if a read-side hang is instead a genuinely slow endpoint, these cheap builder
    // lines cost nothing.
    /** Evict a pooled connection idle longer than this (drop NAT-reaped sockets before reuse). */
    static final Duration CONNECTION_MAX_IDLE = Duration.ofSeconds(5);
    /** Retire a pooled connection older than this regardless of use (bound stale-conn blast radius). */
    static final Duration CONNECTION_TIME_TO_LIVE = Duration.ofMinutes(1);
    /** Fail fast rather than block forever if the pool cannot hand out a lease. */
    static final Duration CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(10);

    private S3ClientFactory() {
    }

    /** The Apache pool size for a given concurrency target {@code T} (= T + 16). */
    public static int maxConnectionsFor(int targetConcurrency) {
        return targetConcurrency + CONNECTION_HEADROOM;
    }

    /** The swath product token prepended to the SDK-generated HTTP User-Agent value. */
    private static String userAgentPrefix() {
        String implementationVersion = S3ClientFactory.class.getPackage().getImplementationVersion();
        return "swath/" + (implementationVersion == null || implementationVersion.isBlank()
                ? "development"
                : implementationVersion);
    }

    public static SdkHttpClient httpClient(int targetConcurrency) {
        return httpClient(targetConcurrency, null);
    }

    /**
     * Same as {@link #httpClient(int)}, but when {@code metrics != null} installs {@link
     * S3HandshakeCountingSocketFactory} so every new pooled connection's TLS handshake is counted on
     * {@code swath.s3.pool.handshakes}. {@code metrics == null} builds byte-for-byte the same
     * client {@link #httpClient(int)} always has (no {@code socketFactory} override — the SDK's own
     * default TLS-factory construction runs unchanged).
     */
    public static SdkHttpClient httpClient(int targetConcurrency, RunMetrics metrics) {
        ApacheHttpClient.Builder builder = ApacheHttpClient.builder()
                .maxConnections(maxConnectionsFor(targetConcurrency))
                .tcpKeepAlive(true)                                       // keep idle sockets alive / detect dead peers
                .connectionMaxIdleTime(CONNECTION_MAX_IDLE)              // evict long-idle (possibly reaped) sockets
                .connectionTimeToLive(CONNECTION_TIME_TO_LIVE)          // retire aged sockets
                .connectionAcquisitionTimeout(CONNECTION_ACQUISITION_TIMEOUT);
        if (metrics != null) {
            builder.socketFactory(new S3HandshakeCountingSocketFactory(
                    S3HandshakeCountingSocketFactory.defaultDelegate(), metrics));
        }
        return builder.build();
    }

    public static S3Client create(S3Config config) {
        return create(config, null);
    }

    /**
     * Same as {@link #create(S3Config)}, but attaches a {@link S3PoolMetricPublisher} wired to
     * {@code metrics} so the §3.8 {@code swath.s3.pool.*} gauges are populated from this client's
     * {@code ApacheHttpClient} connection pool. {@code metrics == null} attaches no metrics
     * publisher. The success-response streaming interceptor is part of both overloads; only its
     * engagement counter is null-safe when no metrics registry was supplied.
     */
    public static S3Client create(S3Config config, RunMetrics metrics) {
        var retry = AwsRetryStrategy.standardRetryStrategy()
                .toBuilder()
                .maxAttempts(config.maxAttempts())
                .build();

        ClientOverrideConfiguration.Builder overrideBuilder = ClientOverrideConfiguration.builder()
                .retryStrategy(retry)
                .apiCallAttemptTimeout(config.apiCallAttemptTimeout())
                .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgentPrefix())
                .addExecutionInterceptor(new StreamingListObjectsV2Interceptor(metrics))
                // The overall per-logical-call ceiling -- see S3Config#DEFAULT_API_CALL_TIMEOUT for
                // why it is the PRIMARY liveness guarantee.
                .apiCallTimeout(config.apiCallTimeout());
        // The §3.8 pool publisher (javadoc above); metrics == null skips it entirely.
        if (metrics != null) {
            overrideBuilder.addMetricPublisher(new S3PoolMetricPublisher(metrics));
            // Attached ALONGSIDE (never instead of) the pool publisher above -- see
            // S3CallClassLatencyPublisher's javadoc for why multiple client-level publishers don't
            // disturb each other's feed (unlike a per-request override list, deliberately avoided).
            overrideBuilder.addMetricPublisher(new S3CallClassLatencyPublisher());
        }
        ClientOverrideConfiguration override = overrideBuilder.build();

        S3ClientBuilder builder = S3Client.builder()
                .httpClient(httpClient(config.maxParallelListings(), metrics))
                .overrideConfiguration(override)
                .forcePathStyle(config.forcePathStyle())
                .credentialsProvider(config.credentials());

        // --bearer-token-command: install in place of the SDK's real SigV4 auth scheme (same scheme
        // id — see BearerTokenAuthScheme's javadoc for why that's what makes the client actually use
        // it). config.credentials() above is still set (the builder requires a non-null provider) but
        // goes unused: this scheme's signer never consults it.
        if (config.bearerTokenSupplier() != null) {
            builder.putAuthScheme(new BearerTokenAuthScheme(config.bearerTokenSupplier()));
        }

        if (config.region() != null) {
            builder.region(config.region());
        }
        if (config.endpointOverride() != null) {
            builder.endpointOverride(config.endpointOverride());
        }
        return builder.build();
    }
}
